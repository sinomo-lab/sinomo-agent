//! Workspace tools. Workspace content is read and written only through
//! [`WorkspaceFiles`] (VFS); direct host filesystem access is denied here and
//! confined to `host_probe` and the owned-path helpers below.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod apply_patch;
#[cfg(unix)]
pub(crate) mod agent_home;
#[cfg(unix)]
pub use agent_home::prepare_agent_home_sifs;
mod changed_files;
mod edit_file;
mod git_diff;
mod host_probe;
mod list_files;
mod read_file;
mod request_user_input;
mod run_command;
mod search;
mod str_replace_editor;
mod write_file;

use super::workspace_files::{WorkspaceFile, WorkspaceFiles, WorkspaceGrantSpec, WorkspaceSifsVfs};
use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentCommandOutput, AgentTool,
    AgentToolArtifactRef, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolInvocationMode, AgentToolProgressEvent, AgentToolProgressSink, AgentToolProviderSpec,
    AgentToolRegistry, AgentToolResult, AgentToolStatus,
};
use crate::{
    AgentCancellationToken, AgentProcessLifecycleStatus, AgentProcessManager, AgentProcessSnapshot,
    AgentProcessStart, AgentStoragePaths, AgentToolCall, AgentToolKind, AgentToolSpec,
    InteractionSession, TaskId, default_coding_agent_tools,
};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use host_probe::{current_path_dirs, program_is_available, workspace_has_git_metadata};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeId;
use sinomo_runtime::sandbox::{
    LocalSandboxInstanceLayout, LocalSandboxInstanceStore, SandboxCancellationToken,
    SandboxCommandExecutionError, SandboxCommandId, SandboxCreateInstanceRequest, SandboxId,
    SandboxInstanceManifest, SandboxInstancePortability, SandboxLifecycleClient,
    SandboxLifecycleError, SandboxLocalExecutionLease, SandboxMount, SandboxPath, SandboxPolicy,
    SandboxRuntimeBinding, SandboxStatusRequest, SandboxStoreError, SandboxTempPolicy,
    SandboxToolchainId, SandboxUpdateInstanceRequest, SandboxWasiError,
    acquire_sandbox_native_launch_lease, sandbox_env_name_is_valid,
    sandbox_execution_placement_for_node,
};
#[cfg(test)]
use sinomo_runtime::sandbox::{SandboxInstanceLifecycleStatus, SandboxMountMode};
use sinomo_runtime::sandbox_simple::{
    SandboxNativeExecutionOptions, SandboxSimpleNativeCommandRunner, SandboxSimpleNativeConfig,
};
use sinomo_runtime::vfs::{VfsCreateMode, VfsError, VfsHandle, VfsObjectKind};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const DEFAULT_LOCAL_MAX_READ_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE: u64 = 512 * 1024;
const DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;
const DEFAULT_LOCAL_MAX_PATCH_BYTES: usize = 512 * 1024;
const DEFAULT_LOCAL_MAX_SEARCH_MATCHES: usize = 100;
const DEFAULT_LOCAL_MAX_LIST_ENTRIES: usize = 200;
const AGENT_SANDBOX_TOOLCHAIN_ID: &str = "agent_tools";
const AGENT_HOME_SIFS_ROOT: &str = "/sinomo/agents";
const ENCODED_AGENT_HOME_ID_PREFIX: &str = "__sinomo_encoded__";
const HASHED_AGENT_HOME_ID_PREFIX: &str = "__sinomo_hashed__";
// Leave headroom below the usual 255-byte POSIX component ceiling for
// backends that reserve a few bytes for their own namespace encoding.
const MAX_AGENT_HOME_ID_COMPONENT_BYTES: usize = 240;
const SIMPLE_SANDBOX_RUNTIME: &str = "simple_sandbox";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalWorkspaceAgentConfig {
    /// Default workspace and resolution base for every relative tool path.
    pub root: PathBuf,
    /// Additional workspace grants in conversation order. Relative paths never
    /// resolve against these roots; read tools and command cwd selection may
    /// address them with absolute paths.
    pub additional_workspace_roots: Vec<PathBuf>,
    pub evidence_dir: Option<PathBuf>,
    pub sifs_evidence: Option<LocalWorkspaceSifsEvidenceConfig>,
    pub command_runtime: LocalWorkspaceCommandRuntime,
    pub sandbox_runtime_root: PathBuf,
    /// Host path where the cluster SIFS root is already mounted. The simple
    /// sandbox projects declared SIFS mounts from this live filesystem; it is
    /// never used as a copy-in/copy-back staging directory.
    pub sandbox_sifs_mount_root: Option<PathBuf>,
    pub sandbox_instance_id: Option<SandboxId>,
    pub sandbox_runtime_node_id: Option<NodeId>,
    pub sandbox_portability: SandboxInstancePortability,
    pub agent_home_sifs_path: Option<String>,
    pub sandbox_allow_unenforced: bool,
    pub allow_shell_commands: bool,
    pub allow_file_writes: bool,
    pub max_read_bytes: usize,
    pub max_search_bytes_per_file: u64,
    pub max_command_output_bytes: usize,
    pub max_patch_bytes: usize,
    pub max_search_matches: usize,
    pub max_list_entries: usize,
    pub git_tool_mode: LocalWorkspaceGitToolMode,
    pub git_program: String,
    /// SIFS VFS for grants under `sandbox_sifs_mount_root`. When set, file
    /// tools reach those grants through SIFS directly instead of the host
    /// FUSE projection.
    pub workspace_sifs_vfs: Option<WorkspaceSifsVfs>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalWorkspaceCommandRuntime {
    SimpleSandbox,
    LocalProcess,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalWorkspaceGitToolMode {
    /// Register Git helpers only when the workspace has Git metadata and the
    /// configured Git executable can be found without invoking it.
    #[default]
    Auto,
    /// Register Git helpers even when the workspace is not currently a Git
    /// worktree. This supports callers that initialize it later or provide a
    /// custom Git-compatible program.
    Enabled,
    /// Never register Git-specific helpers for this workspace.
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalWorkspaceSifsEvidenceConfig {
    pub mount_dir: PathBuf,
    pub sifs_root: String,
}

impl LocalWorkspaceSifsEvidenceConfig {
    pub fn new(mount_dir: impl Into<PathBuf>, sifs_root: impl Into<String>) -> Self {
        Self {
            mount_dir: mount_dir.into(),
            sifs_root: normalize_sifs_evidence_root(sifs_root.into()),
        }
    }

    fn sifs_uri_for_file(&self, file_name: &str) -> String {
        AgentToolArtifactRef::sifs(
            "evidence",
            format!(
                "{}/{}",
                self.sifs_root.trim_end_matches('/'),
                file_name.trim_start_matches('/')
            ),
        )
        .uri
    }
}

impl LocalWorkspaceAgentConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            root,
            additional_workspace_roots: Vec::new(),
            evidence_dir: None,
            sifs_evidence: None,
            command_runtime: LocalWorkspaceCommandRuntime::SimpleSandbox,
            sandbox_runtime_root: default_agent_sandbox_runtime_root(),
            sandbox_sifs_mount_root: None,
            sandbox_instance_id: None,
            sandbox_runtime_node_id: None,
            sandbox_portability: SandboxInstancePortability::NonPortable,
            // A SIFS home is enabled only when the caller can also provide a
            // live host mount. A path label without a mounted filesystem would
            // falsely imply synchronization.
            agent_home_sifs_path: None,
            sandbox_allow_unenforced: false,
            allow_shell_commands: false,
            allow_file_writes: false,
            max_read_bytes: DEFAULT_LOCAL_MAX_READ_BYTES,
            max_search_bytes_per_file: DEFAULT_LOCAL_MAX_SEARCH_BYTES_PER_FILE,
            max_command_output_bytes: DEFAULT_LOCAL_MAX_COMMAND_OUTPUT_BYTES,
            max_patch_bytes: DEFAULT_LOCAL_MAX_PATCH_BYTES,
            max_search_matches: DEFAULT_LOCAL_MAX_SEARCH_MATCHES,
            max_list_entries: DEFAULT_LOCAL_MAX_LIST_ENTRIES,
            git_tool_mode: LocalWorkspaceGitToolMode::Auto,
            git_program: "git".to_string(),
            workspace_sifs_vfs: None,
        }
    }

    pub fn with_workspace_sifs_vfs(mut self, sifs: WorkspaceSifsVfs) -> Self {
        self.workspace_sifs_vfs = Some(sifs);
        self
    }

    /// Open this config's workspace grants (primary first) for one tool call.
    pub(crate) fn workspace_files(&self) -> Result<WorkspaceFiles, String> {
        let grants = std::iter::once(&self.root)
            .chain(&self.additional_workspace_roots)
            .map(|root| WorkspaceGrantSpec::classify(root, self.sandbox_sifs_mount_root.as_deref()))
            .collect::<Vec<_>>();
        WorkspaceFiles::open(&grants, self.workspace_sifs_vfs.as_ref())
    }

    pub fn with_command_runtime(mut self, command_runtime: LocalWorkspaceCommandRuntime) -> Self {
        self.command_runtime = command_runtime;
        self
    }

    /// Append one additional workspace grant after the primary workspace.
    pub fn with_additional_workspace_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.additional_workspace_roots.push(root.into());
        self
    }

    /// Replace the ordered additional workspace grants.
    pub fn with_additional_workspace_roots<I, P>(mut self, roots: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.additional_workspace_roots = roots.into_iter().map(Into::into).collect();
        self
    }

    pub fn using_simple_sandbox_commands(self) -> Self {
        self.with_command_runtime(LocalWorkspaceCommandRuntime::SimpleSandbox)
    }

    pub fn using_local_process_commands(self) -> Self {
        self.with_command_runtime(LocalWorkspaceCommandRuntime::LocalProcess)
    }

    pub fn with_sandbox_runtime_root(mut self, sandbox_runtime_root: impl Into<PathBuf>) -> Self {
        self.sandbox_runtime_root = sandbox_runtime_root.into();
        self
    }

    pub fn with_sandbox_sifs_mount_root(mut self, mount_root: impl Into<PathBuf>) -> Self {
        self.sandbox_sifs_mount_root = Some(mount_root.into());
        self
    }

    pub fn without_sandbox_sifs_mount_root(mut self) -> Self {
        self.sandbox_sifs_mount_root = None;
        self
    }

    pub fn with_sandbox_instance_id(mut self, sandbox_instance_id: SandboxId) -> Self {
        self.sandbox_instance_id = Some(sandbox_instance_id);
        self
    }

    pub fn with_sandbox_runtime_node_id(mut self, sandbox_runtime_node_id: NodeId) -> Self {
        self.sandbox_runtime_node_id = Some(sandbox_runtime_node_id);
        self
    }

    pub fn without_sandbox_runtime_node_id(mut self) -> Self {
        self.sandbox_runtime_node_id = None;
        self
    }

    pub fn with_sandbox_portability(
        mut self,
        sandbox_portability: SandboxInstancePortability,
    ) -> Self {
        self.sandbox_portability = sandbox_portability;
        self
    }

    pub fn with_agent_home_sifs_path(mut self, agent_home_sifs_path: impl Into<String>) -> Self {
        self.agent_home_sifs_path =
            Some(normalize_agent_home_sifs_path(agent_home_sifs_path.into()));
        self
    }

    pub fn without_agent_home_sifs_path(mut self) -> Self {
        self.agent_home_sifs_path = None;
        self
    }

    pub fn allowing_unenforced_simple_sandbox(mut self, allow_unenforced: bool) -> Self {
        self.sandbox_allow_unenforced = allow_unenforced;
        self
    }

    pub fn allowing_shell_commands(mut self, allow_shell_commands: bool) -> Self {
        self.allow_shell_commands = allow_shell_commands;
        self
    }

    pub fn allowing_file_writes(mut self, allow_file_writes: bool) -> Self {
        self.allow_file_writes = allow_file_writes;
        self
    }

    pub fn with_evidence_dir(mut self, evidence_dir: impl Into<PathBuf>) -> Self {
        self.evidence_dir = Some(evidence_dir.into());
        self
    }

    pub fn with_sifs_evidence_mount(
        mut self,
        mount_dir: impl Into<PathBuf>,
        sifs_root: impl Into<String>,
    ) -> Self {
        self.sifs_evidence = Some(LocalWorkspaceSifsEvidenceConfig::new(mount_dir, sifs_root));
        self
    }

    pub fn with_git_program(mut self, git_program: impl Into<String>) -> Self {
        self.git_program = git_program.into();
        self
    }

    pub fn with_git_tool_mode(mut self, git_tool_mode: LocalWorkspaceGitToolMode) -> Self {
        self.git_tool_mode = git_tool_mode;
        self
    }

    pub fn enabling_git_tools(self) -> Self {
        self.with_git_tool_mode(LocalWorkspaceGitToolMode::Enabled)
    }

    pub fn disabling_git_tools(self) -> Self {
        self.with_git_tool_mode(LocalWorkspaceGitToolMode::Disabled)
    }

    pub fn detecting_git_tools(self) -> Self {
        self.with_git_tool_mode(LocalWorkspaceGitToolMode::Auto)
    }

    pub fn with_agent_storage(mut self, paths: &AgentStoragePaths, session_id: &SessionId) -> Self {
        self.evidence_dir = Some(paths.evidence_dir_for_session(session_id));
        self.sandbox_instance_id = Some(agent_sandbox_id_for_session(session_id));
        self
    }

    /// Enable the live SIFS-backed home for one agent identity.
    ///
    /// This is intentionally separate from [`Self::with_agent_storage`]: local
    /// evidence and stable sandbox identity remain usable on hosts where no
    /// live SIFS/FUSE mount is available.
    pub fn with_agent_home_for_agent_id(mut self, agent_id: &str) -> Self {
        self.agent_home_sifs_path = Some(agent_home_sifs_path_for_id(agent_id));
        self
    }

    pub fn with_data_dir_evidence(
        mut self,
        data_dir: &SinomoDataDir,
        session_id: &SessionId,
    ) -> Self {
        self = self.with_agent_storage(&AgentStoragePaths::from_data_dir(data_dir), session_id);
        self.sandbox_runtime_root = data_dir.root().join("sandbox").join("runtime");
        self
    }
}

/// Provision the default sandbox for a newly created ordinary agent session.
///
/// This is an orchestration hook and must be called before tools are exposed.
/// Tool execution only loads the attached sandbox and never calls this path.
/// The sandbox is managed through the sandbox service on the node that hosts
/// it (`config.sandbox_runtime_node_id`, or this node), the same way for local
/// and remote nodes. It is node-local (non-portable) because tool execution
/// runs against that node's sandbox store.
pub fn provision_default_agent_session_sandbox(
    sandboxes: &SandboxLifecycleClient,
    config: &LocalWorkspaceAgentConfig,
    mounts: Vec<SandboxMount>,
) -> Result<SandboxId, String> {
    let sandbox_id = config
        .sandbox_instance_id
        .clone()
        .ok_or_else(|| "agent session has no sandbox attachment id".to_string())?;
    let node = config.sandbox_runtime_node_id.as_ref();
    let toolchain_id = SandboxToolchainId::from_legacy_named(AGENT_SANDBOX_TOOLCHAIN_ID)
        .map_err(|source| format!("invalid agent sandbox toolchain id: {source}"))?;
    match sandboxes.status(
        node,
        SandboxStatusRequest {
            sandbox_id: sandbox_id.clone(),
        },
    ) {
        Ok(status) => {
            if status.manifest.mounts != mounts {
                sandboxes
                    .update(
                        node,
                        SandboxUpdateInstanceRequest {
                            sandbox_id: sandbox_id.clone(),
                            cwd: None,
                            env_set: BTreeMap::new(),
                            env_unset: Vec::new(),
                            mounts: Some(mounts),
                        },
                    )
                    .map_err(|source| {
                        format!("reconcile sandbox `{sandbox_id}` workspace mounts: {source}")
                    })?;
            }
            return Ok(sandbox_id);
        }
        Err(SandboxLifecycleError::NotFound { .. }) => {}
        Err(source) => return Err(format!("load attached sandbox `{sandbox_id}`: {source}")),
    }
    sandboxes
        .create(
            node,
            SandboxCreateInstanceRequest {
                sandbox_id: Some(sandbox_id.clone()),
                toolchain_id,
                temp_policy: SandboxTempPolicy::RetainAcrossSessions,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: Some(config.max_command_output_bytes as u64),
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts,
                portability: Some(SandboxInstancePortability::NonPortable),
            },
        )
        .map_err(|source| format!("provision sandbox `{sandbox_id}`: {source}"))?;
    Ok(sandbox_id)
}

/// Release the default sandbox provisioned for an agent session that is being
/// deleted. This is the orchestration counterpart of
/// [`provision_default_agent_session_sandbox`]: the automatic sandbox derived
/// from `session_id` is destroyed through the sandbox service on `node` (this
/// node when `None`), which settles its running processes first. Callers
/// decide whether the session owns that sandbox (an automatic attachment that
/// no other session shares). Returns whether a sandbox was destroyed.
pub fn release_default_agent_session_sandbox(
    sandboxes: &SandboxLifecycleClient,
    node: Option<&NodeId>,
    session_id: &SessionId,
) -> Result<bool, String> {
    let sandbox_id = agent_sandbox_id_for_session(session_id);
    match sandboxes.destroy(
        node,
        sinomo_runtime::sandbox::SandboxDestroyRequest {
            sandbox_id: sandbox_id.clone(),
        },
    ) {
        Ok(_) => Ok(true),
        Err(SandboxLifecycleError::NotFound { .. }) => Ok(false),
        Err(source) => Err(format!("destroy session sandbox `{sandbox_id}`: {source}")),
    }
}

pub fn register_tools(registry: &AgentToolRegistry, config: LocalWorkspaceAgentConfig) {
    let register_git_tools = should_register_git_tools(&config);
    registry.register(read_file::ReadFileTool::new(config.clone()));
    registry.register(list_files::ListFilesTool::new(config.clone()));
    registry.register(search::SearchFilesTool::new(config.clone()));
    registry.register(edit_file::EditFileTool::new(config.clone()));
    registry.register(write_file::WriteFileTool::new(config.clone()));
    registry.register(apply_patch::ApplyPatchTool::new(config.clone()));
    registry.register(str_replace_editor::StrReplaceEditorTool::new(
        config.clone(),
    ));
    if register_git_tools {
        registry.register(changed_files::ChangedFilesTool::new(config.clone()));
        registry.register(git_diff::GitDiffTool::new(config.clone()));
    }
    registry.register(run_command::RunCommandTool::new(config.clone()));
    registry.register(request_user_input::RequestUserInputTool);
}

pub fn registry(config: LocalWorkspaceAgentConfig) -> AgentToolRegistry {
    let registry = AgentToolRegistry::new();
    register_tools(&registry, config);
    registry
}

pub fn default_local_workspace_tool_descriptors() -> Vec<AgentToolDescriptor> {
    vec![
        read_file::descriptor(),
        list_files::descriptor(),
        search::descriptor(),
        edit_file::descriptor(),
        write_file::descriptor(),
        apply_patch::descriptor(),
        str_replace_editor::descriptor(),
        changed_files::descriptor(),
        git_diff::descriptor(),
        run_command::descriptor(),
        request_user_input::descriptor(),
    ]
}

fn spec_for_kind(kind: AgentToolKind) -> AgentToolSpec {
    default_coding_agent_tools()
        .into_iter()
        .chain(hidden_local_workspace_tool_specs())
        .find(|spec| spec.kind == kind)
        .expect("default coding tool spec exists")
}

fn descriptor_for_kind(kind: AgentToolKind) -> AgentToolDescriptor {
    let realtime_compatible = kind != AgentToolKind::RequestUserInput;
    let descriptor = AgentToolDescriptor::new(spec_for_kind(kind));
    if realtime_compatible {
        descriptor.with_invocation_modes(AgentToolInvocationMode::all())
    } else {
        descriptor
    }
}

fn hidden_local_workspace_tool_specs() -> Vec<AgentToolSpec> {
    vec![
        AgentToolSpec::new(
            AgentToolKind::ApplyPatch,
            "apply_patch",
            "Edit workspace files with a scoped provider-specific patch. Use only when the selected model/provider reliably supports the patch grammar or structured patch calls.",
            true,
        ),
        AgentToolSpec::new(
            AgentToolKind::ChangedFiles,
            "changed_files",
            "List workspace files changed relative to version control, including staged, unstaged, renamed, deleted, and untracked files.",
            false,
        ),
        AgentToolSpec::new(
            AgentToolKind::GitDiff,
            "git_diff",
            "Return a bounded version-control diff summary for the workspace and an evidence ref for the full diff when it is large.",
            false,
        ),
    ]
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LocalCommandOptions {
    interactive: bool,
    cleanup_on_exit: bool,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
    evidence_tool_name: String,
    evidence_reason: String,
    changed_files: Vec<String>,
    summary_override: Option<String>,
}

fn local_run_command_from_call(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    call: &AgentToolCall,
    cancellation_token: AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> AgentToolResult {
    if config.allow_shell_commands {
        let action = call
            .input
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("run");
        if matches!(action, "read" | "write" | "cancel") {
            return run_command::interact(config, session, call, action);
        }
        if !matches!(action, "run" | "start") {
            return AgentToolResult::failed(
                "Unknown command action",
                serde_json::json!({"error":"unknown_action"}),
            );
        }
    }
    local_shell_command_from_call(
        config,
        session,
        call,
        "run_command",
        None,
        "shell command execution enabled for this local workspace",
        cancellation_token,
        progress_sink,
    )
}

fn local_shell_command_from_call(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    call: &AgentToolCall,
    tool_name: &str,
    default_command: Option<&str>,
    evidence_reason: &str,
    cancellation_token: AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> AgentToolResult {
    if !config.allow_shell_commands {
        return AgentToolResult::blocked(
            format!("{tool_name} is disabled for this local workspace agent"),
            serde_json::json!({
                "error": "command_execution_disabled",
                "tool": tool_name,
            }),
        );
    }
    let command =
        match input_string(call, "command").or_else(|| default_command.map(str::to_string)) {
            Some(command) => command,
            None => {
                return AgentToolResult::failed(
                    format!("{tool_name} requires a string command"),
                    serde_json::json!({ "error": "missing_command", "tool": tool_name }),
                );
            }
        };
    #[cfg(windows)]
    let (program, args): (&str, Vec<String>) = ("cmd.exe", vec!["/C".to_string(), command.clone()]);
    #[cfg(not(windows))]
    let (program, args): (&str, Vec<String>) = local_shell_program_and_args(command.clone());
    run_local_command_with_options(
        config,
        session,
        program,
        args,
        None,
        LocalCommandOptions {
            interactive: tool_name == "run_command"
                && input_string(call, "action").as_deref() == Some("start"),
            cleanup_on_exit: tool_name == "run_command"
                && input_bool(call, "cleanup_on_exit") == Some(true),
            cwd: input_string(call, "cwd"),
            timeout_ms: input_timeout_ms(call),
            evidence_tool_name: tool_name.to_string(),
            evidence_reason: evidence_reason.to_string(),
            summary_override: Some(command),
            ..LocalCommandOptions::default()
        },
        cancellation_token,
        progress_sink,
    )
}

#[cfg(not(windows))]
fn local_shell_program_and_args(command: String) -> (&'static str, Vec<String>) {
    if host_probe::bash_is_installed() {
        (
            "bash",
            vec![
                "-o".to_string(),
                "pipefail".to_string(),
                "-lc".to_string(),
                command,
            ],
        )
    } else {
        ("sh", vec!["-lc".to_string(), command])
    }
}

fn with_coding_evidence(
    evidence: AgentToolEvidence,
    update: impl FnOnce(&mut AgentCodingToolEvidence),
) -> AgentToolEvidence {
    let mut coding = coding_evidence(&evidence);
    update(&mut coding);
    evidence.with_record(AGENT_CODING_EVIDENCE_KIND, &coding)
}

fn coding_evidence(evidence: &AgentToolEvidence) -> AgentCodingToolEvidence {
    evidence
        .record(AGENT_CODING_EVIDENCE_KIND)
        .unwrap_or_default()
}

fn run_local_command_with_options(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    program: &str,
    args: Vec<String>,
    stdin: Option<&[u8]>,
    options: LocalCommandOptions,
    cancellation_token: AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> AgentToolResult {
    let native = SandboxNativeExecutionOptions {
        interactive: options.interactive,
        cleanup_on_exit: options.cleanup_on_exit,
    };
    let result = execute_local_command(
        config,
        session,
        program,
        &args,
        stdin,
        options.timeout_ms,
        options.cwd.as_deref(),
        &cancellation_token,
        progress_sink,
        native,
    );
    let execution = match result {
        Ok(output) => output,
        Err(result) => return result,
    };
    if options.interactive && execution.output.success {
        return AgentToolResult::succeeded(
            "Started sandbox execution",
            serde_json::json!({"execution_id":execution.execution_id}),
        );
    }
    let mut result = local_command_result_from_output(
        config,
        execution.root,
        execution.cwd,
        program,
        &args,
        &execution.output,
        execution.timed_out,
        execution.cancelled,
        execution.cleanup_pending,
        execution.duration_ms,
        options.timeout_ms,
        &options.evidence_tool_name,
        &options.evidence_reason,
        options.changed_files,
        options.summary_override,
    );
    if let Some(id) = execution.execution_id {
        result.output["execution_id"] = serde_json::json!(id);
    }
    result
}

struct LocalCommandExecution {
    execution_id: Option<String>,
    root: PathBuf,
    cwd: PathBuf,
    output: LocalCommandOutput,
    timed_out: bool,
    cancelled: bool,
    cleanup_pending: bool,
    duration_ms: u64,
}

struct LocalCommandOutput {
    // Native commands retain only a preview here; their full bytes stay in the capture.
    captured: Option<crate::process::AgentProcessOutput>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status_code: Option<i32>,
    success: bool,
}

impl LocalCommandOutput {
    fn stdout_full(&self) -> Result<std::borrow::Cow<'_, [u8]>, AgentToolResult> {
        match &self.captured {
            Some(captured) => captured
                .window(false, 0, usize::MAX)
                .map(|window| std::borrow::Cow::Owned(window.bytes))
                .map_err(|error| {
                    AgentToolResult::failed(
                        format!("read complete command output: {error}"),
                        serde_json::json!({"error":"command_output_read_failed"}),
                    )
                }),
            None => Ok(std::borrow::Cow::Borrowed(&self.stdout)),
        }
    }
}

fn execute_local_command(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
    native: SandboxNativeExecutionOptions,
) -> Result<LocalCommandExecution, AgentToolResult> {
    match config.command_runtime {
        LocalWorkspaceCommandRuntime::SimpleSandbox => execute_simple_sandbox_command(
            config,
            session,
            program,
            args,
            stdin,
            timeout_ms,
            cwd,
            cancellation_token,
            progress_sink,
            native,
        ),
        LocalWorkspaceCommandRuntime::LocalProcess
            if native.interactive || native.cleanup_on_exit =>
        {
            Err(AgentToolResult::failed(
                "Execution handles require the Simple Native sandbox",
                serde_json::json!({"error":"unsupported_command_runtime"}),
            ))
        }
        LocalWorkspaceCommandRuntime::LocalProcess => execute_local_process_command(
            config,
            program,
            args,
            stdin,
            timeout_ms,
            cwd,
            cancellation_token,
            progress_sink,
        ),
    }
}

fn execute_local_process_command(
    config: &LocalWorkspaceAgentConfig,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
) -> Result<LocalCommandExecution, AgentToolResult> {
    let (root, cwd) = match resolve_workspace_cwd(config, cwd) {
        Ok(paths) => paths,
        Err(detail) => {
            return Err(AgentToolResult::blocked(
                detail.clone(),
                serde_json::json!({ "error": "workspace_root_not_allowed", "detail": detail }),
            ));
        }
    };
    let command_text = std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    let mut manager = AgentProcessManager::new();
    if let Some(directory) = &config.evidence_dir {
        manager = manager.with_output_directory(directory.join("process-capture"));
    }
    let process = manager
        .start(
            AgentProcessStart::new(command_text, program.to_string(), args.iter().cloned(), &cwd)
                .with_stdin_piped(stdin.is_some())
                .with_timeout_ms(timeout_ms)
                .with_cancellation_token(cancellation_token.clone()),
            config.max_command_output_bytes,
        )
        .map_err(|source| {
            AgentToolResult::failed(
                format!("failed to run {program}: {source}"),
                serde_json::json!({ "error": "command_spawn_failed", "detail": source.to_string() }),
            )
        })?;
    progress_sink.emit(AgentToolProgressEvent::ProcessStarted {
        call: None,
        process: process.clone(),
    });
    if let Some(stdin) = stdin {
        manager
            .write_stdin(&process.process_id, stdin, true)
            .map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to write stdin for {program}: {source}"),
                    serde_json::json!({ "error": "command_stdin_failed", "detail": source.to_string() }),
                )
            })?;
    }
    let started = Instant::now();
    let mut stdout_offset = 0usize;
    let mut stderr_offset = 0usize;
    let final_snapshot = loop {
        let snapshot = manager
            .snapshot(
                &process.process_id,
                stdout_offset,
                stderr_offset,
                config.max_command_output_bytes,
            )
            .map_err(|source| {
                AgentToolResult::failed(
                    format!("failed to inspect {program}: {source}"),
                    serde_json::json!({ "error": "command_inspect_failed", "detail": source.to_string() }),
                )
            })?;
        if !snapshot.stdout.is_empty() || !snapshot.stderr.is_empty() {
            progress_sink.emit(AgentToolProgressEvent::ProcessOutput {
                call: None,
                process: snapshot.clone(),
            });
        }
        stdout_offset = snapshot.stdout_offset;
        stderr_offset = snapshot.stderr_offset;
        if snapshot.status != AgentProcessLifecycleStatus::Running {
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                call: None,
                process: snapshot.clone(),
            });
            break snapshot;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let captured = manager
        .captured_output(&process.process_id)
        .map_err(|error| {
            AgentToolResult::failed(
                error.to_string(),
                serde_json::json!({"error":"command_output_read_failed"}),
            )
        })?;
    let read_preview = |stderr| {
        captured
            .window(stderr, 0, config.max_command_output_bytes)
            .map(|window| window.bytes)
            .map_err(|error| {
                AgentToolResult::failed(
                    error.to_string(),
                    serde_json::json!({"error":"command_output_read_failed"}),
                )
            })
    };
    let timed_out = final_snapshot.timed_out;
    let cancelled = final_snapshot.cancel_requested
        || final_snapshot.status == AgentProcessLifecycleStatus::Cancelled;
    let success = final_snapshot.status == AgentProcessLifecycleStatus::Completed;
    let status_code = final_snapshot.exit_code;
    let output = LocalCommandOutput {
        stdout: read_preview(false)?,
        stderr: read_preview(true)?,
        captured: Some(captured),
        status_code,
        success,
    };
    Ok(LocalCommandExecution {
        execution_id: None,
        root,
        cwd,
        output,
        timed_out,
        cancelled,
        cleanup_pending: false,
        duration_ms: duration_ms_from_elapsed(started.elapsed()),
    })
}

fn execute_simple_sandbox_command(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    program: &str,
    args: &[String],
    stdin: Option<&[u8]>,
    timeout_ms: Option<u64>,
    cwd: Option<&str>,
    cancellation_token: &AgentCancellationToken,
    progress_sink: AgentToolProgressSink,
    options: SandboxNativeExecutionOptions,
) -> Result<LocalCommandExecution, AgentToolResult> {
    let (root, cwd) = match resolve_workspace_cwd(config, cwd) {
        Ok(paths) => paths,
        Err(detail) => {
            return Err(AgentToolResult::blocked(
                detail.clone(),
                serde_json::json!({ "error": "workspace_root_not_allowed", "detail": detail }),
            ));
        }
    };
    // A live DFS-backed HOME is an optional projection. If it becomes
    // unavailable after registry setup (ownership drift, disconnected mount,
    // or another device using an incompatible numeric UID), preserve generic
    // shell execution with the inherited host HOME instead of rejecting the
    // command before it starts.
    let mut effective_config = config.clone();
    if agent_home_live_host_path(&effective_config).is_some()
        && !ensure_agent_home_directory(&effective_config)
    {
        effective_config = effective_config.without_agent_home_sifs_path();
    }
    let config = &effective_config;
    let prepared = prepare_sandbox_command(program, args, &cwd)?;
    let sandbox_cwd = sandbox_path_for_host_path(&cwd)?;
    let sandbox_id = agent_sandbox_id(config, &root);
    let node_id = requested_sandbox_runtime_node(config, session);
    if node_id != session.node_id {
        return Err(AgentToolResult::blocked(
            format!(
                "simple sandbox runtime `{sandbox_id}` is pinned to node `{node_id}`, but this turn is executing on `{}`",
                session.node_id
            ),
            serde_json::json!({
                "error": "sandbox_runtime_remote_unavailable",
                "runtime": SIMPLE_SANDBOX_RUNTIME,
                "sandbox_id": sandbox_id,
                "requested_node_id": node_id,
                "executing_node_id": session.node_id,
                "portability": config.sandbox_portability,
            }),
        ));
    }
    let toolchain_id =
        SandboxToolchainId::from_legacy_named(AGENT_SANDBOX_TOOLCHAIN_ID).map_err(|source| {
            AgentToolResult::failed(
                format!("failed to prepare simple sandbox toolchain id: {source}"),
                serde_json::json!({ "error": "sandbox_toolchain_id_invalid", "detail": source.to_string() }),
            )
        })?;
    let store = LocalSandboxInstanceStore::new(&config.sandbox_runtime_root);
    match store.load(&sandbox_id) {
        Ok(_) => {}
        Err(SandboxStoreError::NotFound { .. }) => {
            return Err(AgentToolResult::blocked(
                format!("agent session has no provisioned sandbox `{sandbox_id}`"),
                serde_json::json!({
                    "error": "sandbox_not_attached",
                    "sandbox_id": sandbox_id,
                }),
            ));
        }
        Err(source) => {
            return Err(sandbox_store_failure("load attached sandbox")(source));
        }
    }
    let local_execution_lease = acquire_sandbox_native_launch_lease(&store, &sandbox_id).map_err(
        sandbox_store_failure("acquire simple sandbox execution lease"),
    )?;
    let agent_home_host_path = agent_home_live_host_path(config);
    let (manifest, _layout) = prepare_agent_sandbox_manifest(
        config,
        &store,
        &sandbox_id,
        &toolchain_id,
        sandbox_cwd,
        &root,
        &cwd,
        &prepared.path_prefixes,
        &node_id,
        Some(&local_execution_lease),
    )?;
    let runner = SandboxSimpleNativeCommandRunner::new(SandboxSimpleNativeConfig {
        allow_unenforced: config.sandbox_allow_unenforced,
        host_sifs_root: config.sandbox_sifs_mount_root.clone(),
        extra_read_paths: sandbox_extra_read_paths(
            config,
            &root,
            &cwd,
            &prepared.path_prefixes,
            agent_home_host_path.as_deref(),
        ),
        // Shell execution is a generic host capability. Structured file-tool
        // authorization must not silently turn it into a read-only command.
        extra_write_paths: sandbox_extra_write_paths(
            config,
            &root,
            agent_home_host_path.as_deref(),
        ),
        extra_execute_paths: sandbox_extra_execute_paths(&prepared.path_prefixes),
    });
    let policy = agent_sandbox_policy(&manifest);
    let command_text = std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    let command_id = SandboxCommandId::generate();
    let process_id = command_id.to_string();
    let started = Instant::now();
    progress_sink.emit(AgentToolProgressEvent::ProcessStarted {
        call: None,
        process: sandbox_process_snapshot(
            &process_id,
            &command_text,
            program,
            args,
            &cwd,
            AgentProcessLifecycleStatus::Running,
            None,
            false,
            cancellation_token.is_cancelled(),
            None,
            started.elapsed(),
            timeout_ms,
            &[],
            &[],
            config.max_command_output_bytes,
        ),
    });
    if cancellation_token.is_cancelled() {
        let output = LocalCommandOutput {
            captured: None,
            stdout: Vec::new(),
            stderr: b"command cancelled before sandbox start".to_vec(),
            status_code: None,
            success: false,
        };
        progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
            call: None,
            process: sandbox_process_snapshot(
                &process_id,
                &command_text,
                program,
                args,
                &cwd,
                AgentProcessLifecycleStatus::Cancelled,
                None,
                false,
                true,
                Some("command cancelled before sandbox start".into()),
                started.elapsed(),
                timeout_ms,
                &output.stdout,
                &output.stderr,
                config.max_command_output_bytes,
            ),
        });
        return Ok(LocalCommandExecution {
            execution_id: None,
            root,
            cwd,
            output,
            timed_out: false,
            cancelled: true,
            cleanup_pending: false,
            duration_ms: duration_ms_from_elapsed(started.elapsed()),
        });
    }

    let (sandbox_cancellation, cancellation_bridge) =
        bridge_agent_sandbox_cancellation(cancellation_token);
    let started_execution = runner.start_toolchain_command(
        &store,
        &sandbox_id,
        &prepared.command_name,
        prepared.argv,
        stdin.map(|bytes| bytes.to_vec()),
        timeout_ms.unwrap_or(0),
        &node_id,
        &policy,
        None,
        sandbox_cancellation,
        local_execution_lease,
        command_id,
        options,
    );
    let outcome = match started_execution {
        Ok(handle) if options.interactive => {
            return Ok(LocalCommandExecution {
                execution_id: Some(handle.command_id.to_string()),
                root,
                cwd,
                output: LocalCommandOutput {
                    captured: None,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                    status_code: None,
                    success: true,
                },
                timed_out: false,
                cancelled: false,
                cleanup_pending: false,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            });
        }
        Ok(handle) => handle.wait(),
        Err(e) => Err(e),
    };
    drop(cancellation_bridge);
    let lifecycle_cleanup_failure = if outcome.is_err() {
        acquire_sandbox_native_launch_lease(&store, &sandbox_id)
            .map_err(sandbox_store_failure("settle sandbox process"))
            .err()
            .map(|r| r.summary)
    } else {
        None
    };
    match outcome {
        Ok(outcome) => {
            let status_code = Some(outcome.response.exit_status);
            let cleanup_pending = lifecycle_cleanup_failure.is_some();
            let success = outcome.response.exit_status == 0 && !cleanup_pending;
            let status = if success {
                AgentProcessLifecycleStatus::Completed
            } else {
                AgentProcessLifecycleStatus::Failed
            };
            let stderr = command_stderr_with_cleanup_failure(
                outcome.stderr,
                lifecycle_cleanup_failure.as_deref(),
            );
            if !outcome.stdout.is_empty() || !stderr.is_empty() {
                progress_sink.emit(AgentToolProgressEvent::ProcessOutput {
                    call: None,
                    process: sandbox_process_snapshot(
                        &process_id,
                        &command_text,
                        program,
                        args,
                        &cwd,
                        status.clone(),
                        status_code,
                        false,
                        false,
                        lifecycle_cleanup_failure.clone(),
                        started.elapsed(),
                        timeout_ms,
                        &outcome.stdout,
                        &stderr,
                        config.max_command_output_bytes,
                    ),
                });
            }
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                call: None,
                process: sandbox_process_snapshot(
                    &process_id,
                    &command_text,
                    program,
                    args,
                    &cwd,
                    status,
                    status_code,
                    false,
                    false,
                    lifecycle_cleanup_failure,
                    started.elapsed(),
                    timeout_ms,
                    // Output already went out as a delta above; process
                    // progress carries each byte once.
                    &[],
                    &[],
                    config.max_command_output_bytes,
                ),
            });
            Ok(LocalCommandExecution {
                execution_id: Some(process_id.clone()),
                root,
                cwd,
                output: LocalCommandOutput {
                    captured: None,
                    stdout: outcome.stdout,
                    stderr,
                    status_code,
                    success,
                },
                timed_out: false,
                cancelled: false,
                cleanup_pending,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            })
        }
        Err(SandboxCommandExecutionError::Wasi(SandboxWasiError::TimedOut { timeout_ms })) => {
            let cleanup_pending = lifecycle_cleanup_failure.is_some();
            let stderr = command_stderr_with_cleanup_failure(
                format!("sandbox command timed out after {timeout_ms} ms").into_bytes(),
                lifecycle_cleanup_failure.as_deref(),
            );
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                call: None,
                process: sandbox_process_snapshot(
                    &process_id,
                    &command_text,
                    program,
                    args,
                    &cwd,
                    AgentProcessLifecycleStatus::Failed,
                    None,
                    true,
                    false,
                    Some(
                        lifecycle_cleanup_failure
                            .unwrap_or_else(|| format!("command timed out after {timeout_ms} ms")),
                    ),
                    started.elapsed(),
                    Some(timeout_ms),
                    &[],
                    &stderr,
                    config.max_command_output_bytes,
                ),
            });
            Ok(LocalCommandExecution {
                execution_id: Some(process_id.clone()),
                root,
                cwd,
                output: LocalCommandOutput {
                    captured: None,
                    stdout: Vec::new(),
                    stderr,
                    status_code: None,
                    success: false,
                },
                timed_out: true,
                cancelled: false,
                cleanup_pending,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            })
        }
        Err(SandboxCommandExecutionError::Wasi(error))
            if matches!(
                &error,
                SandboxWasiError::Cancelled | SandboxWasiError::CancelledCleanupFailed { .. }
            ) =>
        {
            let (runtime_cleanup_pending, detail) = match error {
                SandboxWasiError::CancelledCleanupFailed { detail } => (true, detail),
                SandboxWasiError::Cancelled => (false, "sandbox command was cancelled".to_string()),
                _ => unreachable!("guarded cancellation variant"),
            };
            let cleanup_pending = runtime_cleanup_pending || lifecycle_cleanup_failure.is_some();
            let stderr = command_stderr_with_cleanup_failure(
                detail.as_bytes().to_vec(),
                lifecycle_cleanup_failure.as_deref(),
            );
            let progress_detail = lifecycle_cleanup_failure.unwrap_or_else(|| detail.clone());
            progress_sink.emit(AgentToolProgressEvent::ProcessCompleted {
                call: None,
                process: sandbox_process_snapshot(
                    &process_id,
                    &command_text,
                    program,
                    args,
                    &cwd,
                    AgentProcessLifecycleStatus::Cancelled,
                    None,
                    false,
                    true,
                    Some(progress_detail),
                    started.elapsed(),
                    timeout_ms,
                    &[],
                    &stderr,
                    config.max_command_output_bytes,
                ),
            });
            Ok(LocalCommandExecution {
                execution_id: Some(process_id.clone()),
                root,
                cwd,
                output: LocalCommandOutput {
                    captured: None,
                    stdout: Vec::new(),
                    stderr,
                    status_code: None,
                    success: false,
                },
                timed_out: false,
                cancelled: true,
                cleanup_pending,
                duration_ms: duration_ms_from_elapsed(started.elapsed()),
            })
        }
        Err(source) => {
            let cleanup_pending = lifecycle_cleanup_failure.is_some();
            let detail = match lifecycle_cleanup_failure {
                Some(cleanup) => format!("{source}; process lifecycle cleanup failed: {cleanup}"),
                None => source.to_string(),
            };
            Err(AgentToolResult::failed(
                format!("failed to run {program} in simple sandbox: {detail}"),
                serde_json::json!({
                    "error": "simple_sandbox_command_failed",
                    "detail": detail,
                    "runtime": "simple_sandbox",
                    "cleanup_pending": cleanup_pending,
                }),
            ))
        }
    }
}

fn command_stderr_with_cleanup_failure(
    mut stderr: Vec<u8>,
    cleanup_failure: Option<&str>,
) -> Vec<u8> {
    let Some(cleanup_failure) = cleanup_failure else {
        return stderr;
    };
    if !stderr.is_empty() && !stderr.ends_with(b"\n") {
        stderr.push(b'\n');
    }
    stderr.extend_from_slice(b"simple sandbox process lifecycle cleanup remains pending: ");
    stderr.extend_from_slice(cleanup_failure.as_bytes());
    stderr
}

struct AgentSandboxCancellationBridge {
    done: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for AgentSandboxCancellationBridge {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn bridge_agent_sandbox_cancellation(
    agent_token: &AgentCancellationToken,
) -> (SandboxCancellationToken, AgentSandboxCancellationBridge) {
    let sandbox_token = SandboxCancellationToken::new();
    if agent_token.is_cancelled() {
        let _ = sandbox_token.cancel();
    }
    let agent_token = agent_token.clone();
    let monitored_token = sandbox_token.clone();
    let done = Arc::new(AtomicBool::new(false));
    let monitor_done = Arc::clone(&done);
    let join = std::thread::spawn(move || {
        while !monitor_done.load(Ordering::Acquire) {
            if agent_token.is_cancelled() {
                let _ = monitored_token.cancel();
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    (
        sandbox_token,
        AgentSandboxCancellationBridge {
            done,
            join: Some(join),
        },
    )
}

struct PreparedSandboxCommand {
    command_name: String,
    argv: Vec<String>,
    path_prefixes: Vec<PathBuf>,
}

fn prepare_sandbox_command(
    program: &str,
    args: &[String],
    cwd: &Path,
) -> Result<PreparedSandboxCommand, AgentToolResult> {
    let program_uses_path =
        program.contains('/') || program.contains('\\') || Path::new(program).is_absolute();
    if !program_uses_path {
        return Ok(PreparedSandboxCommand {
            command_name: program.to_string(),
            argv: std::iter::once(program.to_string())
                .chain(args.iter().cloned())
                .collect(),
            path_prefixes: Vec::new(),
        });
    }
    let program_path = Path::new(program);
    let command_name = program_path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            AgentToolResult::failed(
                format!("failed to prepare sandbox command for {program}"),
                serde_json::json!({
                    "error": "sandbox_command_name_invalid",
                    "detail": format!("program {program} has no valid file name"),
                }),
            )
        })?
        .to_string();
    let mut path_prefixes = Vec::new();
    if let Some(parent) = program_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let parent = if parent.is_absolute() {
            parent.to_path_buf()
        } else {
            cwd.join(parent)
        };
        path_prefixes.push(parent);
    }
    Ok(PreparedSandboxCommand {
        command_name: command_name.clone(),
        argv: std::iter::once(command_name)
            .chain(args.iter().cloned())
            .collect(),
        path_prefixes,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_agent_sandbox_manifest(
    config: &LocalWorkspaceAgentConfig,
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    toolchain_id: &SandboxToolchainId,
    sandbox_cwd: SandboxPath,
    root: &Path,
    cwd: &Path,
    path_prefixes: &[PathBuf],
    node_id: &NodeId,
    local_execution_lease: Option<&SandboxLocalExecutionLease>,
) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), AgentToolResult> {
    owned_directory(&config.sandbox_runtime_root).map_err(|source| {
        AgentToolResult::failed(
            format!(
                "failed to create sandbox runtime root {}: {source}",
                config.sandbox_runtime_root.display()
            ),
            serde_json::json!({
                "error": "sandbox_runtime_root_unavailable",
                "detail": source.to_string(),
                "path": config.sandbox_runtime_root,
            }),
        )
    })?;
    let now = time::OffsetDateTime::now_utc();
    let (mut manifest, _layout) = match store.load(sandbox_id) {
        Ok(loaded) => loaded,
        Err(SandboxStoreError::NotFound { .. }) => {
            return Err(AgentToolResult::blocked(
                format!("agent session has no provisioned sandbox `{sandbox_id}`"),
                serde_json::json!({
                    "error": "sandbox_not_attached",
                    "sandbox_id": sandbox_id,
                }),
            ));
        }
        Err(source) => {
            return Err(sandbox_store_failure("load simple sandbox instance")(
                source,
            ));
        }
    };
    if let Some(lease) = local_execution_lease {
        lease
            .retain_live_native_processes(store, &mut manifest)
            .map_err(sandbox_store_failure("recover tracked sandbox processes"))?;
    }
    validate_sandbox_runtime_binding(&manifest, node_id, config)?;
    manifest.toolchain_id = toolchain_id.clone();
    manifest.cwd = sandbox_cwd;
    manifest.runtime_binding = Some(SandboxRuntimeBinding {
        runtime: SIMPLE_SANDBOX_RUNTIME.to_string(),
        node_id: node_id.clone(),
        portability: config.sandbox_portability.clone(),
    });
    let agent_home_host_path = agent_home_live_host_path(config);
    if let Some(home) = agent_home_host_path.as_deref() {
        create_agent_home_directory(config).map_err(|source| {
            AgentToolResult::failed(
                format!(
                    "failed to prepare live-mounted sandbox agent home host path {}: {source}",
                    home.display()
                ),
                serde_json::json!({
                    "error": "sandbox_agent_home_live_host_path_unavailable",
                    "detail": source.to_string(),
                    "path": home,
                }),
            )
        })?;
    }
    manifest.env = sandbox_environment(
        config,
        root,
        cwd,
        path_prefixes,
        agent_home_host_path.as_deref(),
    );
    // Mounts belong to the persistent sandbox and are configured by the
    // orchestration layer before tool execution. Never clear or rewrite them
    // from a tool call.
    // Bound the native process itself, not only the later tool-result preview.
    // This prevents an unbounded producer from buffering until process exit.
    manifest.limits.max_output_bytes = config.max_command_output_bytes as u64;
    manifest.last_placement = Some(
        sandbox_execution_placement_for_node(&manifest, node_id).map_err(|source| {
            AgentToolResult::blocked(
                format!("simple sandbox placement rejected: {source}"),
                serde_json::json!({
                    "error": "simple_sandbox_placement_rejected",
                    "detail": source.to_string(),
                    "sandbox_id": sandbox_id,
                    "requested_node_id": node_id,
                }),
            )
        })?,
    );
    manifest.updated_at = now;
    let layout = store
        .save_manifest(&manifest)
        .map_err(sandbox_store_failure("save simple sandbox manifest"))?;
    if let Some(home_sifs_path) = &config.agent_home_sifs_path {
        owned_directory(&layout.home).map_err(|source| {
            AgentToolResult::failed(
                format!(
                    "failed to create sandbox home {}: {source}",
                    layout.home.display()
                ),
                serde_json::json!({
                    "error": "sandbox_home_unavailable",
                    "detail": source.to_string(),
                    "path": layout.home,
                }),
            )
        })?;
        let metadata = owned_directory(&layout.metadata).map_err(|source| {
            AgentToolResult::failed(
                format!(
                    "failed to create sandbox metadata {}: {source}",
                    layout.metadata.display()
                ),
                serde_json::json!({
                    "error": "sandbox_metadata_unavailable",
                    "detail": source.to_string(),
                    "path": layout.metadata,
                }),
            )
        })?;
        let _ = metadata.write(
            "agent-home-sifs-path.txt",
            normalize_agent_home_sifs_path(home_sifs_path.clone()),
        );
        if let Some(home) = agent_home_host_path.as_deref() {
            let _ = metadata.write("agent-home-live-host-path.txt", home.display().to_string());
        }
    }
    Ok((manifest, layout))
}

fn validate_sandbox_runtime_binding(
    manifest: &SandboxInstanceManifest,
    node_id: &NodeId,
    config: &LocalWorkspaceAgentConfig,
) -> Result<(), AgentToolResult> {
    let Some(binding) = &manifest.runtime_binding else {
        return Ok(());
    };
    if binding.runtime != SIMPLE_SANDBOX_RUNTIME {
        return Err(AgentToolResult::blocked(
            format!(
                "sandbox `{}` is already bound to runtime `{}`",
                manifest.sandbox_id, binding.runtime
            ),
            serde_json::json!({
                "error": "sandbox_runtime_binding_mismatch",
                "sandbox_id": manifest.sandbox_id,
                "existing_runtime": binding.runtime,
                "requested_runtime": SIMPLE_SANDBOX_RUNTIME,
            }),
        ));
    }
    if binding.portability == SandboxInstancePortability::NonPortable && binding.node_id != *node_id
    {
        return Err(AgentToolResult::blocked(
            format!(
                "sandbox `{}` is non-portable and is already bound to node `{}`",
                manifest.sandbox_id, binding.node_id
            ),
            serde_json::json!({
                "error": "sandbox_non_portable_node_mismatch",
                "sandbox_id": manifest.sandbox_id,
                "existing_node_id": binding.node_id,
                "requested_node_id": node_id,
                "runtime": binding.runtime,
                "portability": binding.portability,
            }),
        ));
    }
    if config.sandbox_portability == SandboxInstancePortability::NonPortable
        && binding.node_id != *node_id
    {
        return Err(AgentToolResult::blocked(
            format!(
                "simple sandbox `{}` cannot move from node `{}` to node `{node_id}`",
                manifest.sandbox_id, binding.node_id
            ),
            serde_json::json!({
                "error": "sandbox_non_portable_node_mismatch",
                "sandbox_id": manifest.sandbox_id,
                "existing_node_id": binding.node_id,
                "requested_node_id": node_id,
                "runtime": SIMPLE_SANDBOX_RUNTIME,
                "portability": config.sandbox_portability,
            }),
        ));
    }
    Ok(())
}

fn sandbox_store_failure(
    action: &'static str,
) -> impl FnOnce(SandboxStoreError) -> AgentToolResult {
    move |source| {
        AgentToolResult::failed(
            format!("{action} failed: {source}"),
            serde_json::json!({
                "error": "simple_sandbox_store_failed",
                "action": action,
                "detail": source.to_string(),
            }),
        )
    }
}

fn agent_sandbox_policy(manifest: &SandboxInstanceManifest) -> SandboxPolicy {
    let mut policy = SandboxPolicy {
        allow_read_write_sifs_mounts: true,
        ..SandboxPolicy::default()
    };
    if !policy
        .allowed_mount_prefixes
        .iter()
        .any(|prefix| prefix == AGENT_HOME_SIFS_ROOT)
    {
        policy
            .allowed_mount_prefixes
            .push(AGENT_HOME_SIFS_ROOT.to_string());
    }
    // The attached manifest is the management-plane authority for this
    // session's sandbox resources: orchestration materializes conversation
    // grants there, while tool calls cannot add mounts. Admit those exact SIFS
    // sources for this command instead of limiting execution to the
    // sandbox-shared and agent-home namespaces.
    for mount in &manifest.mounts {
        if !policy
            .allowed_mount_prefixes
            .iter()
            .any(|prefix| prefix == &mount.source_sifs_path)
        {
            policy
                .allowed_mount_prefixes
                .push(mount.source_sifs_path.clone());
        }
    }
    policy
}

fn sandbox_environment(
    config: &LocalWorkspaceAgentConfig,
    root: &Path,
    cwd: &Path,
    path_prefixes: &[PathBuf],
    agent_home_host_path: Option<&Path>,
) -> BTreeMap<String, String> {
    // A simple-native command is a generic host command. Preserve the host
    // environment by default, then add the sandbox/workspace context that the
    // runner owns. Environment entries that cannot cross the string-based
    // sandbox request ABI are omitted.
    let mut envs = env::vars_os()
        .filter_map(|(name, value)| {
            let name = name.into_string().ok()?;
            let value = value.into_string().ok()?;
            sandbox_env_name_is_valid(&name).then_some((name, value))
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(path) = sandbox_path_env(path_prefixes) {
        envs.insert("PATH".to_string(), path);
    }
    envs.insert(
        "SINOMO_AGENT_TOOL_RUNTIME".to_string(),
        "simple_sandbox".to_string(),
    );
    envs.insert(
        "SINOMO_WORKSPACE_ROOT".to_string(),
        root.display().to_string(),
    );
    envs.insert(
        "SINOMO_WORKSPACE_CWD".to_string(),
        cwd.display().to_string(),
    );
    if let Some(home_sifs_path) = &config.agent_home_sifs_path {
        envs.insert(
            "SINOMO_AGENT_HOME_SIFS".to_string(),
            normalize_agent_home_sifs_path(home_sifs_path.clone()),
        );
    }
    if let Some(home) = agent_home_host_path {
        let home = home.display().to_string();
        envs.insert("HOME".to_string(), home.clone());
        envs.insert("USERPROFILE".to_string(), home.clone());
        envs.insert("SINOMO_AGENT_HOME_HOST_PATH".to_string(), home);
    }
    envs
}

fn sandbox_extra_read_paths(
    config: &LocalWorkspaceAgentConfig,
    root: &Path,
    cwd: &Path,
    path_prefixes: &[PathBuf],
    agent_home_host_path: Option<&Path>,
) -> Vec<PathBuf> {
    let mut paths = vec![root.to_path_buf(), cwd.to_path_buf()];
    paths.extend(config.additional_workspace_roots.iter().cloned());
    paths.extend(path_prefixes.iter().cloned());
    if let Some(home) = agent_home_host_path {
        paths.push(home.to_path_buf());
    }
    paths.extend(current_path_dirs());
    dedup_pathbufs(paths)
}

fn sandbox_extra_write_paths(
    config: &LocalWorkspaceAgentConfig,
    root: &Path,
    agent_home_host_path: Option<&Path>,
) -> Vec<PathBuf> {
    let mut paths = vec![root.to_path_buf()];
    paths.extend(config.additional_workspace_roots.iter().cloned());
    if let Some(home) = agent_home_host_path {
        paths.push(home.to_path_buf());
    }
    dedup_pathbufs(paths)
}

fn sandbox_extra_execute_paths(path_prefixes: &[PathBuf]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    paths.extend(path_prefixes.iter().cloned());
    paths.extend(current_path_dirs());
    dedup_pathbufs(paths)
}

fn sandbox_path_env(path_prefixes: &[PathBuf]) -> Option<String> {
    let mut entries = Vec::new();
    entries.extend(path_prefixes.iter().cloned());
    entries.extend(current_path_dirs());
    let entries = dedup_pathbufs(entries);
    env::join_paths(entries)
        .ok()
        .and_then(|value| value.into_string().ok())
}

fn should_register_git_tools(config: &LocalWorkspaceAgentConfig) -> bool {
    match config.git_tool_mode {
        LocalWorkspaceGitToolMode::Enabled => true,
        LocalWorkspaceGitToolMode::Disabled => false,
        LocalWorkspaceGitToolMode::Auto => {
            workspace_has_git_metadata(&config.root) && program_is_available(&config.git_program)
        }
    }
}

fn dedup_pathbufs(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut unique = Vec::new();
    for path in paths {
        if !unique.iter().any(|existing| existing == &path) {
            unique.push(path);
        }
    }
    unique
}

fn agent_sandbox_id(config: &LocalWorkspaceAgentConfig, root: &Path) -> SandboxId {
    config
        .sandbox_instance_id
        .clone()
        .unwrap_or_else(|| agent_sandbox_id_for_workspace(root))
}

fn agent_sandbox_id_for_workspace(root: &Path) -> SandboxId {
    SandboxId::derived(
        "sinomo-agent-local-workspace-sandbox",
        root.to_string_lossy().as_bytes(),
    )
}

pub fn agent_sandbox_id_for_session(session_id: &SessionId) -> SandboxId {
    let seed = session_id.to_string().into_bytes();
    SandboxId::derived("sinomo-agent-session-sandbox", &seed)
}

fn requested_sandbox_runtime_node(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
) -> NodeId {
    config
        .sandbox_runtime_node_id
        .clone()
        .unwrap_or_else(|| session.node_id.clone())
}

fn default_agent_sandbox_runtime_root() -> PathBuf {
    env::temp_dir().join("sinomo-agent").join("sandbox-runtime")
}

fn agent_home_live_host_path(config: &LocalWorkspaceAgentConfig) -> Option<PathBuf> {
    let mount_root = config.sandbox_sifs_mount_root.as_ref()?;
    let home_sifs_path = config
        .agent_home_sifs_path
        .as_ref()
        .map(|path| normalize_agent_home_sifs_path(path.clone()))?;
    Some(mount_root.join(home_sifs_path.trim_start_matches('/')))
}

pub fn agent_home_sifs_path_for_id(agent_id: &str) -> String {
    let agent_id = sanitize_agent_home_id(agent_id);
    format!("{AGENT_HOME_SIFS_ROOT}/{agent_id}/home")
}

fn sanitize_agent_home_id(agent_id: &str) -> String {
    let is_path_safe = !agent_id.is_empty()
        && agent_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if is_path_safe
        && agent_id.len() <= MAX_AGENT_HOME_ID_COMPONENT_BYTES
        && !agent_id.starts_with(ENCODED_AGENT_HOME_ID_PREFIX)
        && !agent_id.starts_with(HASHED_AGENT_HOME_ID_PREFIX)
    {
        return agent_id.to_string();
    }

    // Agent package identifiers also allow ':'. Reserve path-safe namespaces
    // and encode every transformed ID, including safe IDs beginning with a
    // reserved prefix. This keeps ordinary mappings injective while preserving
    // the established home paths of short path-safe agent IDs.
    let encoded = format!(
        "{ENCODED_AGENT_HOME_ID_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(agent_id.as_bytes())
    );
    if encoded.len() <= MAX_AGENT_HOME_ID_COMPONENT_BYTES {
        return encoded;
    }

    // A definition id has no declared length ceiling. Bound its filesystem
    // component without truncating a shared prefix; the full digest retains a
    // collision-resistant identity and the reserved namespace prevents a
    // literal safe id from aliasing it.
    format!(
        "{HASHED_AGENT_HOME_ID_PREFIX}{}",
        blake3::hash(agent_id.as_bytes()).to_hex()
    )
}

fn normalize_agent_home_sifs_path(path: String) -> String {
    let path = path
        .trim()
        .trim_start_matches("sifs://")
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();
    if path.is_empty() {
        agent_home_sifs_path_for_id("default")
    } else {
        format!("/{path}")
    }
}

fn sandbox_path_for_host_path(path: &Path) -> Result<SandboxPath, AgentToolResult> {
    SandboxPath::for_host_path(path).map_err(|source| {
        AgentToolResult::failed(
            format!(
                "failed to prepare sandbox cwd for {}: {source}",
                path.display()
            ),
            serde_json::json!({
                "error": "sandbox_cwd_invalid",
                "detail": source.to_string(),
                "path": path,
            }),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn sandbox_process_snapshot(
    process_id: &str,
    command: &str,
    program: &str,
    args: &[String],
    cwd: &Path,
    status: AgentProcessLifecycleStatus,
    exit_code: Option<i32>,
    timed_out: bool,
    cancel_requested: bool,
    error: Option<String>,
    elapsed: Duration,
    timeout_ms: Option<u64>,
    stdout: &[u8],
    stderr: &[u8],
    max_output_bytes: usize,
) -> AgentProcessSnapshot {
    let stdout_text = truncate_utf8_lossy(stdout, max_output_bytes);
    let stderr_text = truncate_utf8_lossy(stderr, max_output_bytes);
    AgentProcessSnapshot {
        process_id: process_id.to_string(),
        command: command.to_string(),
        program: program.to_string(),
        args: args.to_vec(),
        cwd: cwd.display().to_string(),
        status,
        exit_code,
        timed_out,
        cancel_requested,
        error,
        elapsed_ms: duration_ms_from_elapsed(elapsed),
        timeout_ms,
        stdout: stdout_text.text,
        stderr: stderr_text.text,
        stdout_offset: stdout.len().min(max_output_bytes),
        stderr_offset: stderr.len().min(max_output_bytes),
        stdout_total_bytes: stdout.len(),
        stderr_total_bytes: stderr.len(),
        stdout_truncated: stdout_text.truncated,
        stderr_truncated: stderr_text.truncated,
    }
}

fn duration_ms_from_elapsed(elapsed: Duration) -> u64 {
    elapsed.as_millis().min(u128::from(u64::MAX)) as u64
}

fn aggregate_command_output(stdout: &str, stderr: &str) -> String {
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout.to_string(),
        (true, false) => stderr.to_string(),
        (false, false) => format!("stdout:\n{stdout}\nstderr:\n{stderr}"),
    }
}

fn local_command_result_from_output(
    config: &LocalWorkspaceAgentConfig,
    root: PathBuf,
    cwd: PathBuf,
    program: &str,
    args: &[String],
    output: &LocalCommandOutput,
    timed_out: bool,
    cancelled: bool,
    cleanup_pending: bool,
    duration_ms: u64,
    timeout_ms: Option<u64>,
    evidence_tool_name: &str,
    evidence_reason: &str,
    changed_files: Vec<String>,
    summary_override: Option<String>,
) -> AgentToolResult {
    let mut stdout = truncate_utf8_lossy(&output.stdout, config.max_command_output_bytes);
    let mut stderr = truncate_utf8_lossy(&output.stderr, config.max_command_output_bytes);
    if let Some(captured) = &output.captured {
        let sizes = captured.window(false, 0, 0).and_then(|out| {
            captured
                .window(true, 0, 0)
                .map(|err| (out.total_bytes, err.total_bytes))
        });
        match sizes {
            Ok((stdout_size, stderr_size)) => {
                stdout.truncated = stdout_size > output.stdout.len();
                stderr.truncated = stderr_size > output.stderr.len();
            }
            Err(error) => {
                return AgentToolResult::failed(
                    format!("inspect command output: {error}"),
                    serde_json::json!({"error":"command_output_read_failed"}),
                );
            }
        }
    }
    let save_stream = |is_stderr, extension, bytes: &[u8]| match &output.captured {
        Some(captured) => {
            write_captured_evidence(config, evidence_tool_name, extension, captured, is_stderr)
        }
        None => write_local_evidence(config, evidence_tool_name, extension, bytes),
    };
    let status = output.status_code;
    let succeeded = !timed_out && !cancelled && !cleanup_pending && output.success;
    let cwd_display = display_host_workspace_path(config, &cwd);
    let stdout_ref = if stdout.truncated {
        save_stream(false, "stdout.txt", &output.stdout).map(|uri| {
            AgentToolArtifactRef::new("stdout", uri)
                .with_label("complete stdout")
                .with_media_type("text/plain; charset=utf-8")
        })
    } else {
        None
    };
    let stderr_ref = if stderr.truncated {
        save_stream(true, "stderr.txt", &output.stderr).map(|uri| {
            AgentToolArtifactRef::new("stderr", uri)
                .with_label("complete stderr")
                .with_media_type("text/plain; charset=utf-8")
        })
    } else {
        None
    };
    let aggregated_output = aggregate_command_output(&stdout.text, &stderr.text);
    let command_output = AgentCommandOutput {
        exit_code: status,
        stdout: stdout.text.clone(),
        stderr: stderr.text.clone(),
        stdout_ref: stdout_ref.clone(),
        stderr_ref: stderr_ref.clone(),
        aggregated_output: aggregated_output.clone(),
        duration_ms,
        timed_out,
        truncated: stdout.truncated || stderr.truncated,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    };
    let summary = if timed_out {
        format!(
            "{program} {} timed out after {} ms",
            args.join(" "),
            timeout_ms.unwrap_or_default()
        )
    } else if cancelled {
        format!("{} {} cancelled", program, args.join(" "))
    } else {
        summary_override
            .map(|summary| {
                if succeeded {
                    summary
                } else {
                    format!("{summary} failed with exit status {:?}", status)
                }
            })
            .unwrap_or_else(|| format!("{program} {} exited with {:?}", args.join(" "), status))
    };
    let payload = serde_json::json!({
        "program": program,
        "args": args,
        "cwd": cwd_display,
        "status": status,
        "success": succeeded,
        "timed_out": timed_out,
        "cancelled": cancelled,
        "cleanup_pending": cleanup_pending,
        "duration_ms": duration_ms,
        "timeout_ms": timeout_ms,
        "stdout": stdout.text,
        "stderr": stderr.text,
        "stdout_ref": stdout_ref,
        "stderr_ref": stderr_ref,
        "aggregated_output": aggregated_output,
        "command_output": command_output,
        "stdout_truncated": stdout.truncated,
        "stderr_truncated": stderr.truncated,
        "changed_files": changed_files.clone(),
    });
    let full_output_ref = if stdout.truncated || stderr.truncated {
        let payload = if output.captured.is_some() {
            // Complete streams are separate artifacts. Do not rebuild them in
            // RAM just to duplicate their text into the outcome metadata file.
            payload.clone()
        } else {
            serde_json::json!({
                "program": program,
                "args": args,
                "cwd": cwd_display,
                "duration_ms": duration_ms,
                "timed_out": timed_out,
                "cancelled": cancelled,
                "cleanup_pending": cleanup_pending,
                "timeout_ms": timeout_ms,
                "stdout": String::from_utf8_lossy(&output.stdout),
                "stderr": String::from_utf8_lossy(&output.stderr),
            })
        };
        serde_json::to_vec_pretty(&payload)
            .ok()
            .and_then(|bytes| write_local_evidence(config, evidence_tool_name, "json", &bytes))
    } else {
        None
    };
    let mut evidence = AgentToolEvidence::availability_only(true, evidence_reason)
        .with_command(cwd.display().to_string(), status)
        .with_output_value(&payload, stdout.truncated || stderr.truncated)
        .with_metadata(serde_json::json!({
            "workspace_root": root.display().to_string(),
            "timed_out": timed_out,
            "cancelled": cancelled,
            "cleanup_pending": cleanup_pending,
            "timeout_ms": timeout_ms,
            "sandbox": local_workspace_sandbox_metadata(config),
        }));
    if !changed_files.is_empty() {
        evidence = evidence.with_record(
            AGENT_CODING_EVIDENCE_KIND,
            &AgentCodingToolEvidence {
                changed_files,
                ..AgentCodingToolEvidence::default()
            },
        );
    }
    if let Some(full_output_ref) = full_output_ref {
        evidence = evidence.with_artifact(
            AgentToolArtifactRef::new("full_output", full_output_ref)
                .with_media_type("application/json"),
        );
    }
    if let Some(stdout_ref) = command_output.stdout_ref.clone() {
        evidence = evidence.with_artifact(stdout_ref);
    }
    if let Some(stderr_ref) = command_output.stderr_ref.clone() {
        evidence = evidence.with_artifact(stderr_ref);
    }
    if cancelled {
        AgentToolResult::cancelled(summary, payload).with_evidence(evidence)
    } else if succeeded {
        AgentToolResult::succeeded(summary, payload).with_evidence(evidence)
    } else {
        AgentToolResult::failed(summary, payload).with_evidence(evidence)
    }
}

fn local_workspace_sandbox_metadata(config: &LocalWorkspaceAgentConfig) -> serde_json::Value {
    match config.command_runtime {
        LocalWorkspaceCommandRuntime::SimpleSandbox => serde_json::json!({
            "runtime": SIMPLE_SANDBOX_RUNTIME,
            "checked": true,
            "allow_unenforced": config.sandbox_allow_unenforced,
            "runtime_root": config.sandbox_runtime_root,
            "sandbox_instance_id": config.sandbox_instance_id,
            "runtime_node_id": config.sandbox_runtime_node_id,
            "portability": config.sandbox_portability,
            "agent_home_sifs_path": config.agent_home_sifs_path,
            "sandbox_sifs_mount_root": config.sandbox_sifs_mount_root,
        }),
        LocalWorkspaceCommandRuntime::LocalProcess => serde_json::json!({
            "runtime": "local_process",
            "checked": false,
        }),
    }
}

/// Create (if needed) and open a directory the agent runtime owns, such as
/// the sandbox runtime root or an evidence directory. Owned paths are not
/// workspace grants; they are reached through the local VFS.
fn owned_directory(path: &Path) -> Result<VfsHandle, VfsError> {
    let handle = VfsHandle::local(path)?;
    handle.create_dir_all("/")?;
    Ok(handle)
}

/// The agent home lives on SIFS. Create it through the SIFS VFS when one is
/// configured, otherwise through its live host projection.
fn create_agent_home_directory(config: &LocalWorkspaceAgentConfig) -> Result<(), String> {
    if let (Some(sifs), Some(home)) = (
        config.workspace_sifs_vfs.as_ref(),
        config.agent_home_sifs_path.as_deref(),
    ) {
        let files = WorkspaceFiles::open(
            &[WorkspaceGrantSpec {
                host_root: PathBuf::from("/"),
                sifs_root: Some("/".to_string()),
            }],
            Some(sifs),
        )?;
        let home = files.resolve(&normalize_agent_home_sifs_path(home.to_string()))?;
        return files
            .create_dir_all(&home)
            .map_err(|source| source.to_string());
    }
    let home = agent_home_live_host_path(config)
        .ok_or_else(|| "agent home has no live host path".to_string())?;
    owned_directory(&home)
        .map(drop)
        .map_err(|source| source.to_string())
}

fn ensure_agent_home_directory(config: &LocalWorkspaceAgentConfig) -> bool {
    if create_agent_home_directory(config).is_err() {
        return false;
    }
    agent_home_live_host_path(config).is_some_and(|home| {
        VfsHandle::local(home).is_ok_and(|handle| {
            handle
                .stat("/")
                .is_ok_and(|metadata| metadata.kind == VfsObjectKind::Directory)
        })
    })
}

fn write_captured_evidence(
    config: &LocalWorkspaceAgentConfig,
    tool_name: &str,
    extension: &str,
    captured: &crate::process::AgentProcessOutput,
    stderr: bool,
) -> Option<String> {
    let name = format!(
        "{}-{}.{}",
        sanitize_evidence_component(tool_name),
        TaskId::generate(),
        sanitize_evidence_component(extension)
    );
    let copy = |directory: &Path| {
        let handle = owned_directory(directory).ok()?;
        copy_captured_output(captured, stderr, &handle, &name).ok()?;
        Some(directory.join(&name))
    };
    let local = config.evidence_dir.as_deref().and_then(copy);
    let sifs = config
        .sifs_evidence
        .as_ref()
        .and_then(|sifs| copy(&sifs.mount_dir).map(|_| sifs.sifs_uri_for_file(&name)));
    sifs.or_else(|| local.map(|path| path.display().to_string()))
}

fn write_local_evidence(
    config: &LocalWorkspaceAgentConfig,
    tool_name: &str,
    extension: &str,
    bytes: &[u8],
) -> Option<String> {
    let file_name = format!(
        "{}-{}.{}",
        sanitize_evidence_component(&tool_name.replace('_', "-")),
        TaskId::generate(),
        sanitize_evidence_component(extension.trim_start_matches('.'))
    );
    let local_ref = config.evidence_dir.as_ref().and_then(|evidence_dir| {
        owned_directory(evidence_dir)
            .ok()?
            .write(&file_name, bytes)
            .ok()?;
        Some(evidence_dir.join(&file_name).display().to_string())
    });
    let sifs_ref = config.sifs_evidence.as_ref().and_then(|sifs| {
        owned_directory(&sifs.mount_dir)
            .ok()?
            .write(&file_name, bytes)
            .ok()?;
        Some(sifs.sifs_uri_for_file(&file_name))
    });
    sifs_ref.or(local_ref)
}

fn copy_captured_output(
    captured: &crate::process::AgentProcessOutput,
    stderr: bool,
    directory: &VfsHandle,
    name: &str,
) -> Result<(), String> {
    const CHUNK_BYTES: usize = 1024 * 1024;
    directory
        .write_with_options(
            name,
            [],
            sinomo_runtime::vfs::VfsWriteOptions::local_replace().create_new(),
        )
        .map_err(|source| source.to_string())?;
    let mut offset = 0;
    loop {
        let window = captured
            .window(stderr, offset, CHUNK_BYTES)
            .map_err(|source| source.to_string())?;
        if window.bytes.is_empty() {
            return Ok(());
        }
        directory
            .append_with_options(
                name,
                &window.bytes,
                sinomo_runtime::vfs::VfsAppendOptions::local().without_create(),
            )
            .map_err(|source| source.to_string())?;
        offset = window.offset;
    }
}

fn normalize_sifs_evidence_root(root: String) -> String {
    let root = root
        .trim()
        .trim_start_matches("sifs://")
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();
    if root.is_empty() {
        "/events/agent/evidence".to_string()
    } else {
        format!("/{root}")
    }
}

fn sanitize_evidence_component(component: &str) -> String {
    let sanitized: String = component
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect();
    let sanitized = sanitized.trim_matches('-').trim_matches('.').to_string();
    if sanitized.is_empty() {
        "evidence".to_string()
    } else {
        sanitized
    }
}

fn input_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

fn input_raw_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

fn input_bool(call: &AgentToolCall, key: &str) -> Option<bool> {
    call.input.get(key).and_then(serde_json::Value::as_bool)
}

fn input_usize(call: &AgentToolCall, key: &str) -> Option<usize> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn input_timeout_ms(call: &AgentToolCall) -> Option<u64> {
    call.input
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)
}

fn open_workspace_files(config: &LocalWorkspaceAgentConfig) -> Result<WorkspaceFiles, String> {
    config.workspace_files()
}

/// Model-facing name for a host path inside a grant (for example a command
/// cwd); falls back to the host path itself.
fn display_host_workspace_path(config: &LocalWorkspaceAgentConfig, path: &Path) -> String {
    open_workspace_files(config)
        .ok()
        .and_then(|files| {
            let file = files.resolve(&path.display().to_string()).ok()?;
            Some(files.display(&file))
        })
        .unwrap_or_else(|| path.display().to_string())
}

/// Resolve a command cwd to `(primary root, cwd)` host paths. The directory
/// check goes through the workspace VFS.
fn resolve_workspace_cwd(
    config: &LocalWorkspaceAgentConfig,
    requested: Option<&str>,
) -> Result<(PathBuf, PathBuf), String> {
    let files = open_workspace_files(config)?;
    let root = files.primary_root().to_path_buf();
    let Some(requested) = requested else {
        return Ok((root.clone(), root));
    };
    let cwd = files.resolve(requested)?;
    match files.stat(&cwd) {
        Ok(Some(metadata)) if metadata.kind == VfsObjectKind::Directory => {}
        Ok(Some(_)) => {
            return Err(format!(
                "cwd {} is not a directory",
                files.host_path(&cwd).display()
            ));
        }
        Ok(None) => {
            return Err(format!(
                "cwd {} is not accessible: not found",
                files.host_path(&cwd).display()
            ));
        }
        Err(source) => {
            return Err(format!(
                "cwd {} is not accessible: {source}",
                files.host_path(&cwd).display()
            ));
        }
    }
    Ok((root, files.host_path(&cwd)))
}

/// Resolve a path that must already exist in a granted workspace.
fn resolve_existing_workspace_file(
    files: &WorkspaceFiles,
    requested: &str,
) -> Result<WorkspaceFile, String> {
    let file = files.resolve(requested)?;
    match files.stat(&file) {
        Ok(Some(_)) => Ok(file),
        Ok(None) => Err(format!(
            "path {} is not accessible: not found",
            files.host_path(&file).display()
        )),
        Err(source) => Err(format!(
            "path {} is not accessible: {source}",
            files.host_path(&file).display()
        )),
    }
}

fn ensure_local_tool_not_cancelled(
    cancellation_token: &AgentCancellationToken,
    tool_name: &str,
) -> Result<(), String> {
    if cancellation_token.is_cancelled() {
        Err(format!("{tool_name} cancelled"))
    } else {
        Ok(())
    }
}

fn local_tool_cancelled_result(tool_name: &str, detail: impl Into<String>) -> AgentToolResult {
    let detail = detail.into();
    let payload = serde_json::json!({
        "error": "tool_cancelled",
        "tool": tool_name,
        "detail": detail,
    });
    AgentToolResult::cancelled(format!("{tool_name} cancelled"), payload.clone()).with_evidence(
        AgentToolEvidence::availability_only(false, "turn cancellation requested")
            .with_output_value(&payload, false),
    )
}

fn collect_workspace_search_matches(
    config: &LocalWorkspaceAgentConfig,
    files: &WorkspaceFiles,
    file: &WorkspaceFile,
    is_directory: bool,
    query: &str,
    case_sensitive: bool,
    max_matches: usize,
    matches: &mut Vec<serde_json::Value>,
    cancellation_token: &AgentCancellationToken,
) -> Result<bool, String> {
    ensure_local_tool_not_cancelled(cancellation_token, "search")?;
    if matches.len() >= max_matches {
        return Ok(true);
    }
    if is_directory {
        let entries = files
            .list(file)
            .map_err(|source| format!("failed to list {}: {source}", files.display(file)))?;
        for (child, entry) in entries {
            ensure_local_tool_not_cancelled(cancellation_token, "search")?;
            // Links found while recursing are not followed: the search walks
            // the tree as it is laid out inside the grant.
            let child_is_directory = match entry.kind {
                VfsObjectKind::Directory => true,
                VfsObjectKind::File => false,
                _ => continue,
            };
            let truncated = collect_workspace_search_matches(
                config,
                files,
                &child,
                child_is_directory,
                query,
                case_sensitive,
                max_matches,
                matches,
                cancellation_token,
            )?;
            if truncated {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    let Ok(read) = files.read_prefix(file, config.max_search_bytes_per_file.saturating_add(1))
    else {
        return Ok(false);
    };
    if read.truncated || read.bytes.len() as u64 > config.max_search_bytes_per_file {
        return Ok(false);
    }
    let Ok(content) = String::from_utf8(read.bytes) else {
        return Ok(false);
    };
    let query_cmp = if case_sensitive {
        query.to_string()
    } else {
        query.to_ascii_lowercase()
    };
    for (line_index, line) in content.lines().enumerate() {
        ensure_local_tool_not_cancelled(cancellation_token, "search")?;
        let haystack = if case_sensitive {
            line.to_string()
        } else {
            line.to_ascii_lowercase()
        };
        if haystack.contains(&query_cmp) {
            matches.push(serde_json::json!({
                "path": files.display(file),
                "line": line_index + 1,
                "text": line,
            }));
            if matches.len() >= max_matches {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct TruncatedText {
    text: String,
    truncated: bool,
}

fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> TruncatedText {
    let truncated = bytes.len() > max_bytes;
    TruncatedText {
        text: String::from_utf8_lossy(&bytes[..bytes.len().min(max_bytes)]).to_string(),
        truncated,
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn workspace_tools_share_implementations_with_realtime_except_ui_prompting() {
        for descriptor in default_local_workspace_tool_descriptors() {
            let expected_realtime = descriptor.spec.kind != AgentToolKind::RequestUserInput;
            assert_eq!(
                descriptor.supports_invocation_mode(AgentToolInvocationMode::Realtime),
                expected_realtime,
                "{}",
                descriptor.spec.name
            );
            assert!(
                descriptor.supports_invocation_mode(AgentToolInvocationMode::TurnBased),
                "{}",
                descriptor.spec.name
            );
        }
    }

    fn with_remote_management_for_test(registry: AgentToolRegistry) -> AgentToolRegistry {
        registry.with_local_permission_checker(
            |permission: sinomo_runtime::node::LocalNodePermission| {
                Ok(permission == sinomo_runtime::node::LocalNodePermission::RemoteManagement)
            },
        )
    }

    fn test_session(name: &str) -> InteractionSession {
        InteractionSession::new(
            SessionId::from_legacy_named(format!("session-{name}")).expect("session id"),
            NodeId::from_legacy_named(format!("node-{name}")).expect("node id"),
            crate::InteractionSurface::new(
                "surface.test",
                crate::InteractionSurfaceKind::CommandLine,
            ),
        )
    }

    fn provision_test_agent_sandbox(
        mut config: LocalWorkspaceAgentConfig,
        session: &InteractionSession,
    ) -> LocalWorkspaceAgentConfig {
        if config.sandbox_instance_id.is_none() {
            config =
                config.with_sandbox_instance_id(agent_sandbox_id_for_session(&session.session_id));
        }
        provision_default_agent_session_sandbox(
            &SandboxLifecycleClient::in_process_at_runtime_root(&config.sandbox_runtime_root),
            &config,
            Vec::new(),
        )
        .expect("provision test agent sandbox before tool execution");
        config
    }

    #[test]
    fn local_workspace_commands_default_to_simple_sandbox_without_fake_sifs_mount() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-default");
        let config = LocalWorkspaceAgentConfig::new(&root);

        assert_eq!(
            config.command_runtime,
            LocalWorkspaceCommandRuntime::SimpleSandbox
        );
        assert!(config.agent_home_sifs_path.is_none());
        assert!(config.sandbox_sifs_mount_root.is_none());
        assert_eq!(config.git_tool_mode, LocalWorkspaceGitToolMode::Auto);
    }

    #[test]
    fn subprocess_descriptors_require_local_permission_for_every_current_runtime() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-command-permission-descriptor");
        let default_runtime = LocalWorkspaceAgentConfig::new(&root);
        assert_eq!(
            run_command::descriptor_for_config(&default_runtime).required_local_permissions,
            vec!["remote-management"]
        );

        let local_process = LocalWorkspaceAgentConfig::new(&root).using_local_process_commands();
        assert_eq!(
            run_command::descriptor_for_config(&local_process).required_local_permissions,
            vec!["remote-management"]
        );

        let unenforced = LocalWorkspaceAgentConfig::new(&root)
            .using_simple_sandbox_commands()
            .allowing_unenforced_simple_sandbox(true);
        assert_eq!(
            run_command::descriptor_for_config(&unenforced).required_local_permissions,
            vec!["remote-management"]
        );

        let enforced_explicit = LocalWorkspaceAgentConfig::new(&root)
            .using_simple_sandbox_commands()
            .allowing_unenforced_simple_sandbox(false);
        assert_eq!(
            run_command::descriptor_for_config(&enforced_explicit).required_local_permissions,
            vec!["remote-management"]
        );

        assert_eq!(
            changed_files::descriptor().required_local_permissions,
            vec!["remote-management"]
        );
        assert_eq!(
            git_diff::descriptor().required_local_permissions,
            vec!["remote-management"]
        );
    }

    #[test]
    fn ordered_additional_workspace_roots_expand_read_access_but_not_relative_resolution() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-ordered-workspaces");
        let primary = root.join("primary");
        let additional_a = root.join("additional-a");
        let additional_b = root.join("additional-b");
        let outside = root.join("outside");
        for directory in [&primary, &additional_a, &additional_b, &outside] {
            fs::create_dir_all(directory).expect("workspace directory");
        }
        fs::write(primary.join("shared.txt"), "primary content\n").expect("primary file");
        fs::write(additional_a.join("shared.txt"), "additional alpha\n").expect("additional file");
        fs::write(
            additional_b.join("search.txt"),
            "ordered workspace needle\n",
        )
        .expect("search file");
        fs::write(outside.join("secret.txt"), "outside grant\n").expect("outside file");

        let primary = fs::canonicalize(primary).expect("canonical primary");
        let additional_a = fs::canonicalize(additional_a).expect("canonical additional a");
        let additional_b = fs::canonicalize(additional_b).expect("canonical additional b");
        let outside = fs::canonicalize(outside).expect("canonical outside");
        let config = LocalWorkspaceAgentConfig::new(&primary)
            .with_additional_workspace_roots([additional_a.clone(), additional_b.clone()])
            .allowing_file_writes(true);
        assert_eq!(
            config.additional_workspace_roots,
            vec![additional_a.clone(), additional_b.clone()]
        );
        let registry = AgentToolRegistry::local_workspace_with_config(config);
        let session = test_session("ordered-workspace-reads");
        let execute = |call_index: usize, tool: &str, input: serde_json::Value| {
            registry.execute(
                &session,
                0,
                call_index,
                &AgentToolCall {
                    call_id: None,
                    tool: tool.to_string(),
                    input,
                    r#async: false,
                    summary: format!("test {tool}"),
                },
            )
        };

        let relative_read = execute(0, "read_file", serde_json::json!({ "path": "shared.txt" }));
        assert_eq!(relative_read.status, AgentToolStatus::Succeeded);
        assert_eq!(relative_read.output["content"], "primary content\n");

        let additional_read = execute(
            1,
            "read_file",
            serde_json::json!({ "path": additional_a.join("shared.txt") }),
        );
        assert_eq!(additional_read.status, AgentToolStatus::Succeeded);
        assert_eq!(additional_read.output["content"], "additional alpha\n");

        let relative_escape = execute(
            2,
            "read_file",
            serde_json::json!({ "path": "../additional-a/shared.txt" }),
        );
        assert_eq!(relative_escape.status, AgentToolStatus::Blocked);
        assert_eq!(relative_escape.output["error"], "path_not_allowed");

        let additional_list = execute(3, "list_files", serde_json::json!({ "path": additional_a }));
        assert_eq!(additional_list.status, AgentToolStatus::Succeeded);
        assert!(
            additional_list.output["entries"]
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| {
                    entry["path"]
                        == serde_json::Value::String(
                            additional_a.join("shared.txt").display().to_string(),
                        )
                }))
        );

        let additional_search = execute(
            4,
            "search",
            serde_json::json!({
                "path": additional_b,
                "query": "needle",
            }),
        );
        assert_eq!(additional_search.status, AgentToolStatus::Succeeded);
        assert_eq!(
            additional_search.output["matches"].as_array().map(Vec::len),
            Some(1)
        );

        let outside_read = execute(
            5,
            "read_file",
            serde_json::json!({ "path": outside.join("secret.txt") }),
        );
        assert_eq!(outside_read.status, AgentToolStatus::Blocked);
        assert_eq!(outside_read.output["error"], "path_not_allowed");

        let additional_write = execute(
            6,
            "write_file",
            serde_json::json!({
                "path": additional_a.join("structured-write.txt"),
                "content": "must remain primary-only",
            }),
        );
        assert_eq!(additional_write.status, AgentToolStatus::Failed);
        assert_eq!(additional_write.output["error"], "path_not_allowed");
        assert!(!additional_a.join("structured-write.txt").exists());
    }

    /// A directory link planted inside the workspace (by a sandboxed command,
    /// say) must not let file tools read or write outside the grant.
    fn plant_directory_link(link: &Path, target: &Path) {
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, link).expect("plant symlink");
        #[cfg(windows)]
        {
            let created = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(link)
                .arg(target)
                .output()
                .expect("run mklink");
            assert!(created.status.success(), "mklink /J failed: {created:?}");
        }
    }

    #[test]
    fn planted_links_cannot_carry_file_tools_outside_the_workspace() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-planted-link");
        let workspace = root.join("workspace");
        let outside = root.join("outside");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(&outside).expect("outside");
        fs::write(outside.join("secret.txt"), "outside needle\n").expect("secret");
        plant_directory_link(&workspace.join("escape"), &outside);
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&workspace).allowing_file_writes(true),
        );
        let session = test_session("planted-link");
        let execute = |call_index: usize, tool: &str, input: serde_json::Value| {
            registry.execute(
                &session,
                0,
                call_index,
                &AgentToolCall {
                    call_id: None,
                    tool: tool.to_string(),
                    input,
                    r#async: false,
                    summary: format!("test {tool}"),
                },
            )
        };

        let read = execute(
            0,
            "read_file",
            serde_json::json!({ "path": "escape/secret.txt" }),
        );
        assert_ne!(read.status, AgentToolStatus::Succeeded, "{read:?}");
        let list = execute(1, "list_files", serde_json::json!({ "path": "escape" }));
        assert_ne!(list.status, AgentToolStatus::Succeeded, "{list:?}");
        let search = execute(2, "search", serde_json::json!({ "query": "needle" }));
        assert_eq!(search.status, AgentToolStatus::Succeeded);
        assert_eq!(search.output["matches"].as_array().map(Vec::len), Some(0));

        let write = execute(
            3,
            "write_file",
            serde_json::json!({
                "path": "escape/planted.txt",
                "content": "pwned",
                "create_parents": true,
            }),
        );
        assert_ne!(write.status, AgentToolStatus::Succeeded, "{write:?}");
        let patch = execute(
            4,
            "apply_patch",
            serde_json::json!({
                "patch": "*** Begin Patch\n*** Add File: escape/patched.txt\n+pwned\n*** End Patch\n",
            }),
        );
        assert_ne!(patch.status, AgentToolStatus::Succeeded, "{patch:?}");

        assert!(!outside.join("planted.txt").exists());
        assert!(!outside.join("patched.txt").exists());
        assert_eq!(
            fs::read_to_string(outside.join("secret.txt")).expect("secret intact"),
            "outside needle\n"
        );
    }

    /// The platform shell's form of a test command: `sh` on Unix, `cmd` on
    /// Windows.
    fn shell(unix: &str, windows: &str) -> String {
        if cfg!(windows) {
            windows.to_string()
        } else {
            unix.to_string()
        }
    }

    /// Command output or file content without the line ending `cmd` adds.
    fn text(value: &str) -> &str {
        value.trim_end_matches(['\r', '\n'])
    }

    /// A host path as a shell prints it (without Windows' verbatim prefix).
    fn shell_path(path: &Path) -> String {
        let display = path.display().to_string();
        display
            .strip_prefix(r"\\?\")
            .map(str::to_string)
            .unwrap_or(display)
    }

    #[test]
    fn simple_sandbox_commands_default_to_primary_and_accept_absolute_additional_cwd() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-ordered-workspace-command");
        let primary = root.join("primary");
        let additional = root.join("additional");
        let outside = root.join("outside");
        let runtime_root = root.join("sandbox-runtime");
        for directory in [&primary, &additional, &outside] {
            fs::create_dir_all(directory).expect("workspace directory");
        }
        fs::write(additional.join("granted.txt"), "granted").expect("additional input");
        let primary = fs::canonicalize(primary).expect("canonical primary");
        let additional = fs::canonicalize(additional).expect("canonical additional");
        let outside = fs::canonicalize(outside).expect("canonical outside");
        let session = test_session("ordered-workspace-command");
        let config = LocalWorkspaceAgentConfig::new(&primary)
            .with_additional_workspace_root(&additional)
            .with_sandbox_runtime_root(runtime_root)
            .allowing_shell_commands(true);
        let config = provision_test_agent_sandbox(config, &session);

        let read_paths = sandbox_extra_read_paths(&config, &primary, &primary, &[], None);
        let write_paths = sandbox_extra_write_paths(&config, &primary, None);
        assert!(read_paths.iter().any(|path| path == &additional));
        assert!(write_paths.iter().any(|path| path == &additional));

        let registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));
        let execute = |call_index: usize, input: serde_json::Value| {
            registry.execute(
                &session,
                0,
                call_index,
                &AgentToolCall {
                    call_id: None,
                    tool: "run_command".to_string(),
                    input,
                    r#async: false,
                    summary: "test ordered workspace command".to_string(),
                },
            )
        };

        let print_cwd = shell("pwd", "cd");
        let primary_cwd = execute(0, serde_json::json!({ "command": print_cwd }));
        assert_eq!(
            primary_cwd.status,
            AgentToolStatus::Succeeded,
            "{primary_cwd:#?}"
        );
        let primary_display = shell_path(&primary);
        assert_eq!(
            primary_cwd.output["stdout"].as_str().map(str::trim),
            Some(primary_display.as_str())
        );

        let additional_cwd = execute(
            1,
            serde_json::json!({
                "command": shell(
                    "cat granted.txt; printf updated > command-write.txt",
                    "type granted.txt && echo updated> command-write.txt",
                ),
                "cwd": additional,
            }),
        );
        assert_eq!(
            additional_cwd.status,
            AgentToolStatus::Succeeded,
            "{additional_cwd:#?}"
        );
        assert_eq!(additional_cwd.output["stdout"], "granted");
        assert_eq!(
            text(&fs::read_to_string(additional.join("command-write.txt")).expect("command write")),
            "updated"
        );

        let outside_cwd = execute(
            2,
            serde_json::json!({
                "command": print_cwd,
                "cwd": outside,
            }),
        );
        assert_eq!(outside_cwd.status, AgentToolStatus::Blocked);
        assert_eq!(outside_cwd.output["error"], "workspace_root_not_allowed");
    }

    #[test]
    fn tool_call_never_creates_a_missing_sandbox() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-tool-no-sandbox-create");
        let workspace = root.join("workspace");
        let runtime_root = root.join("sandbox-runtime");
        fs::create_dir_all(&workspace).expect("workspace");
        let session = test_session("missing-sandbox");
        let sandbox_id = agent_sandbox_id_for_session(&session.session_id);
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_instance_id(sandbox_id.clone())
            .allowing_shell_commands(true);
        let registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));

        let outcome = registry.execute(
            &session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": "true" }),
                r#async: false,
                summary: "missing sandbox must block".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Blocked);
        assert_eq!(outcome.output["error"], "sandbox_not_attached");
        assert!(matches!(
            LocalSandboxInstanceStore::new(runtime_root).load(&sandbox_id),
            Err(SandboxStoreError::NotFound { .. })
        ));
    }

    #[test]
    fn orchestration_reconciles_mounts_on_an_existing_agent_sandbox() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-mount-reconcile");
        let workspace = root.join("workspace");
        let runtime_root = root.join("sandbox-runtime");
        fs::create_dir_all(&workspace).expect("workspace");
        let session = test_session("sandbox-mount-reconcile");
        let sandbox_id = agent_sandbox_id_for_session(&session.session_id);
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_instance_id(sandbox_id.clone())
            .allowing_shell_commands(true);
        let primary = SandboxMount {
            source_sifs_path: "/workspaces/primary".to_string(),
            mount_path: SandboxPath::new("/work").expect("primary mount path"),
            mode: SandboxMountMode::ReadWrite,
        };
        provision_default_agent_session_sandbox(
            &SandboxLifecycleClient::in_process_at_runtime_root(&config.sandbox_runtime_root),
            &config,
            Vec::new(),
        )
        .expect("create default sandbox");
        let registry = with_remote_management_for_test(
            AgentToolRegistry::local_workspace_with_config(config.clone()),
        );
        let command = registry.execute(
            &session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": "true" }),
                r#async: false,
                summary: "persist simple-native host cwd".to_string(),
            },
        );
        assert_eq!(command.status, AgentToolStatus::Succeeded, "{command:#?}");
        let initial_generation = LocalSandboxInstanceStore::new(&runtime_root)
            .load(&sandbox_id)
            .expect("load initial sandbox")
            .0
            .generation;

        let secondary = SandboxMount {
            source_sifs_path: "/workspaces/secondary".to_string(),
            mount_path: SandboxPath::new("/workspaces/2").expect("secondary mount path"),
            mode: SandboxMountMode::ReadOnly,
        };
        provision_default_agent_session_sandbox(
            &SandboxLifecycleClient::in_process_at_runtime_root(&config.sandbox_runtime_root),
            &config,
            vec![primary.clone(), secondary.clone()],
        )
        .expect("reconcile existing sandbox mounts");

        let (manifest, _) = LocalSandboxInstanceStore::new(&runtime_root)
            .load(&sandbox_id)
            .expect("load reconciled sandbox");
        assert_eq!(manifest.mounts, vec![primary, secondary]);
        assert_eq!(manifest.generation, initial_generation + 1);

        provision_default_agent_session_sandbox(
            &SandboxLifecycleClient::in_process_at_runtime_root(&config.sandbox_runtime_root),
            &config,
            Vec::new(),
        )
        .expect("revoke every workspace mount");
        let (revoked, _) = LocalSandboxInstanceStore::new(runtime_root)
            .load(&sandbox_id)
            .expect("load sandbox after mount revocation");
        assert!(revoked.mounts.is_empty());
        assert_eq!(revoked.generation, manifest.generation + 1);
    }

    #[test]
    fn large_multibyte_write_truncates_its_diff_on_a_char_boundary() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-multibyte-diff");
        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_file_writes(true),
        );
        // Three-byte characters guarantee the 128 KiB diff cap lands mid-character.
        let content = "\u{6587}".repeat(64 * 1024);
        let result = registry.execute(
            &test_session("multibyte-diff"),
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "write_file".into(),
                input: serde_json::json!({"path": "large.txt", "content": content}),
                r#async: false,
                summary: "write large multibyte file".into(),
            },
        );
        assert_eq!(result.status, AgentToolStatus::Succeeded, "{result:?}");
        assert_eq!(
            fs::read_to_string(root.join("large.txt")).expect("written file"),
            content
        );
    }

    #[test]
    fn generic_non_git_workspace_omits_git_helpers_but_keeps_shell_tool() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-generic-non-git-workspace");
        fs::write(root.join("notes.txt"), "an arbitrary workspace\n").expect("workspace file");

        let registry = AgentToolRegistry::local_workspace_with_config(
            LocalWorkspaceAgentConfig::new(&root).allowing_shell_commands(true),
        );

        assert!(registry.contains("run_command"));
        assert!(!registry.contains("changed_files"));
        assert!(!registry.contains("git_diff"));
    }

    #[cfg(unix)]
    #[test]
    fn git_helpers_auto_detect_and_honor_explicit_modes() {

        let root = sinomo_testdir::TestDir::new("sinomo-agent-optional-git-tools");
        fs::create_dir(root.join(".git")).expect("git metadata");
        let fake_git = root.join("fake-git");
        fs::write(&fake_git, "#!/bin/sh\nexit 0\n").expect("fake git");
        sinomo_testdir::install_shell_fixture(&fake_git).expect("install shell fixture");
        let auto_config =
            LocalWorkspaceAgentConfig::new(&root).with_git_program(fake_git.display().to_string());

        // Git itself rejects an empty `.git` directory, so helpers must not appear.
        let stray = AgentToolRegistry::local_workspace_with_config(auto_config.clone());
        assert!(!stray.contains("changed_files"));
        fs::write(root.join(".git/HEAD"), "ref: refs/heads/main\n").expect("git HEAD");

        let automatic = AgentToolRegistry::local_workspace_with_config(auto_config.clone());
        assert!(automatic.contains("changed_files"));
        assert!(automatic.contains("git_diff"));

        let disabled = AgentToolRegistry::local_workspace_with_config(
            auto_config.clone().disabling_git_tools(),
        );
        assert!(!disabled.contains("changed_files"));
        assert!(!disabled.contains("git_diff"));

        fs::remove_dir_all(root.join(".git")).expect("remove git metadata");
        fs::write(root.join(".git"), "gitdir: ../main/.git/worktrees/w\n").expect("worktree link");
        let worktree = AgentToolRegistry::local_workspace_with_config(auto_config.clone());
        assert!(worktree.contains("changed_files"));

        fs::remove_file(root.join(".git")).expect("remove worktree link");
        let forced =
            AgentToolRegistry::local_workspace_with_config(auto_config.enabling_git_tools());
        assert!(forced.contains("changed_files"));
        assert!(forced.contains("git_diff"));
    }

    #[test]
    fn simple_sandbox_manifest_uses_live_agent_home_without_redundant_projection() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-home");
        let runtime_root = root.join("sandbox-runtime");
        let workspace = root.join("workspace");
        let sifs_mount_root = root.join("sifs");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(sifs_mount_root.join("sinomo/agents/test-agent/home"))
            .expect("mounted SIFS home");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_sifs_mount_root(&sifs_mount_root)
            .with_agent_home_sifs_path("/sinomo/agents/test-agent/home");
        let canonical_workspace = fs::canonicalize(&workspace).expect("canonical workspace");
        let sandbox_id = agent_sandbox_id(&config, &canonical_workspace);
        let node_id = NodeId::from_legacy_named("node-home").expect("node");
        let toolchain_id =
            SandboxToolchainId::from_legacy_named(AGENT_SANDBOX_TOOLCHAIN_ID).expect("toolchain");
        let store = LocalSandboxInstanceStore::new(&runtime_root);
        store
            .create(
                sandbox_id.clone(),
                toolchain_id.clone(),
                time::OffsetDateTime::now_utc(),
            )
            .expect("provision sandbox before tool configuration");
        let sandbox_cwd =
            sandbox_path_for_host_path(&canonical_workspace).expect("sandbox cwd path");

        let (_manifest, layout) = prepare_agent_sandbox_manifest(
            &config,
            &store,
            &sandbox_id,
            &toolchain_id,
            sandbox_cwd,
            &canonical_workspace,
            &canonical_workspace,
            &[],
            &node_id,
            None,
        )
        .expect("manifest");
        let (manifest, _layout) = store.load(&sandbox_id).expect("load manifest");

        assert!(manifest.mounts.is_empty());
        assert!(layout.home.exists());
        let home_host_path = agent_home_live_host_path(&config).expect("live agent home host path");
        assert!(home_host_path.exists());
        assert_eq!(
            manifest.env.get("HOME").map(String::as_str),
            Some(home_host_path.to_str().expect("utf8 live host path"))
        );
        assert_eq!(
            manifest.env.get("USERPROFILE").map(String::as_str),
            Some(home_host_path.to_str().expect("utf8 live host path"))
        );
        assert_eq!(
            manifest
                .env
                .get("SINOMO_AGENT_HOME_HOST_PATH")
                .map(String::as_str),
            Some(home_host_path.to_str().expect("utf8 live host path"))
        );
        assert_eq!(
            fs::read_to_string(layout.metadata.join("agent-home-sifs-path.txt"))
                .expect("home metadata"),
            "/sinomo/agents/test-agent/home"
        );
        assert_eq!(
            fs::read_to_string(layout.metadata.join("agent-home-live-host-path.txt"))
                .expect("live agent home host metadata"),
            home_host_path.display().to_string()
        );
    }

    #[test]
    fn agent_home_path_is_the_same_live_sifs_mount_across_workspaces() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-home-roaming");
        let runtime_root = root.join("sandbox-runtime");
        let workspace_a = root.join("workspace-a");
        let workspace_b = root.join("workspace-b");
        let sifs_mount_root = root.join("sifs");
        fs::create_dir_all(&workspace_a).expect("workspace a");
        fs::create_dir_all(&workspace_b).expect("workspace b");
        let config_a = LocalWorkspaceAgentConfig::new(&workspace_a)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_sifs_mount_root(&sifs_mount_root)
            .with_agent_home_sifs_path("/sinomo/agents/test-agent/home");
        let config_b = LocalWorkspaceAgentConfig::new(&workspace_b)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_sifs_mount_root(&sifs_mount_root)
            .with_agent_home_sifs_path("/sinomo/agents/test-agent/home");

        assert_eq!(
            agent_home_live_host_path(&config_a),
            agent_home_live_host_path(&config_b)
        );
        assert_eq!(
            agent_home_live_host_path(&config_a),
            Some(sifs_mount_root.join("sinomo/agents/test-agent/home"))
        );
    }

    #[test]
    fn agent_home_is_keyed_by_agent_identity_across_sessions() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-home-data-dir");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let sifs_mount_root = root.join("sifs");
        let session_a = SessionId::from_legacy_named("sess-agent-home-a").expect("session a");
        let session_b = SessionId::from_legacy_named("sess-agent-home-b").expect("session b");
        let agent_id = "coding-agent";
        let config_a = LocalWorkspaceAgentConfig::new(&workspace)
            .with_data_dir_evidence(&data_dir, &session_a)
            .with_sandbox_sifs_mount_root(&sifs_mount_root)
            .with_agent_home_for_agent_id(agent_id);
        let config_b = LocalWorkspaceAgentConfig::new(&workspace)
            .with_data_dir_evidence(&data_dir, &session_b)
            .with_sandbox_sifs_mount_root(&sifs_mount_root)
            .with_agent_home_for_agent_id(agent_id);

        assert_eq!(
            config_a.agent_home_sifs_path.as_deref(),
            Some(agent_home_sifs_path_for_id(agent_id).as_str())
        );
        assert_eq!(
            agent_home_live_host_path(&config_a),
            Some(
                sifs_mount_root
                    .join("sinomo/agents")
                    .join(agent_id)
                    .join("home")
            )
        );
        assert_eq!(
            agent_home_live_host_path(&config_a),
            agent_home_live_host_path(&config_b),
            "sessions of the same agent must share its home"
        );
        assert_ne!(
            agent_home_sifs_path_for_id(agent_id),
            agent_home_sifs_path_for_id("main-agent")
        );
    }

    #[test]
    fn agent_home_path_keeps_transformed_agent_identities_distinct() {
        assert_eq!(
            agent_home_sifs_path_for_id("foo-bar"),
            "/sinomo/agents/foo-bar/home"
        );
        assert_ne!(
            agent_home_sifs_path_for_id("foo:bar"),
            agent_home_sifs_path_for_id("foo-bar")
        );
        assert_eq!(
            agent_home_sifs_path_for_id("foo:bar"),
            agent_home_sifs_path_for_id("foo:bar")
        );
        let encoded_component = sanitize_agent_home_id("foo:bar");
        assert_ne!(
            agent_home_sifs_path_for_id(&encoded_component),
            agent_home_sifs_path_for_id("foo:bar"),
            "the reserved encoded namespace must not collide with a literal safe id"
        );
        assert_ne!(
            agent_home_sifs_path_for_id(" foo-bar"),
            agent_home_sifs_path_for_id("foo-bar"),
            "invalid callers must not collapse distinct identities by trimming"
        );
        let long_id = format!("agent:{}", "x".repeat(2_048));
        let long_component = sanitize_agent_home_id(&long_id);
        assert!(long_component.starts_with(HASHED_AGENT_HOME_ID_PREFIX));
        assert!(long_component.len() <= MAX_AGENT_HOME_ID_COMPONENT_BYTES);
        assert_ne!(
            agent_home_sifs_path_for_id(&long_id),
            agent_home_sifs_path_for_id(&format!("agent:{}", "y".repeat(2_048)))
        );
        assert_ne!(
            agent_home_sifs_path_for_id(&long_component),
            agent_home_sifs_path_for_id(&long_id),
            "the reserved hashed namespace must not collide with a literal safe id"
        );
    }

    #[test]
    fn data_dir_evidence_uses_distinct_sandbox_instance_per_session() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-session-id");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let session_a = SessionId::from_legacy_named("session-a").expect("session a");
        let session_b = SessionId::from_legacy_named("session-b").expect("session b");
        let config_a = LocalWorkspaceAgentConfig::new(&workspace)
            .with_data_dir_evidence(&data_dir, &session_a);
        let config_b = LocalWorkspaceAgentConfig::new(&workspace)
            .with_data_dir_evidence(&data_dir, &session_b);
        let canonical_workspace = fs::canonicalize(&workspace).expect("canonical workspace");

        assert_ne!(
            agent_sandbox_id(&config_a, &canonical_workspace),
            agent_sandbox_id(&config_b, &canonical_workspace)
        );
        assert!(config_a.agent_home_sifs_path.is_none());
        assert!(config_b.agent_home_sifs_path.is_none());
    }

    #[test]
    fn data_dir_evidence_keeps_simple_shell_usable_without_live_sifs_mount() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-evidence-no-sifs-mount");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let session = test_session("evidence-no-sifs-mount");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_data_dir_evidence(&data_dir, &session.session_id)
            .allowing_shell_commands(true);

        assert!(config.agent_home_sifs_path.is_none());
        assert!(config.sandbox_sifs_mount_root.is_none());
        let config = provision_test_agent_sandbox(config, &session);
        let registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));
        let outcome = registry.execute(
            &session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({
                    "command": shell("printf usable", "echo usable"),
                    "timeout_ms": 5_000,
                }),
                r#async: false,
                summary: "verify direct API without live SIFS".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded, "{outcome:#?}");
        assert_eq!(
            outcome
                .output
                .get("stdout")
                .and_then(serde_json::Value::as_str)
                .map(text),
            Some("usable")
        );
    }

    #[test]
    fn unavailable_live_agent_home_does_not_veto_generic_shell() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-unavailable-live-home");
        let workspace = root.join("workspace");
        let mount_root = root.join("mount");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(&mount_root).expect("mount root");
        fs::write(mount_root.join("sinomo"), b"blocks directory traversal").expect("blocking file");
        let session = test_session("unavailable-live-home");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(root.join("runtime"))
            .with_sandbox_sifs_mount_root(&mount_root)
            .with_agent_home_sifs_path("/sinomo/agents/test/home")
            .allowing_shell_commands(true);
        let config = provision_test_agent_sandbox(config, &session);
        let registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));

        let outcome = registry.execute(
            &session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({
                    "command": shell(
                        "printf usable-without-dfs-home",
                        "echo usable-without-dfs-home",
                    ),
                    "timeout_ms": 5_000,
                }),
                r#async: false,
                summary: "verify optional DFS home failure".to_string(),
            },
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded, "{outcome:#?}");
        assert_eq!(
            outcome.output["stdout"].as_str().map(text),
            Some("usable-without-dfs-home")
        );
    }

    #[test]
    fn lifecycle_cleanup_failure_is_visible_and_prevents_success() {
        let stderr = command_stderr_with_cleanup_failure(
            b"command stderr".to_vec(),
            Some("manifest save failed"),
        );
        assert_eq!(
            String::from_utf8(stderr.clone()).expect("utf8 stderr"),
            "command stderr\nsimple sandbox process lifecycle cleanup remains pending: manifest save failed"
        );

        let root = sinomo_testdir::TestDir::new("sinomo-agent-cleanup-pending-result");
        let config = LocalWorkspaceAgentConfig::new(&root);
        let result = local_command_result_from_output(
            &config,
            root.to_path_buf(),
            root.to_path_buf(),
            "true",
            &[],
            &LocalCommandOutput {
                captured: None,
                stdout: Vec::new(),
                stderr,
                status_code: Some(0),
                success: true,
            },
            false,
            false,
            true,
            1,
            None,
            "run_command",
            "cleanup test",
            Vec::new(),
            Some("true".to_string()),
        );
        assert_eq!(result.status, AgentToolStatus::Failed);
        assert_eq!(
            result
                .output
                .get("cleanup_pending")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            result
                .output
                .get("success")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn simple_sandbox_manifest_blocks_non_portable_node_move() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-node-binding");
        let runtime_root = root.join("sandbox-runtime");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let sandbox_id = SandboxId::from_legacy_named("sandbox-node-binding").expect("sandbox id");
        let node_a = NodeId::from_legacy_named("node-a").expect("node a");
        let node_b = NodeId::from_legacy_named("node-b").expect("node b");
        let config_a = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_instance_id(sandbox_id.clone())
            .with_sandbox_runtime_node_id(node_a.clone());
        let config_b = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .with_sandbox_instance_id(sandbox_id.clone())
            .with_sandbox_runtime_node_id(node_b.clone());
        let canonical_workspace = fs::canonicalize(&workspace).expect("canonical workspace");
        let toolchain_id =
            SandboxToolchainId::from_legacy_named(AGENT_SANDBOX_TOOLCHAIN_ID).expect("toolchain");
        let store = LocalSandboxInstanceStore::new(&runtime_root);
        store
            .create(
                sandbox_id.clone(),
                toolchain_id.clone(),
                time::OffsetDateTime::now_utc(),
            )
            .expect("provision sandbox before attachment configuration");
        let sandbox_cwd =
            sandbox_path_for_host_path(&canonical_workspace).expect("sandbox cwd path");

        prepare_agent_sandbox_manifest(
            &config_a,
            &store,
            &sandbox_id,
            &toolchain_id,
            sandbox_cwd.clone(),
            &canonical_workspace,
            &canonical_workspace,
            &[],
            &node_a,
            None,
        )
        .expect("first placement");
        let result = prepare_agent_sandbox_manifest(
            &config_b,
            &store,
            &sandbox_id,
            &toolchain_id,
            sandbox_cwd,
            &canonical_workspace,
            &canonical_workspace,
            &[],
            &node_b,
            None,
        );

        let error = result.expect_err("non-portable sandbox should reject node move");
        assert_eq!(
            error
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("sandbox_non_portable_node_mismatch")
        );
    }

    #[test]
    fn simple_sandbox_manifest_tracks_active_process_lifecycle() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-process-tracking");
        let runtime_root = root.join("sandbox-runtime");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let session = test_session("process-tracking");
        let config = provision_test_agent_sandbox(
            LocalWorkspaceAgentConfig::new(&workspace)
                .with_sandbox_runtime_root(&runtime_root)
                .without_agent_home_sifs_path()
                .allowing_shell_commands(true),
            &session,
        );
        let sandbox_id = config.sandbox_instance_id.clone().unwrap();
        let store = LocalSandboxInstanceStore::new(&runtime_root);
        let registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));
        let run = |input| {
            registry.execute(
                &session,
                0,
                0,
                &AgentToolCall {
                    call_id: None,
                    tool: "run_command".into(),
                    input,
                    r#async: false,
                    summary: "track sandbox process".into(),
                },
            )
        };
        let command = if cfg!(windows) { "cmd.exe /Q" } else { "sh" };
        let start = run(serde_json::json!({"action":"start","command":command,"timeout_ms":5000}));
        assert_eq!(start.status, AgentToolStatus::Succeeded, "{start:?}");
        let (manifest, _layout) = store.load(&sandbox_id).expect("load running manifest");
        assert_eq!(
            manifest.lifecycle.status,
            SandboxInstanceLifecycleStatus::Running
        );
        assert_eq!(manifest.active_processes.len(), 1);

        let cancelled =
            run(serde_json::json!({"action":"cancel","execution_id":start.output["execution_id"]}));
        assert_eq!(
            cancelled.status,
            AgentToolStatus::Succeeded,
            "{cancelled:?}"
        );
        let (manifest, _layout) = store.load(&sandbox_id).expect("load stopped manifest");
        assert_eq!(
            manifest.lifecycle.status,
            SandboxInstanceLifecycleStatus::Stopped
        );
        assert!(manifest.active_processes.is_empty());
        assert!(manifest.lifecycle.stopped_at.is_some());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn run_command_reuses_live_shell_and_cancels_its_descendants_after_exit() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-native-interaction");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(root.join("runtime"))
            .without_agent_home_sifs_path()
            .allowing_shell_commands(true);
        let session = test_session("interactive");
        let other = test_session("other");
        let attached = provision_test_agent_sandbox(config.clone(), &session);
        let sandbox_id = attached.sandbox_instance_id.clone().unwrap();
        let store = LocalSandboxInstanceStore::new(&attached.sandbox_runtime_root);
        let registry = with_remote_management_for_test(
            AgentToolRegistry::local_workspace_with_config(attached.clone()),
        );
        let other_registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(
                provision_test_agent_sandbox(config, &other),
            ));
        let invoke = |registry: &AgentToolRegistry, session: &InteractionSession, input| {
            registry.execute(
                session,
                0,
                0,
                &AgentToolCall {
                    call_id: None,
                    tool: "run_command".into(),
                    input,
                    r#async: false,
                    summary: "sandbox interaction".into(),
                },
            )
        };
        let run = |input| invoke(&registry, &session, input);
        let start = run(
            serde_json::json!({"action":"start","command":"read -r name; printf 'hello %s' \"$name\"; read -r value; printf '%s' \"$value\" >&2; setsid sh -c 'echo $$ > child.pid; exec sleep 30' & exit 7","timeout_ms":15000}),
        );
        assert_eq!(start.status, AgentToolStatus::Succeeded, "{start:?}");
        let lifecycle = SandboxLifecycleClient::in_process_at_runtime_root(store.runtime_root());
        assert_eq!(
            provision_default_agent_session_sandbox(&lifecycle, &attached, Vec::new()).unwrap(),
            sandbox_id
        );
        assert!(matches!(
            store.destroy(&sandbox_id),
            Err(SandboxStoreError::LocalExecutionBusy { .. })
        ));
        let id = start.output["execution_id"].as_str().unwrap();
        let denied = invoke(
            &other_registry,
            &other,
            serde_json::json!({"action":"cancel","execution_id":id}),
        );
        assert_eq!(denied.status, AgentToolStatus::Failed);
        let poll = |stdout, stderr| {
            run(
                serde_json::json!({"action":"read","execution_id":id,"stdout_offset":stdout,"stderr_offset":stderr}),
            )
        };
        let wait = |check: &dyn Fn() -> bool| {
            let start = Instant::now();
            while !check() {
                assert!(start.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        let write = |s, close| {
            run(
                serde_json::json!({"action":"write","execution_id":id,"input":s,"close_stdin":close}),
            )
        };
        assert_eq!(write("agent\n", false).status, AgentToolStatus::Succeeded);
        wait(&|| poll(0, 0).output["stdout"] == "hello agent");
        // A second foreground command can finish while the first awaits stdin.
        assert_eq!(
            run(serde_json::json!({"command":"printf shared > peer.txt"})).status,
            AgentToolStatus::Succeeded
        );
        assert_eq!(store.load(&sandbox_id).unwrap().0.active_processes.len(), 1);
        assert_eq!(write("again\n", true).status, AgentToolStatus::Succeeded);
        wait(&|| poll(11, 0).output["exit_status"] == 7 && workspace.join("child.pid").is_file());
        let view = poll(11, 0);
        assert_eq!(view.output["active"], true);
        assert_eq!(view.output["stdout"], "");
        assert_eq!(view.output["stderr"], "again");
        let pid = fs::read_to_string(workspace.join("child.pid")).unwrap();
        let child = PathBuf::from(format!("/proc/{}", pid.trim()));
        assert!(child.is_dir());
        let cancel = run(serde_json::json!({"action":"cancel","execution_id":id}));
        assert_eq!(cancel.status, AgentToolStatus::Succeeded, "{cancel:?}");
        assert_eq!(cancel.output["active"], false);
        assert!(!child.exists());
        let archived = poll(6, 2);
        assert_eq!(archived.output["active"], false, "{archived:?}");
        assert_eq!(archived.output["stdout"], "agent");
        assert_eq!(archived.output["stderr"], "ain");
        let manifest = store.load(&sandbox_id).unwrap().0;
        assert!(manifest.active_processes.is_empty());
        assert_eq!(manifest.command_history.len(), 2);
    }

    #[test]
    fn simple_sandbox_shell_writes_are_independent_of_structured_file_permissions() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-generic-host-shell");
        let workspace = root.join("workspace");
        let runtime_root = root.join("sandbox-runtime");
        fs::create_dir_all(&workspace).expect("workspace");
        let workspace_target = workspace.join("shell-write.txt");
        let host_target = root.join("outside-workspace.txt");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .without_agent_home_sifs_path()
            .allowing_shell_commands(true)
            .allowing_file_writes(false);
        let script = if cfg!(windows) {
            format!(
                "(echo workspace)> \"{}\" && (echo host)> \"{}\"",
                workspace_target.display(),
                host_target.display(),
            )
        } else {
            format!(
                "printf workspace >'{}'; printf host >'{}'",
                workspace_target.display(),
                host_target.display(),
            )
        };

        let session = test_session("generic-host-shell");
        let config = provision_test_agent_sandbox(config, &session);
        let registry =
            with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config));
        assert!(registry.contains("run_command"));
        let outcome = registry.execute(
            &session,
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": script, "timeout_ms": 5000 }),
                r#async: false,
                summary: "run generic host command".to_string(),
            },
        );

        assert_eq!(
            outcome.status,
            crate::AgentToolStatus::Succeeded,
            "{outcome:#?}"
        );
        assert_eq!(
            text(&fs::read_to_string(&workspace_target).expect("workspace write")),
            "workspace"
        );
        assert_eq!(
            text(&fs::read_to_string(&host_target).expect("host write")),
            "host"
        );
    }

    #[test]
    fn simple_sandbox_policy_admits_manifest_workspace_and_agent_home_mounts() {
        let node_id = NodeId::from_legacy_named("agent-home-policy-node").expect("node");
        let mut manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("agent-home-policy-sandbox").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("agent-home-policy-toolchain")
                .expect("toolchain"),
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .expect("manifest");
        manifest.mounts.push(SandboxMount {
            source_sifs_path: "/sinomo/agents/test-agent/home".to_string(),
            mount_path: SandboxPath::new("/home").expect("mount path"),
            mode: SandboxMountMode::ReadWrite,
        });
        manifest.mounts.push(SandboxMount {
            source_sifs_path: "/conversations/test-channel/workspaces/project".to_string(),
            mount_path: SandboxPath::new("/work").expect("workspace mount path"),
            mode: SandboxMountMode::ReadWrite,
        });
        let policy = agent_sandbox_policy(&manifest);

        assert!(policy.allow_read_write_sifs_mounts);
        assert!(
            policy
                .allowed_mount_prefixes
                .iter()
                .any(|prefix| prefix == "/sinomo/agents")
        );
        assert!(
            policy
                .allowed_mount_prefixes
                .iter()
                .any(|prefix| { prefix == "/conversations/test-channel/workspaces/project" })
        );
        policy
            .validate_manifest(&manifest, &node_id)
            .expect("manifest-declared workspace and agent home mounts admitted");
    }

    #[test]
    #[cfg(windows)]
    fn local_workspace_command_uses_simple_sandbox_on_windows() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-sandbox-command");
        let runtime_root = root.join("sandbox-runtime");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let session = test_session("sandbox-command");
        let config = LocalWorkspaceAgentConfig::new(&workspace)
            .with_sandbox_runtime_root(&runtime_root)
            .allowing_shell_commands(true);
        let config = provision_test_agent_sandbox(config, &session);
        let args = vec![
            "/C".to_string(),
            "echo %SINOMO_AGENT_TOOL_RUNTIME% && echo %USERPROFILE% && echo roaming>%USERPROFILE%\\roaming.txt && type %USERPROFILE%\\roaming.txt".to_string(),
        ];

        let execution = execute_local_command(
            &config,
            &session,
            "cmd.exe",
            &args,
            None,
            Some(5000),
            None,
            &AgentCancellationToken::new(),
            AgentToolProgressSink::default(),
            SandboxNativeExecutionOptions::default(),
        )
        .expect("sandbox command");

        assert!(
            execution.output.success,
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&execution.output.stdout),
            String::from_utf8_lossy(&execution.output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&execution.output.stdout)
                .to_ascii_lowercase()
                .contains("simple_sandbox")
        );
        assert!(
            String::from_utf8_lossy(&execution.output.stdout)
                .to_ascii_lowercase()
                .contains("roaming")
        );
    }
}
