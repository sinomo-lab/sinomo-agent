//! Trusted native-dylib implementation for remote machine management.
//!
//! The shared sinomo-remote-management crate owns the wire contract. This
//! crate owns only host execution and deliberately exposes no permission
//! mutation operation: node-local policy is enforced by the daemon before a
//! route reaches this module.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde::Serialize;
use sinomo_core::node::LocalNodePermission;
use sinomo_module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDeclarativeResource,
    NativeModuleDescriptor, NativeModuleErrorResponse, NativeModuleInvocationRequest,
    NativeModuleInvocationResponse, NativeModuleObjectRpcRoute, NativeModuleRequest,
    NativeModuleResponse, NativeModuleSurfaces, SINOMO_MODULE_STATUS_INVALID_ARGUMENT,
    SINOMO_MODULE_STATUS_OK, SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    SINOMO_MODULE_STATUS_UNSUPPORTED_ABI, SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV1, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};
use sinomo_remote_management::*;
use sysinfo::{Disks, Networks, ProcessesToUpdate, System};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const REMOTE_MANAGEMENT_LOCAL_PERMISSION: &str = "remote-management";

const COMPUTE_USE_SKILL_ID: &str = "skill.remote-management.compute-use";
const COMPUTE_USE_SKILL_PATH: &str = "skills/remote-management/SKILL.md";
const FILE_TRANSFER_SKILL_ID: &str = "skill.remote-management.file-transfer";
const FILE_TRANSFER_SKILL_PATH: &str = "skills/remote-management/file-transfer/SKILL.md";
const NODE_CONFIG_SKILL_ID: &str = "skill.remote-management.node-config";
const NODE_CONFIG_SKILL_PATH: &str = "skills/remote-management/node-config/SKILL.md";

const COMPUTE_USE_PROMPT_ID: &str = "prompt.remote-management.compute-use";
const COMPUTE_USE_PROMPT_PATH: &str = "prompts/remote-management-compute-use.md";
const FILE_TRANSFER_PROMPT_ID: &str = "prompt.remote-management.file-transfer";
const FILE_TRANSFER_PROMPT_PATH: &str = "prompts/remote-management-file-transfer.md";
const NODE_CONFIG_PROMPT_ID: &str = "prompt.remote-management.node-config";
const NODE_CONFIG_PROMPT_PATH: &str = "prompts/remote-management-node-config.md";

const COMPUTE_USE_SKILL_MARKDOWN: &str = include_str!("../skills/remote-management/SKILL.md");
const FILE_TRANSFER_SKILL_MARKDOWN: &str =
    include_str!("../skills/remote-management/file-transfer/SKILL.md");
const NODE_CONFIG_SKILL_MARKDOWN: &str =
    include_str!("../skills/remote-management/node-config/SKILL.md");
const COMPUTE_USE_PROMPT_MARKDOWN: &str =
    include_str!("../prompts/remote-management-compute-use.md");
const FILE_TRANSFER_PROMPT_MARKDOWN: &str =
    include_str!("../prompts/remote-management-file-transfer.md");
const NODE_CONFIG_PROMPT_MARKDOWN: &str =
    include_str!("../prompts/remote-management-node-config.md");

const CAPABILITY_IMPORTS: &[&str] = &[
    "process.spawn",
    "filesystem.read",
    "filesystem.write",
    "system.info.read",
];
const COMPUTE_USE_CAPABILITIES: &[&str] = &[
    REMOTE_MANAGEMENT_SHELL_RUN_OPERATION,
    REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION,
    REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION,
];
const FILE_TRANSFER_CAPABILITIES: &[&str] = &[
    REMOTE_MANAGEMENT_FS_READ_OPERATION,
    REMOTE_MANAGEMENT_FS_WRITE_OPERATION,
    REMOTE_MANAGEMENT_FS_LIST_OPERATION,
    REMOTE_MANAGEMENT_FS_STAT_OPERATION,
];

const MAX_OBSERVED_DISKS: usize = 256;
const MAX_OBSERVED_NETWORK_INTERFACES: usize = 256;
const MAX_NETWORK_ADDRESSES_PER_INTERFACE: usize = 64;
const MAX_PROCESS_COMMAND_ARGUMENTS: usize = 64;
const MAX_PROCESS_TEXT_BYTES: usize = 4096;
const SHELL_POLL_INTERVAL: Duration = Duration::from_millis(10);
const SHELL_CLEANUP_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy)]
struct StoredHostApi(SinomoModuleHostApiV1);

unsafe impl Send for StoredHostApi {}
unsafe impl Sync for StoredHostApi {}

static HOST_API: Mutex<Option<StoredHostApi>> = Mutex::new(None);

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write_buffer(out, encode_sobj(&remote_management_module_descriptor()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV1) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = Some(StoredHostApi(host));
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        invoke_unchecked(request_sobj, response_sobj)
    }));
    match result {
        Ok(status) => status,
        Err(_) => write_response(
            response_sobj,
            NativeModuleResponse::Error(native_error_response(
                "panic",
                "remote-management module panicked while handling invocation",
            )),
            SINOMO_MODULE_STATUS_RUNTIME_ERROR,
        ),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    match HOST_API.lock() {
        Ok(mut api) => {
            *api = None;
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}

pub fn remote_management_module_descriptor() -> NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: REMOTE_MANAGEMENT_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: REMOTE_MANAGEMENT_MODULE_OWNER.to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: REMOTE_MANAGEMENT_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: REMOTE_MANAGEMENT_OPERATIONS
            .iter()
            .map(|operation| operation.to_string())
            .collect(),
        capabilities: REMOTE_MANAGEMENT_OPERATIONS
            .iter()
            .map(|capability| capability.to_string())
            .collect(),
        surfaces: NativeModuleSurfaces {
            object_rpc_routes: vec![
                remote_management_route(
                    REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
                    REMOTE_MANAGEMENT_SHELL_RUN_OPERATION,
                ),
                remote_management_route(
                    REMOTE_MANAGEMENT_FS_READ_ROUTE,
                    REMOTE_MANAGEMENT_FS_READ_OPERATION,
                ),
                remote_management_route(
                    REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
                    REMOTE_MANAGEMENT_FS_WRITE_OPERATION,
                ),
                remote_management_route(
                    REMOTE_MANAGEMENT_FS_LIST_ROUTE,
                    REMOTE_MANAGEMENT_FS_LIST_OPERATION,
                ),
                remote_management_route(
                    REMOTE_MANAGEMENT_FS_STAT_ROUTE,
                    REMOTE_MANAGEMENT_FS_STAT_OPERATION,
                ),
                remote_management_route(
                    REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
                    REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION,
                ),
                remote_management_route(
                    REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
                    REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION,
                ),
            ],
            declarative_resources: declarative_resources(),
            storage_owners: Vec::new(),
            capability_imports: CAPABILITY_IMPORTS
                .iter()
                .map(|capability| capability.to_string())
                .collect(),
            capability_exports: REMOTE_MANAGEMENT_OPERATIONS
                .iter()
                .map(|capability| capability.to_string())
                .collect(),
            settings: Vec::new(),
            network_transports: Vec::new(),
        },
    }
}

fn remote_management_route(route: &str, operation: &str) -> NativeModuleObjectRpcRoute {
    debug_assert_eq!(
        LocalNodePermission::RemoteManagement.as_str(),
        REMOTE_MANAGEMENT_LOCAL_PERMISSION
    );
    NativeModuleObjectRpcRoute::new(route, REMOTE_MANAGEMENT_RPC_CATEGORY, operation, "admin")
        .require_local_permission(REMOTE_MANAGEMENT_LOCAL_PERMISSION)
}

fn declarative_resources() -> Vec<NativeModuleDeclarativeResource> {
    vec![
        declarative_resource(
            COMPUTE_USE_SKILL_ID,
            "skill",
            COMPUTE_USE_SKILL_PATH,
            "Bounded remote shell and system observation guidance.",
            &["compute-use", "remote-management"],
            COMPUTE_USE_CAPABILITIES,
            COMPUTE_USE_SKILL_MARKDOWN,
        ),
        declarative_resource(
            FILE_TRANSFER_SKILL_ID,
            "skill",
            FILE_TRANSFER_SKILL_PATH,
            "Retry-safe bounded remote file transfer guidance.",
            &["file-transfer", "remote-management"],
            FILE_TRANSFER_CAPABILITIES,
            FILE_TRANSFER_SKILL_MARKDOWN,
        ),
        declarative_resource(
            NODE_CONFIG_SKILL_ID,
            "skill",
            NODE_CONFIG_SKILL_PATH,
            "Typed node configuration guidance using the existing config service.",
            &["node-config", "remote-management"],
            &[],
            NODE_CONFIG_SKILL_MARKDOWN,
        ),
        declarative_resource(
            COMPUTE_USE_PROMPT_ID,
            "prompt",
            COMPUTE_USE_PROMPT_PATH,
            "Prompt fragment for bounded remote compute use.",
            &["compute-use", "remote-management"],
            COMPUTE_USE_CAPABILITIES,
            COMPUTE_USE_PROMPT_MARKDOWN,
        ),
        declarative_resource(
            FILE_TRANSFER_PROMPT_ID,
            "prompt",
            FILE_TRANSFER_PROMPT_PATH,
            "Prompt fragment for retry-safe remote file transfer.",
            &["file-transfer", "remote-management"],
            FILE_TRANSFER_CAPABILITIES,
            FILE_TRANSFER_PROMPT_MARKDOWN,
        ),
        declarative_resource(
            NODE_CONFIG_PROMPT_ID,
            "prompt",
            NODE_CONFIG_PROMPT_PATH,
            "Prompt fragment for typed node configuration through config routes.",
            &["node-config", "remote-management"],
            &[],
            NODE_CONFIG_PROMPT_MARKDOWN,
        ),
    ]
}

fn declarative_resource(
    id: &str,
    kind: &str,
    path: &str,
    description: &str,
    activation_tags: &[&str],
    required_capabilities: &[&str],
    embedded_text: &str,
) -> NativeModuleDeclarativeResource {
    NativeModuleDeclarativeResource {
        id: id.to_string(),
        kind: kind.to_string(),
        path: path.to_string(),
        description: description.to_string(),
        target_agents: Vec::new(),
        model_families: vec!["gpt".to_string()],
        activation_tags: activation_tags.iter().map(|tag| tag.to_string()).collect(),
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| capability.to_string())
            .collect(),
        embedded_text: Some(embedded_text.to_string()),
    }
}

fn invoke_unchecked(
    request_sobj: SinomoModuleByteSlice,
    response_sobj: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let response = match decode_native_module_request(request_sobj) {
        Ok(NativeModuleRequest::ProbeCapabilities(_)) => {
            NativeModuleResponse::Capabilities(NativeModuleCapabilitiesResponse {
                descriptor: remote_management_module_descriptor(),
            })
        }
        Ok(NativeModuleRequest::Invoke(request)) => invoke_remote_management_operation(request),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "invalid-request",
            &format!("decode remote-management module request: {error}"),
        )),
    };
    write_response(response_sobj, response, SINOMO_MODULE_STATUS_OK)
}

fn invoke_remote_management_operation(
    request: NativeModuleInvocationRequest,
) -> NativeModuleResponse {
    match request.operation.as_str() {
        REMOTE_MANAGEMENT_SHELL_RUN_OPERATION => {
            invoke_shell_run_operation(request.operation, &request.payload_sobj)
        }
        REMOTE_MANAGEMENT_FS_READ_OPERATION => {
            invoke_file_read_operation(request.operation, &request.payload_sobj)
        }
        REMOTE_MANAGEMENT_FS_WRITE_OPERATION => {
            invoke_file_write_operation(request.operation, &request.payload_sobj)
        }
        REMOTE_MANAGEMENT_FS_LIST_OPERATION => {
            invoke_file_list_operation(request.operation, &request.payload_sobj)
        }
        REMOTE_MANAGEMENT_FS_STAT_OPERATION => {
            invoke_file_stat_operation(request.operation, &request.payload_sobj)
        }
        REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION => {
            invoke_system_info_operation(request.operation, &request.payload_sobj)
        }
        REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION => {
            invoke_system_observe_operation(request.operation, &request.payload_sobj)
        }
        other => NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!("unsupported remote-management operation {other}"),
        )),
    }
}

fn invoke_shell_run_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementShellRunRequest>(payload_sobj) {
        Ok(request) => match run_shell(request) {
            Ok(outcome) => RemoteManagementShellRunResponse::success(outcome),
            Err(error) => RemoteManagementShellRunResponse::failure(error),
        },
        Err(error) => RemoteManagementShellRunResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode shell request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_file_read_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementFileReadRequest>(payload_sobj) {
        Ok(request) => match read_file(request) {
            Ok(file) => RemoteManagementFileReadResponse::success(file),
            Err(error) => RemoteManagementFileReadResponse::failure(error),
        },
        Err(error) => RemoteManagementFileReadResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode file read request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_file_write_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementFileWriteRequest>(payload_sobj) {
        Ok(request) => match write_file(request) {
            Ok(written) => RemoteManagementFileWriteResponse::success(written),
            Err(error) => RemoteManagementFileWriteResponse::failure(error),
        },
        Err(error) => RemoteManagementFileWriteResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode file write request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_file_list_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementFileListRequest>(payload_sobj) {
        Ok(request) => match list_files(request) {
            Ok((entries, truncated)) => {
                RemoteManagementFileListResponse::success(entries, truncated)
            }
            Err(error) => RemoteManagementFileListResponse::failure(error),
        },
        Err(error) => RemoteManagementFileListResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode file list request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_file_stat_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementFileStatRequest>(payload_sobj) {
        Ok(request) => match stat_file(request) {
            Ok(stat) => RemoteManagementFileStatResponse::success(stat),
            Err(error) => RemoteManagementFileStatResponse::failure(error),
        },
        Err(error) => RemoteManagementFileStatResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode file stat request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_system_info_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementSystemInfoRequest>(payload_sobj) {
        Ok(request) => RemoteManagementSystemInfoResponse::success(system_info(request)),
        Err(error) => RemoteManagementSystemInfoResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode system info request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_system_observe_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementSystemObserveRequest>(payload_sobj) {
        Ok(request) => RemoteManagementSystemObserveResponse::success(system_observation(request)),
        Err(error) => RemoteManagementSystemObserveResponse::failure(RemoteManagementError::new(
            "invalid-request",
            format!("decode system observation request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn encode_invocation_response<T: Serialize>(operation: &str, response: &T) -> NativeModuleResponse {
    match encode_sobj(response) {
        Ok(payload_sobj) => NativeModuleResponse::Invocation(NativeModuleInvocationResponse {
            operation: operation.to_string(),
            payload_sobj,
        }),
        Err(error) => NativeModuleResponse::Error(native_error_response(
            "encode-response",
            &format!("encode remote-management response payload: {error}"),
        )),
    }
}

fn run_shell(
    request: RemoteManagementShellRunRequest,
) -> Result<RemoteManagementShellOutcome, RemoteManagementError> {
    request
        .validate()
        .map_err(|error| protocol_error("invalid-command", error))?;
    if invocation_cancelled() {
        return Err(RemoteManagementError::new(
            "cancelled",
            "shell command was cancelled before it started",
        ));
    }

    let output_limit = request.effective_max_output_bytes();
    let timeout = Duration::from_millis(request.effective_timeout_ms());
    let mut command = shell_command(request.shell.as_deref(), &request.command);
    if let Some(cwd) = request.cwd.as_ref() {
        command.current_dir(cwd);
    }
    for (key, value) in &request.env {
        command.env(key, value);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    configure_shell_process_tree(&mut command);

    let start = Instant::now();
    let mut child = command.spawn().map_err(|source| {
        RemoteManagementError::new("spawn-failed", format!("spawn shell command: {source}"))
    })?;
    let mut process_tree = ShellProcessTree::attach(&child).map_err(|source| {
        terminate_untracked_shell_process(&mut child);
        RemoteManagementError::new(
            "process-tree-failed",
            format!("attach shell command to a managed process tree: {source}"),
        )
    })?;
    let mut stdout_reader = child
        .stdout
        .take()
        .map(|stdout| thread::spawn(move || read_pipe_limited(stdout, output_limit)));
    let mut stderr_reader = child
        .stderr
        .take()
        .map(|stderr| thread::spawn(move || read_pipe_limited(stderr, output_limit)));

    let mut timed_out = false;
    let mut status = None;
    loop {
        if status.is_none() {
            status = child.try_wait().map_err(|source| {
                cleanup_shell_process(
                    &mut process_tree,
                    &mut child,
                    &mut stdout_reader,
                    &mut stderr_reader,
                );
                RemoteManagementError::new(
                    "wait-failed",
                    format!("wait for shell command: {source}"),
                )
            })?;
        }

        // The root shell can exit before a background descendant. Do not join
        // its inherited output pipes until they close, and keep that drain
        // inside the same command deadline.
        if status.is_some()
            && pipe_reader_finished(&stdout_reader)
            && pipe_reader_finished(&stderr_reader)
        {
            break;
        }
        if invocation_cancelled() {
            cleanup_shell_process(
                &mut process_tree,
                &mut child,
                &mut stdout_reader,
                &mut stderr_reader,
            );
            return Err(RemoteManagementError::new(
                "cancelled",
                "shell command was cancelled",
            ));
        }
        if start.elapsed() >= timeout {
            timed_out = true;
            terminate_shell_process(
                &mut process_tree,
                &mut child,
                &mut status,
                &mut stdout_reader,
                &mut stderr_reader,
            )?;
            break;
        }
        thread::sleep(SHELL_POLL_INTERVAL);
    }

    let stdout = join_pipe_reader(stdout_reader.take())?;
    let stderr = join_pipe_reader(stderr_reader.take())?;
    process_tree.disarm();
    let duration_ms = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;

    Ok(RemoteManagementShellOutcome {
        exit_code: status.and_then(|status| status.code()),
        stdout: String::from_utf8_lossy(&stdout.bytes).into_owned(),
        stderr: String::from_utf8_lossy(&stderr.bytes).into_owned(),
        duration_ms,
        timed_out,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    })
}

fn invocation_cancelled() -> bool {
    let Ok(api) = HOST_API.lock() else {
        return false;
    };
    let Some(StoredHostApi(host)) = *api else {
        return false;
    };
    let Some(is_cancelled) = host.is_cancelled else {
        return false;
    };
    // The callback and user_data originate from the validated native-module
    // host ABI and remain valid until module shutdown.
    unsafe { is_cancelled(host.user_data) != 0 }
}

fn terminate_shell_process(
    process_tree: &mut ShellProcessTree,
    child: &mut Child,
    status: &mut Option<ExitStatus>,
    stdout_reader: &mut Option<PipeReader>,
    stderr_reader: &mut Option<PipeReader>,
) -> Result<(), RemoteManagementError> {
    process_tree.terminate(child).map_err(|source| {
        RemoteManagementError::new(
            "terminate-failed",
            format!("terminate shell process tree: {source}"),
        )
    })?;
    let deadline = Instant::now()
        .checked_add(SHELL_CLEANUP_GRACE)
        .unwrap_or_else(Instant::now);
    if status.is_none() {
        *status = wait_for_child_exit(child, deadline).map_err(|source| {
            RemoteManagementError::new(
                "wait-failed",
                format!("wait for terminated shell command: {source}"),
            )
        })?;
        if status.is_none() {
            return Err(RemoteManagementError::new(
                "terminate-timeout",
                "shell command did not exit after its process tree was terminated",
            ));
        }
    }
    wait_for_pipe_readers(stdout_reader, stderr_reader, deadline);
    if !pipe_reader_finished(stdout_reader) || !pipe_reader_finished(stderr_reader) {
        return Err(RemoteManagementError::new(
            "terminate-timeout",
            "shell output pipes did not close after its process tree was terminated",
        ));
    }
    Ok(())
}

fn cleanup_shell_process(
    process_tree: &mut ShellProcessTree,
    child: &mut Child,
    stdout_reader: &mut Option<PipeReader>,
    stderr_reader: &mut Option<PipeReader>,
) {
    let _ = process_tree.terminate(child);
    let deadline = Instant::now()
        .checked_add(SHELL_CLEANUP_GRACE)
        .unwrap_or_else(Instant::now);
    let _ = wait_for_child_exit(child, deadline);
    wait_for_pipe_readers(stdout_reader, stderr_reader, deadline);
    if pipe_reader_finished(stdout_reader) {
        let _ = join_pipe_reader(stdout_reader.take());
    } else {
        // A failed platform termination must not turn RPC cancellation or an
        // error path into an unbounded join. Dropping detaches only the reader
        // thread; the process-tree guard retries termination on drop.
        stdout_reader.take();
    }
    if pipe_reader_finished(stderr_reader) {
        let _ = join_pipe_reader(stderr_reader.take());
    } else {
        stderr_reader.take();
    }
}

fn wait_for_child_exit(
    child: &mut Child,
    deadline: Instant,
) -> std::io::Result<Option<ExitStatus>> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        thread::sleep(SHELL_POLL_INTERVAL);
    }
}

fn wait_for_pipe_readers(
    stdout_reader: &Option<PipeReader>,
    stderr_reader: &Option<PipeReader>,
    deadline: Instant,
) {
    while (!pipe_reader_finished(stdout_reader) || !pipe_reader_finished(stderr_reader))
        && Instant::now() < deadline
    {
        thread::sleep(SHELL_POLL_INTERVAL);
    }
}

fn pipe_reader_finished(reader: &Option<PipeReader>) -> bool {
    reader.as_ref().is_none_or(thread::JoinHandle::is_finished)
}

#[cfg(unix)]
fn configure_shell_process_tree(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(windows)]
fn configure_shell_process_tree(_command: &mut Command) {}

#[cfg(not(any(unix, windows)))]
fn configure_shell_process_tree(_command: &mut Command) {}

#[cfg(unix)]
struct ShellProcessTree {
    process_group_id: i32,
    armed: bool,
}

#[cfg(unix)]
impl ShellProcessTree {
    fn attach(child: &Child) -> std::io::Result<Self> {
        let process_group_id = i32::try_from(child.id()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("child process id {} does not fit in i32", child.id()),
            )
        })?;
        Ok(Self {
            process_group_id,
            armed: true,
        })
    }

    fn terminate(&mut self, _child: &mut Child) -> std::io::Result<()> {
        if self.armed {
            unix_shell_process::kill_process_group(self.process_group_id)?;
            self.armed = false;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(unix)]
impl Drop for ShellProcessTree {
    fn drop(&mut self) {
        if self.armed {
            let _ = unix_shell_process::kill_process_group(self.process_group_id);
        }
    }
}

#[cfg(unix)]
mod unix_shell_process {
    const SIGKILL: i32 = 9;
    const ESRCH: i32 = 3;

    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }

    pub(super) fn kill_process_group(process_group_id: i32) -> std::io::Result<()> {
        // The shell is spawned as its own process-group leader. A negative PID
        // asks POSIX kill(2) to signal every process in that group.
        let result = unsafe { kill(-process_group_id, SIGKILL) };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            // The whole group already exited between the poll and cleanup.
            Ok(())
        } else {
            Err(error)
        }
    }
}

#[cfg(windows)]
struct ShellProcessTree {
    job: windows_shell_process::Job,
    armed: bool,
}

#[cfg(windows)]
impl ShellProcessTree {
    fn attach(child: &Child) -> std::io::Result<Self> {
        Ok(Self {
            job: windows_shell_process::Job::attach(child)?,
            armed: true,
        })
    }

    fn terminate(&mut self, _child: &mut Child) -> std::io::Result<()> {
        if self.armed {
            self.job.terminate()?;
            self.armed = false;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(windows)]
impl Drop for ShellProcessTree {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.job.terminate();
        }
    }
}

#[cfg(windows)]
mod windows_shell_process {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;
    use std::process::Child;

    type Handle = *mut c_void;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateJobObjectW(attributes: *const c_void, name: *const u16) -> Handle;
        fn AssignProcessToJobObject(job: Handle, process: Handle) -> i32;
        fn TerminateJobObject(job: Handle, exit_code: u32) -> i32;
        fn CloseHandle(object: Handle) -> i32;
    }

    pub(super) struct Job(Handle);

    impl Job {
        pub(super) fn attach(child: &Child) -> std::io::Result<Self> {
            let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if job.is_null() {
                return Err(std::io::Error::last_os_error());
            }
            if unsafe { AssignProcessToJobObject(job, child.as_raw_handle()) } == 0 {
                let error = std::io::Error::last_os_error();
                unsafe {
                    CloseHandle(job);
                }
                return Err(error);
            }
            Ok(Self(job))
        }

        pub(super) fn terminate(&self) -> std::io::Result<()> {
            if unsafe { TerminateJobObject(self.0, 1) } == 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(not(any(unix, windows)))]
struct ShellProcessTree {
    armed: bool,
}

#[cfg(not(any(unix, windows)))]
impl ShellProcessTree {
    fn attach(_child: &Child) -> std::io::Result<Self> {
        Ok(Self { armed: true })
    }

    fn terminate(&mut self, child: &mut Child) -> std::io::Result<()> {
        if self.armed {
            child.kill()?;
            self.armed = false;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

fn terminate_untracked_shell_process(child: &mut Child) {
    #[cfg(windows)]
    {
        let pid = child.id().to_string();
        let _ = Command::new("taskkill")
            .args(["/PID", &pid, "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let _ = child.kill();
}

#[cfg(windows)]
fn shell_command(shell: Option<&str>, command: &str) -> Command {
    let mut process = Command::new(shell.unwrap_or("cmd"));
    process.arg("/C").arg(command);
    process
}

#[cfg(not(windows))]
fn shell_command(shell: Option<&str>, command: &str) -> Command {
    let shell = match shell {
        Some(shell) => shell,
        None => default_unix_shell(),
    };
    let mut process = Command::new(shell);
    process.arg("-lc").arg(command);
    process
}

#[cfg(not(windows))]
fn default_unix_shell() -> &'static str {
    if Path::new("/bin/sh").exists() {
        "/bin/sh"
    } else {
        "sh"
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LimitedBytes {
    bytes: Vec<u8>,
    truncated: bool,
}

type PipeReader = thread::JoinHandle<std::io::Result<LimitedBytes>>;

fn read_pipe_limited(mut reader: impl Read, limit: usize) -> std::io::Result<LimitedBytes> {
    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0u8; 8192];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        if remaining > 0 {
            let keep = remaining.min(read);
            bytes.extend_from_slice(&buffer[..keep]);
            truncated |= keep < read;
        } else {
            truncated = true;
        }
    }
    Ok(LimitedBytes { bytes, truncated })
}

fn join_pipe_reader(handle: Option<PipeReader>) -> Result<LimitedBytes, RemoteManagementError> {
    let Some(handle) = handle else {
        return Ok(LimitedBytes {
            bytes: Vec::new(),
            truncated: false,
        });
    };
    match handle.join() {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(source)) => Err(RemoteManagementError::new(
            "read-output-failed",
            format!("read command output: {source}"),
        )),
        Err(_) => Err(RemoteManagementError::new(
            "read-output-panicked",
            "command output reader panicked",
        )),
    }
}

fn read_file(
    request: RemoteManagementFileReadRequest,
) -> Result<RemoteManagementFileContent, RemoteManagementError> {
    request
        .validate()
        .map_err(|error| protocol_error("invalid-path", error))?;
    let path = PathBuf::from(&request.path);
    let metadata = fs::metadata(&path).map_err(|source| {
        RemoteManagementError::new(
            "metadata-failed",
            format!("stat {}: {source}", path.display()),
        )
    })?;
    if !metadata.is_file() {
        return Err(RemoteManagementError::new(
            "not-a-file",
            format!("{} is not a regular file", path.display()),
        ));
    }
    let offset = request.offset.unwrap_or(0);
    let max_bytes = request.effective_max_bytes();
    let mut file = File::open(&path).map_err(|source| {
        RemoteManagementError::new("open-failed", format!("open {}: {source}", path.display()))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|source| {
        RemoteManagementError::new("seek-failed", format!("seek {}: {source}", path.display()))
    })?;
    let mut bytes = Vec::with_capacity(max_bytes.min(8192));
    file.take(max_bytes as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| {
            RemoteManagementError::new("read-failed", format!("read {}: {source}", path.display()))
        })?;
    let len = metadata.len();
    let truncated = offset.saturating_add(bytes.len() as u64) < len;
    let text = String::from_utf8(bytes.clone()).ok();
    Ok(RemoteManagementFileContent {
        path: path.display().to_string(),
        bytes,
        text,
        len,
        truncated,
    })
}

fn write_file(
    request: RemoteManagementFileWriteRequest,
) -> Result<RemoteManagementFileWriteOutcome, RemoteManagementError> {
    request
        .validate()
        .map_err(|error| protocol_error("invalid-write", error))?;
    let path = PathBuf::from(&request.path);
    if request.create_parent
        && let Some(parent) = path.parent()
    {
        fs::create_dir_all(parent).map_err(|source| {
            RemoteManagementError::new(
                "create-parent-failed",
                format!("create parent {}: {source}", parent.display()),
            )
        })?;
    }

    let should_truncate = request.should_truncate();
    let effective_offset = request.effective_offset();
    let explicit_chunk = request.offset.is_some() || request.truncate.is_some();
    let next_offset = request
        .next_offset()
        .map_err(|error| protocol_error("invalid-write", error))?;
    let existing_len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(RemoteManagementError::new(
                "metadata-failed",
                format!("stat {} before write: {source}", path.display()),
            ));
        }
    };
    let write_start = if request.append {
        existing_len
    } else {
        effective_offset.unwrap_or(0)
    };
    let write_end = write_start
        .checked_add(request.bytes.len() as u64)
        .ok_or_else(|| {
            protocol_error(
                "invalid-write",
                RemoteManagementProtocolError::FileWriteOffsetOverflow,
            )
        })?;
    let projected_len = if should_truncate {
        write_end
    } else {
        existing_len.max(write_end)
    };
    if projected_len > MAX_REMOTE_FILE_SIZE_BYTES {
        return Err(protocol_error(
            "file-too-large",
            RemoteManagementProtocolError::FileWriteExceedsMaximum {
                end: projected_len,
                maximum: MAX_REMOTE_FILE_SIZE_BYTES,
            },
        ));
    }

    let mut options = OpenOptions::new();
    options.create(true).write(true);
    if request.append {
        options.append(true);
    } else if should_truncate {
        options.truncate(true);
    }
    let mut file = options.open(&path).map_err(|source| {
        RemoteManagementError::new("open-failed", format!("open {}: {source}", path.display()))
    })?;
    if let Some(offset) = effective_offset {
        file.seek(SeekFrom::Start(offset)).map_err(|source| {
            RemoteManagementError::new("seek-failed", format!("seek {}: {source}", path.display()))
        })?;
    }
    file.write_all(&request.bytes).map_err(|source| {
        RemoteManagementError::new(
            "write-failed",
            format!("write {}: {source}", path.display()),
        )
    })?;
    file.flush().map_err(|source| {
        RemoteManagementError::new(
            "flush-failed",
            format!("flush {}: {source}", path.display()),
        )
    })?;
    let len = file.metadata().ok().map(|metadata| metadata.len());

    Ok(RemoteManagementFileWriteOutcome {
        path: path.display().to_string(),
        bytes_written: request.bytes.len() as u64,
        append: request.append,
        offset: explicit_chunk.then_some(effective_offset).flatten(),
        next_offset: explicit_chunk.then_some(next_offset).flatten(),
        len,
    })
}

fn list_files(
    request: RemoteManagementFileListRequest,
) -> Result<(Vec<RemoteManagementFileEntry>, bool), RemoteManagementError> {
    request
        .validate()
        .map_err(|error| protocol_error("invalid-path", error))?;
    let root = PathBuf::from(&request.path);
    let max_entries = request.effective_max_entries();
    let mut entries = Vec::new();
    let mut pending = VecDeque::new();
    let mut truncated = false;

    let metadata = fs::symlink_metadata(&root).map_err(|source| {
        RemoteManagementError::new(
            "metadata-failed",
            format!("stat {}: {source}", root.display()),
        )
    })?;
    if !metadata.is_dir() {
        entries.push(file_entry(&root, metadata));
        return Ok((entries, false));
    }
    pending.push_back(root);

    while let Some(dir) = pending.pop_front() {
        let read_dir = fs::read_dir(&dir).map_err(|source| {
            RemoteManagementError::new(
                "read-dir-failed",
                format!("read directory {}: {source}", dir.display()),
            )
        })?;
        for child in read_dir {
            let child_path = child.map(|entry| entry.path()).map_err(|source| {
                RemoteManagementError::new(
                    "read-dir-entry-failed",
                    format!("read directory entry in {}: {source}", dir.display()),
                )
            })?;
            if entries.len() >= max_entries {
                truncated = true;
                break;
            }
            let metadata = fs::symlink_metadata(&child_path).map_err(|source| {
                RemoteManagementError::new(
                    "metadata-failed",
                    format!("stat {}: {source}", child_path.display()),
                )
            })?;
            let is_directory = metadata.is_dir();
            let is_symlink = metadata.file_type().is_symlink();
            entries.push(file_entry(&child_path, metadata));
            if request.recursive && is_directory && !is_symlink {
                pending.push_back(child_path);
            }
        }
        if truncated {
            break;
        }
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((entries, truncated))
}

fn file_entry(path: &Path, metadata: fs::Metadata) -> RemoteManagementFileEntry {
    RemoteManagementFileEntry {
        path: path.display().to_string(),
        file_type: remote_file_type(&metadata),
        len: metadata.is_file().then_some(metadata.len()),
        readonly: metadata.permissions().readonly(),
    }
}

fn stat_file(
    request: RemoteManagementFileStatRequest,
) -> Result<RemoteManagementFileStat, RemoteManagementError> {
    request
        .validate()
        .map_err(|error| protocol_error("invalid-path", error))?;
    let path = PathBuf::from(&request.path);
    let metadata = fs::symlink_metadata(&path).map_err(|source| {
        RemoteManagementError::new(
            "metadata-failed",
            format!("stat {}: {source}", path.display()),
        )
    })?;
    let blake3 = if request.include_blake3 && metadata.is_file() {
        Some(hash_file(&path)?)
    } else {
        None
    };
    let modified_unix_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64);
    Ok(RemoteManagementFileStat {
        path: path.display().to_string(),
        file_type: remote_file_type(&metadata),
        len: metadata.is_file().then_some(metadata.len()),
        readonly: metadata.permissions().readonly(),
        modified_unix_ms,
        blake3,
    })
}

fn hash_file(path: &Path) -> Result<String, RemoteManagementError> {
    let mut file = File::open(path).map_err(|source| {
        RemoteManagementError::new("open-failed", format!("open {}: {source}", path.display()))
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| {
            RemoteManagementError::new("read-failed", format!("hash {}: {source}", path.display()))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn remote_file_type(metadata: &fs::Metadata) -> RemoteManagementFileType {
    if metadata.file_type().is_symlink() {
        RemoteManagementFileType::Symlink
    } else if metadata.is_file() {
        RemoteManagementFileType::File
    } else if metadata.is_dir() {
        RemoteManagementFileType::Directory
    } else {
        RemoteManagementFileType::Other
    }
}

fn system_info(request: RemoteManagementSystemInfoRequest) -> RemoteManagementSystemInfo {
    let env = selected_env(&request);
    let env_redacted = env
        .iter()
        .filter_map(|(key, value)| value.is_none().then_some(key.clone()))
        .collect::<BTreeSet<_>>();
    RemoteManagementSystemInfo {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        family: std::env::consts::FAMILY.to_string(),
        current_dir: std::env::current_dir()
            .ok()
            .map(|path| path.display().to_string()),
        executable: std::env::current_exe()
            .ok()
            .map(|path| path.display().to_string()),
        default_shell: default_shell_name(),
        env,
        env_redacted,
    }
}

fn selected_env(request: &RemoteManagementSystemInfoRequest) -> BTreeMap<String, Option<String>> {
    let mut env = BTreeMap::new();
    if request.include_env {
        for (key, value) in std::env::vars() {
            let value = (!env_name_is_sensitive(&key)).then_some(value);
            env.insert(key, value);
        }
    }
    for key in &request.env_names {
        if env.contains_key(key) {
            continue;
        }
        let value = std::env::var(key).ok();
        env.insert(key.clone(), value.filter(|_| !env_name_is_sensitive(key)));
    }
    env
}

fn env_name_is_sensitive(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "PASSWORD",
        "PASSWD",
        "TOKEN",
        "SECRET",
        "CREDENTIAL",
        "AUTH",
        "KEY",
    ]
    .iter()
    .any(|needle| upper.contains(needle))
}

#[cfg(windows)]
fn default_shell_name() -> String {
    std::env::var("COMSPEC").unwrap_or_else(|_| "cmd".to_string())
}

#[cfg(not(windows))]
fn default_shell_name() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| default_unix_shell().to_string())
}

fn system_observation(
    request: RemoteManagementSystemObserveRequest,
) -> RemoteManagementSystemObservation {
    let mut system = System::new_all();
    let supported = sysinfo::IS_SUPPORTED_SYSTEM;
    if supported {
        // Global and per-process CPU usage are deltas. `new_all` supplies the
        // first sample; a second refresh after sysinfo's platform-specific
        // minimum interval makes the returned values meaningful.
        thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        system.refresh_cpu_usage();
        if request.include_processes {
            system.refresh_processes(ProcessesToUpdate::All, true);
        }
    }
    let cpu_usage = system.global_cpu_usage();
    let load = System::load_average();
    let (processes, processes_truncated) = if request.include_processes {
        observe_processes(&system, request.effective_max_processes())
    } else {
        (Vec::new(), false)
    };

    RemoteManagementSystemObservation {
        observed_at: observed_at(),
        hostname: supported.then(System::host_name).flatten(),
        uptime_seconds: supported.then_some(System::uptime()),
        logical_cpu_count: supported
            .then(|| u32::try_from(system.cpus().len()).ok())
            .flatten(),
        cpu_usage_percent: supported
            .then_some(cpu_usage)
            .filter(|usage| usage.is_finite()),
        load_average: supported.then_some(RemoteManagementLoadAverage {
            one_minute: load.one,
            five_minutes: load.five,
            fifteen_minutes: load.fifteen,
        }),
        memory: supported.then_some(RemoteManagementMemoryObservation {
            total_bytes: system.total_memory(),
            available_bytes: system.available_memory(),
            swap_total_bytes: Some(system.total_swap()),
            swap_free_bytes: Some(system.total_swap().saturating_sub(system.used_swap())),
        }),
        disks: if request.include_disks {
            observe_disks()
        } else {
            Vec::new()
        },
        network_interfaces: if request.include_network {
            observe_network_interfaces()
        } else {
            Vec::new()
        },
        processes,
        processes_truncated,
        // This host does not provide verified native-device inventory. Local
        // resource exposure declarations are reported separately below and
        // must not be presented as proof that a device exists.
        devices: Vec::new(),
        resource_exposure_policies: if request.include_resource_exposures {
            request
                .host_authorized_resource_exposures
                .into_iter()
                .take(MAX_OBSERVED_RESOURCE_EXPOSURES)
                .collect()
        } else {
            Vec::new()
        },
    }
}

fn observed_at() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn observe_disks() -> Vec<RemoteManagementDiskObservation> {
    let disks = Disks::new_with_refreshed_list();
    let mut observations = disks
        .list()
        .iter()
        .map(|disk| {
            let file_system = disk.file_system().to_string_lossy().into_owned();
            RemoteManagementDiskObservation {
                name: bounded_os_string(disk.name()),
                mount_point: bounded_text(
                    disk.mount_point().display().to_string(),
                    MAX_PROCESS_TEXT_BYTES,
                ),
                file_system: (!file_system.is_empty()).then_some(file_system),
                total_bytes: disk.total_space(),
                available_bytes: disk.available_space(),
                removable: disk.is_removable(),
            }
        })
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| {
        left.mount_point
            .cmp(&right.mount_point)
            .then_with(|| left.name.cmp(&right.name))
    });
    observations.truncate(MAX_OBSERVED_DISKS);
    observations
}

fn observe_network_interfaces() -> Vec<RemoteManagementNetworkObservation> {
    let networks = Networks::new_with_refreshed_list();
    let mut observations = networks
        .iter()
        .map(|(name, network)| RemoteManagementNetworkObservation {
            name: bounded_text(name.clone(), MAX_PROCESS_TEXT_BYTES),
            addresses: network
                .ip_networks()
                .iter()
                .take(MAX_NETWORK_ADDRESSES_PER_INTERFACE)
                .map(ToString::to_string)
                .collect(),
            received_bytes: Some(network.total_received()),
            transmitted_bytes: Some(network.total_transmitted()),
        })
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| left.name.cmp(&right.name));
    observations.truncate(MAX_OBSERVED_NETWORK_INTERFACES);
    observations
}

fn observe_processes(
    system: &System,
    maximum: usize,
) -> (Vec<RemoteManagementProcessObservation>, bool) {
    let mut observations = system
        .processes()
        .iter()
        .map(|(pid, process)| RemoteManagementProcessObservation {
            pid: pid.as_u32(),
            name: bounded_os_string(process.name()),
            command: process
                .cmd()
                .iter()
                .take(MAX_PROCESS_COMMAND_ARGUMENTS)
                .map(|argument| bounded_os_string(argument))
                .collect(),
            cpu_usage_percent: process
                .cpu_usage()
                .is_finite()
                .then_some(process.cpu_usage()),
            memory_bytes: Some(process.memory()),
            started_unix_ms: process.start_time().checked_mul(1000),
        })
        .collect::<Vec<_>>();
    observations.sort_by_key(|process| process.pid);
    let truncated = observations.len() > maximum;
    observations.truncate(maximum);
    (observations, truncated)
}

fn bounded_os_string(value: &OsStr) -> String {
    bounded_text(value.to_string_lossy().into_owned(), MAX_PROCESS_TEXT_BYTES)
}

fn bounded_text(mut value: String, maximum_bytes: usize) -> String {
    if value.len() <= maximum_bytes {
        return value;
    }
    let mut end = maximum_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

fn protocol_error(code: &str, error: RemoteManagementProtocolError) -> RemoteManagementError {
    RemoteManagementError::new(code, error.to_string())
}

fn decode_native_module_request(
    request_sobj: SinomoModuleByteSlice,
) -> Result<NativeModuleRequest, sinomo_module_api::NativeModuleApiError> {
    let request_bytes = unsafe { request_sobj.as_slice() };
    decode_sobj(request_bytes)
}

fn write_response(
    out: *mut SinomoModuleByteBuffer,
    response: NativeModuleResponse,
    fallback_status: SinomoModuleStatusCode,
) -> SinomoModuleStatusCode {
    match encode_sobj(&response) {
        Ok(bytes) => write_buffer(out, Ok(bytes)),
        Err(error) => write_buffer(out, Err(error)).max(fallback_status),
    }
}

fn write_buffer(
    out: *mut SinomoModuleByteBuffer,
    bytes: Result<Vec<u8>, sinomo_module_api::NativeModuleApiError>,
) -> SinomoModuleStatusCode {
    let Ok(bytes) = bytes else {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    };
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    unsafe {
        *out = SinomoModuleByteBuffer::from_vec(bytes);
    }
    SINOMO_MODULE_STATUS_OK
}

fn native_error_response(code: &str, detail: &str) -> NativeModuleErrorResponse {
    NativeModuleErrorResponse {
        code: code.to_string(),
        detail: detail.to_string(),
    }
}

fn platform_triple() -> String {
    format!(
        "{}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        std::env::consts::FAMILY
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-remote-management-module-{name}"))
    }

    #[test]
    fn descriptor_exports_typed_routes_permissions_and_focused_resources() {
        let descriptor = remote_management_module_descriptor();
        assert_eq!(descriptor.module_id, REMOTE_MANAGEMENT_MODULE_ID);
        assert_eq!(
            descriptor.supported_operations,
            REMOTE_MANAGEMENT_OPERATIONS
                .iter()
                .map(|operation| operation.to_string())
                .collect::<Vec<_>>()
        );
        assert!(
            descriptor
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == REMOTE_MANAGEMENT_FS_STAT_ROUTE)
        );
        assert!(
            descriptor
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE)
        );
        assert!(
            descriptor
                .surfaces
                .object_rpc_routes
                .iter()
                .all(|route| route.required_local_permissions
                    == vec![REMOTE_MANAGEMENT_LOCAL_PERMISSION.to_string()])
        );

        let resource_ids = descriptor
            .surfaces
            .declarative_resources
            .iter()
            .map(|resource| resource.id.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            COMPUTE_USE_SKILL_ID,
            FILE_TRANSFER_SKILL_ID,
            NODE_CONFIG_SKILL_ID,
            COMPUTE_USE_PROMPT_ID,
            FILE_TRANSFER_PROMPT_ID,
            NODE_CONFIG_PROMPT_ID,
        ] {
            assert!(
                resource_ids.contains(expected),
                "missing resource {expected}"
            );
        }
        let node_config = descriptor
            .surfaces
            .declarative_resources
            .iter()
            .find(|resource| resource.id == NODE_CONFIG_SKILL_ID)
            .expect("node-config skill");
        let text = node_config
            .embedded_text
            .as_deref()
            .expect("embedded skill");
        assert!(text.contains(CONFIG_GET_ROUTE));
        assert!(text.contains(CONFIG_SET_ROUTE));
        assert!(!text.contains("/remote-management/config"));
        assert!(text.contains("locally"));
    }

    #[test]
    fn shell_run_captures_stdout() {
        let outcome = run_shell(
            RemoteManagementShellRunRequest::new("echo sinomo-remote-management")
                .with_timeout_ms(5_000)
                .with_max_output_bytes(1024),
        )
        .expect("run shell");

        assert_eq!(outcome.exit_code, Some(0));
        assert!(outcome.stdout.contains("sinomo-remote-management"));
        assert!(!outcome.timed_out);
    }

    #[cfg(unix)]
    #[test]
    fn shell_timeout_kills_descendants_that_hold_output_pipes() {
        let root = temp_root("shell-process-tree-timeout");
        let marker = root.join("descendant-survived");
        let started = Instant::now();
        let outcome = run_shell(
            RemoteManagementShellRunRequest::new(
                r#"(sleep 1; printf leaked > "$SINOMO_REMOTE_TEST_MARKER") &"#,
            )
            .with_env("SINOMO_REMOTE_TEST_MARKER", marker.display().to_string())
            .with_timeout_ms(100)
            .with_max_output_bytes(1024),
        )
        .expect("time out shell process tree");

        assert!(outcome.timed_out);
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "descendant-held pipes exceeded the bounded cleanup window: {:?}",
            started.elapsed()
        );
        thread::sleep(Duration::from_millis(1_100));
        assert!(
            !marker.exists(),
            "timed-out descendant survived long enough to write {}",
            marker.display()
        );
    }

    #[test]
    fn retry_safe_chunk_writes_and_stat_hash_round_trip() {
        let root = temp_root("chunks");
        let path = root.join("nested").join("hello.txt");
        let first = write_file(
            RemoteManagementFileWriteRequest::chunk(
                path.display().to_string(),
                0,
                b"hello".to_vec(),
            )
            .creating_parent(true)
            .truncating(true),
        )
        .expect("first chunk");
        assert_eq!(first.offset, Some(0));
        assert_eq!(first.next_offset, Some(5));

        let second_request = RemoteManagementFileWriteRequest::chunk(
            path.display().to_string(),
            5,
            b" remote".to_vec(),
        );
        let second = write_file(second_request.clone()).expect("second chunk");
        assert_eq!(second.next_offset, Some(12));
        write_file(second_request).expect("retry second chunk");

        let read = read_file(
            RemoteManagementFileReadRequest::new(path.display().to_string()).with_max_bytes(64),
        )
        .expect("read file");
        assert_eq!(read.text.as_deref(), Some("hello remote"));

        let stat = stat_file(
            RemoteManagementFileStatRequest::new(path.display().to_string()).including_blake3(true),
        )
        .expect("stat file");
        assert_eq!(stat.len, Some(12));
        let expected_hash = blake3::hash(b"hello remote").to_hex().to_string();
        assert_eq!(stat.blake3.as_deref(), Some(expected_hash.as_str()));

        let (entries, truncated) = list_files(
            RemoteManagementFileListRequest::new(root.display().to_string()).recursively(true),
        )
        .expect("list files");
        assert!(!truncated);
        assert!(
            entries
                .iter()
                .any(|entry| entry.path.ends_with("hello.txt"))
        );
    }

    #[test]
    fn system_observations_are_bounded() {
        let observation = system_observation(
            RemoteManagementSystemObserveRequest::new()
                .including_processes(true)
                .with_max_processes(2)
                .including_disks(true)
                .including_network(true),
        );

        assert!(!observation.observed_at.is_empty());
        assert!(observation.processes.len() <= 2);
        assert!(observation.disks.len() <= MAX_OBSERVED_DISKS);
        assert!(observation.network_interfaces.len() <= MAX_OBSERVED_NETWORK_INTERFACES);
        assert!(observation.devices.is_empty());
        if let Some(cpu_count) = observation.logical_cpu_count {
            assert!(cpu_count > 0);
        }
        assert!(observation.processes.iter().all(|process| {
            process.command.len() <= MAX_PROCESS_COMMAND_ARGUMENTS
                && process.name.len() <= MAX_PROCESS_TEXT_BYTES
                && process
                    .command
                    .iter()
                    .all(|argument| argument.len() <= MAX_PROCESS_TEXT_BYTES)
        }));
    }

    #[test]
    fn resource_exposure_observation_is_separate_from_verified_device_inventory() {
        let observation = system_observation(
            RemoteManagementSystemObserveRequest::new()
                .including_devices(true)
                .including_resource_exposures(true)
                .with_host_authorized_resource_exposures([
                    RemoteManagementResourceExposurePolicy {
                        resource_id: "camera.front".to_string(),
                        allowed_operations: vec!["capture.raw".to_string(), "observe".to_string()],
                    },
                    RemoteManagementResourceExposurePolicy {
                        resource_id: "microphone.main".to_string(),
                        allowed_operations: vec!["capture.raw".to_string()],
                    },
                ]),
        );

        assert!(observation.devices.is_empty());
        let policies = observation.resource_exposure_policies;
        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0].resource_id, "camera.front");
        assert_eq!(
            policies[0].allowed_operations,
            vec!["capture.raw".to_string(), "observe".to_string()]
        );
        assert_eq!(policies[1].resource_id, "microphone.main");
        assert!(
            policies
                .iter()
                .all(|policy| policy.resource_id != "unexposed.discovered-device")
        );
    }

    #[test]
    fn system_info_redacts_sensitive_env_names() {
        assert!(env_name_is_sensitive("OPENAI_API_KEY"));
        assert!(env_name_is_sensitive("cluster_secret_token"));
        assert!(!env_name_is_sensitive("PATH"));

        let info = system_info(RemoteManagementSystemInfoRequest::new().with_env_name("PATH"));
        assert_eq!(info.os, std::env::consts::OS);
        assert!(info.env.contains_key("PATH"));
    }
}
