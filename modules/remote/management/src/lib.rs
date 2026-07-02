//! Trusted native dylib implementation for remote machine management.
//!
//! This module intentionally starts with bounded shell, filesystem, and system
//! information operations. Screen capture and GUI interaction are left for a
//! later interface revision.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sinomo_module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDeclarativeResource,
    NativeModuleDescriptor, NativeModuleErrorResponse, NativeModuleInvocationRequest,
    NativeModuleInvocationResponse, NativeModuleObjectRpcRoute, NativeModuleRequest,
    NativeModuleResponse, NativeModuleSurfaces, SINOMO_MODULE_STATUS_INVALID_ARGUMENT,
    SINOMO_MODULE_STATUS_OK, SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    SINOMO_MODULE_STATUS_UNSUPPORTED_ABI, SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleByteBuffer,
    SinomoModuleByteSlice, SinomoModuleHostApiV1, SinomoModuleStatusCode, decode_sobj, encode_sobj,
};

const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const MAX_COMMAND_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const DEFAULT_FILE_READ_BYTES: usize = 64 * 1024;
const MAX_FILE_READ_BYTES: usize = 1024 * 1024;
const DEFAULT_LIST_ENTRIES: usize = 512;
const MAX_LIST_ENTRIES: usize = 4096;

const REMOTE_MANAGEMENT_MODULE_ID: &str = "module.remote-management";
const REMOTE_MANAGEMENT_MODULE_OWNER: &str = "sinomo-remote-management";
const REMOTE_MANAGEMENT_INTERFACE_VERSION: &str = "sinomo.remote-management.v1";
const REMOTE_MANAGEMENT_RPC_CATEGORY: &str = "remote.management";
const REMOTE_MANAGEMENT_SHELL_RUN_ROUTE: &str = "/remote-management/shell/run";
const REMOTE_MANAGEMENT_FS_READ_ROUTE: &str = "/remote-management/fs/read";
const REMOTE_MANAGEMENT_FS_WRITE_ROUTE: &str = "/remote-management/fs/write";
const REMOTE_MANAGEMENT_FS_LIST_ROUTE: &str = "/remote-management/fs/list";
const REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE: &str = "/remote-management/system/info";
const REMOTE_MANAGEMENT_SHELL_RUN_OPERATION: &str = "remote.management.shell.run";
const REMOTE_MANAGEMENT_FS_READ_OPERATION: &str = "remote.management.fs.read";
const REMOTE_MANAGEMENT_FS_WRITE_OPERATION: &str = "remote.management.fs.write";
const REMOTE_MANAGEMENT_FS_LIST_OPERATION: &str = "remote.management.fs.list";
const REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION: &str = "remote.management.system.info.read";
const REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_ID: &str = "skill.remote-management.compute-use";
const REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_PATH: &str = "skills/remote-management/SKILL.md";
const REMOTE_MANAGEMENT_COMPUTE_USE_PROMPT_ID: &str = "prompt.remote-management.compute-use";
const REMOTE_MANAGEMENT_COMPUTE_USE_PROMPT_PATH: &str = "prompts/remote-management-compute-use.md";
const REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_MARKDOWN: &str =
    include_str!("../skills/remote-management/SKILL.md");
const REMOTE_MANAGEMENT_COMPUTE_USE_PROMPT_MARKDOWN: &str =
    include_str!("../prompts/remote-management-compute-use.md");

const REMOTE_MANAGEMENT_CAPABILITY_IMPORTS: &[&str] = &[
    "process.spawn",
    "filesystem.read",
    "filesystem.write",
    "system.info.read",
];

const REMOTE_MANAGEMENT_CAPABILITY_EXPORTS: &[&str] = &[
    REMOTE_MANAGEMENT_SHELL_RUN_OPERATION,
    REMOTE_MANAGEMENT_FS_READ_OPERATION,
    REMOTE_MANAGEMENT_FS_WRITE_OPERATION,
    REMOTE_MANAGEMENT_FS_LIST_OPERATION,
    REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION,
];

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementShellRunRequest {
    command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shell: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_output_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementShellRunResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<RemoteManagementShellOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<RemoteManagementError>,
}

impl RemoteManagementShellRunResponse {
    fn outcome(outcome: RemoteManagementShellOutcome) -> Self {
        Self {
            outcome: Some(outcome),
            error: None,
        }
    }

    fn error(error: RemoteManagementError) -> Self {
        Self {
            outcome: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementShellOutcome {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    duration_ms: u64,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileReadRequest {
    path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileReadResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file: Option<RemoteManagementFileContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<RemoteManagementError>,
}

impl RemoteManagementFileReadResponse {
    fn file(file: RemoteManagementFileContent) -> Self {
        Self {
            file: Some(file),
            error: None,
        }
    }

    fn error(error: RemoteManagementError) -> Self {
        Self {
            file: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileContent {
    path: String,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    len: u64,
    truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileWriteRequest {
    path: String,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
    #[serde(default)]
    create_parent: bool,
    #[serde(default)]
    append: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileWriteResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    written: Option<RemoteManagementFileWriteOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<RemoteManagementError>,
}

impl RemoteManagementFileWriteResponse {
    fn written(written: RemoteManagementFileWriteOutcome) -> Self {
        Self {
            written: Some(written),
            error: None,
        }
    }

    fn error(error: RemoteManagementError) -> Self {
        Self {
            written: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileWriteOutcome {
    path: String,
    bytes_written: u64,
    append: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileListRequest {
    path: String,
    #[serde(default)]
    recursive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_entries: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileListResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    entries: Vec<RemoteManagementFileEntry>,
    truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<RemoteManagementError>,
}

impl RemoteManagementFileListResponse {
    fn entries(entries: Vec<RemoteManagementFileEntry>, truncated: bool) -> Self {
        Self {
            entries,
            truncated,
            error: None,
        }
    }

    fn error(error: RemoteManagementError) -> Self {
        Self {
            entries: Vec::new(),
            truncated: false,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementFileEntry {
    path: String,
    file_type: RemoteManagementFileType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    len: Option<u64>,
    readonly: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum RemoteManagementFileType {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementSystemInfoRequest {
    #[serde(default)]
    include_env: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    env_names: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementSystemInfoResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    info: Option<RemoteManagementSystemInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<RemoteManagementError>,
}

impl RemoteManagementSystemInfoResponse {
    fn info(info: RemoteManagementSystemInfo) -> Self {
        Self {
            info: Some(info),
            error: None,
        }
    }

    fn error(error: RemoteManagementError) -> Self {
        Self {
            info: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementSystemInfo {
    os: String,
    arch: String,
    family: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    current_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    executable: Option<String>,
    default_shell: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    env: BTreeMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    env_redacted: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RemoteManagementError {
    code: String,
    detail: String,
}

impl RemoteManagementError {
    fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
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

pub fn remote_management_module_descriptor() -> sinomo_module_api::NativeModuleDescriptor {
    NativeModuleDescriptor {
        module_id: REMOTE_MANAGEMENT_MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        owner: REMOTE_MANAGEMENT_MODULE_OWNER.to_string(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: REMOTE_MANAGEMENT_INTERFACE_VERSION.to_string(),
        platform_triple: platform_triple(),
        supported_operations: REMOTE_MANAGEMENT_CAPABILITY_EXPORTS
            .iter()
            .map(|operation| operation.to_string())
            .collect(),
        capabilities: REMOTE_MANAGEMENT_CAPABILITY_EXPORTS
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
                    REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
                    REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION,
                ),
            ],
            declarative_resources: vec![
                NativeModuleDeclarativeResource {
                    id: REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_ID.to_string(),
                    kind: "skill".to_string(),
                    path: REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_PATH.to_string(),
                    description:
                        "Guide agents through bounded shell, filesystem, and system inspection on a managed host."
                            .to_string(),
                    target_agents: Vec::new(),
                    model_families: vec!["gpt".to_string()],
                    activation_tags: vec![
                        "compute-use".to_string(),
                        "remote-management".to_string(),
                    ],
                    required_capabilities: REMOTE_MANAGEMENT_CAPABILITY_EXPORTS
                        .iter()
                        .map(|capability| capability.to_string())
                        .collect(),
                    embedded_text: Some(REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_MARKDOWN.to_string()),
                },
                NativeModuleDeclarativeResource {
                    id: REMOTE_MANAGEMENT_COMPUTE_USE_PROMPT_ID.to_string(),
                    kind: "prompt".to_string(),
                    path: REMOTE_MANAGEMENT_COMPUTE_USE_PROMPT_PATH.to_string(),
                    description:
                        "Prompt fragment for machine interaction through remote-management tools."
                            .to_string(),
                    target_agents: Vec::new(),
                    model_families: vec!["gpt".to_string()],
                    activation_tags: vec![
                        "compute-use".to_string(),
                        "remote-management".to_string(),
                    ],
                    required_capabilities: REMOTE_MANAGEMENT_CAPABILITY_EXPORTS
                        .iter()
                        .map(|capability| capability.to_string())
                        .collect(),
                    embedded_text: Some(REMOTE_MANAGEMENT_COMPUTE_USE_PROMPT_MARKDOWN.to_string()),
                },
            ],
            storage_owners: vec![REMOTE_MANAGEMENT_MODULE_OWNER.to_string()],
            capability_imports: REMOTE_MANAGEMENT_CAPABILITY_IMPORTS
                .iter()
                .map(|capability| capability.to_string())
                .collect(),
            capability_exports: REMOTE_MANAGEMENT_CAPABILITY_EXPORTS
                .iter()
                .map(|capability| capability.to_string())
                .collect(),
            // Not a network module: no runtime transports.
            network_transports: Vec::new(),
        },
    }
}

fn remote_management_route(route: &str, operation: &str) -> NativeModuleObjectRpcRoute {
    NativeModuleObjectRpcRoute {
        route: route.to_string(),
        category: REMOTE_MANAGEMENT_RPC_CATEGORY.to_string(),
        operation: operation.to_string(),
        auth: "admin".to_string(),
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
        REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION => {
            invoke_system_info_operation(request.operation, &request.payload_sobj)
        }
        other => NativeModuleResponse::Error(native_error_response(
            "unsupported-operation",
            &format!("unsupported remote-management operation `{other}`"),
        )),
    }
}

fn invoke_shell_run_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementShellRunRequest>(payload_sobj) {
        Ok(request) => match run_shell(request) {
            Ok(outcome) => RemoteManagementShellRunResponse::outcome(outcome),
            Err(error) => RemoteManagementShellRunResponse::error(error),
        },
        Err(error) => RemoteManagementShellRunResponse::error(RemoteManagementError::new(
            "invalid-request",
            format!("decode shell request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_file_read_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementFileReadRequest>(payload_sobj) {
        Ok(request) => match read_file(request) {
            Ok(file) => RemoteManagementFileReadResponse::file(file),
            Err(error) => RemoteManagementFileReadResponse::error(error),
        },
        Err(error) => RemoteManagementFileReadResponse::error(RemoteManagementError::new(
            "invalid-request",
            format!("decode file read request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_file_write_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementFileWriteRequest>(payload_sobj) {
        Ok(request) => match write_file(request) {
            Ok(written) => RemoteManagementFileWriteResponse::written(written),
            Err(error) => RemoteManagementFileWriteResponse::error(error),
        },
        Err(error) => RemoteManagementFileWriteResponse::error(RemoteManagementError::new(
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
                RemoteManagementFileListResponse::entries(entries, truncated)
            }
            Err(error) => RemoteManagementFileListResponse::error(error),
        },
        Err(error) => RemoteManagementFileListResponse::error(RemoteManagementError::new(
            "invalid-request",
            format!("decode file list request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn invoke_system_info_operation(operation: String, payload_sobj: &[u8]) -> NativeModuleResponse {
    let response = match decode_sobj::<RemoteManagementSystemInfoRequest>(payload_sobj) {
        Ok(request) => RemoteManagementSystemInfoResponse::info(system_info(request)),
        Err(error) => RemoteManagementSystemInfoResponse::error(RemoteManagementError::new(
            "invalid-request",
            format!("decode system info request: {error}"),
        )),
    };
    encode_invocation_response(&operation, &response)
}

fn encode_invocation_response<T: serde::Serialize>(
    operation: &str,
    response: &T,
) -> NativeModuleResponse {
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
    if request.command.trim().is_empty() {
        return Err(RemoteManagementError::new(
            "invalid-command",
            "shell command must be non-empty",
        ));
    }

    let output_limit = clamp_limit(
        request.max_output_bytes,
        DEFAULT_OUTPUT_BYTES,
        MAX_OUTPUT_BYTES,
    );
    let timeout = Duration::from_millis(
        request
            .timeout_ms
            .unwrap_or(DEFAULT_COMMAND_TIMEOUT_MS)
            .clamp(1, MAX_COMMAND_TIMEOUT_MS),
    );

    let mut command = shell_command(request.shell.as_deref(), &request.command);
    if let Some(cwd) = request.cwd.as_ref() {
        command.current_dir(cwd);
    }
    for (key, value) in &request.env {
        command.env(key, value);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let start = Instant::now();
    let mut child = command.spawn().map_err(|source| {
        RemoteManagementError::new("spawn-failed", format!("spawn shell command: {source}"))
    })?;

    let stdout_reader = child
        .stdout
        .take()
        .map(|stdout| thread::spawn(move || read_pipe_limited(stdout, output_limit)));
    let stderr_reader = child
        .stderr
        .take()
        .map(|stderr| thread::spawn(move || read_pipe_limited(stderr, output_limit)));

    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if start.elapsed() >= timeout {
                    timed_out = true;
                    let _ = child.kill();
                    break child.wait().map_err(|source| {
                        RemoteManagementError::new(
                            "wait-failed",
                            format!("wait for killed shell command: {source}"),
                        )
                    })?;
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(source) => {
                return Err(RemoteManagementError::new(
                    "wait-failed",
                    format!("wait for shell command: {source}"),
                ));
            }
        }
    };

    let stdout = join_pipe_reader(stdout_reader)?;
    let stderr = join_pipe_reader(stderr_reader)?;
    let duration_ms = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;

    Ok(RemoteManagementShellOutcome {
        exit_code: status.code(),
        stdout: String::from_utf8_lossy(&stdout.bytes).into_owned(),
        stderr: String::from_utf8_lossy(&stderr.bytes).into_owned(),
        duration_ms,
        timed_out,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    })
}

#[cfg(windows)]
fn shell_command(shell: Option<&str>, command: &str) -> Command {
    let mut process = Command::new(shell.unwrap_or("cmd"));
    process.arg("/C").arg(command);
    process
}

#[cfg(not(windows))]
fn shell_command(shell: Option<&str>, command: &str) -> Command {
    let mut process = Command::new(match shell {
        Some(shell) => shell,
        None => default_unix_shell(),
    });
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
            if keep < read {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }
    Ok(LimitedBytes { bytes, truncated })
}

fn join_pipe_reader(
    handle: Option<thread::JoinHandle<std::io::Result<LimitedBytes>>>,
) -> Result<LimitedBytes, RemoteManagementError> {
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
    if request.path.trim().is_empty() {
        return Err(RemoteManagementError::new(
            "invalid-path",
            "file read path must be non-empty",
        ));
    }
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
    let max_bytes = clamp_limit(
        request.max_bytes,
        DEFAULT_FILE_READ_BYTES,
        MAX_FILE_READ_BYTES,
    );
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
    if request.path.trim().is_empty() {
        return Err(RemoteManagementError::new(
            "invalid-path",
            "file write path must be non-empty",
        ));
    }
    let path = PathBuf::from(&request.path);
    if request.create_parent {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| {
                RemoteManagementError::new(
                    "create-parent-failed",
                    format!("create parent {}: {source}", parent.display()),
                )
            })?;
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true);
    if request.append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    let mut file = options.open(&path).map_err(|source| {
        RemoteManagementError::new("open-failed", format!("open {}: {source}", path.display()))
    })?;
    file.write_all(&request.bytes).map_err(|source| {
        RemoteManagementError::new(
            "write-failed",
            format!("write {}: {source}", path.display()),
        )
    })?;
    Ok(RemoteManagementFileWriteOutcome {
        path: path.display().to_string(),
        bytes_written: request.bytes.len() as u64,
        append: request.append,
    })
}

fn list_files(
    request: RemoteManagementFileListRequest,
) -> Result<(Vec<RemoteManagementFileEntry>, bool), RemoteManagementError> {
    if request.path.trim().is_empty() {
        return Err(RemoteManagementError::new(
            "invalid-path",
            "file list path must be non-empty",
        ));
    }
    let root = PathBuf::from(&request.path);
    let max_entries = clamp_limit(request.max_entries, DEFAULT_LIST_ENTRIES, MAX_LIST_ENTRIES);
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
        let mut children = Vec::new();
        let read_dir = fs::read_dir(&dir).map_err(|source| {
            RemoteManagementError::new(
                "read-dir-failed",
                format!("read directory {}: {source}", dir.display()),
            )
        })?;
        for child in read_dir {
            let child = child.map_err(|source| {
                RemoteManagementError::new(
                    "read-dir-entry-failed",
                    format!("read directory entry in {}: {source}", dir.display()),
                )
            })?;
            children.push(child.path());
        }
        children.sort();
        for child_path in children {
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

    Ok((entries, truncated))
}

fn file_entry(path: &Path, metadata: fs::Metadata) -> RemoteManagementFileEntry {
    let file_type = if metadata.file_type().is_symlink() {
        RemoteManagementFileType::Symlink
    } else if metadata.is_file() {
        RemoteManagementFileType::File
    } else if metadata.is_dir() {
        RemoteManagementFileType::Directory
    } else {
        RemoteManagementFileType::Other
    };
    RemoteManagementFileEntry {
        path: path.display().to_string(),
        file_type,
        len: metadata.is_file().then_some(metadata.len()),
        readonly: metadata.permissions().readonly(),
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

fn clamp_limit(requested: Option<usize>, default: usize, max: usize) -> usize {
    requested.unwrap_or(default).min(max)
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
    fn descriptor_exports_remote_management_operations() {
        let descriptor = remote_management_module_descriptor();
        assert_eq!(descriptor.module_id, REMOTE_MANAGEMENT_MODULE_ID);
        assert!(
            descriptor
                .supported_operations
                .contains(&REMOTE_MANAGEMENT_SHELL_RUN_OPERATION.to_string())
        );
        assert!(
            descriptor
                .supported_operations
                .contains(&REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION.to_string())
        );
        assert!(
            descriptor
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE
                    && route.operation == REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION
                    && route.auth == "admin")
        );
        assert!(
            descriptor
                .surfaces
                .declarative_resources
                .iter()
                .any(
                    |resource| resource.id == REMOTE_MANAGEMENT_COMPUTE_USE_SKILL_ID
                        && resource
                            .embedded_text
                            .as_deref()
                            .is_some_and(|text| text.contains("Remote Management Compute Use"))
                )
        );
    }

    #[test]
    fn shell_run_captures_stdout() {
        let outcome = run_shell(RemoteManagementShellRunRequest {
            command: "echo sinomo-remote-management".to_string(),
            cwd: None,
            shell: None,
            timeout_ms: Some(5_000),
            max_output_bytes: Some(1024),
            env: BTreeMap::new(),
        })
        .expect("run shell");

        assert_eq!(outcome.exit_code, Some(0));
        assert!(outcome.stdout.contains("sinomo-remote-management"));
        assert!(!outcome.timed_out);
    }

    #[test]
    fn file_write_read_and_list_round_trip() {
        let root = temp_root("files");
        let path = root.join("nested").join("hello.txt");
        let write = write_file(RemoteManagementFileWriteRequest {
            path: path.display().to_string(),
            bytes: b"hello remote".to_vec(),
            create_parent: true,
            append: false,
        })
        .expect("write file");
        assert_eq!(write.bytes_written, 12);

        let read = read_file(RemoteManagementFileReadRequest {
            path: path.display().to_string(),
            offset: None,
            max_bytes: Some(64),
        })
        .expect("read file");
        assert_eq!(read.text.as_deref(), Some("hello remote"));

        let (entries, truncated) = list_files(RemoteManagementFileListRequest {
            path: root.display().to_string(),
            recursive: true,
            max_entries: Some(10),
        })
        .expect("list files");
        assert!(!truncated);
        assert!(
            entries
                .iter()
                .any(|entry| entry.path.ends_with("hello.txt"))
        );
    }

    #[test]
    fn system_info_redacts_sensitive_env_names() {
        assert!(env_name_is_sensitive("OPENAI_API_KEY"));
        assert!(env_name_is_sensitive("cluster_secret_token"));
        assert!(!env_name_is_sensitive("PATH"));

        let info = system_info(RemoteManagementSystemInfoRequest {
            include_env: false,
            env_names: vec!["PATH".to_string()],
        });
        assert_eq!(info.os, std::env::consts::OS);
        assert!(info.env.contains_key("PATH"));
    }
}
