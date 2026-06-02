//! Stable native-dylib ABI and typed SOBJ envelopes for the WASIX sandbox engine.
//!
//! This crate intentionally stays small. The host binary can depend on it without
//! compiling Wasmtime, Wasmer, WASIX, `virtual-fs`, or other execution runtime
//! machinery. Native WASIX modules also depend on the same crate so the FFI
//! boundary remains explicit and versioned.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_sandbox::{
    SandboxCommandRequest, SandboxLocalCommandOutcome, SandboxMountMode, SandboxOutputChunk,
    SandboxPath, SandboxResourceLimits, SandboxWasiAbi,
};
use thiserror::Error;

pub use sinomo_module_api::{
    NativeModuleBackend, NativeModuleCapabilitiesResponse, NativeModuleDescriptor,
    NativeModuleErrorResponse, NativeModuleInvocationRequest, NativeModuleInvocationResponse,
    NativeModuleProbeCapabilitiesRequest, NativeModuleRequest, NativeModuleResponse,
    SINOMO_NATIVE_MODULE_ABI_VERSION, SinomoModuleApiVersionFn as SinomoWasixApiVersionFn,
    SinomoModuleByteBuffer as SinomoWasixByteBuffer, SinomoModuleByteSlice as SinomoWasixByteSlice,
    SinomoModuleDescriptorFn as SinomoWasixDescriptorFn,
    SinomoModuleFreeBufferFn as SinomoWasixFreeBufferFn,
    SinomoModuleHostApiV1 as SinomoWasixHostApiV1,
    SinomoModuleHostIsCancelledFn as SinomoWasixHostIsCancelledFn,
    SinomoModuleHostLogFn as SinomoWasixHostLogFn,
    SinomoModuleHostOutputFn as SinomoWasixHostOutputChunkFn,
    SinomoModuleInitFn as SinomoWasixInitFn, SinomoModuleInvokeFn as SinomoWasixInvokeFn,
    SinomoModuleShutdownFn as SinomoWasixShutdownFn,
    SinomoModuleStatusCode as SinomoWasixStatusCode,
};

pub const SINOMO_SANDBOX_WASIX_MODULE_ID: &str = "module.sandbox.wasix";
pub const SINOMO_SANDBOX_WASIX_NATIVE_ABI_VERSION: u32 = SINOMO_NATIVE_MODULE_ABI_VERSION;
pub const SINOMO_SANDBOX_WASIX_INTERFACE_VERSION: &str = "sinomo.sandbox.wasix.v1";
pub const SINOMO_SANDBOX_WASIX_OPERATION: &str = "sandbox.wasix";
pub const SINOMO_SANDBOX_WASIX_OBJECT_VERSION: u16 = 1;

pub const SINOMO_WASIX_STATUS_OK: SinomoWasixStatusCode =
    sinomo_module_api::SINOMO_MODULE_STATUS_OK;
pub const SINOMO_WASIX_STATUS_INVALID_ARGUMENT: SinomoWasixStatusCode =
    sinomo_module_api::SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
pub const SINOMO_WASIX_STATUS_UNSUPPORTED_ABI: SinomoWasixStatusCode =
    sinomo_module_api::SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
pub const SINOMO_WASIX_STATUS_RUNTIME_ERROR: SinomoWasixStatusCode =
    sinomo_module_api::SINOMO_MODULE_STATUS_RUNTIME_ERROR;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixNativeDescriptor {
    pub module_id: String,
    pub module_version: String,
    pub native_abi_version: u32,
    pub interface_version: String,
    pub platform_triple: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_requests: Vec<SandboxWasixRequestKind>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxWasixRequestKind {
    ProbeCapabilities,
    RunModule,
    RunToolchainCommand,
    RunReplayFixture,
    ValidateComponent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SandboxWasixNativeRequest {
    ProbeCapabilities(SandboxWasixProbeCapabilitiesRequest),
    RunModule(SandboxWasixRunModuleRequest),
    RunToolchainCommand(SandboxWasixRunToolchainCommandRequest),
    RunReplayFixture(SandboxWasixRunReplayFixtureRequest),
    ValidateComponent(SandboxWasixValidateComponentRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct SandboxWasixProbeCapabilitiesRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixRunModuleRequest {
    pub target_abi: SandboxWasiAbi,
    pub wasm: Vec<u8>,
    pub command: SandboxCommandRequest,
    pub options: SandboxWasixRunModuleOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixRunToolchainCommandRequest {
    pub module: SandboxWasixResolvedCommandModule,
    pub command: SandboxCommandRequest,
    pub options: SandboxWasixRunModuleOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SandboxWasixRunReplayFixtureRequest {
    pub data_dir_root: PathBuf,
    pub fixture: sinomo_module::ModuleReplayFixture,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixValidateComponentRequest {
    pub wasm: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixResolvedCommandModule {
    pub command_name: String,
    pub target_abi: SandboxWasiAbi,
    pub wasm: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixRunModuleOptions {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preopened_dirs: Vec<SandboxWasixHostPreopen>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory_host_path: Option<PathBuf>,
    #[serde(default)]
    pub enable_virtual_devices: bool,
    #[serde(default)]
    pub zero_device_bytes: u64,
    #[serde(default)]
    pub memory_limit_bytes: u64,
    #[serde(default)]
    pub fuel: u64,
    #[serde(default)]
    pub input_limit_bytes: u64,
    #[serde(default)]
    pub output_limit_bytes: u64,
    #[serde(default)]
    pub workspace_limit_bytes: u64,
    #[serde(default)]
    pub file_count_limit: u64,
    #[serde(default)]
    pub allow_preview1_pinned_subprocesses: bool,
    #[serde(default = "default_true")]
    pub wasix_pinned_child_exec_only: bool,
    #[serde(default = "default_true")]
    pub wasix_instruction_metering: bool,
    #[serde(default = "default_true")]
    pub wasix_auto_asyncify: bool,
    #[serde(default)]
    pub wasix_fpcast_emu: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
}

impl Default for SandboxWasixRunModuleOptions {
    fn default() -> Self {
        let limits = SandboxResourceLimits::default();
        Self {
            preopened_dirs: Vec::new(),
            working_directory_host_path: None,
            enable_virtual_devices: false,
            zero_device_bytes: 0,
            memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024),
            fuel: u64::MAX,
            input_limit_bytes: limits.max_input_bytes,
            output_limit_bytes: limits.max_output_bytes,
            workspace_limit_bytes: limits.max_workspace_bytes,
            file_count_limit: limits.max_file_count,
            allow_preview1_pinned_subprocesses: false,
            wasix_pinned_child_exec_only: true,
            wasix_instruction_metering: true,
            wasix_auto_asyncify: true,
            wasix_fpcast_emu: false,
            wasix_command_overrides: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixHostPreopen {
    pub host_path: PathBuf,
    pub guest_path: SandboxPath,
    pub mode: SandboxMountMode,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixCommandOverride {
    pub name: String,
    pub argv_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SandboxWasixNativeResponse {
    Capabilities(SandboxWasixCapabilitiesResponse),
    RunModule(SandboxWasixRunModuleResponse),
    RunToolchainCommand(SandboxWasixRunModuleResponse),
    RunReplayFixture(SandboxWasixRunReplayFixtureResponse),
    ValidateComponent(SandboxWasixValidateComponentResponse),
    Error(SandboxWasixNativeErrorResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixCapabilitiesResponse {
    pub descriptor: SandboxWasixNativeDescriptor,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixRunModuleResponse {
    pub outcome: SandboxLocalCommandOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SandboxWasixRunReplayFixtureResponse {
    pub report: sinomo_module::ModuleReplayReport,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct SandboxWasixValidateComponentResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWasixNativeErrorResponse {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum SandboxWasixApiError {
    #[error("SOBJ error: {0}")]
    Object(#[from] sinomo_object::Error),
    #[error("invalid native WASIX ABI version {actual}; expected {expected}")]
    InvalidAbiVersion { expected: u32, actual: u32 },
    #[error("{0}")]
    Runtime(String),
}

pub fn encode_sobj<T: Serialize>(value: &T) -> Result<Vec<u8>, SandboxWasixApiError> {
    Ok(sinomo_object_serde::to_bytes(
        value,
        SINOMO_SANDBOX_WASIX_OBJECT_VERSION,
        LengthMode::U32,
    )?)
}

pub fn decode_sobj<T>(bytes: &[u8]) -> Result<T, SandboxWasixApiError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(sinomo_object_serde::from_bytes(bytes)?)
}

pub fn encode_output_chunk(chunk: &SandboxOutputChunk) -> Result<Vec<u8>, SandboxWasixApiError> {
    encode_sobj(chunk)
}

fn default_true() -> bool {
    true
}
