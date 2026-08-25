//! Provider-neutral protocol for an external sandbox execution component.

mod host;
mod runner;

pub use host::*;
pub use runner::*;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use thiserror::Error;

use crate::{
    SandboxCommandRequest, SandboxLocalCommandOutcome, SandboxMountMode, SandboxOutputChunk,
    SandboxPath, SandboxResourceLimits, SandboxWasiAbi,
};

pub const SANDBOX_EXECUTOR_INTERFACE_ID: &str = "sinomo.sandbox-executor/v1";
pub const SANDBOX_EXECUTOR_INTERFACE_VERSION: &str = "1.0.0";
pub const SANDBOX_EXECUTOR_OPERATION: &str = "sandbox.execute";
pub const SANDBOX_EXECUTOR_OBJECT_VERSION: u16 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxExecutorRequestKind {
    ProbeCapabilities,
    RunModule,
    RunToolchainCommand,
    RunReplayFixture,
    ValidateComponent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SandboxExecutorRequest {
    ProbeCapabilities(SandboxExecutorProbeCapabilitiesRequest),
    RunModule(SandboxExecutorRunModuleRequest),
    RunToolchainCommand(SandboxExecutorRunToolchainCommandRequest),
    RunReplayFixture(SandboxExecutorRunReplayFixtureRequest),
    ValidateComponent(SandboxExecutorValidateComponentRequest),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorProbeCapabilitiesRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorRunModuleRequest {
    pub target_abi: SandboxWasiAbi,
    pub wasm: Vec<u8>,
    pub command: SandboxCommandRequest,
    pub options: SandboxExecutorOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorRunToolchainCommandRequest {
    pub module: SandboxExecutorResolvedCommandModule,
    pub command: SandboxCommandRequest,
    pub options: SandboxExecutorOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SandboxExecutorRunReplayFixtureRequest {
    pub data_dir_root: PathBuf,
    pub fixture: sinomo_module::ModuleReplayFixture,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorValidateComponentRequest {
    pub wasm: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorResolvedCommandModule {
    pub command_name: String,
    pub target_abi: SandboxWasiAbi,
    pub wasm: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorOptions {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preopened_dirs: Vec<SandboxExecutorHostPreopen>,
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
    #[serde(default = "default_true")]
    pub pinned_child_exec_only: bool,
    #[serde(default = "default_true")]
    pub instruction_metering: bool,
    #[serde(default = "default_true")]
    pub auto_asyncify: bool,
    #[serde(default)]
    pub fpcast_emulation: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_overrides: Vec<SandboxExecutorCommandOverride>,
}

impl Default for SandboxExecutorOptions {
    fn default() -> Self {
        let limits = SandboxResourceLimits::default();
        Self {
            preopened_dirs: Vec::new(),
            enable_virtual_devices: false,
            zero_device_bytes: 0,
            memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024),
            fuel: u64::MAX,
            input_limit_bytes: limits.max_input_bytes,
            output_limit_bytes: limits.max_output_bytes,
            workspace_limit_bytes: limits.max_workspace_bytes,
            file_count_limit: limits.max_file_count,
            pinned_child_exec_only: true,
            instruction_metering: true,
            auto_asyncify: true,
            fpcast_emulation: false,
            command_overrides: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorHostPreopen {
    pub host_path: PathBuf,
    pub guest_path: SandboxPath,
    pub mode: SandboxMountMode,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorCommandOverride {
    pub name: String,
    pub argv_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SandboxExecutorResponse {
    Capabilities(SandboxExecutorCapabilitiesResponse),
    RunModule(SandboxExecutorRunModuleResponse),
    RunToolchainCommand(SandboxExecutorRunModuleResponse),
    RunReplayFixture(SandboxExecutorRunReplayFixtureResponse),
    ValidateComponent(SandboxExecutorValidateComponentResponse),
    Error(SandboxExecutorErrorResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorCapabilitiesResponse {
    pub interface_id: String,
    pub interface_version: String,
    pub supported_requests: Vec<SandboxExecutorRequestKind>,
    pub provider_capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorRunModuleResponse {
    pub outcome: SandboxLocalCommandOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SandboxExecutorRunReplayFixtureResponse {
    pub report: sinomo_module::ModuleReplayReport,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorValidateComponentResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutorErrorResponse {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum SandboxExecutorProtocolError {
    #[error("SOBJ error: {0}")]
    Object(#[from] sinomo_core::object::Error),
    #[error("{0}")]
    Runtime(String),
}

pub fn encode_sandbox_executor<T: Serialize>(
    value: &T,
) -> Result<Vec<u8>, SandboxExecutorProtocolError> {
    Ok(sinomo_object_serde::to_bytes(
        value,
        SANDBOX_EXECUTOR_OBJECT_VERSION,
        LengthMode::U32,
    )?)
}

pub fn decode_sandbox_executor<T>(bytes: &[u8]) -> Result<T, SandboxExecutorProtocolError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(sinomo_object_serde::from_bytes(bytes)?)
}

pub fn encode_sandbox_executor_output(
    chunk: &SandboxOutputChunk,
) -> Result<Vec<u8>, SandboxExecutorProtocolError> {
    encode_sandbox_executor(chunk)
}

fn default_true() -> bool {
    true
}
