//! Sandbox runtime *contracts*: the WASI ABI selector and capability descriptors.
//! The wasmtime/wasmer execution backends live in the native WASIX module
//! crate, behind the `sandbox` feature.

mod abi;
mod capabilities;

pub use abi::{SandboxWasiAbi, default_sandbox_wasi_abi};
pub use capabilities::{
    SandboxRuntimeBudgetCapability, SandboxRuntimeProcessCapability,
    SandboxRuntimeVirtualDeviceCapability, sandbox_runtime_budget_capabilities,
    sandbox_runtime_budget_capabilities_for_abi, sandbox_runtime_disables_ambient_network,
    sandbox_runtime_mvp_virtual_device_capabilities, sandbox_runtime_process_capabilities,
    sandbox_runtime_process_capabilities_for_abi, sandbox_runtime_supports_child_processes,
    sandbox_runtime_supports_child_processes_for_abi,
    sandbox_runtime_supports_command_capability_for_abi,
    sandbox_runtime_supports_mvp_virtual_devices,
};
