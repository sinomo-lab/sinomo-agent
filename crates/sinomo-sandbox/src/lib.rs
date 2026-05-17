//! SIFS-backed agent sandbox contracts and local harness primitives.

mod commands;
mod constants;
mod events;
mod execution;
mod fidelity;
mod filesystem;
mod harness;
mod http;
mod internal;
mod model;
mod paths;
mod policy;
mod runtime;
mod storage;
mod store;
mod toolchain;

pub use commands::*;
pub use constants::*;
pub use events::*;
pub use execution::*;
pub use fidelity::*;
pub use filesystem::*;
pub use harness::*;
pub use http::*;
pub use model::*;
pub use paths::*;
pub use policy::*;
pub use runtime::LocalWasiCommandRunner;
#[cfg(test)]
pub(crate) use runtime::preview1::{
    SINOMO_WASI_ERRNO_NOTCAPABLE, SINOMO_WASI_ERRNO_ROFS, sandbox_wasi_extension_read_host_path,
};
pub use runtime::{
    SandboxRuntimeBudgetCapability, SandboxRuntimeProcessCapability,
    SandboxRuntimeVirtualDeviceCapability, SandboxWasiAbi, default_sandbox_wasi_abi,
    sandbox_runtime_budget_capabilities, sandbox_runtime_budget_capabilities_for_abi,
    sandbox_runtime_disables_ambient_network, sandbox_runtime_mvp_virtual_device_capabilities,
    sandbox_runtime_process_capabilities, sandbox_runtime_process_capabilities_for_abi,
    sandbox_runtime_supports_child_processes, sandbox_runtime_supports_child_processes_for_abi,
    sandbox_runtime_supports_command_capability_for_abi,
    sandbox_runtime_supports_mvp_virtual_devices,
};
pub use storage::register_storage_paths;
pub use store::*;
pub use toolchain::*;

#[cfg(test)]
mod tests;
