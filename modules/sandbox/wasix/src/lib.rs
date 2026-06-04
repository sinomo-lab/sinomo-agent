//! WASIX/wasmtime execution backend for the sinomo agent sandbox.
//!
//! This crate carries the build-heavy wasm runtime (wasmtime + vendored
//! wasmer/WASIX). It implements the contracts defined in `sinomo-sandbox` and is
//! compiled with the `wasix-runtime` feature when a real native-dylib runtime
//! module is needed. The default build only provides the native ABI descriptor
//! and runtime-disabled error responses.

#[cfg(feature = "wasix-runtime")]
mod backend;
#[cfg(feature = "wasix-runtime")]
mod execution;
mod ffi;
#[cfg(feature = "wasix-runtime")]
mod internal;
#[cfg(feature = "wasix-runtime")]
mod module_runner;
#[cfg(feature = "wasix-runtime")]
mod runtime;

// Re-export the sandbox contracts at this crate's root so the moved backend
// modules keep resolving contract types via `crate::<Type>` (their original paths).
pub use sinomo_sandbox::*;

#[cfg(feature = "wasix-runtime")]
pub use backend::RuntimeSandboxBackend;
#[cfg(feature = "wasix-runtime")]
pub use execution::*;
#[cfg(feature = "wasix-runtime")]
pub use module_runner::{
    SandboxModuleComponentContractError, SandboxModuleReplayError, SandboxWasmModuleArtifactRunner,
    run_sandbox_wasm_module_replay_fixture, validate_sinomo_module_v1_component_contract,
};
#[cfg(all(test, feature = "wasix-runtime"))]
mod tests;
