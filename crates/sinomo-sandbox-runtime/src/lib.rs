//! WASIX/wasmtime execution backend for the sinomo agent sandbox.
//!
//! This crate carries the build-heavy wasm runtime (wasmtime + vendored
//! wasmer/WASIX). It implements the contracts defined in `sinomo-sandbox` and is
//! pulled in only when the `sandbox` feature is enabled by a downstream crate, so
//! the default build never compiles the wasm runtime.

mod execution;
mod internal;
mod runtime;

// Re-export the sandbox contracts at this crate's root so the moved backend
// modules keep resolving contract types via `crate::<Type>` (their original paths).
pub use sinomo_sandbox::*;

pub use execution::*;
pub use runtime::LocalWasiCommandRunner;

// Lift a couple of preview1-internal items to the crate root so the in-crate
// test suite can import them via `crate::` the same way it did before the split.
#[cfg(test)]
pub(crate) use runtime::preview1::{
    SINOMO_WASI_ERRNO_ROFS, sandbox_wasi_extension_read_host_path,
};

#[cfg(test)]
mod tests;
