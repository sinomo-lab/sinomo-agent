//! WASI/WASIX execution backends. The ABI + capability descriptors live in the
//! sinomo-sandbox contracts crate; this module holds the heavy wasmtime/wasmer
//! runners.

pub(crate) mod preview1;
pub(crate) mod wasix;

pub use preview1::LocalWasiCommandRunner;
pub(crate) use wasix::run_wasix_module;
#[cfg(feature = "wasix-runtime")]
pub(crate) use wasix::{
    wasix_sandbox_filesystem_preopen, wasix_sandbox_workspace_filesystem_preopens,
};
