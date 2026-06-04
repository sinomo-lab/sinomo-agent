//! WASIX execution backend. The ABI + capability descriptors live in the
//! sinomo-sandbox contracts crate; this module holds the heavy wasmer/WASIX
//! runner.

pub(crate) mod wasix;

pub(crate) use wasix::run_wasix_module;
#[cfg(feature = "wasix-runtime")]
pub(crate) use wasix::{
    wasix_sandbox_filesystem_preopen, wasix_sandbox_workspace_filesystem_preopens,
};
