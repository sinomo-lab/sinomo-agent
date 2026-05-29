mod runner;

#[cfg(feature = "wasix-runtime")]
mod budget;
#[cfg(feature = "wasix-runtime")]
mod memory;
#[cfg(feature = "wasix-runtime")]
mod metering;
#[cfg(feature = "wasix-runtime")]
mod network;
#[cfg(feature = "wasix-runtime")]
mod output;
#[cfg(feature = "wasix-runtime")]
mod preopen;
#[cfg(feature = "wasix-runtime")]
mod process;
#[cfg(feature = "wasix-runtime")]
mod sandbox_fs;
#[cfg(feature = "wasix-runtime")]
mod setup;
#[cfg(all(test, feature = "wasix-runtime"))]
mod tests;
#[cfg(feature = "wasix-runtime")]
mod tunables;

#[cfg(all(test, feature = "wasix-runtime"))]
use process::sandbox_wasix_capabilities;
pub(crate) use runner::run_wasix_module;
#[cfg(feature = "wasix-runtime")]
pub(crate) use sandbox_fs::{
    wasix_sandbox_filesystem_preopen, wasix_sandbox_workspace_filesystem_preopens,
};
