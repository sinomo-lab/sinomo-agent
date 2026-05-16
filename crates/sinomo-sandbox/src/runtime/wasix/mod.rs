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
mod setup;
#[cfg(all(test, feature = "wasix-runtime"))]
mod tests;
#[cfg(feature = "wasix-runtime")]
mod tunables;

#[cfg(all(test, feature = "wasix-runtime"))]
use process::sandbox_wasix_capabilities;
#[cfg(all(test, feature = "wasix-runtime"))]
use process::wasix_command_exceeded_requested_timeout;
pub(crate) use runner::run_wasix_module;
