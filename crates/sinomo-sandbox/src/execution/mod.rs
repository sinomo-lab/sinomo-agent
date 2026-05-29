//! Sandbox command execution *contracts*: error types, the output-sink trait,
//! audit/event-emission helpers, and process-plan request/response/validation
//! types. The wasmtime/WASIX execution backends that run these live in the
//! `sinomo-sandbox-runtime` crate, behind the `sandbox` feature.

mod audit;
mod errors;
mod output;
mod process;

pub use audit::*;
pub use errors::*;
pub use output::*;
pub use process::*;
