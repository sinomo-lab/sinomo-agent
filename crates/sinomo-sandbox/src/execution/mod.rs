//! Sandbox command execution *contracts*: error types, the output-sink trait,
//! audit/event-emission helpers, and process-plan request/response/validation
//! types. The native WASIX backend lives in the WASIX module crate; the
//! Docker/Podman backend is lightweight host-process orchestration over the
//! local sandbox store.

mod audit;
mod container;
mod errors;
mod output;
mod process;

pub use audit::*;
pub use container::*;
pub use errors::*;
pub use output::*;
pub use process::*;
