//! Sandbox command execution backends (local + SIFS-staged). The error,
//! output-sink, audit/event-emission, and process-plan contract types live in
//! the sinomo-sandbox contracts crate.

mod local;
mod local_fidelity;
mod local_process_plan;
mod options;
mod sifs;
mod sifs_process_plan;
mod staging;
mod wasi;

pub use local::*;
pub use options::*;
pub use sifs::*;
pub(crate) use wasi::*;
