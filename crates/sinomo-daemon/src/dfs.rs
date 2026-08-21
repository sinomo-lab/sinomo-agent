//! Daemon host integration for the distributed filesystem runtime.
//!
//! DFS semantics and resident runtime state live in `sinomo-dfs`. This module
//! exposes only the daemon-facing host adapters used by the control loop and
//! service registration.

mod runtime_host;

pub(crate) use runtime_host::*;
