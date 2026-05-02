//! Explicit R0/R1 runtime boundary facade.
//!
//! R2 service crates should treat this crate as the public foundation surface
//! for trusted runtime contracts. The facade intentionally aggregates only R0
//! and R1 crates; it must never depend on R2 service or application crates.
//!
//! The crate does not add behavior. It makes the trust-ring cut visible in the
//! Cargo graph and gives architecture tests one concrete boundary to enforce.

/// R0 continuity-root contracts.
///
/// These are the smallest durable contracts that must remain understandable
/// across standby handoff, rollback, recovery, and future self-evolution.
pub mod r0 {
    pub use sinomo_core as core;
    pub use sinomo_core::*;
}

/// R1 trusted-runtime services and contracts.
///
/// R1 owns the local runtime substrate: events, nodes, DFS metadata, module
/// loading, policy, registry, rollout, sandboxing, supervision, local paths,
/// and command evaluation. R2 services may build on these crates, but R1 crates
/// must not depend back on R2 service crates.
pub mod r1 {
    pub use sinomo_api as api;
    pub use sinomo_device as device;
    pub use sinomo_dfs as dfs;
    pub use sinomo_eval as eval;
    pub use sinomo_events as events;
    pub use sinomo_local as local;
    pub use sinomo_module as module;
    pub use sinomo_network as network;
    pub use sinomo_node as node;
    pub use sinomo_policy as policy;
    pub use sinomo_registry as registry;
    pub use sinomo_rollout as rollout;
    pub use sinomo_sandbox as sandbox;
    pub use sinomo_supervision as supervision;
    pub use sinomo_sync as sync;
}

pub use r0::core;
pub use r1::{
    api, device, dfs, eval, events, local, module, network, node, policy, registry, rollout,
    sandbox, supervision, sync,
};
