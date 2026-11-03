//! Stable data-only contracts shared by Sinomo frontends and daemon services.
//!
//! Domain modules keep unrelated app surfaces namespaced while allowing the
//! lockstep app protocol to remain one versioned workspace package.

pub mod agent;
pub mod chat;
pub mod evolution;
pub mod skill;
pub mod update;
pub mod usage;
