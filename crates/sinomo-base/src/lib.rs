//! Sinomo base contracts.
//!
//! This crate owns stable cross-service schemas and intentionally avoids
//! service-specific runtime dependencies. It sits at the bottom of the R0
//! ring; crates outside R0 consume it through the `sinomo-core` facade.

pub mod cluster;
pub mod domain;
pub mod event;
pub mod id;
pub mod scalar;

pub use cluster::*;
pub use domain::*;
pub use event::*;
pub use id::*;
pub use scalar::*;
