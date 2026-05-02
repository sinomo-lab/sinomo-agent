//! Shared Sinomo contracts.
//!
//! This crate owns stable cross-service schemas and intentionally avoids
//! service-specific runtime dependencies.

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
