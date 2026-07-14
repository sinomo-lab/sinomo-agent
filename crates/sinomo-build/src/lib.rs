//! Cluster build-service contracts and deterministic artifact preparation.
//!
//! This crate owns build inputs, target support, builder placement, queued-job
//! protocol types, and immutable artifact bundles. It deliberately has no
//! dependency on rollout: building produces verified artifacts; rollout decides
//! where and whether those artifacts are activated.

mod artifact;
mod executor;
mod module;
mod paths;
mod protocol;
mod target;

pub use artifact::*;
pub use executor::*;
pub use module::*;
pub use paths::*;
pub use protocol::*;
pub use target::*;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("invalid build value: {detail}")]
    InvalidValue { detail: String },
    #[error("invalid build artifact bundle: {detail}")]
    InvalidBundle { detail: String },
    #[error("build artifact hash mismatch for {artifact}: expected {expected}, got {actual}")]
    HashMismatch {
        artifact: String,
        expected: String,
        actual: String,
    },
    #[error("build I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("build command `{command}` failed with status {status}: {stderr}")]
    Process {
        command: String,
        status: i32,
        stderr: String,
    },
}

pub type Result<T> = std::result::Result<T, BuildError>;
