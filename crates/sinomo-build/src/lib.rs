//! Cluster build-service contracts and deterministic artifact preparation.
//!
//! This crate owns build inputs, target support, builder placement, queued-job
//! protocol types, runtime service implementation, and immutable artifact
//! bundles. Building produces verified artifacts; rollout alone decides where
//! and whether those artifacts are activated.

mod artifact;
mod client_protocol;
mod deployment_source;
mod executor;
mod log;
mod manager;
mod module;
mod paths;
mod process;
mod protocol;
mod resource;
mod service;
mod source;
mod target;

pub use artifact::*;
pub use client_protocol::*;
pub use deployment_source::{
    DeploymentDependencyPin, DeploymentSourcePins, prepare_deployment_source,
    validate_deployment_source,
};
pub use executor::*;
pub use log::{BUILD_LOG_MAX_BYTES, BUILD_LOG_ROUTE, BuildLogRequest, BuildLogResponse};
pub use manager::*;
pub use module::*;
pub use paths::*;
pub use protocol::*;
pub use resource::*;
pub use service::{
    BuildService, component_factory, register_services, register_services_for_data_dir,
};
pub use source::*;
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
