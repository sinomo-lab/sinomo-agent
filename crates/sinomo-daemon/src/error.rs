use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("device authorization was denied")]
    AuthDenied,
    #[error("device authorization expired")]
    AuthExpired,
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("json error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("failed to parse command: {detail}")]
    Parse { detail: String },
    #[error("network error at {address}: {detail}")]
    Network { address: String, detail: String },
    #[error("sinomo daemon is already running or locked at {lock_file}: {detail}")]
    AlreadyRunning { lock_file: String, detail: String },
    #[error(
        "this node is not part of a cluster; run `sinomo cluster init` or `sinomo cluster join <invite>`"
    )]
    ClusterOrphan,
}

impl From<sinomo_node::NodeIdentityError> for DaemonError {
    fn from(error: sinomo_node::NodeIdentityError) -> Self {
        match error {
            sinomo_node::NodeIdentityError::Parse { detail } => Self::Parse { detail },
            sinomo_node::NodeIdentityError::Io { path, detail } => Self::Io { path, detail },
            sinomo_node::NodeIdentityError::Json { path, detail } => Self::Json { path, detail },
        }
    }
}

impl From<sinomo_network::NetworkError> for DaemonError {
    fn from(error: sinomo_network::NetworkError) -> Self {
        match error {
            sinomo_network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_network::NetworkError::Json { path, detail } => Self::Json { path, detail },
            sinomo_network::NetworkError::StorageIo { path, detail } => Self::Io { path, detail },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_local::LocalDataError> for DaemonError {
    fn from(error: sinomo_local::LocalDataError) -> Self {
        Self::Io {
            path: "sinomo data directory".to_string(),
            detail: error.to_string(),
        }
    }
}
