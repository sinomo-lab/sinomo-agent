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
    #[error("sinomo daemon is not running: {detail}")]
    NotRunning { detail: String },
    #[error(
        "this node is not part of a cluster; run `sinomo cluster init` or `sinomo cluster join <invite>`"
    )]
    ClusterOrphan,
    #[error("cluster is read-only: {detail}")]
    ClusterReadOnly { detail: String },
}

impl From<sinomo_node::NodeIdentityError> for DaemonError {
    fn from(error: sinomo_node::NodeIdentityError) -> Self {
        match error {
            sinomo_node::NodeIdentityError::Parse { detail } => Self::Parse { detail },
            sinomo_node::NodeIdentityError::Io { path, detail } => Self::Io { path, detail },
            sinomo_node::NodeIdentityError::Json { path, detail } => Self::Json { path, detail },
            sinomo_node::NodeIdentityError::Object { path, detail } => Self::Parse {
                detail: format!("node identity object error at {path}: {detail}"),
            },
        }
    }
}

impl From<sinomo_network::NetworkError> for DaemonError {
    fn from(error: sinomo_network::NetworkError) -> Self {
        match error {
            sinomo_network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_network::NetworkError::ClusterReadOnly { detail } => {
                Self::ClusterReadOnly { detail }
            }
            sinomo_network::NetworkError::Json { path, detail } => Self::Json { path, detail },
            sinomo_network::NetworkError::StorageIo { path, detail } => Self::Io { path, detail },
            sinomo_network::NetworkError::Io { operation, detail } => Self::Network {
                address: operation,
                detail,
            },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_kv::KvError> for DaemonError {
    fn from(error: sinomo_kv::KvError) -> Self {
        Self::Parse {
            detail: error.to_string(),
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

impl From<sinomo_dfs::DfsExposeStoreError> for DaemonError {
    fn from(error: sinomo_dfs::DfsExposeStoreError) -> Self {
        match error {
            sinomo_dfs::DfsExposeStoreError::Io { path, source } => Self::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            },
            sinomo_dfs::DfsExposeStoreError::Serde(source) => Self::Json {
                path: "dfs expose config".to_string(),
                detail: source.to_string(),
            },
            sinomo_dfs::DfsExposeStoreError::InvalidConfig(detail) => Self::Parse { detail },
        }
    }
}

impl From<sinomo_dfs::MetadataBackendError> for DaemonError {
    fn from(error: sinomo_dfs::MetadataBackendError) -> Self {
        match error {
            sinomo_dfs::MetadataBackendError::Io { path, source } => Self::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_rollout::RolloutError> for DaemonError {
    fn from(error: sinomo_rollout::RolloutError) -> Self {
        Self::Io {
            path: "sinomo rollout".to_string(),
            detail: error.to_string(),
        }
    }
}
