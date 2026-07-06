use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaemonApiError {
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

impl From<sinomo_core::service::ServiceError> for DaemonApiError {
    fn from(error: sinomo_core::service::ServiceError) -> Self {
        use sinomo_core::service::ServiceError;
        match error {
            ServiceError::Network(source) => source.into(),
            ServiceError::Kv(source) => source.into(),
            ServiceError::NotClustered(route) => DaemonApiError::ClusterReadOnly {
                detail: format!("service route {route} requires an initialized cluster"),
            },
            ServiceError::UnknownRoute(route) => DaemonApiError::Parse {
                detail: format!("unknown service route: {route}"),
            },
            ServiceError::Service(detail) => DaemonApiError::Parse { detail },
        }
    }
}

impl From<sinomo_core::node::NodeIdentityError> for DaemonApiError {
    fn from(error: sinomo_core::node::NodeIdentityError) -> Self {
        match error {
            sinomo_core::node::NodeIdentityError::Parse { detail } => Self::Parse { detail },
            sinomo_core::node::NodeIdentityError::Io { path, detail } => Self::Io { path, detail },
            sinomo_core::node::NodeIdentityError::Json { path, detail } => {
                Self::Json { path, detail }
            }
            sinomo_core::node::NodeIdentityError::Object { path, detail } => Self::Parse {
                detail: format!("node identity object error at {path}: {detail}"),
            },
        }
    }
}

impl From<sinomo_core::network::NetworkError> for DaemonApiError {
    fn from(error: sinomo_core::network::NetworkError) -> Self {
        match error {
            sinomo_core::network::NetworkError::AuthDenied => Self::AuthDenied,
            sinomo_core::network::NetworkError::AuthExpired => Self::AuthExpired,
            sinomo_core::network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_core::network::NetworkError::ClusterReadOnly { detail } => {
                Self::ClusterReadOnly { detail }
            }
            sinomo_core::network::NetworkError::InvalidClusterCommand { detail } => {
                Self::Parse { detail }
            }
            sinomo_core::network::NetworkError::Json { path, detail } => {
                Self::Json { path, detail }
            }
            sinomo_core::network::NetworkError::StorageIo { path, detail } => {
                Self::Io { path, detail }
            }
            sinomo_core::network::NetworkError::ObjectRpc { route, detail } => Self::Network {
                address: route,
                detail,
            },
            sinomo_core::network::NetworkError::Io { operation, detail } => Self::Network {
                address: operation,
                detail,
            },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_core::kv::KvError> for DaemonApiError {
    fn from(error: sinomo_core::kv::KvError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_core::local::LocalDataError> for DaemonApiError {
    fn from(error: sinomo_core::local::LocalDataError) -> Self {
        Self::Io {
            path: "sinomo data directory".to_string(),
            detail: error.to_string(),
        }
    }
}
