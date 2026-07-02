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

impl From<sinomo_service::ServiceError> for DaemonError {
    fn from(error: sinomo_service::ServiceError) -> Self {
        use sinomo_service::ServiceError;
        match error {
            ServiceError::Network(source) => source.into(),
            ServiceError::Kv(source) => source.into(),
            ServiceError::NotClustered(route) => DaemonError::ClusterReadOnly {
                detail: format!("service route {route} requires an initialized cluster"),
            },
            ServiceError::UnknownRoute(route) => DaemonError::Parse {
                detail: format!("unknown service route: {route}"),
            },
            ServiceError::Service(detail) => DaemonError::Parse { detail },
        }
    }
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
            sinomo_network::NetworkError::AuthDenied => Self::AuthDenied,
            sinomo_network::NetworkError::AuthExpired => Self::AuthExpired,
            sinomo_network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_network::NetworkError::ClusterReadOnly { detail } => {
                Self::ClusterReadOnly { detail }
            }
            sinomo_network::NetworkError::InvalidClusterCommand { detail } => {
                Self::Parse { detail }
            }
            sinomo_network::NetworkError::Json { path, detail } => Self::Json { path, detail },
            sinomo_network::NetworkError::StorageIo { path, detail } => Self::Io { path, detail },
            sinomo_network::NetworkError::ObjectRpc { route, detail } => Self::Network {
                address: route,
                detail,
            },
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

impl From<sinomo_dfs::ContentStoreError> for DaemonError {
    fn from(error: sinomo_dfs::ContentStoreError) -> Self {
        match error {
            sinomo_dfs::ContentStoreError::Io { path, source } => Self::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_dfs::DfsError> for DaemonError {
    fn from(error: sinomo_dfs::DfsError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_dfs::DfsDistributedError> for DaemonError {
    fn from(error: sinomo_dfs::DfsDistributedError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_dfs::DfsPosixError> for DaemonError {
    fn from(error: sinomo_dfs::DfsPosixError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_sandbox::SandboxStoreError> for DaemonError {
    fn from(error: sinomo_sandbox::SandboxStoreError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_sandbox::SandboxFsError> for DaemonError {
    fn from(error: sinomo_sandbox::SandboxFsError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_sandbox::SandboxToolchainError> for DaemonError {
    fn from(error: sinomo_sandbox::SandboxToolchainError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_sandbox::SandboxEventEmissionError> for DaemonError {
    fn from(error: sinomo_sandbox::SandboxEventEmissionError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_sandbox::SandboxRecordedHttpFetchError> for DaemonError {
    fn from(error: sinomo_sandbox::SandboxRecordedHttpFetchError) -> Self {
        match error {
            sinomo_sandbox::SandboxRecordedHttpFetchError::Network(
                sinomo_sandbox::SandboxNetworkError::Transport { detail },
            ) => Self::Network {
                address: "sandbox mediated http".to_string(),
                detail,
            },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_events::EventStoreError> for DaemonError {
    fn from(error: sinomo_events::EventStoreError) -> Self {
        Self::Io {
            path: "events/event-log.jsonl".to_string(),
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_registry::RegistryError> for DaemonError {
    fn from(error: sinomo_registry::RegistryError) -> Self {
        Self::Parse {
            detail: error.to_string(),
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

impl From<sinomo_observe::ObserveError> for DaemonError {
    fn from(error: sinomo_observe::ObserveError) -> Self {
        match error {
            sinomo_observe::ObserveError::Io { path, detail } => Self::Io { path, detail },
            sinomo_observe::ObserveError::Json { path, detail } => Self::Json { path, detail },
            sinomo_observe::ObserveError::Parse { detail }
            | sinomo_observe::ObserveError::Runtime { detail }
            | sinomo_observe::ObserveError::ModuleState { detail } => Self::Parse { detail },
        }
    }
}

impl From<sinomo_module::NativeDylibModuleInstallError> for DaemonError {
    fn from(error: sinomo_module::NativeDylibModuleInstallError) -> Self {
        match error {
            sinomo_module::NativeDylibModuleInstallError::Io { path, detail } => {
                Self::Io { path, detail }
            }
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}
