use super::*;

#[derive(Debug)]
pub enum CliError {
    Usage,
    AuthDenied,
    AuthExpired,
    AuthProvider {
        detail: String,
    },
    NoActiveJob,
    AmbiguousJob {
        job_ids: Vec<String>,
    },
    InvalidJobState {
        job_id: String,
        status: String,
        expected: String,
    },
    ClusterOrphan,
    MissingArtifact {
        artifact: String,
    },
    Parse {
        detail: String,
    },
    Io {
        path: String,
        detail: String,
    },
    Json {
        path: String,
        detail: String,
    },
    LocalData(sinomo_runtime::local::LocalDataError),
    EventStore(sinomo_runtime::events::EventStoreError),
    DfsMetadata(sinomo_runtime::dfs::MetadataBackendError),
    DfsPin(sinomo_runtime::dfs::DfsPinStoreError),
    Kv(KvError),
    LanguageModel(LanguageModelError),
    Evolution(sinomo_evolution::EvolutionError),
    Eval(sinomo_runtime::eval::EvalError),
    DaemonAlreadyRunning {
        lock_file: String,
        detail: String,
    },
    DaemonNotRunning {
        detail: String,
    },
    Network {
        address: String,
        detail: String,
    },
    Process {
        command: String,
        status: i32,
        stderr: String,
    },
}

impl CliError {
    pub(super) fn stdio(source: io::Error) -> Self {
        Self::Io {
            path: "stdio".to_string(),
            detail: source.to_string(),
        }
    }
}

impl From<sinomo_runtime::local::LocalDataError> for CliError {
    fn from(error: sinomo_runtime::local::LocalDataError) -> Self {
        Self::LocalData(error)
    }
}

impl From<sinomo_runtime::events::EventStoreError> for CliError {
    fn from(error: sinomo_runtime::events::EventStoreError) -> Self {
        Self::EventStore(error)
    }
}

impl From<sinomo_runtime::dfs::MetadataBackendError> for CliError {
    fn from(error: sinomo_runtime::dfs::MetadataBackendError) -> Self {
        Self::DfsMetadata(error)
    }
}

impl From<sinomo_runtime::dfs::DfsPinStoreError> for CliError {
    fn from(error: sinomo_runtime::dfs::DfsPinStoreError) -> Self {
        Self::DfsPin(error)
    }
}

impl From<KvError> for CliError {
    fn from(error: KvError) -> Self {
        Self::Kv(error)
    }
}

impl From<network::NetworkError> for CliError {
    fn from(error: network::NetworkError) -> Self {
        match error {
            network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            network::NetworkError::Json { path, detail } => Self::Json { path, detail },
            network::NetworkError::StorageIo { path, detail } => Self::Io { path, detail },
            network::NetworkError::Kv(error) => Self::Kv(error),
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_runtime::daemon::DaemonError> for CliError {
    fn from(error: sinomo_runtime::daemon::DaemonError) -> Self {
        match error {
            sinomo_runtime::daemon::DaemonError::AuthDenied => Self::AuthDenied,
            sinomo_runtime::daemon::DaemonError::AuthExpired => Self::AuthExpired,
            sinomo_runtime::daemon::DaemonError::Io { path, detail } => Self::Io { path, detail },
            sinomo_runtime::daemon::DaemonError::Json { path, detail } => {
                Self::Json { path, detail }
            }
            sinomo_runtime::daemon::DaemonError::Parse { detail } => Self::Parse { detail },
            sinomo_runtime::daemon::DaemonError::Network { address, detail } => {
                Self::Network { address, detail }
            }
            sinomo_runtime::daemon::DaemonError::AlreadyRunning { lock_file, detail } => {
                Self::DaemonAlreadyRunning { lock_file, detail }
            }
            sinomo_runtime::daemon::DaemonError::ClusterOrphan => Self::ClusterOrphan,
        }
    }
}

impl From<sinomo_runtime::node::NodeIdentityError> for CliError {
    fn from(error: sinomo_runtime::node::NodeIdentityError) -> Self {
        match error {
            sinomo_runtime::node::NodeIdentityError::Parse { detail } => Self::Parse { detail },
            sinomo_runtime::node::NodeIdentityError::Io { path, detail } => {
                Self::Io { path, detail }
            }
            sinomo_runtime::node::NodeIdentityError::Json { path, detail } => {
                Self::Json { path, detail }
            }
        }
    }
}

impl From<LanguageModelError> for CliError {
    fn from(error: LanguageModelError) -> Self {
        Self::LanguageModel(error)
    }
}

impl From<sinomo_evolution::EvolutionError> for CliError {
    fn from(error: sinomo_evolution::EvolutionError) -> Self {
        Self::Evolution(error)
    }
}

impl From<sinomo_evolution::EvolutionPersistenceError> for CliError {
    fn from(error: sinomo_evolution::EvolutionPersistenceError) -> Self {
        match error {
            sinomo_evolution::EvolutionPersistenceError::Io { path, detail } => {
                Self::Io { path, detail }
            }
            sinomo_evolution::EvolutionPersistenceError::Json { path, detail } => {
                Self::Json { path, detail }
            }
        }
    }
}

impl From<RolloutError> for CliError {
    fn from(error: RolloutError) -> Self {
        match error {
            RolloutError::Io { path, detail } => Self::Io { path, detail },
            RolloutError::Json { path, detail } => Self::Json { path, detail },
            RolloutError::Process {
                command,
                status,
                stderr,
            } => Self::Process {
                command,
                status,
                stderr,
            },
            RolloutError::MissingArtifact { artifact } => Self::MissingArtifact { artifact },
        }
    }
}

impl From<sinomo_runtime::eval::EvalError> for CliError {
    fn from(error: sinomo_runtime::eval::EvalError) -> Self {
        Self::Eval(error)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(formatter, "unknown command; run `sinomo help`"),
            Self::AuthDenied => write!(formatter, "device authorization was denied"),
            Self::AuthExpired => write!(formatter, "device authorization expired"),
            Self::AuthProvider { detail } => write!(formatter, "provider auth failed: {detail}"),
            Self::NoActiveJob => write!(formatter, "no active evolution job"),
            Self::AmbiguousJob { job_ids } => {
                write!(
                    formatter,
                    "multiple active evolution jobs; rerun with --job ({})",
                    job_ids.join(", ")
                )
            }
            Self::InvalidJobState {
                job_id,
                status,
                expected,
            } => write!(
                formatter,
                "evolution job {job_id} is {status}; expected {expected}"
            ),
            Self::ClusterOrphan => write!(
                formatter,
                "this node is not part of a cluster; run `sinomo cluster init` or `sinomo cluster join <invite>`"
            ),
            Self::MissingArtifact { artifact } => {
                write!(formatter, "{artifact} has not been written")
            }
            Self::Parse { detail } => write!(formatter, "failed to parse command: {detail}"),
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::EventStore(error) => write!(formatter, "{error}"),
            Self::DfsMetadata(error) => write!(formatter, "{error}"),
            Self::DfsPin(error) => write!(formatter, "{error}"),
            Self::Kv(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
            Self::Evolution(error) => write!(formatter, "{error}"),
            Self::Eval(error) => write!(formatter, "{error}"),
            Self::DaemonAlreadyRunning { lock_file, detail } => {
                write!(
                    formatter,
                    "sinomo daemon is already running or locked at {lock_file}: {detail}"
                )
            }
            Self::DaemonNotRunning { detail } => {
                write!(formatter, "sinomo daemon is not running: {detail}")
            }
            Self::Network { address, detail } => {
                write!(formatter, "network error at {address}: {detail}")
            }
            Self::Process {
                command,
                status,
                stderr,
            } => write!(
                formatter,
                "process `{command}` failed with status {status}: {stderr}"
            ),
        }
    }
}

impl std::error::Error for CliError {}
