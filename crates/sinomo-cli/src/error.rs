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
    ClusterReadOnly {
        detail: String,
    },
    SandboxPolicyDenied {
        detail: String,
    },
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
    DfsExpose(sinomo_runtime::dfs::DfsExposeStoreError),
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

impl From<sinomo_agent::AgentError> for CliError {
    fn from(error: sinomo_agent::AgentError) -> Self {
        match error {
            sinomo_agent::AgentError::EventStore(error) => Self::EventStore(error),
            sinomo_agent::AgentError::Model(error) => Self::LanguageModel(error),
            sinomo_agent::AgentError::ModelRequest { detail }
            | sinomo_agent::AgentError::ModelResponse { detail } => Self::Parse { detail },
            sinomo_agent::AgentError::TraceStore { detail } => Self::Io {
                path: "agent trace store".to_string(),
                detail,
            },
            sinomo_agent::AgentError::SessionStore { detail } => Self::Io {
                path: "agent session store".to_string(),
                detail,
            },
        }
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

impl From<sinomo_runtime::dfs::DfsExposeStoreError> for CliError {
    fn from(error: sinomo_runtime::dfs::DfsExposeStoreError) -> Self {
        Self::DfsExpose(error)
    }
}

impl From<sinomo_runtime::storage::StorageContractError> for CliError {
    fn from(error: sinomo_runtime::storage::StorageContractError) -> Self {
        match error {
            sinomo_runtime::storage::StorageContractError::Io { path, detail } => {
                Self::Io { path, detail }
            }
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxStoreError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxStoreError) -> Self {
        match error {
            sinomo_runtime::sandbox::SandboxStoreError::Io { path, detail }
            | sinomo_runtime::sandbox::SandboxStoreError::Object { path, detail } => {
                Self::Io { path, detail }
            }
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxFsError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxFsError) -> Self {
        match error {
            sinomo_runtime::sandbox::SandboxFsError::Path(error) => Self::Parse {
                detail: error.to_string(),
            },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxPathError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxPathError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxWasiError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxWasiError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxToolchainError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxToolchainError) -> Self {
        match error {
            sinomo_runtime::sandbox::SandboxToolchainError::Io { path, detail }
            | sinomo_runtime::sandbox::SandboxToolchainError::Object { path, detail } => {
                Self::Io { path, detail }
            }
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxCommandExecutionError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxCommandExecutionError) -> Self {
        match error {
            sinomo_runtime::sandbox::SandboxCommandExecutionError::Store(error) => error.into(),
            sinomo_runtime::sandbox::SandboxCommandExecutionError::Toolchain(error) => error.into(),
            sinomo_runtime::sandbox::SandboxCommandExecutionError::Fs(error) => error.into(),
            sinomo_runtime::sandbox::SandboxCommandExecutionError::Wasi(error) => error.into(),
            sinomo_runtime::sandbox::SandboxCommandExecutionError::Scheduling(error) => {
                Self::Parse {
                    detail: error.to_string(),
                }
            }
            sinomo_runtime::sandbox::SandboxCommandExecutionError::PolicyDenied(error) => {
                Self::SandboxPolicyDenied {
                    detail: error.to_string(),
                }
            }
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxRecordedHttpFetchError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxRecordedHttpFetchError) -> Self {
        match error {
            sinomo_runtime::sandbox::SandboxRecordedHttpFetchError::Network(error) => match error {
                sinomo_runtime::sandbox::SandboxNetworkError::PolicyDenied(error) => {
                    Self::SandboxPolicyDenied {
                        detail: error.to_string(),
                    }
                }
                other => Self::Network {
                    address: "sandbox mediated http".to_string(),
                    detail: other.to_string(),
                },
            },
            sinomo_runtime::sandbox::SandboxRecordedHttpFetchError::Store(error) => error.into(),
            sinomo_runtime::sandbox::SandboxRecordedHttpFetchError::Fs(error) => error.into(),
            sinomo_runtime::sandbox::SandboxRecordedHttpFetchError::AuditPolicy(error) => {
                Self::SandboxPolicyDenied {
                    detail: error.to_string(),
                }
            }
        }
    }
}

impl From<sinomo_runtime::sandbox::SandboxPolicyConfigError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxPolicyConfigError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
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
            network::NetworkError::AuthDenied => Self::AuthDenied,
            network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            network::NetworkError::Json { path, detail } => Self::Json { path, detail },
            network::NetworkError::StorageIo { path, detail } => Self::Io { path, detail },
            network::NetworkError::ClusterReadOnly { detail } => Self::ClusterReadOnly { detail },
            network::NetworkError::Kv(error) => Self::Kv(error),
            network::NetworkError::Io { operation, detail } => Self::Network {
                address: operation,
                detail,
            },
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
            sinomo_runtime::daemon::DaemonError::NotRunning { detail } => {
                Self::DaemonNotRunning { detail }
            }
            sinomo_runtime::daemon::DaemonError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_runtime::daemon::DaemonError::ClusterReadOnly { detail } => {
                Self::ClusterReadOnly { detail }
            }
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
            sinomo_runtime::node::NodeIdentityError::Object { path, detail } => Self::Parse {
                detail: format!("node identity object error at {path}: {detail}"),
            },
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

impl From<sinomo_runtime::registry::RegistryError> for CliError {
    fn from(error: sinomo_runtime::registry::RegistryError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
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
            sinomo_evolution::EvolutionPersistenceError::MissingArtifact { artifact } => {
                Self::MissingArtifact { artifact }
            }
        }
    }
}

impl From<sinomo_evolution::EvolutionJobSelectionError> for CliError {
    fn from(error: sinomo_evolution::EvolutionJobSelectionError) -> Self {
        match error {
            sinomo_evolution::EvolutionJobSelectionError::NoActiveJob => Self::NoActiveJob,
            sinomo_evolution::EvolutionJobSelectionError::AmbiguousJob { job_ids } => {
                Self::AmbiguousJob { job_ids }
            }
            sinomo_evolution::EvolutionJobSelectionError::Persistence(error) => error.into(),
        }
    }
}

impl From<sinomo_evolution::EvolutionJobStateError> for CliError {
    fn from(error: sinomo_evolution::EvolutionJobStateError) -> Self {
        match error {
            sinomo_evolution::EvolutionJobStateError::UnexpectedStatus {
                job_id,
                status,
                expected,
            } => Self::InvalidJobState {
                job_id,
                status,
                expected,
            },
        }
    }
}

impl From<sinomo_evolution::EvolutionInputParseError> for CliError {
    fn from(error: sinomo_evolution::EvolutionInputParseError) -> Self {
        match error {
            sinomo_evolution::EvolutionInputParseError::Invalid { detail } => {
                Self::Parse { detail }
            }
        }
    }
}

impl From<RolloutError> for CliError {
    fn from(error: RolloutError) -> Self {
        match error {
            RolloutError::Io { path, detail } => Self::Io { path, detail },
            RolloutError::Json { path, detail } => Self::Json { path, detail },
            RolloutError::Object { path, detail } => Self::Parse {
                detail: format!("sinomo object error at {path}: {detail}"),
            },
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
            RolloutError::InvalidTransition { detail } => Self::Parse { detail },
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
            Self::ClusterReadOnly { detail } => {
                write!(formatter, "cluster KV is read-only: {detail}")
            }
            Self::SandboxPolicyDenied { detail } => {
                write!(formatter, "sandbox policy denied command: {detail}")
            }
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
            Self::DfsExpose(error) => write!(formatter, "{error}"),
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
