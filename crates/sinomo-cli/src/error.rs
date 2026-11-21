use super::*;

#[derive(Debug)]
pub enum CliError {
    NetworkDelivery {
        address: String,
        detail: String,
        delivery_state: network::NetworkDeliveryState,
    },
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
    ModuleCommandExit {
        command: String,
        detail: String,
        exit_code: i32,
    },
    DesktopApp {
        detail: String,
        exit_code: i32,
    },
    ExternalCommand {
        command: String,
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
    Memory(sinomo_memory::MemoryError),
    DfsMetadata(sinomo_runtime::dfs::MetadataBackendError),
    DfsPin(sinomo_runtime::dfs::DfsPinStoreError),
    Kv(KvError),
    LanguageModel(LanguageModelError),
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

impl From<CliError> for sinomo_runtime::cli_registry::CliCommandError {
    fn from(error: CliError) -> Self {
        Self::new(error.to_string()).with_exit_code(error.exit_code())
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

impl From<sinomo_memory::MemoryError> for CliError {
    fn from(error: sinomo_memory::MemoryError) -> Self {
        Self::Memory(error)
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
            sinomo_agent::AgentError::Runtime { detail } => Self::Io {
                path: "agent runtime".to_string(),
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

impl From<sinomo_runtime::dfs::DfsError> for CliError {
    fn from(error: sinomo_runtime::dfs::DfsError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_runtime::dfs::DfsPinStoreError> for CliError {
    fn from(error: sinomo_runtime::dfs::DfsPinStoreError) -> Self {
        Self::DfsPin(error)
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

impl From<sinomo_runtime::sandbox::SandboxBackendError> for CliError {
    fn from(error: sinomo_runtime::sandbox::SandboxBackendError) -> Self {
        match error {
            // Preserve the existing typed mapping for the underlying store/fs errors.
            sinomo_runtime::sandbox::SandboxBackendError::Store(error) => error.into(),
            sinomo_runtime::sandbox::SandboxBackendError::Fs(error) => error.into(),
            sinomo_runtime::sandbox::SandboxBackendError::PolicyDenied(detail)
            | sinomo_runtime::sandbox::SandboxBackendError::Other(detail) => Self::Parse { detail },
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

impl From<sinomo_runtime::config::ConfigError> for CliError {
    fn from(error: sinomo_runtime::config::ConfigError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<CliParseError> for CliError {
    fn from(error: CliParseError) -> Self {
        match error {
            CliParseError::Usage => Self::Usage,
            CliParseError::Parse { detail } => Self::Parse { detail },
            error @ CliParseError::InvalidUsage { .. } => Self::Parse {
                detail: error.to_string(),
            },
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
        if let Some(delivery_state) = error.delivery_state() {
            return Self::NetworkDelivery {
                address: "object RPC".into(),
                detail: error.to_string(),
                delivery_state,
            };
        }
        match error {
            network::NetworkError::AuthDenied => Self::AuthDenied,
            network::NetworkError::AuthExpired => Self::AuthExpired,
            network::NetworkError::ClusterOrphan => Self::ClusterOrphan,
            network::NetworkError::Json { path, detail } => Self::Json { path, detail },
            network::NetworkError::StorageIo { path, detail } => Self::Io { path, detail },
            network::NetworkError::ClusterReadOnly { detail } => Self::ClusterReadOnly { detail },
            network::NetworkError::Directory { detail } => Self::Network {
                address: "cluster-directory".to_string(),
                detail,
            },
            network::NetworkError::ObjectRpc { route, detail } => Self::Network {
                address: route,
                detail,
            },
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
            sinomo_runtime::daemon::DaemonError::NetworkDelivery {
                address,
                detail,
                delivery_state,
            } => Self::NetworkDelivery {
                address,
                detail,
                delivery_state,
            },
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

impl From<sinomo_runtime::api::DaemonApiError> for CliError {
    fn from(error: sinomo_runtime::api::DaemonApiError) -> Self {
        match error {
            sinomo_runtime::api::DaemonApiError::AuthDenied => Self::AuthDenied,
            sinomo_runtime::api::DaemonApiError::AuthExpired => Self::AuthExpired,
            sinomo_runtime::api::DaemonApiError::Io { path, detail } => Self::Io { path, detail },
            sinomo_runtime::api::DaemonApiError::Json { path, detail } => {
                Self::Json { path, detail }
            }
            sinomo_runtime::api::DaemonApiError::Parse { detail } => Self::Parse { detail },
            sinomo_runtime::api::DaemonApiError::Network { address, detail } => {
                Self::Network { address, detail }
            }
            sinomo_runtime::api::DaemonApiError::RemoteService { route, detail } => Self::Parse {
                detail: format!("daemon service error at {route}: {detail}"),
            },
            sinomo_runtime::api::DaemonApiError::NetworkDelivery {
                detail,
                delivery_state,
            } => Self::NetworkDelivery {
                address: "daemon RPC".to_string(),
                detail,
                delivery_state,
            },
            sinomo_runtime::api::DaemonApiError::ForwardNetwork {
                detail,
                delivery_state: Some(delivery_state),
            } => Self::NetworkDelivery {
                address: "daemon object RPC forward".to_string(),
                detail,
                delivery_state,
            },
            sinomo_runtime::api::DaemonApiError::UnknownRoute { route } => Self::Network {
                address: "daemon RPC".to_string(),
                detail: format!("unknown service route: {route}"),
            },
            sinomo_runtime::api::DaemonApiError::ForwardNetwork { detail, .. } => Self::Network {
                address: "daemon object RPC forward".to_string(),
                detail,
            },
            sinomo_runtime::api::DaemonApiError::AlreadyRunning { lock_file, detail } => {
                Self::DaemonAlreadyRunning { lock_file, detail }
            }
            sinomo_runtime::api::DaemonApiError::NotRunning { detail } => {
                Self::DaemonNotRunning { detail }
            }
            sinomo_runtime::api::DaemonApiError::ClusterOrphan => Self::ClusterOrphan,
            sinomo_runtime::api::DaemonApiError::ClusterReadOnly { detail } => {
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

impl From<sinomo_runtime::registry::RegistryError> for CliError {
    fn from(error: sinomo_runtime::registry::RegistryError) -> Self {
        Self::Parse {
            detail: error.to_string(),
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
            RolloutError::Config { key, detail } => Self::Parse {
                detail: format!("rollout config `{key}`: {detail}"),
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
            RolloutError::InvalidArtifact { detail }
            | RolloutError::InvalidTarget { detail }
            | RolloutError::ModuleInstall { detail }
            | RolloutError::InvalidTransition { detail } => Self::Parse { detail },
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
            Self::NetworkDelivery {
                address,
                detail,
                delivery_state,
            } => write!(
                formatter,
                "network delivery error at {address} ({delivery_state:?}): {detail}"
            ),
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
                "this node has no local cluster state; run `sinomo cluster init` or `sinomo cluster join <invite>`"
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
            Self::DesktopApp { detail, .. } => formatter.write_str(detail),
            Self::ExternalCommand { command, detail }
            | Self::ModuleCommandExit {
                command, detail, ..
            } => {
                write!(formatter, "command `{command}` failed: {detail}")
            }
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::EventStore(error) => write!(formatter, "{error}"),
            Self::Memory(error) => write!(formatter, "{error}"),
            Self::DfsMetadata(error) => write!(formatter, "{error}"),
            Self::DfsPin(error) => write!(formatter, "{error}"),
            Self::Kv(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
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

impl From<sinomo_runtime::rollout_host::RolloutHostError> for CliError {
    fn from(error: sinomo_runtime::rollout_host::RolloutHostError) -> Self {
        match error {
            sinomo_runtime::rollout_host::RolloutHostError::Io { path, detail } => {
                Self::Io { path, detail }
            }
            sinomo_runtime::rollout_host::RolloutHostError::Network { address, detail } => {
                Self::Network { address, detail }
            }
            sinomo_runtime::rollout_host::RolloutHostError::NotRunning { detail } => {
                Self::DaemonNotRunning { detail }
            }
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl CliError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::ModuleCommandExit { exit_code, .. } | Self::DesktopApp { exit_code, .. } => {
                *exit_code
            }
            Self::Parse { .. } | Self::Usage => 2,
            _ => 1,
        }
    }
}
