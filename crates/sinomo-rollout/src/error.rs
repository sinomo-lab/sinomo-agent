use std::fmt;

#[derive(Debug)]
pub enum RolloutError {
    Io {
        path: String,
        detail: String,
    },
    Json {
        path: String,
        detail: String,
    },
    Object {
        path: String,
        detail: String,
    },
    Config {
        key: String,
        detail: String,
    },
    Process {
        command: String,
        status: i32,
        stderr: String,
    },
    MissingArtifact {
        artifact: String,
    },
    InvalidArtifact {
        detail: String,
    },
    InvalidTarget {
        detail: String,
    },
    ModuleInstall {
        detail: String,
    },
    InvalidTransition {
        detail: String,
    },
}

impl fmt::Display for RolloutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::Object { path, detail } => {
                write!(formatter, "sinomo object error at {path}: {detail}")
            }
            Self::Config { key, detail } => {
                write!(formatter, "config error for {key}: {detail}")
            }
            Self::Process {
                command,
                status,
                stderr,
            } => write!(
                formatter,
                "process `{command}` failed with status {status}: {stderr}"
            ),
            Self::MissingArtifact { artifact } => {
                write!(formatter, "{artifact} has not been written")
            }
            Self::InvalidArtifact { detail } => {
                write!(formatter, "invalid rollout artifact: {detail}")
            }
            Self::InvalidTarget { detail } => write!(formatter, "invalid rollout target: {detail}"),
            Self::ModuleInstall { detail } => {
                write!(formatter, "native module install failed: {detail}")
            }
            Self::InvalidTransition { detail } => {
                write!(formatter, "invalid rollout transition: {detail}")
            }
        }
    }
}

impl std::error::Error for RolloutError {}

impl From<sinomo_events::EventStoreError> for RolloutError {
    fn from(error: sinomo_events::EventStoreError) -> Self {
        Self::Object {
            path: "events/event-log.jsonl".to_string(),
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_module::NativeDylibModuleInstallError> for RolloutError {
    fn from(error: sinomo_module::NativeDylibModuleInstallError) -> Self {
        match error {
            sinomo_module::NativeDylibModuleInstallError::Io { path, detail } => {
                Self::Io { path, detail }
            }
            other => Self::ModuleInstall {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_module::ModuleLoaderStoreError> for RolloutError {
    fn from(error: sinomo_module::ModuleLoaderStoreError) -> Self {
        match error {
            sinomo_module::ModuleLoaderStoreError::Io { path, detail } => Self::Io { path, detail },
            sinomo_module::ModuleLoaderStoreError::Object { path, detail } => {
                Self::Object { path, detail }
            }
        }
    }
}
