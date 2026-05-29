use crate::internal::*;

pub fn sandbox_command_record_status_for_wasi_error(
    error: &SandboxWasiError,
) -> SandboxCommandRecordStatus {
    match error {
        SandboxWasiError::Cancelled => SandboxCommandRecordStatus::Cancelled,
        SandboxWasiError::TimedOut { .. }
        | SandboxWasiError::InputLimitExceeded { .. }
        | SandboxWasiError::MemoryLimitExceeded { .. }
        | SandboxWasiError::OutputLimitExceeded { .. } => {
            SandboxCommandRecordStatus::ResourceExhausted
        }
        SandboxWasiError::InvalidRequest { .. } | SandboxWasiError::Runtime { .. } => {
            SandboxCommandRecordStatus::Failed
        }
    }
}

#[derive(Debug, Error)]
pub enum SandboxCommandExecutionError {
    #[error(transparent)]
    Store(#[from] SandboxStoreError),
    #[error(transparent)]
    Toolchain(#[from] SandboxToolchainError),
    #[error(transparent)]
    Fs(#[from] SandboxFsError),
    #[error(transparent)]
    Wasi(#[from] SandboxWasiError),
    #[error(transparent)]
    Scheduling(#[from] SandboxSchedulingError),
    #[error("sandbox policy denied: {0:?}")]
    PolicyDenied(SandboxPolicyDenial),
}

#[derive(Debug, Error)]
pub enum SandboxWasiError {
    #[error("invalid sandbox WASI request: {detail}")]
    InvalidRequest { detail: String },
    #[error("sandbox WASI command timed out after {timeout_ms}ms")]
    TimedOut { timeout_ms: u64 },
    #[error("sandbox WASI command was cancelled")]
    Cancelled,
    #[error("sandbox WASI command exceeded memory limit of {limit} bytes")]
    MemoryLimitExceeded { limit: usize },
    #[error("sandbox WASI command exceeded input limit of {limit} bytes")]
    InputLimitExceeded { limit: usize },
    #[error("sandbox WASI command exceeded output limit of {limit} bytes")]
    OutputLimitExceeded { limit: usize },
    #[error("sandbox WASI runtime error: {detail}")]
    Runtime { detail: String },
}
