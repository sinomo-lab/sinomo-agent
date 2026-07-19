use crate::{LocalSandboxBackend, SandboxBackend, SandboxFileSystem};
use sinomo_cli_registry::{
    CliAutocompleteOption, CliAutocompleteSpec, CliCommand, CliCommandContext, CliCommandError,
    CliCommandHandler, CliCommandHelp, CliCommandMetadata, CliCommandPolicy, CliParseError,
};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::events::EventStore;
#[cfg(test)]
use sinomo_core::kv::KvTxnResponse;
use sinomo_core::kv::{ClusterKvTxnCommitResult, KvSnapshot, KvTxnRequest};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network;
use sinomo_core::network::ClusterPaths;
use sinomo_core::network::ObjectRpcResponse;
use sinomo_core::node::NodeId;
use sinomo_module::ModuleReplayReport;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

const SANDBOX_CLI_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();
const SANDBOX_DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
#[cfg(not(test))]
const SANDBOX_DAEMON_CLUSTER_KV_TXN_TIMEOUT: Duration = Duration::from_secs(15);

pub type CliError = SandboxCliError;

#[derive(Debug)]
pub enum SandboxCliError {
    Usage,
    AuthDenied,
    AuthExpired,
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
    LocalData(sinomo_core::local::LocalDataError),
    EventStore(sinomo_core::events::EventStoreError),
    Kv(sinomo_core::kv::KvError),
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

impl SandboxCliError {
    fn stdio(source: io::Error) -> Self {
        Self::Io {
            path: "stdio".to_string(),
            detail: source.to_string(),
        }
    }
}

impl fmt::Display for SandboxCliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "invalid sandbox command usage"),
            Self::AuthDenied => write!(formatter, "device authorization was denied"),
            Self::AuthExpired => write!(formatter, "device authorization expired"),
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
            Self::Parse { detail } => {
                write!(formatter, "failed to parse sandbox command: {detail}")
            }
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::EventStore(error) => write!(formatter, "{error}"),
            Self::Kv(error) => write!(formatter, "{error}"),
            Self::DaemonAlreadyRunning { lock_file, detail } => write!(
                formatter,
                "sinomo daemon is already running or locked at {lock_file}: {detail}"
            ),
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

impl Error for SandboxCliError {}

impl From<SandboxCliError> for CliCommandError {
    fn from(error: SandboxCliError) -> Self {
        CliCommandError::new(error.to_string())
    }
}

impl From<SandboxCliError> for CliParseError {
    fn from(error: SandboxCliError) -> Self {
        match error {
            SandboxCliError::Usage => Self::Usage,
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<sinomo_core::local::LocalDataError> for SandboxCliError {
    fn from(error: sinomo_core::local::LocalDataError) -> Self {
        Self::LocalData(error)
    }
}

impl From<sinomo_core::events::EventStoreError> for SandboxCliError {
    fn from(error: sinomo_core::events::EventStoreError) -> Self {
        Self::EventStore(error)
    }
}

impl From<sinomo_core::kv::KvError> for SandboxCliError {
    fn from(error: sinomo_core::kv::KvError) -> Self {
        Self::Kv(error)
    }
}

impl From<crate::SandboxStoreError> for SandboxCliError {
    fn from(error: crate::SandboxStoreError) -> Self {
        match error {
            crate::SandboxStoreError::Io { path, detail }
            | crate::SandboxStoreError::Object { path, detail } => Self::Io { path, detail },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<crate::SandboxFsError> for SandboxCliError {
    fn from(error: crate::SandboxFsError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<crate::SandboxBackendError> for SandboxCliError {
    fn from(error: crate::SandboxBackendError) -> Self {
        match error {
            crate::SandboxBackendError::Store(error) => error.into(),
            crate::SandboxBackendError::Fs(error) => error.into(),
            crate::SandboxBackendError::PolicyDenied(detail)
            | crate::SandboxBackendError::Other(detail) => Self::Parse { detail },
        }
    }
}

impl From<crate::SandboxPathError> for SandboxCliError {
    fn from(error: crate::SandboxPathError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<crate::SandboxWasiError> for SandboxCliError {
    fn from(error: crate::SandboxWasiError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<crate::SandboxToolchainError> for SandboxCliError {
    fn from(error: crate::SandboxToolchainError) -> Self {
        match error {
            crate::SandboxToolchainError::Io { path, detail }
            | crate::SandboxToolchainError::Object { path, detail } => Self::Io { path, detail },
            other => Self::Parse {
                detail: other.to_string(),
            },
        }
    }
}

impl From<crate::SandboxCommandExecutionError> for SandboxCliError {
    fn from(error: crate::SandboxCommandExecutionError) -> Self {
        match error {
            crate::SandboxCommandExecutionError::Store(error) => error.into(),
            crate::SandboxCommandExecutionError::Toolchain(error) => error.into(),
            crate::SandboxCommandExecutionError::Fs(error) => error.into(),
            crate::SandboxCommandExecutionError::Wasi(error) => error.into(),
            crate::SandboxCommandExecutionError::Scheduling(error) => Self::Parse {
                detail: error.to_string(),
            },
            crate::SandboxCommandExecutionError::PolicyDenied(error) => Self::SandboxPolicyDenied {
                detail: error.to_string(),
            },
        }
    }
}

impl From<crate::SandboxRecordedHttpFetchError> for SandboxCliError {
    fn from(error: crate::SandboxRecordedHttpFetchError) -> Self {
        match error {
            crate::SandboxRecordedHttpFetchError::Network(error) => match error {
                crate::SandboxNetworkError::PolicyDenied(error) => Self::SandboxPolicyDenied {
                    detail: error.to_string(),
                },
                other => Self::Network {
                    address: "sandbox mediated http".to_string(),
                    detail: other.to_string(),
                },
            },
            crate::SandboxRecordedHttpFetchError::Store(error) => error.into(),
            crate::SandboxRecordedHttpFetchError::Fs(error) => error.into(),
            crate::SandboxRecordedHttpFetchError::AuditPolicy(error) => Self::SandboxPolicyDenied {
                detail: error.to_string(),
            },
        }
    }
}

impl From<crate::SandboxPolicyConfigError> for SandboxCliError {
    fn from(error: crate::SandboxPolicyConfigError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<sinomo_config::ConfigError> for SandboxCliError {
    fn from(error: sinomo_config::ConfigError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

impl From<network::NetworkError> for SandboxCliError {
    fn from(error: network::NetworkError) -> Self {
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

impl From<sinomo_core::node::NodeIdentityError> for SandboxCliError {
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

impl From<sinomo_registry::RegistryError> for SandboxCliError {
    fn from(error: sinomo_registry::RegistryError) -> Self {
        Self::Parse {
            detail: error.to_string(),
        }
    }
}

fn unsupported_sandbox_wasix_error() -> crate::SandboxCommandExecutionError {
    crate::SandboxWasiError::Runtime {
        detail: "sandbox execution support is disabled in this build; use a build with the sandbox host loader or a daemon-backed run (`--daemon`)".to_string(),
    }
    .into()
}

fn unsupported_sandbox_simple_error() -> crate::SandboxCommandExecutionError {
    crate::SandboxWasiError::Runtime {
        detail: "simple-native sandbox execution support is not available in this CLI host"
            .to_string(),
    }
    .into()
}

pub trait SandboxCliHost: Send + Sync {
    fn force_local_mode(&self) -> bool {
        false
    }

    fn default_main_daemon_address(&self) -> Result<Option<String>, SandboxCliError> {
        Ok(None)
    }

    fn send_daemon_rpc(
        &self,
        _address: &str,
        _request: &network::ObjectRpcRequest,
        _timeout: Duration,
    ) -> Result<ObjectRpcResponse, SandboxCliError> {
        Err(SandboxCliError::DaemonNotRunning {
            detail: "sandbox daemon command requires a CLI daemon transport".to_string(),
        })
    }

    fn send_daemon_rpc_stream(
        &self,
        _address: &str,
        _request: &network::ObjectRpcRequest,
        _timeout: Duration,
        _on_response: &mut dyn FnMut(ObjectRpcResponse) -> Result<bool, SandboxCliError>,
    ) -> Result<(), SandboxCliError> {
        Err(SandboxCliError::DaemonNotRunning {
            detail: "sandbox daemon command requires a CLI daemon transport".to_string(),
        })
    }

    /// Local-only test seam used by CLI integration tests when no daemon is
    /// available. Production clustered policy reads use the authoritative
    /// snapshot RPC and never fall back here after an authority failure.
    #[doc(hidden)]
    fn read_cluster_kv_local_snapshot_for_test(
        &self,
        _paths: &ClusterPaths,
    ) -> Option<Result<KvSnapshot, SandboxCliError>> {
        None
    }

    /// Local-only test seam for applying an already identified request. The
    /// request envelope is retained so test hosts exercise the same idempotency
    /// contract as the daemon RPC path.
    #[doc(hidden)]
    fn commit_cluster_kv_request_for_test(
        &self,
        _data_dir: &SinomoDataDir,
        _request: &KvTxnRequest,
    ) -> Option<Result<ClusterKvTxnCommitResult, SandboxCliError>> {
        None
    }

    fn run_wasix_module_with_stdin(
        &self,
        _store: &crate::LocalSandboxInstanceStore,
        _toolchain_store: &crate::LocalSandboxToolchainStore,
        _sandbox_id: &crate::SandboxId,
        _wasm: Vec<u8>,
        _argv: Vec<String>,
        _stdin: Option<Vec<u8>>,
        _timeout_ms: u64,
        _output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
    ) -> Result<crate::SandboxLocalCommandOutcome, crate::SandboxCommandExecutionError> {
        Err(unsupported_sandbox_wasix_error())
    }

    #[allow(clippy::too_many_arguments)]
    fn run_wasix_toolchain_command(
        &self,
        _store: &crate::LocalSandboxInstanceStore,
        _toolchain_store: &crate::LocalSandboxToolchainStore,
        _sandbox_id: &crate::SandboxId,
        _command_name: &str,
        _argv: Vec<String>,
        _stdin: Option<Vec<u8>>,
        _timeout_ms: u64,
        _execution_node_id: &NodeId,
        _policy: &crate::SandboxPolicy,
        _output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
    ) -> Result<crate::SandboxLocalCommandOutcome, crate::SandboxCommandExecutionError> {
        Err(unsupported_sandbox_wasix_error())
    }

    fn run_wasix_process_plan(
        &self,
        _store: &crate::LocalSandboxInstanceStore,
        _toolchain_store: &crate::LocalSandboxToolchainStore,
        _sandbox_id: &crate::SandboxId,
        _plan: &crate::SandboxProcessPlan,
        _timeout_ms: u64,
        _execution_node_id: &NodeId,
        _policy: &crate::SandboxPolicy,
        _output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
    ) -> Result<crate::SandboxProcessPlanOutcome, crate::SandboxCommandExecutionError> {
        Err(unsupported_sandbox_wasix_error())
    }

    #[allow(clippy::too_many_arguments)]
    fn run_simple_native_toolchain_command(
        &self,
        _store: &crate::LocalSandboxInstanceStore,
        _toolchain_store: &crate::LocalSandboxToolchainStore,
        _sandbox_id: &crate::SandboxId,
        _command_name: &str,
        _argv: Vec<String>,
        _stdin: Option<Vec<u8>>,
        _timeout_ms: u64,
        _execution_node_id: &NodeId,
        _policy: &crate::SandboxPolicy,
        _output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
    ) -> Result<crate::SandboxLocalCommandOutcome, crate::SandboxCommandExecutionError> {
        Err(unsupported_sandbox_simple_error())
    }

    #[allow(clippy::too_many_arguments)]
    fn run_simple_native_process_plan(
        &self,
        _store: &crate::LocalSandboxInstanceStore,
        _toolchain_store: &crate::LocalSandboxToolchainStore,
        _sandbox_id: &crate::SandboxId,
        _plan: &crate::SandboxProcessPlan,
        _timeout_ms: u64,
        _execution_node_id: &NodeId,
        _policy: &crate::SandboxPolicy,
        _output_sink: Option<Arc<dyn crate::SandboxOutputSink>>,
    ) -> Result<crate::SandboxProcessPlanOutcome, crate::SandboxCommandExecutionError> {
        Err(unsupported_sandbox_simple_error())
    }

    fn verify_busybox_toolchain_fidelity(
        &self,
        _store: &crate::LocalSandboxInstanceStore,
        _toolchain_store: &crate::LocalSandboxToolchainStore,
        _sandbox_id: &crate::SandboxId,
        _execution_node_id: &NodeId,
        _policy: &crate::SandboxPolicy,
        _cases: &[crate::SandboxBusyBoxFidelityCase],
    ) -> Result<Vec<crate::SandboxToolFidelityResult>, crate::SandboxToolFidelityError> {
        Err(crate::SandboxToolFidelityError::Command(
            unsupported_sandbox_wasix_error(),
        ))
    }

    fn validate_component_artifact_bytes(&self, _bytes: &[u8]) -> Result<(), SandboxCliError> {
        Err(SandboxCliError::Parse {
            detail: "sandbox component validation requires the sandbox host loader".to_string(),
        })
    }

    fn run_module_replay_fixture(
        &self,
        _data_dir_root: &Path,
        _fixture: &sinomo_module::ModuleReplayFixture,
    ) -> Result<ModuleReplayReport, SandboxCliError> {
        Err(SandboxCliError::Parse {
            detail: "sandbox module replay requires the sandbox host loader".to_string(),
        })
    }
}

#[derive(Debug, Default)]
pub struct UnsupportedSandboxCliHost;

impl SandboxCliHost for UnsupportedSandboxCliHost {}

pub fn sandbox_cli_command_handler(host: Arc<dyn SandboxCliHost>) -> SandboxCliCommandHandler {
    SandboxCliCommandHandler { host }
}

thread_local! {
    static SANDBOX_CLI_HOST: RefCell<Option<Arc<dyn SandboxCliHost>>> = const { RefCell::new(None) };
}

pub fn with_sandbox_cli_host<T>(
    host: Arc<dyn SandboxCliHost>,
    f: impl FnOnce() -> Result<T, SandboxCliError>,
) -> Result<T, SandboxCliError> {
    let previous = SANDBOX_CLI_HOST.with(|current| current.replace(Some(host)));
    let result = f();
    SANDBOX_CLI_HOST.with(|current| {
        current.replace(previous);
    });
    result
}

fn current_sandbox_cli_host() -> Arc<dyn SandboxCliHost> {
    SANDBOX_CLI_HOST
        .with(|current| current.borrow().as_ref().cloned())
        .unwrap_or_else(|| Arc::new(UnsupportedSandboxCliHost))
}

fn default_main_daemon_address() -> Result<Option<String>, SandboxCliError> {
    current_sandbox_cli_host().default_main_daemon_address()
}

fn cli_force_local_mode() -> bool {
    current_sandbox_cli_host().force_local_mode()
}

fn send_daemon_rpc(
    address: &str,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<ObjectRpcResponse, SandboxCliError> {
    current_sandbox_cli_host().send_daemon_rpc(address, request, timeout)
}

fn send_daemon_rpc_stream(
    address: &str,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
    mut on_response: impl FnMut(ObjectRpcResponse) -> Result<bool, SandboxCliError>,
) -> Result<(), SandboxCliError> {
    current_sandbox_cli_host().send_daemon_rpc_stream(address, request, timeout, &mut on_response)
}

fn write_cli_stdout_bytes(bytes: &[u8]) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes)?;
    stdout.flush()
}

fn write_cli_stderr_bytes(bytes: &[u8]) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    stderr.write_all(bytes)?;
    stderr.flush()
}

#[derive(Clone)]
pub struct SandboxCliCommandHandler {
    host: Arc<dyn SandboxCliHost>,
}

impl fmt::Debug for SandboxCliCommandHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("SandboxCliCommandHandler").finish()
    }
}

impl CliCommandHandler for SandboxCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        sandbox_cli_metadata()
    }

    fn help(&self) -> CliCommandHelp {
        sandbox_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        sandbox_cli_autocomplete()
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(SandboxCliCommandRunner {
            command: parse_sandbox_command(args)?,
            host: Arc::clone(&self.host),
        }))
    }
}

struct SandboxCliCommandRunner {
    command: SandboxCliCommand,
    host: Arc<dyn SandboxCliHost>,
}

impl CliCommand for SandboxCliCommandRunner {
    fn name(&self) -> &'static str {
        "sandbox"
    }

    fn policy(&self) -> CliCommandPolicy {
        SANDBOX_CLI_POLICY
    }

    fn is_help(&self) -> bool {
        matches!(self.command, SandboxCliCommand::Help)
    }

    fn help(&self) -> CliCommandHelp {
        sandbox_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        sandbox_cli_autocomplete()
    }

    fn run(self: Box<Self>, _context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        let SandboxCliCommandRunner { command, host } = *self;
        with_sandbox_cli_host(host, || run_sandbox_cli_command(command)).map_err(Into::into)
    }
}

fn sandbox_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "sandbox",
        &[],
        "Manage local sandbox instances and toolchains.",
        SANDBOX_CLI_POLICY,
    )
}

fn sandbox_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Sandbox:",
        [
            "  sinomo sandbox",
            "      Show sandbox commands.",
            "  sinomo sandbox create [--daemon] [--id <lowercase-ulid>] [--toolchain <toolchain-id>] [--memory-mb <n>] [--wall-time-ms <ms>] [--tmp clear|retain] [--mount <sifs-path>:<mount-path>[:ro|rw]]",
            "      Create a sandbox instance.",
            "  sinomo sandbox status|config|doctor|logs|ls|cat|write|fetch <sandbox-id> ... [--daemon]",
            "      View and manage sandbox metadata and files.",
            "  sinomo sandbox checkpoint|resume|reboot|destroy <sandbox-id> ... [--daemon]",
            "      Manage the sandbox lifecycle, including durable cleanup with `destroy`.",
            "  sinomo sandbox run <sandbox-id> [--daemon|--simple-native|--container-runtime docker|podman --container-image <image>] [--stdin] [--timeout-ms <ms>] <command> -- [args...]",
            "      Run a pinned toolchain command, native host command, container command, or explicit WASI module.",
            "  sinomo sandbox process-plan <sandbox-id> [--daemon|--simple-native|--container-runtime docker|podman --container-image <image>] <plan.json> [--timeout-ms <ms>]",
            "      Run a structured process plan.",
            "  sinomo sandbox shell <sandbox-id> [--daemon|--simple-native|--container-runtime docker|podman --container-image <image>] [--timeout-ms <ms>] [-- <script>]",
            "      Run the pinned real `sh` command.",
            "  sinomo sandbox toolchain ...",
            "      Manage toolchain commands, packages, plans, and verification.",
            "  sinomo sandbox module-artifact register ...",
            "      Register a sandbox-backed module artifact.",
            "  sinomo sandbox module-replay run --fixture <fixture.json> --report <report.json>",
            "      Run a module replay fixture.",
            "  sinomo sandbox policy show|set ...",
            "      Inspect or update sandbox policy.",
        ],
    )
}

fn sandbox_cli_autocomplete() -> CliAutocompleteSpec {
    CliAutocompleteSpec::new("sandbox", "manage local sandbox instances and toolchains")
        .with_children([
            sandbox_child("help", "show sandbox help", &[]),
            sandbox_child(
                "create",
                "create a sandbox instance",
                &[
                    "--daemon",
                    "--id",
                    "--toolchain",
                    "--memory-mb",
                    "--wall-time-ms",
                    "--max-input-bytes",
                    "--max-output-bytes",
                    "--max-workspace-bytes",
                    "--max-written-bytes-per-command",
                    "--max-file-count",
                    "--tmp",
                    "--keep-tmp",
                    "--mount",
                ],
            ),
            sandbox_child("status", "show sandbox status", &["--daemon"]),
            sandbox_child(
                "config",
                "update sandbox cwd and environment",
                &["--daemon", "--cwd", "--env", "--unset-env"],
            ),
            sandbox_child("doctor", "diagnose sandbox state", &[]),
            sandbox_child("ls", "list sandbox files", &["--daemon"]),
            sandbox_child("cat", "read sandbox file", &["--daemon", "--range"]),
            sandbox_child(
                "write",
                "write sandbox file",
                &["--daemon", "--literal", "--file", "--stdin"],
            ),
            sandbox_child(
                "fetch",
                "fetch URL into sandbox",
                &[
                    "--daemon",
                    "--timeout-ms",
                    "--max-response-bytes",
                    "--header",
                ],
            ),
            sandbox_child("checkpoint", "checkpoint sandbox state", &["--daemon"]),
            sandbox_child("resume", "resume sandbox from checkpoint", &["--daemon"]),
            sandbox_child("reboot", "reboot sandbox", &["--daemon"]),
            sandbox_child("destroy", "destroy sandbox", &["--daemon"]),
            sandbox_child(
                "run",
                "run sandbox command",
                &[
                    "--daemon",
                    "--simple-native",
                    "--container-runtime",
                    "--container-image",
                    "--docker",
                    "--podman",
                    "--wasm",
                    "--stdin",
                    "--timeout-ms",
                    "--allow-unsigned",
                    "--allow-capability",
                ],
            ),
            sandbox_child(
                "process-plan",
                "run sandbox process plan",
                &[
                    "--daemon",
                    "--simple-native",
                    "--container-runtime",
                    "--container-image",
                    "--docker",
                    "--podman",
                    "--timeout-ms",
                ],
            ),
            sandbox_child(
                "shell",
                "run sandbox shell",
                &[
                    "--daemon",
                    "--simple-native",
                    "--container-runtime",
                    "--container-image",
                    "--docker",
                    "--podman",
                    "--timeout-ms",
                ],
            ),
            sandbox_child("logs", "show sandbox logs", &["--daemon", "--command"]),
            CliAutocompleteSpec::new("policy", "manage sandbox policy").with_children([
                sandbox_child("show", "show sandbox policy", &[]),
                sandbox_child(
                    "set",
                    "set sandbox policy",
                    &[
                        "--allow-mount-prefix",
                        "--allow-capability",
                        "--allow-toolchain",
                        "--allow-http-host",
                        "--allow-unsigned-toolchains",
                        "--deny-unsigned-toolchains",
                        "--max-memory-mb",
                        "--max-wall-time-ms",
                    ],
                ),
            ]),
            CliAutocompleteSpec::new("toolchain", "manage sandbox toolchains").with_children([
                sandbox_child(
                    "add-command",
                    "add toolchain command",
                    &[
                        "--daemon",
                        "--toolchain",
                        "--version",
                        "--target-abi",
                        "--provenance",
                        "--capability",
                    ],
                ),
                sandbox_child(
                    "add-artifact",
                    "add toolchain artifact",
                    &["--daemon", "--toolchain", "--version", "--provenance"],
                ),
                sandbox_child(
                    "import-busybox",
                    "import BusyBox package",
                    &["--daemon", "--version", "--provenance"],
                ),
                sandbox_child(
                    "import-package",
                    "import toolchain package",
                    &["--daemon", "--into"],
                ),
                sandbox_child("verify-package", "verify toolchain package", &[]),
                sandbox_child("hash", "hash toolchain file", &[]),
                sandbox_child("busybox-plan", "show BusyBox toolchain plan", &[]),
                sandbox_child("bash-plan", "show Bash toolchain plan", &[]),
                sandbox_child("python-plan", "show Python toolchain plan", &[]),
                sandbox_child("build-tools-plan", "show build tools plan", &[]),
                sandbox_child(
                    "verify-busybox",
                    "verify BusyBox fidelity",
                    &["--daemon", "--allow-unsigned"],
                ),
                sandbox_child(
                    "verify-bash",
                    "verify Bash fidelity",
                    &["--daemon", "--allow-unsigned"],
                ),
                sandbox_child(
                    "verify-python",
                    "verify Python fidelity",
                    &["--daemon", "--allow-unsigned"],
                ),
                sandbox_child(
                    "verify-coding",
                    "verify coding surface",
                    &["--daemon", "--allow-unsigned"],
                ),
                sandbox_child(
                    "verify-make",
                    "verify make",
                    &["--daemon", "--allow-unsigned"],
                ),
                sandbox_child(
                    "verify-makepkg",
                    "verify makepkg",
                    &["--daemon", "--allow-unsigned"],
                ),
                sandbox_child(
                    "verify-tcc",
                    "verify tcc",
                    &["--daemon", "--allow-unsigned"],
                ),
            ]),
            CliAutocompleteSpec::new("module-artifact", "manage sandbox module artifacts")
                .with_children([sandbox_child(
                    "register",
                    "register module artifact",
                    &[
                        "--artifact",
                        "--interface",
                        "--component-model",
                        "--abi",
                        "--sandbox",
                        "--argv",
                        "--cwd",
                        "--env",
                        "--timeout-ms",
                        "--candidate-bundle",
                        "--module",
                        "--active-artifact",
                    ],
                )]),
            CliAutocompleteSpec::new("module-replay", "run sandbox module replay").with_children([
                sandbox_child("run", "run replay fixture", &["--fixture", "--report"]),
            ]),
        ])
}

fn sandbox_child(word: &str, detail: &str, options: &[&str]) -> CliAutocompleteSpec {
    CliAutocompleteSpec::new(word, detail).with_options(
        options
            .iter()
            .copied()
            .map(|option| CliAutocompleteOption::flag(option, "")),
    )
}

fn print_sandbox_help() {
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr();
    let mut context = CliCommandContext::new(&mut stdout, &mut stderr);
    let _ = sandbox_cli_help().write(&mut context);
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxCliCommand {
    Help,
    Config(SandboxConfigOptions),
    Create(SandboxCreateOptions),
    Cat(SandboxCatOptions),
    Checkpoint(SandboxCheckpointOptions),
    Destroy(SandboxDestroyOptions),
    Doctor(SandboxDoctorOptions),
    Fetch(SandboxFetchOptions),
    Ls(SandboxLsOptions),
    Logs(SandboxLogsOptions),
    ModuleArtifact(SandboxModuleArtifactCliCommand),
    ModuleReplay(SandboxModuleReplayCliCommand),
    Policy(SandboxPolicyCliCommand),
    ProcessPlan(SandboxProcessPlanOptions),
    Reboot(SandboxRebootOptions),
    Resume(SandboxResumeOptions),
    Run(SandboxRunOptions),
    Shell(SandboxShellOptions),
    Status(SandboxStatusOptions),
    Write(SandboxWriteOptions),
    ToolchainAddCommand(SandboxToolchainAddCommandOptions),
    ToolchainAddArtifact(SandboxToolchainAddArtifactOptions),
    ToolchainBashPlan,
    ToolchainBuildToolsPlan,
    ToolchainBusyBoxPlan,
    ToolchainHash(SandboxToolchainHashOptions),
    ToolchainImportBusyBox(SandboxToolchainImportBusyBoxOptions),
    ToolchainImportPackage(SandboxToolchainImportPackageOptions),
    ToolchainPythonPlan,
    ToolchainVerifyPackage(SandboxToolchainVerifyPackageOptions),
    ToolchainVerifyBash(SandboxToolchainVerifyBashOptions),
    ToolchainVerifyBusyBox(SandboxToolchainVerifyBusyBoxOptions),
    ToolchainVerifyCoding(SandboxToolchainVerifyCodingOptions),
    ToolchainVerifyMake(SandboxToolchainVerifyMakeOptions),
    ToolchainVerifyMakepkg(SandboxToolchainVerifyMakepkgOptions),
    ToolchainVerifyPython(SandboxToolchainVerifyPythonOptions),
    ToolchainVerifyTcc(SandboxToolchainVerifyTccOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxModuleArtifactCliCommand {
    Register(SandboxModuleArtifactRegisterOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxModuleReplayCliCommand {
    Run(SandboxModuleReplayRunOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxModuleArtifactRegisterOptions {
    pub artifact_id: sinomo_module::ArtifactId,
    pub source_path: PathBuf,
    pub interface_version: Option<String>,
    pub component_model: bool,
    pub abi: crate::SandboxWasiAbi,
    pub sandbox_id: Option<crate::SandboxId>,
    pub argv: Vec<String>,
    pub cwd: Option<crate::SandboxPath>,
    pub env: BTreeMap<String, String>,
    pub limits: crate::SandboxResourceLimits,
    pub timeout_ms: Option<u64>,
    pub rollout_material: Option<SandboxModuleArtifactRolloutMaterialOptions>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxModuleArtifactRolloutMaterialOptions {
    pub candidate_bundle_path: PathBuf,
    pub module_id: sinomo_module::ModuleId,
    pub active_artifact_id: sinomo_module::ArtifactId,
    pub rollout_plan_id: Option<sinomo_core::Id>,
    pub policy_decision_id: Option<sinomo_core::Id>,
    pub module_version: String,
    pub module_owner: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxModuleReplayRunOptions {
    pub fixture_path: PathBuf,
    pub report_path: PathBuf,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxConfigOptions {
    pub sandbox_id: crate::SandboxId,
    pub cwd: Option<crate::SandboxPath>,
    pub env_set: BTreeMap<String, String>,
    pub env_unset: Vec<String>,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxPolicyCliCommand {
    Show,
    Set(SandboxPolicySetOptions),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct SandboxPolicySetOptions {
    pub allowed_mount_prefixes: Vec<String>,
    pub allow_unsigned_toolchains: Option<bool>,
    pub allow_remote_local_owned_mounts: Option<bool>,
    pub max_memory_mb: Option<u64>,
    pub max_wall_time_ms: Option<u64>,
    pub max_input_bytes: Option<u64>,
    pub max_output_bytes: Option<u64>,
    pub max_workspace_bytes: Option<u64>,
    pub max_written_bytes_per_command: Option<u64>,
    pub max_file_count: Option<u64>,
    pub max_http_timeout_ms: Option<u64>,
    pub max_concurrent_commands_per_sandbox: Option<u64>,
    pub max_concurrent_sandboxes_per_node: Option<u64>,
    pub allowed_capabilities: Vec<String>,
    pub allowed_toolchains: Vec<crate::SandboxToolchainId>,
    pub allowed_toolchain_provenance_prefixes: Vec<String>,
    pub allowed_package_cache_prefixes: Vec<String>,
    pub allowed_http_hosts: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxCreateOptions {
    pub sandbox_id: Option<crate::SandboxId>,
    pub toolchain_id: crate::SandboxToolchainId,
    pub temp_policy: crate::SandboxTempPolicy,
    pub memory_mb: Option<u64>,
    pub wall_time_ms: Option<u64>,
    pub max_input_bytes: Option<u64>,
    pub max_output_bytes: Option<u64>,
    pub max_workspace_bytes: Option<u64>,
    pub max_written_bytes_per_command: Option<u64>,
    pub max_file_count: Option<u64>,
    pub mounts: Vec<crate::SandboxMount>,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxStatusOptions {
    pub sandbox_id: crate::SandboxId,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxDoctorOptions {
    pub sandbox_id: Option<crate::SandboxId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxDoctorFinding {
    pub severity: SandboxDoctorSeverity,
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxDoctorSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxLsOptions {
    pub sandbox_id: crate::SandboxId,
    pub path: crate::SandboxPath,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxCatOptions {
    pub sandbox_id: crate::SandboxId,
    pub path: crate::SandboxPath,
    pub range: Option<crate::ByteRange>,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxWriteInput {
    Literal(Vec<u8>),
    HostFile(PathBuf),
    Stdin,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxWriteOptions {
    pub sandbox_id: crate::SandboxId,
    pub path: crate::SandboxPath,
    pub input: SandboxWriteInput,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxFetchOptions {
    pub sandbox_id: crate::SandboxId,
    pub url: String,
    pub output_path: crate::SandboxPath,
    pub timeout_ms: u64,
    pub max_response_bytes: u64,
    pub headers: BTreeMap<String, String>,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxCheckpointOptions {
    pub sandbox_id: crate::SandboxId,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxResumeOptions {
    pub sandbox_id: crate::SandboxId,
    pub checkpoint_id: crate::SandboxCheckpointId,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxRebootOptions {
    pub sandbox_id: crate::SandboxId,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxDestroyOptions {
    pub sandbox_id: crate::SandboxId,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxRunOptions {
    pub sandbox_id: crate::SandboxId,
    pub target: SandboxRunTarget,
    pub argv: Vec<String>,
    pub timeout_ms: u64,
    pub stdin: bool,
    pub allow_unsigned: bool,
    pub allowed_capabilities: Vec<String>,
    pub execution: SandboxRunExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxProcessPlanOptions {
    pub sandbox_id: crate::SandboxId,
    pub plan_path: PathBuf,
    pub timeout_ms: u64,
    pub execution: SandboxRunExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxShellOptions {
    pub sandbox_id: crate::SandboxId,
    pub script: Option<String>,
    pub timeout_ms: u64,
    pub execution: SandboxRunExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxRunTarget {
    ToolchainCommand(String),
    WasmModule(PathBuf),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SandboxRunExecution {
    Local,
    Daemon,
    SimpleNative,
    Container(crate::SandboxContainerBackendConfig),
}

#[derive(Debug, Clone, Default)]
struct SandboxContainerRunBuilder {
    runtime: Option<crate::SandboxContainerRuntime>,
    image: Option<String>,
    requested: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxLogsOptions {
    pub sandbox_id: crate::SandboxId,
    pub command_id: Option<crate::SandboxCommandId>,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainAddCommandOptions {
    pub toolchain_id: crate::SandboxToolchainId,
    pub version: String,
    pub target_abi: crate::SandboxWasiAbi,
    pub provenance: String,
    pub command_name: String,
    pub module_path: PathBuf,
    pub execution: SandboxMetadataExecution,
    pub required_capabilities: Vec<String>,
    pub default_limits: crate::SandboxResourceLimits,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainAddArtifactOptions {
    pub toolchain_id: crate::SandboxToolchainId,
    pub version: String,
    pub provenance: String,
    pub relative_path: String,
    pub source_path: PathBuf,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainHashOptions {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainImportBusyBoxOptions {
    pub version: String,
    pub provenance: String,
    pub module_path: PathBuf,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainImportPackageOptions {
    pub package_path: PathBuf,
    pub target_toolchain_id: Option<crate::SandboxToolchainId>,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyPackageOptions {
    pub package_path: PathBuf,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SandboxMetadataExecution {
    Local,
    Daemon,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyBusyBoxOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyBashOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyCodingOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
    pub full: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyMakeOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyMakepkgOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyTccOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SandboxToolchainVerifyPythonOptions {
    pub sandbox_id: crate::SandboxId,
    pub allow_unsigned: bool,
    pub execution: SandboxMetadataExecution,
}

pub fn parse_sandbox_command(args: &[String]) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        None => Ok(SandboxCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(SandboxCliCommand::Help)
        }
        Some((command, rest)) if command == "create" => Ok(SandboxCliCommand::Create(
            parse_sandbox_create_options(rest)?,
        )),
        Some((command, rest)) if command == "config" => Ok(SandboxCliCommand::Config(
            parse_sandbox_config_options(rest)?,
        )),
        Some((command, rest)) if command == "run" => {
            Ok(SandboxCliCommand::Run(parse_sandbox_run_options(rest)?))
        }
        Some((command, rest)) if command == "process-plan" => Ok(SandboxCliCommand::ProcessPlan(
            parse_sandbox_process_plan_options(rest)?,
        )),
        Some((command, rest)) if command == "shell" => {
            Ok(SandboxCliCommand::Shell(parse_sandbox_shell_options(rest)?))
        }
        Some((command, rest)) if command == "logs" => {
            Ok(SandboxCliCommand::Logs(parse_sandbox_logs_options(rest)?))
        }
        Some((command, rest)) if command == "doctor" => Ok(SandboxCliCommand::Doctor(
            parse_sandbox_doctor_options(rest)?,
        )),
        Some((command, rest)) if command == "policy" => parse_sandbox_policy_command(rest),
        Some((command, rest)) if command == "module-artifact" => {
            parse_sandbox_module_artifact_command(rest)
        }
        Some((command, rest)) if command == "module-replay" => {
            parse_sandbox_module_replay_command(rest)
        }
        Some((command, rest)) if command == "ls" => {
            Ok(SandboxCliCommand::Ls(parse_sandbox_ls_options(rest)?))
        }
        Some((command, rest)) if command == "cat" => {
            Ok(SandboxCliCommand::Cat(parse_sandbox_cat_options(rest)?))
        }
        Some((command, rest)) if command == "write" => {
            Ok(SandboxCliCommand::Write(parse_sandbox_write_options(rest)?))
        }
        Some((command, rest)) if command == "fetch" => {
            Ok(SandboxCliCommand::Fetch(parse_sandbox_fetch_options(rest)?))
        }
        Some((command, rest)) if command == "checkpoint" => Ok(SandboxCliCommand::Checkpoint(
            parse_sandbox_checkpoint_options(rest)?,
        )),
        Some((command, rest)) if command == "resume" => Ok(SandboxCliCommand::Resume(
            parse_sandbox_resume_options(rest)?,
        )),
        Some((command, rest)) if command == "reboot" => Ok(SandboxCliCommand::Reboot(
            parse_sandbox_reboot_options(rest)?,
        )),
        Some((command, rest)) if command == "destroy" => Ok(SandboxCliCommand::Destroy(
            parse_sandbox_destroy_options(rest)?,
        )),
        Some((command, rest)) if command == "status" => {
            let (sandbox_id, rest) = rest.split_first().ok_or(CliError::Usage)?;
            let mut execution = SandboxMetadataExecution::Local;
            for arg in rest {
                match arg.as_str() {
                    "--daemon" => execution = SandboxMetadataExecution::Daemon,
                    _ => return Err(CliError::Usage),
                }
            }
            Ok(SandboxCliCommand::Status(SandboxStatusOptions {
                sandbox_id: parse_sandbox_id(sandbox_id)?,
                execution,
            }))
        }
        Some((command, rest)) if command == "toolchain" => parse_sandbox_toolchain_command(rest),
        Some((command, _)) => Err(CliError::Parse {
            detail: format!(
                "sandbox command `{command}` is not implemented yet; run `sinomo sandbox help`"
            ),
        }),
    }
}

pub fn parse_sandbox_policy_command(args: &[String]) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        Some((command, [])) if command == "show" => {
            Ok(SandboxCliCommand::Policy(SandboxPolicyCliCommand::Show))
        }
        Some((command, rest)) if command == "set" => Ok(SandboxCliCommand::Policy(
            SandboxPolicyCliCommand::Set(parse_sandbox_policy_set_options(rest)?),
        )),
        _ => Err(CliError::Usage),
    }
}

pub fn parse_sandbox_module_artifact_command(
    args: &[String],
) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "register" => Ok(SandboxCliCommand::ModuleArtifact(
            SandboxModuleArtifactCliCommand::Register(
                parse_sandbox_module_artifact_register_options(rest)?,
            ),
        )),
        _ => Err(CliError::Usage),
    }
}

pub fn parse_sandbox_module_replay_command(args: &[String]) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "run" => Ok(SandboxCliCommand::ModuleReplay(
            SandboxModuleReplayCliCommand::Run(parse_sandbox_module_replay_run_options(rest)?),
        )),
        _ => Err(CliError::Usage),
    }
}

pub fn parse_sandbox_module_artifact_register_options(
    args: &[String],
) -> Result<SandboxModuleArtifactRegisterOptions, CliError> {
    let mut artifact_id = None;
    let mut source_path = None;
    let mut interface_version = None;
    let mut component_model = false;
    let mut abi = crate::default_sandbox_wasi_abi();
    let mut sandbox_id = None;
    let mut argv = Vec::new();
    let mut cwd = None;
    let mut env = BTreeMap::new();
    let mut limits = crate::SandboxResourceLimits::default();
    let mut timeout_ms = None;
    let mut rollout_material_bundle = None;
    let mut rollout_material_module_id = None;
    let mut rollout_material_active_artifact_id = None;
    let mut rollout_material_rollout_plan_id = None;
    let mut rollout_material_policy_decision_id = None;
    let mut rollout_material_module_version = "0.1.0".to_string();
    let mut rollout_material_module_owner = "sandbox-module-artifact".to_string();
    let mut rollout_material_evidence_refs = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--artifact" | "--artifact-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                artifact_id = Some(parse_module_artifact_id(value)?);
            }
            "--interface" | "--interface-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox module artifact interface version must be non-empty"
                            .to_string(),
                    });
                }
                interface_version = Some(value.clone());
            }
            "--component-model" => component_model = true,
            "--abi" | "--target-abi" => {
                index += 1;
                abi = parse_sandbox_wasi_abi(args.get(index))?;
            }
            "--sandbox" | "--sandbox-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                sandbox_id = Some(parse_sandbox_id(value)?);
            }
            "--argv" | "--arg" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_argv_value(value, "sandbox module artifact argv")?;
                argv.push(value.clone());
            }
            "--cwd" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                cwd = Some(parse_sandbox_path(value)?);
            }
            "--env" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                let (key, value) = parse_sandbox_env_assignment(value)?;
                env.insert(key, value);
            }
            "--memory-mb" => {
                index += 1;
                limits.memory_mb =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module memory mb")?;
            }
            "--wall-time-ms" => {
                index += 1;
                limits.wall_time_ms =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module wall time")?;
            }
            "--max-input-bytes" => {
                index += 1;
                limits.max_input_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module input bytes")?;
            }
            "--max-output-bytes" => {
                index += 1;
                limits.max_output_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module output bytes")?;
            }
            "--max-workspace-bytes" => {
                index += 1;
                limits.max_workspace_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module workspace bytes")?;
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                limits.max_written_bytes_per_command =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module written bytes")?;
            }
            "--max-file-count" => {
                index += 1;
                limits.max_file_count =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "module file count")?;
            }
            "--timeout-ms" => {
                index += 1;
                timeout_ms = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "module timeout",
                )?);
            }
            "--rollout-material-bundle" | "--candidate-bundle" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                rollout_material_bundle = Some(PathBuf::from(value));
            }
            "--module" | "--module-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                rollout_material_module_id = Some(parse_module_id(value)?);
            }
            "--active-artifact" | "--active-artifact-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                rollout_material_active_artifact_id = Some(parse_module_artifact_id(value)?);
            }
            "--rollout-plan" | "--rollout-plan-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                rollout_material_rollout_plan_id = Some(parse_sinomo_id(value, "rollout plan")?);
            }
            "--policy-decision" | "--policy-decision-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                rollout_material_policy_decision_id =
                    Some(parse_sinomo_id(value, "policy decision")?);
            }
            "--module-version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox module artifact rollout material module version must be non-empty".to_string(),
                    });
                }
                rollout_material_module_version = value.clone();
            }
            "--module-owner" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox module artifact rollout material module owner must be non-empty".to_string(),
                    });
                }
                rollout_material_module_owner = value.clone();
            }
            "--evidence" | "--evidence-ref" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox module artifact rollout material evidence ref must be non-empty".to_string(),
                    });
                }
                rollout_material_evidence_refs.push(value.clone());
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value if source_path.is_none() => source_path = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }

    if component_model {
        interface_version = match interface_version {
            Some(version) if version == sinomo_module::SINOMO_MODULE_V1_INTERFACE_VERSION => {
                Some(version)
            }
            Some(version) => {
                return Err(CliError::Parse {
                    detail: format!(
                        "component-model sandbox module artifacts currently require interface `{}`; got `{version}`",
                        sinomo_module::SINOMO_MODULE_V1_INTERFACE_VERSION
                    ),
                });
            }
            None => Some(sinomo_module::SINOMO_MODULE_V1_INTERFACE_VERSION.to_string()),
        };
    }

    let rollout_material = match (
        rollout_material_bundle,
        rollout_material_module_id,
        rollout_material_active_artifact_id,
    ) {
        (None, None, None) => None,
        (Some(candidate_bundle_path), Some(module_id), Some(active_artifact_id)) => {
            Some(SandboxModuleArtifactRolloutMaterialOptions {
                candidate_bundle_path,
                module_id,
                active_artifact_id,
                rollout_plan_id: rollout_material_rollout_plan_id,
                policy_decision_id: rollout_material_policy_decision_id,
                module_version: rollout_material_module_version,
                module_owner: rollout_material_module_owner,
                evidence_refs: rollout_material_evidence_refs,
            })
        }
        _ => {
            return Err(CliError::Parse {
                detail: "sandbox module artifact rollout material requires --rollout-material-bundle, --module, and --active-artifact together".to_string(),
            });
        }
    };

    Ok(SandboxModuleArtifactRegisterOptions {
        artifact_id: artifact_id.ok_or(CliError::Usage)?,
        source_path: source_path.ok_or(CliError::Usage)?,
        interface_version,
        component_model,
        abi,
        sandbox_id,
        argv,
        cwd,
        env,
        limits,
        timeout_ms,
        rollout_material,
    })
}

pub fn parse_sandbox_module_replay_run_options(
    args: &[String],
) -> Result<SandboxModuleReplayRunOptions, CliError> {
    let mut fixture_path = None;
    let mut report_path = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--fixture" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                fixture_path = Some(PathBuf::from(value));
            }
            "--report" | "--output" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                report_path = Some(PathBuf::from(value));
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value if fixture_path.is_none() => fixture_path = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxModuleReplayRunOptions {
        fixture_path: fixture_path.ok_or(CliError::Usage)?,
        report_path: report_path.ok_or(CliError::Usage)?,
    })
}

pub fn parse_sandbox_doctor_options(args: &[String]) -> Result<SandboxDoctorOptions, CliError> {
    match args {
        [] => Ok(SandboxDoctorOptions { sandbox_id: None }),
        [sandbox_id] => Ok(SandboxDoctorOptions {
            sandbox_id: Some(parse_sandbox_id(sandbox_id)?),
        }),
        _ => Err(CliError::Usage),
    }
}

pub fn parse_sandbox_config_options(args: &[String]) -> Result<SandboxConfigOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut options = SandboxConfigOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        cwd: None,
        env_set: BTreeMap::new(),
        env_unset: Vec::new(),
        execution: SandboxMetadataExecution::Local,
    };
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => options.execution = SandboxMetadataExecution::Daemon,
            "--cwd" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cwd = Some(crate::SandboxPath::new(value.clone())?);
            }
            "--env" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                let (key, env_value) = parse_sandbox_env_assignment(value)?;
                options.env_set.insert(key, env_value);
            }
            "--unset-env" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_env_name(value)?;
                options.env_unset.push(value.clone());
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    if options.cwd.is_none() && options.env_set.is_empty() && options.env_unset.is_empty() {
        return Err(CliError::Usage);
    }
    Ok(options)
}

pub fn parse_sandbox_policy_set_options(
    args: &[String],
) -> Result<SandboxPolicySetOptions, CliError> {
    let mut options = SandboxPolicySetOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--allow-unsigned-toolchains" => options.allow_unsigned_toolchains = Some(true),
            "--require-signed-toolchains" => options.allow_unsigned_toolchains = Some(false),
            "--allow-remote-local-owned-mounts" => {
                options.allow_remote_local_owned_mounts = Some(true)
            }
            "--deny-remote-local-owned-mounts" => {
                options.allow_remote_local_owned_mounts = Some(false)
            }
            "--max-memory-mb" => {
                index += 1;
                options.max_memory_mb = Some(parse_sandbox_memory_policy_value(
                    args.get(index),
                    "max memory",
                )?);
            }
            "--max-wall-time-ms" => {
                index += 1;
                options.max_wall_time_ms = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max wall time",
                )?);
            }
            "--max-output-bytes" => {
                index += 1;
                options.max_output_bytes = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max output bytes",
                )?);
            }
            "--max-input-bytes" => {
                index += 1;
                options.max_input_bytes = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max input bytes",
                )?);
            }
            "--max-workspace-bytes" => {
                index += 1;
                options.max_workspace_bytes = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max workspace bytes",
                )?);
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                options.max_written_bytes_per_command = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max written bytes per command",
                )?);
            }
            "--max-file-count" => {
                index += 1;
                options.max_file_count = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max file count",
                )?);
            }
            "--max-http-timeout-ms" => {
                index += 1;
                options.max_http_timeout_ms = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "max HTTP timeout",
                )?);
            }
            "--max-concurrent-commands-per-sandbox" => {
                index += 1;
                options.max_concurrent_commands_per_sandbox = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max concurrent commands per sandbox",
                )?);
            }
            "--max-concurrent-sandboxes-per-node" => {
                index += 1;
                options.max_concurrent_sandboxes_per_node = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max concurrent sandboxes per node",
                )?);
            }
            "--allow-capability" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_command_capability(value, "sandbox capability")?;
                options.allowed_capabilities.push(value.clone());
            }
            "--allow-toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.allowed_toolchains.push(parse_toolchain_id(value)?);
            }
            "--allow-toolchain-provenance-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain provenance prefix cannot be empty".to_string(),
                    });
                }
                if value.contains('\0') {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain provenance prefix contains an embedded NUL byte"
                            .to_string(),
                    });
                }
                options
                    .allowed_toolchain_provenance_prefixes
                    .push(value.clone());
            }
            "--allow-package-cache-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options
                    .allowed_package_cache_prefixes
                    .push(parse_sifs_mount_prefix(value)?);
            }
            "--allow-http-host" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.allowed_http_hosts.push(parse_http_host(value)?);
            }
            "--allow-mount-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options
                    .allowed_mount_prefixes
                    .push(parse_sifs_mount_prefix(value)?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_u64_sandbox_policy_value(value: Option<&String>, name: &str) -> Result<u64, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    if value == "unlimited" {
        return Ok(u64::MAX);
    }
    value.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox {name} `{value}`: {source}"),
    })
}

fn parse_sandbox_memory_policy_value(value: Option<&String>, name: &str) -> Result<u64, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    if matches!(value.as_str(), "host" | "available" | "all") {
        return Ok(crate::SandboxResourceLimits::default().memory_mb);
    }
    parse_u64_sandbox_policy_value(Some(value), name)
}

fn parse_nonzero_u64_sandbox_cli_value(
    value: Option<&String>,
    name: &str,
) -> Result<u64, CliError> {
    let parsed = parse_u64_sandbox_policy_value(value, name)?;
    if parsed == 0 {
        return Err(CliError::Parse {
            detail: format!("sandbox {name} must be non-zero"),
        });
    }
    Ok(parsed)
}

pub fn parse_sandbox_checkpoint_options(
    args: &[String],
) -> Result<SandboxCheckpointOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxCheckpointOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        execution,
    })
}

pub fn parse_sandbox_resume_options(args: &[String]) -> Result<SandboxResumeOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let (checkpoint_id, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxResumeOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        checkpoint_id: parse_sandbox_checkpoint_id(checkpoint_id)?,
        execution,
    })
}

pub fn parse_sandbox_reboot_options(args: &[String]) -> Result<SandboxRebootOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxRebootOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        execution,
    })
}

pub fn parse_sandbox_destroy_options(args: &[String]) -> Result<SandboxDestroyOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxDestroyOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        execution,
    })
}

pub fn parse_sandbox_ls_options(args: &[String]) -> Result<SandboxLsOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut path = None;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            value if path.is_none() => path = Some(value),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxLsOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        path: parse_sandbox_path(path.unwrap_or("/"))?,
        execution,
    })
}

pub fn parse_sandbox_cat_options(args: &[String]) -> Result<SandboxCatOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut path = None;
    let mut range = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxMetadataExecution::Daemon;
                index += 1;
            }
            "--range" => {
                index += 1;
                range = Some(parse_sandbox_byte_range(rest.get(index))?);
                index += 1;
            }
            value if path.is_none() => {
                path = Some(value);
                index += 1;
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxCatOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        path: parse_sandbox_path(path.ok_or(CliError::Usage)?)?,
        range,
        execution,
    })
}

fn parse_sandbox_byte_range(value: Option<&String>) -> Result<crate::ByteRange, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    let (start, end) = value.split_once(':').ok_or_else(|| CliError::Parse {
        detail: format!("invalid sandbox byte range `{value}`; expected start:end"),
    })?;
    if start.is_empty() {
        return Err(CliError::Parse {
            detail: format!("invalid sandbox byte range `{value}`; start is required"),
        });
    }
    let start = start.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox byte range start `{start}`: {source}"),
    })?;
    let end = if end.is_empty() {
        None
    } else {
        let end = end.parse::<u64>().map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox byte range end `{end}`: {source}"),
        })?;
        if end < start {
            return Err(CliError::Parse {
                detail: format!("invalid sandbox byte range `{value}`; end is before start"),
            });
        }
        Some(end)
    };
    Ok(crate::ByteRange { start, end })
}

pub fn parse_sandbox_write_options(args: &[String]) -> Result<SandboxWriteOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let (path, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut input = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxMetadataExecution::Daemon;
                index += 1;
            }
            "--stdin" => {
                if input.is_some() {
                    return Err(CliError::Usage);
                }
                input = Some(SandboxWriteInput::Stdin);
                index += 1;
            }
            "--file" => {
                let Some(path) = rest.get(index + 1) else {
                    return Err(CliError::Usage);
                };
                if input.is_some() {
                    return Err(CliError::Usage);
                }
                input = Some(SandboxWriteInput::HostFile(PathBuf::from(path)));
                index += 2;
            }
            value => {
                if input.is_some() {
                    return Err(CliError::Usage);
                }
                input = Some(SandboxWriteInput::Literal(value.as_bytes().to_vec()));
                index += 1;
            }
        }
    }
    Ok(SandboxWriteOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        path: parse_sandbox_path(path)?,
        input: input.unwrap_or(SandboxWriteInput::Stdin),
        execution,
    })
}

pub fn parse_sandbox_fetch_options(args: &[String]) -> Result<SandboxFetchOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let (url, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let (output_path, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let mut timeout_ms = crate::default_sandbox_http_timeout_ms();
    let mut max_response_bytes = crate::default_sandbox_http_response_bytes();
    let mut headers = BTreeMap::new();
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxMetadataExecution::Daemon;
            }
            "--timeout-ms" => {
                index += 1;
                timeout_ms = parse_u64_sandbox_policy_value(rest.get(index), "HTTP timeout")?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox HTTP timeout must be non-zero".to_string(),
                    });
                }
            }
            "--max-response-bytes" => {
                index += 1;
                max_response_bytes =
                    parse_u64_sandbox_policy_value(rest.get(index), "HTTP response bytes")?;
                if max_response_bytes == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox HTTP max response bytes must be non-zero".to_string(),
                    });
                }
            }
            "--header" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                let (name, value) = parse_http_header(value)?;
                headers.insert(name, value);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxFetchOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        url: url.clone(),
        output_path: parse_sandbox_path(output_path)?,
        timeout_ms,
        max_response_bytes,
        headers,
        execution,
    })
}

fn parse_sandbox_container_runtime_arg(
    value: Option<&String>,
) -> Result<crate::SandboxContainerRuntime, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    crate::SandboxContainerRuntime::parse(value).map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })
}

fn parse_sandbox_container_image_arg(value: Option<&String>) -> Result<String, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    crate::SandboxContainerBackendConfig::new(crate::SandboxContainerRuntime::Docker, value.clone())
        .map(|config| config.image)
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })
}

fn finalize_sandbox_run_execution(
    execution: SandboxRunExecution,
    container: SandboxContainerRunBuilder,
) -> Result<SandboxRunExecution, CliError> {
    if !container.requested {
        return Ok(execution);
    }
    if matches!(&execution, SandboxRunExecution::Daemon) {
        return Err(CliError::Parse {
            detail: "container sandbox execution is local-only; do not combine it with --daemon"
                .to_string(),
        });
    }
    if matches!(&execution, SandboxRunExecution::SimpleNative) {
        return Err(CliError::Parse {
            detail: "container sandbox execution cannot be combined with --simple-native"
                .to_string(),
        });
    }
    let runtime = container
        .runtime
        .unwrap_or(crate::SandboxContainerRuntime::Docker);
    let Some(image) = container.image else {
        return Err(CliError::Parse {
            detail: "container sandbox execution requires --container-image <image>".to_string(),
        });
    };
    let config = crate::SandboxContainerBackendConfig::new(runtime, image).map_err(|source| {
        CliError::Parse {
            detail: source.to_string(),
        }
    })?;
    Ok(SandboxRunExecution::Container(config))
}

pub fn parse_sandbox_shell_options(args: &[String]) -> Result<SandboxShellOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    let mut timeout_ms = 0;
    let mut script = None;
    let mut execution = SandboxRunExecution::Local;
    let mut container = SandboxContainerRunBuilder::default();
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxRunExecution::Daemon;
                index += 1;
            }
            "--simple-native" => {
                execution = SandboxRunExecution::SimpleNative;
                index += 1;
            }
            "--container-runtime" => {
                index += 1;
                container.runtime = Some(parse_sandbox_container_runtime_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--container-image" => {
                index += 1;
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--docker" => {
                index += 1;
                container.runtime = Some(crate::SandboxContainerRuntime::Docker);
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--podman" => {
                index += 1;
                container.runtime = Some(crate::SandboxContainerRuntime::Podman);
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--timeout-ms" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                timeout_ms = value.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox timeout `{value}`: {source}"),
                })?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox timeout must be non-zero".to_string(),
                    });
                }
                index += 1;
            }
            "--" => {
                script = Some(rest[index + 1..].join(" "));
                index = rest.len();
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxShellOptions {
        sandbox_id,
        script,
        timeout_ms,
        execution: finalize_sandbox_run_execution(execution, container)?,
    })
}

pub fn parse_sandbox_run_options(args: &[String]) -> Result<SandboxRunOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    let mut timeout_ms = 0;
    let mut target = None;
    let mut execution = SandboxRunExecution::Local;
    let mut container = SandboxContainerRunBuilder::default();
    let mut stdin = false;
    let mut allow_unsigned = false;
    let mut allowed_capabilities = Vec::new();
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxRunExecution::Daemon;
                index += 1;
            }
            "--simple-native" => {
                execution = SandboxRunExecution::SimpleNative;
                index += 1;
            }
            "--container-runtime" => {
                index += 1;
                container.runtime = Some(parse_sandbox_container_runtime_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--container-image" => {
                index += 1;
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--docker" => {
                index += 1;
                container.runtime = Some(crate::SandboxContainerRuntime::Docker);
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--podman" => {
                index += 1;
                container.runtime = Some(crate::SandboxContainerRuntime::Podman);
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--timeout-ms" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                timeout_ms = value.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox timeout `{value}`: {source}"),
                })?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox timeout must be non-zero".to_string(),
                    });
                }
                index += 1;
            }
            "--stdin" => {
                stdin = true;
                index += 1;
            }
            "--allow-unsigned" => {
                allow_unsigned = true;
                index += 1;
            }
            "--allow-capability" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox run command capability must be non-empty".to_string(),
                    });
                }
                allowed_capabilities.push(value.to_string());
                index += 1;
            }
            "--wasm" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                target = Some(SandboxRunTarget::WasmModule(PathBuf::from(value)));
                index += 1;
            }
            "--" => {
                index += 1;
                break;
            }
            value if target.is_none() => {
                target = Some(SandboxRunTarget::ToolchainCommand(value.to_string()));
                index += 1;
            }
            _ => return Err(CliError::Usage),
        }
    }
    let execution = finalize_sandbox_run_execution(execution, container)?;
    let target = target.ok_or(CliError::Usage)?;
    if matches!(&execution, SandboxRunExecution::Daemon)
        && matches!(target, SandboxRunTarget::WasmModule(_))
    {
        return Err(CliError::Parse {
            detail: "sandbox daemon execution requires a pinned toolchain command, not --wasm"
                .to_string(),
        });
    }
    if matches!(&execution, SandboxRunExecution::Container(_))
        && matches!(target, SandboxRunTarget::WasmModule(_))
    {
        return Err(CliError::Parse {
            detail: "container sandbox execution runs commands inside an image and does not accept --wasm"
                .to_string(),
        });
    }
    if matches!(&execution, SandboxRunExecution::SimpleNative)
        && matches!(target, SandboxRunTarget::WasmModule(_))
    {
        return Err(CliError::Parse {
            detail: "simple-native sandbox execution runs host commands and does not accept --wasm"
                .to_string(),
        });
    }
    if matches!(&execution, SandboxRunExecution::Daemon)
        && (allow_unsigned || !allowed_capabilities.is_empty())
    {
        return Err(CliError::Parse {
            detail: "--allow-unsigned and --allow-capability only apply to local sandbox runs; daemon-backed runs use the cluster sandbox policy (`sinomo sandbox policy set ...`)"
                .to_string(),
        });
    }
    if matches!(&execution, SandboxRunExecution::Container(_))
        && (allow_unsigned || !allowed_capabilities.is_empty())
    {
        return Err(CliError::Parse {
            detail: "--allow-unsigned and --allow-capability apply to WASIX toolchain execution, not container sandbox execution"
                .to_string(),
        });
    }
    if matches!(&execution, SandboxRunExecution::SimpleNative)
        && (allow_unsigned || !allowed_capabilities.is_empty())
    {
        return Err(CliError::Parse {
            detail:
                "--allow-unsigned and --allow-capability apply to WASIX toolchain execution, not simple-native sandbox execution"
                    .to_string(),
        });
    }
    if matches!(&target, SandboxRunTarget::WasmModule(_)) && !allowed_capabilities.is_empty() {
        return Err(CliError::Parse {
            detail: "--allow-capability applies to pinned toolchain commands, not explicit --wasm modules"
                .to_string(),
        });
    }
    let argv0 = match &target {
        SandboxRunTarget::ToolchainCommand(command) => command.clone(),
        SandboxRunTarget::WasmModule(module_path) => module_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sandbox-command.wasm")
            .to_string(),
    };
    let mut argv = vec![argv0];
    argv.extend(rest[index..].iter().cloned());
    Ok(SandboxRunOptions {
        sandbox_id,
        target,
        argv,
        timeout_ms,
        stdin,
        allow_unsigned: allow_unsigned || matches!(&execution, SandboxRunExecution::Local),
        allowed_capabilities,
        execution,
    })
}

pub fn parse_sandbox_process_plan_options(
    args: &[String],
) -> Result<SandboxProcessPlanOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    let mut timeout_ms = crate::SandboxResourceLimits::default().wall_time_ms;
    let mut execution = SandboxRunExecution::Local;
    let mut container = SandboxContainerRunBuilder::default();
    let mut plan_path = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--timeout-ms" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                timeout_ms = value.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox timeout `{value}`: {source}"),
                })?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox timeout must be non-zero".to_string(),
                    });
                }
                index += 1;
            }
            "--daemon" => {
                execution = SandboxRunExecution::Daemon;
                index += 1;
            }
            "--simple-native" => {
                execution = SandboxRunExecution::SimpleNative;
                index += 1;
            }
            "--container-runtime" => {
                index += 1;
                container.runtime = Some(parse_sandbox_container_runtime_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--container-image" => {
                index += 1;
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--docker" => {
                index += 1;
                container.runtime = Some(crate::SandboxContainerRuntime::Docker);
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            "--podman" => {
                index += 1;
                container.runtime = Some(crate::SandboxContainerRuntime::Podman);
                container.image = Some(parse_sandbox_container_image_arg(rest.get(index))?);
                container.requested = true;
                index += 1;
            }
            value if plan_path.is_none() => {
                plan_path = Some(PathBuf::from(value));
                index += 1;
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxProcessPlanOptions {
        sandbox_id,
        plan_path: plan_path.ok_or(CliError::Usage)?,
        timeout_ms,
        execution: finalize_sandbox_run_execution(execution, container)?,
    })
}

pub fn parse_sandbox_logs_options(args: &[String]) -> Result<SandboxLogsOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut command_id = None;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            value if command_id.is_none() => command_id = Some(parse_sandbox_command_id(value)?),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxLogsOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        command_id,
        execution,
    })
}

pub fn parse_sandbox_toolchain_command(args: &[String]) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "add-command" => {
            Ok(SandboxCliCommand::ToolchainAddCommand(
                parse_sandbox_toolchain_add_command_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "add-artifact" => {
            Ok(SandboxCliCommand::ToolchainAddArtifact(
                parse_sandbox_toolchain_add_artifact_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "import-busybox" => {
            Ok(SandboxCliCommand::ToolchainImportBusyBox(
                parse_sandbox_toolchain_import_busybox_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "import-package" => {
            Ok(SandboxCliCommand::ToolchainImportPackage(
                parse_sandbox_toolchain_import_package_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-package" => {
            Ok(SandboxCliCommand::ToolchainVerifyPackage(
                parse_sandbox_toolchain_verify_package_options(rest)?,
            ))
        }
        Some((command, [])) if command == "busybox-plan" => {
            Ok(SandboxCliCommand::ToolchainBusyBoxPlan)
        }
        Some((command, [])) if command == "bash-plan" => Ok(SandboxCliCommand::ToolchainBashPlan),
        Some((command, [])) if command == "build-tools-plan" => {
            Ok(SandboxCliCommand::ToolchainBuildToolsPlan)
        }
        Some((command, [])) if command == "python-plan" => {
            Ok(SandboxCliCommand::ToolchainPythonPlan)
        }
        Some((command, rest)) if command == "hash" => Ok(SandboxCliCommand::ToolchainHash(
            parse_sandbox_toolchain_hash_options(rest)?,
        )),
        Some((command, rest)) if command == "verify-bash" => {
            Ok(SandboxCliCommand::ToolchainVerifyBash(
                parse_sandbox_toolchain_verify_bash_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-busybox" => {
            Ok(SandboxCliCommand::ToolchainVerifyBusyBox(
                parse_sandbox_toolchain_verify_busybox_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-coding" => {
            Ok(SandboxCliCommand::ToolchainVerifyCoding(
                parse_sandbox_toolchain_verify_coding_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-make" => {
            Ok(SandboxCliCommand::ToolchainVerifyMake(
                parse_sandbox_toolchain_verify_make_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-makepkg" => {
            Ok(SandboxCliCommand::ToolchainVerifyMakepkg(
                parse_sandbox_toolchain_verify_makepkg_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-tcc" => {
            Ok(SandboxCliCommand::ToolchainVerifyTcc(
                parse_sandbox_toolchain_verify_tcc_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-python" => {
            Ok(SandboxCliCommand::ToolchainVerifyPython(
                parse_sandbox_toolchain_verify_python_options(rest)?,
            ))
        }
        _ => Err(CliError::Usage),
    }
}

pub fn parse_sandbox_toolchain_import_package_options(
    args: &[String],
) -> Result<SandboxToolchainImportPackageOptions, CliError> {
    let mut package_path = None;
    let mut target_toolchain_id = None;
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--into" | "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                target_toolchain_id = Some(parse_toolchain_id(value)?);
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value if package_path.is_none() => package_path = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxToolchainImportPackageOptions {
        package_path: package_path.ok_or(CliError::Usage)?,
        target_toolchain_id,
        execution,
    })
}

pub fn parse_sandbox_toolchain_hash_options(
    args: &[String],
) -> Result<SandboxToolchainHashOptions, CliError> {
    let (path, rest) = args.split_first().ok_or(CliError::Usage)?;
    if !rest.is_empty() || path.starts_with('-') {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainHashOptions {
        path: PathBuf::from(path),
    })
}

pub fn parse_sandbox_toolchain_verify_package_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyPackageOptions, CliError> {
    let (path, rest) = args.split_first().ok_or(CliError::Usage)?;
    if !rest.is_empty() || path.starts_with('-') {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainVerifyPackageOptions {
        package_path: PathBuf::from(path),
    })
}

pub fn parse_sandbox_toolchain_verify_busybox_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyBusyBoxOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyBusyBoxOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub fn parse_sandbox_toolchain_verify_bash_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyBashOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyBashOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub fn parse_sandbox_toolchain_verify_python_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyPythonOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyPythonOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub fn parse_sandbox_toolchain_verify_coding_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyCodingOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    let mut full = false;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--full" => full = true,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyCodingOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
        full,
    })
}

pub fn parse_sandbox_toolchain_verify_make_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyMakeOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyMakeOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub fn parse_sandbox_toolchain_verify_makepkg_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyMakepkgOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyMakepkgOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub fn parse_sandbox_toolchain_verify_tcc_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyTccOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyTccOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

fn validate_sandbox_verify_unsigned_scope(
    allow_unsigned: bool,
    execution: SandboxMetadataExecution,
) -> Result<(), CliError> {
    if allow_unsigned && execution == SandboxMetadataExecution::Daemon {
        return Err(CliError::Parse {
            detail: "--allow-unsigned only applies to local verification; daemon-backed verification uses the cluster sandbox policy (`sinomo sandbox policy set --allow-unsigned-toolchains`)"
                .to_string(),
        });
    }
    Ok(())
}

pub fn parse_sandbox_toolchain_import_busybox_options(
    args: &[String],
) -> Result<SandboxToolchainImportBusyBoxOptions, CliError> {
    let mut version = None;
    let mut provenance = "Sinomo-maintained BusyBox WASI module import".to_string();
    let mut module_path = None;
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "BusyBox toolchain version must be non-empty".to_string(),
                    });
                }
                version = Some(value.clone());
            }
            "--provenance" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                provenance = value.clone();
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value if module_path.is_none() => module_path = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxToolchainImportBusyBoxOptions {
        version: version.ok_or(CliError::Usage)?,
        provenance,
        module_path: module_path.ok_or(CliError::Usage)?,
        execution,
    })
}

pub fn parse_sandbox_toolchain_add_command_options(
    args: &[String],
) -> Result<SandboxToolchainAddCommandOptions, CliError> {
    let mut toolchain_id = crate::SandboxToolchainId::from_legacy_named("toolchain_busybox")
        .expect("static toolchain id");
    let mut version = "local".to_string();
    let mut target_abi = crate::default_sandbox_wasi_abi();
    let mut provenance = "local CLI import".to_string();
    let mut execution = SandboxMetadataExecution::Local;
    let mut required_capabilities = Vec::new();
    let mut default_limits = crate::SandboxResourceLimits::default();
    let mut positionals = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                toolchain_id = parse_toolchain_id(value)?;
            }
            "--version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain version must be non-empty".to_string(),
                    });
                }
                version = value.clone();
            }
            "--target-abi" | "--abi" => {
                index += 1;
                target_abi = parse_sandbox_wasi_abi(args.get(index))?;
            }
            "--provenance" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                provenance = value.clone();
            }
            "--capability" | "--require-capability" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_command_capability(value, "sandbox toolchain command capability")?;
                required_capabilities.push(value.clone());
            }
            "--memory-mb" => {
                index += 1;
                default_limits.memory_mb =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command memory mb")?;
            }
            "--wall-time-ms" => {
                index += 1;
                default_limits.wall_time_ms =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command wall time")?;
            }
            "--max-output-bytes" => {
                index += 1;
                default_limits.max_output_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command output bytes")?;
            }
            "--max-input-bytes" => {
                index += 1;
                default_limits.max_input_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command input bytes")?;
            }
            "--max-workspace-bytes" => {
                index += 1;
                default_limits.max_workspace_bytes = parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "command workspace bytes",
                )?;
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                default_limits.max_written_bytes_per_command =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command written bytes")?;
            }
            "--max-file-count" => {
                index += 1;
                default_limits.max_file_count =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command file count")?;
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }
    if positionals.len() != 2 {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainAddCommandOptions {
        toolchain_id,
        version,
        target_abi,
        provenance,
        command_name: positionals[0].clone(),
        module_path: PathBuf::from(&positionals[1]),
        execution,
        required_capabilities,
        default_limits,
    })
}

fn parse_sandbox_wasi_abi(value: Option<&String>) -> Result<crate::SandboxWasiAbi, CliError> {
    match value.map(String::as_str) {
        Some("wasix") => Ok(crate::SandboxWasiAbi::Wasix),
        Some("preview1") | Some("wasi-preview1") | Some("wasi") => Err(CliError::Parse {
            detail: "WASI Preview1 sandbox execution has been removed; use --abi wasix".to_string(),
        }),
        Some(value) => Err(CliError::Parse {
            detail: format!("unsupported sandbox WASI target ABI `{value}`"),
        }),
        None => Err(CliError::Usage),
    }
}

pub fn parse_sandbox_toolchain_add_artifact_options(
    args: &[String],
) -> Result<SandboxToolchainAddArtifactOptions, CliError> {
    let mut toolchain_id = crate::SandboxToolchainId::from_legacy_named("toolchain_busybox")
        .expect("static toolchain id");
    let mut version = "local".to_string();
    let mut provenance = "local CLI import".to_string();
    let mut execution = SandboxMetadataExecution::Local;
    let mut positionals = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                toolchain_id = parse_toolchain_id(value)?;
            }
            "--version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain version must be non-empty".to_string(),
                    });
                }
                version = value.clone();
            }
            "--provenance" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                provenance = value.clone();
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }
    if positionals.len() != 2 {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainAddArtifactOptions {
        toolchain_id,
        version,
        provenance,
        relative_path: positionals[0].clone(),
        source_path: PathBuf::from(&positionals[1]),
        execution,
    })
}

pub fn parse_sandbox_create_options(args: &[String]) -> Result<SandboxCreateOptions, CliError> {
    let mut sandbox_id = None;
    let mut toolchain_id = crate::SandboxToolchainId::from_legacy_named("toolchain_busybox")
        .expect("static toolchain id");
    let mut temp_policy = crate::SandboxTempPolicy::ClearOnSessionStart;
    let mut memory_mb = None;
    let mut wall_time_ms = None;
    let mut max_input_bytes = None;
    let mut max_output_bytes = None;
    let mut max_workspace_bytes = None;
    let mut max_written_bytes_per_command = None;
    let mut max_file_count = None;
    let mut mounts = Vec::new();
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                sandbox_id = Some(parse_sandbox_id(value)?);
            }
            "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                toolchain_id = parse_toolchain_id(value)?;
            }
            "--memory-mb" => {
                index += 1;
                memory_mb = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "memory mb",
                )?);
            }
            "--wall-time-ms" => {
                index += 1;
                wall_time_ms = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "wall time",
                )?);
            }
            "--max-output-bytes" => {
                index += 1;
                max_output_bytes = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "output bytes",
                )?);
            }
            "--max-input-bytes" => {
                index += 1;
                max_input_bytes = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "input bytes",
                )?);
            }
            "--max-workspace-bytes" => {
                index += 1;
                max_workspace_bytes = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "workspace bytes",
                )?);
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                max_written_bytes_per_command = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "written bytes",
                )?);
            }
            "--max-file-count" => {
                index += 1;
                max_file_count = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "file count",
                )?);
            }
            "--tmp" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                temp_policy = parse_sandbox_temp_policy(value)?;
            }
            "--keep-tmp" => {
                temp_policy = crate::SandboxTempPolicy::RetainAcrossSessions;
            }
            "--mount" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                mounts.push(parse_sandbox_mount(value)?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxCreateOptions {
        sandbox_id,
        toolchain_id,
        temp_policy,
        memory_mb,
        wall_time_ms,
        max_input_bytes,
        max_output_bytes,
        max_workspace_bytes,
        max_written_bytes_per_command,
        max_file_count,
        mounts,
        execution,
    })
}

pub fn run_sandbox_cli_command(command: SandboxCliCommand) -> Result<(), CliError> {
    match command {
        SandboxCliCommand::Help => {
            print_sandbox_help();
            Ok(())
        }
        SandboxCliCommand::Create(options) => sandbox_create(options),
        SandboxCliCommand::Config(options) => sandbox_config(options),
        SandboxCliCommand::Cat(options) => sandbox_cat(options),
        SandboxCliCommand::Checkpoint(options) => sandbox_checkpoint(options),
        SandboxCliCommand::Destroy(options) => sandbox_destroy(options),
        SandboxCliCommand::Doctor(options) => sandbox_doctor(options),
        SandboxCliCommand::Fetch(options) => sandbox_fetch(options),
        SandboxCliCommand::Ls(options) => sandbox_ls(options),
        SandboxCliCommand::Logs(options) => sandbox_logs(options),
        SandboxCliCommand::ModuleArtifact(command) => sandbox_module_artifact(command),
        SandboxCliCommand::ModuleReplay(command) => sandbox_module_replay(command),
        SandboxCliCommand::Policy(command) => sandbox_policy(command),
        SandboxCliCommand::ProcessPlan(options) => sandbox_process_plan(options),
        SandboxCliCommand::Reboot(options) => sandbox_reboot(options),
        SandboxCliCommand::Resume(options) => sandbox_resume(options),
        SandboxCliCommand::Run(options) => sandbox_run(options),
        SandboxCliCommand::Shell(options) => sandbox_shell(options),
        SandboxCliCommand::Status(options) => sandbox_status(options),
        SandboxCliCommand::Write(options) => sandbox_write(options),
        SandboxCliCommand::ToolchainAddCommand(options) => sandbox_toolchain_add_command(options),
        SandboxCliCommand::ToolchainAddArtifact(options) => sandbox_toolchain_add_artifact(options),
        SandboxCliCommand::ToolchainBashPlan => {
            sandbox_toolchain_bash_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainBusyBoxPlan => {
            sandbox_toolchain_busybox_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainBuildToolsPlan => {
            sandbox_toolchain_build_tools_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainHash(options) => sandbox_toolchain_hash(options),
        SandboxCliCommand::ToolchainImportBusyBox(options) => {
            sandbox_toolchain_import_busybox(options)
        }
        SandboxCliCommand::ToolchainImportPackage(options) => {
            sandbox_toolchain_import_package(options)
        }
        SandboxCliCommand::ToolchainVerifyPackage(options) => {
            sandbox_toolchain_verify_package(options)
        }
        SandboxCliCommand::ToolchainPythonPlan => {
            sandbox_toolchain_python_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainVerifyBash(options) => sandbox_toolchain_verify_bash(options),
        SandboxCliCommand::ToolchainVerifyBusyBox(options) => {
            sandbox_toolchain_verify_busybox(options)
        }
        SandboxCliCommand::ToolchainVerifyCoding(options) => {
            sandbox_toolchain_verify_coding(options)
        }
        SandboxCliCommand::ToolchainVerifyMake(options) => sandbox_toolchain_verify_make(options),
        SandboxCliCommand::ToolchainVerifyMakepkg(options) => {
            sandbox_toolchain_verify_makepkg(options)
        }
        SandboxCliCommand::ToolchainVerifyPython(options) => {
            sandbox_toolchain_verify_python(options)
        }
        SandboxCliCommand::ToolchainVerifyTcc(options) => sandbox_toolchain_verify_tcc(options),
    }
}

pub fn sandbox_create(options: SandboxCreateOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_create_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_create_with_daemon(options),
    }
}

pub fn sandbox_status(options: SandboxStatusOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_status_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_status_with_daemon(options),
    }
}

pub fn sandbox_config(options: SandboxConfigOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_config_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_config_with_daemon(options),
    }
}

pub fn sandbox_run(options: SandboxRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let stdin = if options.stdin {
        let mut input = Vec::new();
        io::stdin()
            .read_to_end(&mut input)
            .map_err(|source| CliError::Parse {
                detail: format!("failed to read sandbox command stdin: {source}"),
            })?;
        Some(input)
    } else {
        None
    };
    match &options.execution {
        SandboxRunExecution::Local
        | SandboxRunExecution::SimpleNative
        | SandboxRunExecution::Container(_) => sandbox_run_with_stores_and_stdin(
            options,
            stdin,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxRunExecution::Daemon => sandbox_run_with_daemon_and_stdin(options, stdin),
    }
}

pub fn sandbox_process_plan(options: SandboxProcessPlanOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match &options.execution {
        SandboxRunExecution::Local
        | SandboxRunExecution::SimpleNative
        | SandboxRunExecution::Container(_) => sandbox_process_plan_with_stores(
            options,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxRunExecution::Daemon => sandbox_process_plan_with_daemon(options),
    }
}

pub fn sandbox_shell(options: SandboxShellOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let stdin = match &options.script {
        Some(script) => script.as_bytes().to_vec(),
        None => {
            let mut bytes = Vec::new();
            io::stdin()
                .lock()
                .read_to_end(&mut bytes)
                .map_err(CliError::stdio)?;
            bytes
        }
    };
    match &options.execution {
        SandboxRunExecution::Local
        | SandboxRunExecution::SimpleNative
        | SandboxRunExecution::Container(_) => sandbox_shell_with_stores(
            options,
            stdin,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxRunExecution::Daemon => sandbox_shell_with_daemon(options, stdin),
    }
}

pub fn sandbox_logs(options: SandboxLogsOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_logs_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_logs_with_daemon(options),
    }
}

pub fn sandbox_ls(options: SandboxLsOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_ls_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_ls_with_daemon(options),
    }
}

pub fn sandbox_cat(options: SandboxCatOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_cat_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_cat_with_daemon(options),
    }
}

pub fn sandbox_write(options: SandboxWriteOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let bytes = sandbox_write_input_bytes(&options.input)?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            let mut event_store = sinomo_core::events::JsonlEventStore::new(
                data_dir.root().join("events/event-log.jsonl"),
            );
            sandbox_write_with_store_and_event_store(
                options,
                bytes,
                &sandbox_store_from_data_dir(&data_dir),
                &mut event_store,
            )
        }
        SandboxMetadataExecution::Daemon => sandbox_write_with_daemon(options, bytes),
    }
}

fn sandbox_write_input_bytes(input: &SandboxWriteInput) -> Result<Vec<u8>, CliError> {
    match input {
        SandboxWriteInput::Literal(bytes) => Ok(bytes.clone()),
        SandboxWriteInput::HostFile(path) => std::fs::read(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
        SandboxWriteInput::Stdin => {
            let mut bytes = Vec::new();
            io::stdin()
                .lock()
                .read_to_end(&mut bytes)
                .map_err(CliError::stdio)?;
            Ok(bytes)
        }
    }
}

pub fn sandbox_fetch(options: SandboxFetchOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    if options.execution == SandboxMetadataExecution::Daemon {
        return sandbox_fetch_with_daemon(options);
    }
    let (config, _source) = load_sandbox_policy_config_for_node(&data_dir)?;
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox fetch requires the local daemon for mediated network access"
                .to_string(),
        });
    };
    let transport = DaemonMediatedSandboxHttpTransport { address: address };
    let mut event_store =
        sinomo_core::events::JsonlEventStore::new(data_dir.root().join("events/event-log.jsonl"));
    let _ = sandbox_fetch_with_store_transport_and_event_store(
        options,
        &sandbox_store_from_data_dir(&data_dir),
        &config.policy,
        &transport,
        &mut event_store,
    )?;
    Ok(())
}

pub fn sandbox_fetch_with_daemon(options: SandboxFetchOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon fetch requires the local daemon".to_string(),
        });
    };
    let mut request = crate::SandboxHttpRequest::get(options.url);
    request.timeout_ms = options.timeout_ms;
    request.max_response_bytes = options.max_response_bytes;
    request.headers = options.headers;
    let request = crate::SandboxHttpFetchToFileRequest {
        sandbox_id: options.sandbox_id,
        request,
        output_path: options.output_path,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let timeout =
        std::time::Duration::from_millis(request.request.timeout_ms.saturating_add(1_000));
    let response = send_daemon_rpc(&address, &rpc, timeout)?;
    let result: crate::SandboxHttpFetchFileResult =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_http_fetch_file_result(&result);
    Ok(())
}

pub fn sandbox_checkpoint(options: SandboxCheckpointOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_checkpoint_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_checkpoint_with_daemon(options),
    }
}

pub fn sandbox_resume(options: SandboxResumeOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_resume_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_resume_with_daemon(options),
    }
}

pub fn sandbox_reboot(options: SandboxRebootOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_reboot_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_reboot_with_daemon(options),
    }
}

pub fn sandbox_destroy(options: SandboxDestroyOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_destroy_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_destroy_with_daemon(options),
    }
}

pub fn sandbox_doctor(options: SandboxDoctorOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let findings = sandbox_doctor_findings_with_stores_and_events(
        options,
        &sandbox_store_from_data_dir(&data_dir),
        &sandbox_toolchain_store_from_data_dir(&data_dir),
        &crate::SandboxPolicy::default(),
        &sandbox_doctor_audit_events(&data_dir)?,
    )?;
    print_sandbox_doctor_findings(&findings);
    Ok(())
}

pub fn sandbox_doctor_findings_with_stores(
    options: SandboxDoctorOptions,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
    policy: &crate::SandboxPolicy,
) -> Result<Vec<SandboxDoctorFinding>, CliError> {
    sandbox_doctor_findings_with_stores_and_events(options, store, toolchain_store, policy, &[])
}

pub fn sandbox_doctor_findings_with_stores_and_events(
    options: SandboxDoctorOptions,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
    policy: &crate::SandboxPolicy,
    resource_events: &[sinomo_core::events::StoredEvent],
) -> Result<Vec<SandboxDoctorFinding>, CliError> {
    let mut findings = Vec::new();
    findings.push(SandboxDoctorFinding::info(
        "local-staging-backend",
        "local CLI sandbox commands use local staging by default; pass --daemon on supported commands for SIFS-backed daemon execution",
    ));
    append_sandbox_doctor_runtime_capability_findings(&mut findings);
    append_sandbox_doctor_policy_posture_findings(&mut findings, policy);

    let busybox_id =
        crate::SandboxToolchainId::from_legacy_named(crate::BUSYBOX_TOOLCHAIN_LEGACY_ID)
            .expect("static toolchain id");
    match toolchain_store.load_manifest(&busybox_id) {
        Ok((manifest, _layout)) => {
            for command in ["cat", "grep", "find", "printf", "sh"] {
                if !manifest.commands.contains_key(command) {
                    findings.push(SandboxDoctorFinding::warning(
                        "busybox-command-missing",
                        format!("default BusyBox toolchain is missing `{command}`"),
                    ));
                }
            }
        }
        Err(crate::SandboxToolchainError::NotFound { .. }) => {
            findings.push(SandboxDoctorFinding::warning(
                "busybox-toolchain-missing",
                "default BusyBox toolchain is not installed in the local toolchain cache; import it with `sinomo sandbox toolchain import-busybox`",
            ));
        }
        Err(error) => return Err(error.into()),
    }

    let Some(sandbox_id) = options.sandbox_id else {
        append_sandbox_doctor_audit_findings(&mut findings, None, resource_events);
        return Ok(findings);
    };
    let (manifest, layout) = store.load(&sandbox_id)?;
    for dir in layout.required_dirs() {
        if !dir.is_dir() {
            findings.push(SandboxDoctorFinding::error(
                "sandbox-directory-missing",
                format!("required sandbox directory is missing: {}", dir.display()),
            ));
        }
    }

    match toolchain_store.load_manifest(&manifest.toolchain_id) {
        Ok((toolchain, _layout)) => {
            append_sandbox_doctor_coding_toolchain_findings(&mut findings, &manifest, &toolchain);
            append_sandbox_doctor_rich_toolchain_findings(&mut findings, &manifest, &toolchain);
            append_sandbox_doctor_mvp_toolchain_findings(&mut findings, &manifest, &toolchain);
            match toolchain.command("sh") {
                Ok(command) => {
                    let command_abi = command.effective_target_abi(&toolchain);
                    if crate::sandbox_command_requires_child_process_substrate(&command.name)
                        && !crate::sandbox_runtime_supports_child_processes_for_abi(&command_abi)
                    {
                        findings.push(SandboxDoctorFinding::error(
                            "shell-process-substrate-missing",
                            format!(
                                "sandbox {} is pinned to {:?} toolchain {}, and `sh` requires a real child-process execution substrate but the current runtime does not support `{}` for that ABI",
                                manifest.sandbox_id,
                                command_abi,
                                manifest.toolchain_id,
                                crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY
                            ),
                        ));
                    }
                    if matches!(command_abi, crate::SandboxWasiAbi::Wasix)
                        && command.required_capabilities.iter().any(|capability| {
                            capability == crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                        })
                    {
                        findings.push(SandboxDoctorFinding::warning(
                            "wasix-unpinned-child-exec-acknowledged",
                            format!(
                                "sandbox {} is pinned to WASIX `sh` from toolchain {}, and that command relies on `{}` so shell commands can execute sandbox-local WASM modules and scripts from writable mounts such as /work",
                                manifest.sandbox_id,
                                manifest.toolchain_id,
                                crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                            ),
                        ));
                    }
                }
                Err(_) => {
                    findings.push(SandboxDoctorFinding::error(
                        "shell-command-missing",
                        format!(
                            "sandbox {} is pinned to toolchain {}, but that toolchain does not provide real `sh`; the sandbox must not claim shell support until a genuine shell/process substrate is available",
                            manifest.sandbox_id, manifest.toolchain_id
                        ),
                    ));
                }
            }
        }
        Err(crate::SandboxToolchainError::NotFound { .. }) => {
            findings.push(SandboxDoctorFinding::error(
                "sandbox-toolchain-missing",
                format!(
                    "sandbox {} is pinned to missing toolchain {}",
                    manifest.sandbox_id, manifest.toolchain_id
                ),
            ));
        }
        Err(error) => return Err(error.into()),
    }

    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    if let Err(denial) = policy.validate_manifest(&manifest, &execution_node_id) {
        findings.push(SandboxDoctorFinding::error(
            "policy-denied",
            denial.to_string(),
        ));
    }
    if !manifest.mounts.is_empty() {
        findings.push(SandboxDoctorFinding::warning(
            "local-cli-mounts-not-staged",
            "local CLI runs validate declared SIFS mounts but do not stage them into WASI; use the SIFS staged runner path for real mounted execution",
        ));
    }
    if let Some(placement) = &manifest.last_placement {
        findings.push(SandboxDoctorFinding::info(
            "last-placement",
            format!(
                "last SIFS placement selected node {} for {:?}",
                placement.node_id, placement.reason
            ),
        ));
    }
    append_sandbox_doctor_audit_findings(&mut findings, Some(&sandbox_id), resource_events);
    Ok(findings)
}

fn append_sandbox_doctor_runtime_capability_findings(findings: &mut Vec<SandboxDoctorFinding>) {
    if crate::sandbox_runtime_disables_ambient_network() {
        findings.push(SandboxDoctorFinding::info(
            "ambient-wasi-network-disabled",
            "WASI command execution has no ambient TCP, UDP, or name-lookup access; use mediated sandbox fetch for network egress",
        ));
    }
    for abi in [crate::SandboxWasiAbi::Wasix] {
        for capability in crate::sandbox_runtime_process_capabilities_for_abi(&abi) {
            if capability.supported {
                findings.push(SandboxDoctorFinding::info(
                    "process-substrate-available",
                    format!(
                        "{abi:?} sandbox runtime capability `{}` is available: {}",
                        capability.capability, capability.detail
                    ),
                ));
                continue;
            }
            findings.push(SandboxDoctorFinding::warning(
                "process-substrate-missing",
                format!(
                    "{abi:?} sandbox runtime capability `{}` is not available yet: {}",
                    capability.capability, capability.detail
                ),
            ));
        }
        for capability in crate::sandbox_runtime_budget_capabilities_for_abi(&abi) {
            if capability.supported {
                findings.push(SandboxDoctorFinding::info(
                    "runtime-budget-supported",
                    format!(
                        "{abi:?} sandbox runtime budget capability `{}` is available: {}",
                        capability.capability, capability.detail
                    ),
                ));
                continue;
            }
            findings.push(SandboxDoctorFinding::warning(
                "runtime-budget-missing",
                format!(
                    "{abi:?} sandbox runtime budget capability `{}` is not available yet: {}",
                    capability.capability, capability.detail
                ),
            ));
        }
    }
    for capability in crate::sandbox_runtime_mvp_virtual_device_capabilities() {
        if capability.supported {
            continue;
        }
        findings.push(SandboxDoctorFinding::warning(
            "mvp-wasi-device-missing",
            format!(
                "sandbox filesystem exposes `{}`, but WASI command execution cannot open it through the runtime device layer yet: {}",
                capability.device, capability.detail
            ),
        ));
    }
}

fn append_sandbox_doctor_policy_posture_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    policy: &crate::SandboxPolicy,
) {
    if policy
        .allowed_command_capabilities
        .contains(crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
    {
        findings.push(SandboxDoctorFinding::warning(
            "policy-allows-wasix-unpinned-child-exec",
            format!(
                "sandbox policy allows `{}`; WASIX shell, Python, and build-tool commands that use this capability can execute sandbox-local WASM modules and scripts from writable mounts such as /work",
                crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
            ),
        ));
    }
}

fn append_sandbox_doctor_mvp_toolchain_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    manifest: &crate::SandboxInstanceManifest,
    toolchain: &crate::SandboxToolchainManifest,
) {
    let mut missing = Vec::new();
    for command in crate::SANDBOX_MVP_COMMANDS {
        if toolchain.command(command).is_err() {
            missing.push(*command);
            let reason = crate::sandbox_mvp_command_gap(command)
                .map(|gap| format!(": {}", gap.reason))
                .unwrap_or_default();
            findings.push(SandboxDoctorFinding::warning(
                "mvp-command-missing",
                format!(
                    "sandbox {} is pinned to toolchain {}, but that toolchain is missing `{command}` from the planned minimal POSIX command environment{reason}",
                    manifest.sandbox_id, manifest.toolchain_id
                ),
            ));
        }
    }
    if !missing.is_empty() {
        findings.push(SandboxDoctorFinding::error(
            "mvp-command-surface-incomplete",
            format!(
                "sandbox {} is pinned to toolchain {}, but the planned minimal POSIX command surface is incomplete; missing commands: {}",
                manifest.sandbox_id,
                manifest.toolchain_id,
                missing
                    .iter()
                    .map(|command| format!("`{command}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
}

fn append_sandbox_doctor_rich_toolchain_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    manifest: &crate::SandboxInstanceManifest,
    toolchain: &crate::SandboxToolchainManifest,
) {
    for target in crate::SANDBOX_RICH_TOOLCHAIN_TARGETS {
        match target.status {
            crate::SandboxRichToolchainStatus::Implemented => {
                if toolchain.command(target.command).is_err() {
                    let recipe = target
                        .recipe
                        .map(|recipe| format!("; recipe: {recipe}"))
                        .unwrap_or_default();
                    findings.push(SandboxDoctorFinding::warning(
                        "rich-toolchain-command-missing",
                        format!(
                            "sandbox {} is pinned to toolchain {}, but maintained richer toolchain target `{}` is missing{}",
                            manifest.sandbox_id, manifest.toolchain_id, target.command, recipe
                        ),
                    ));
                }
            }
            crate::SandboxRichToolchainStatus::Planned => {
                findings.push(SandboxDoctorFinding::info(
                    "rich-toolchain-target-planned",
                    format!(
                        "planned richer toolchain target `{}`: {}",
                        target.command, target.note
                    ),
                ));
            }
            crate::SandboxRichToolchainStatus::Required => {
                if toolchain.command(target.command).is_err() {
                    let recipe = target
                        .recipe
                        .map(|recipe| format!("; target doc: {recipe}"))
                        .unwrap_or_default();
                    findings.push(SandboxDoctorFinding::error(
                        "required-toolchain-command-missing",
                        format!(
                            "sandbox {} is pinned to toolchain {}, but required coding sandbox command `{}` is missing{}; {}",
                            manifest.sandbox_id,
                            manifest.toolchain_id,
                            target.command,
                            recipe,
                            target.note
                        ),
                    ));
                }
            }
        }
    }
}

fn append_sandbox_doctor_coding_toolchain_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    manifest: &crate::SandboxInstanceManifest,
    toolchain: &crate::SandboxToolchainManifest,
) {
    for command in crate::BUSYBOX_TOOLCHAIN_COMMANDS {
        if toolchain.command(command).is_err() {
            findings.push(SandboxDoctorFinding::warning(
                "coding-command-missing",
                format!(
                    "sandbox {} is pinned to toolchain {}, but that toolchain is missing `{command}` required by `sinomo sandbox toolchain verify-coding`",
                    manifest.sandbox_id, manifest.toolchain_id
                ),
            ));
        }
    }
}

fn sandbox_doctor_audit_events(
    data_dir: &SinomoDataDir,
) -> Result<Vec<sinomo_core::events::StoredEvent>, CliError> {
    let event_path = data_dir.root().join("events").join("event-log.jsonl");
    if !event_path.exists() {
        return Ok(Vec::new());
    }
    let event_store = sinomo_core::events::JsonlEventStore::new(event_path);
    let mut query = sinomo_core::events::ReplayQuery::by_kind_prefix("sandbox.").limit(32);
    query.direction = sinomo_core::events::ReplayDirection::Reverse;
    event_store.replay(query).map_err(CliError::from)
}

fn append_sandbox_doctor_audit_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    sandbox_id: Option<&crate::SandboxId>,
    events: &[sinomo_core::events::StoredEvent],
) {
    for event in events {
        if !sandbox_doctor_event_matches_sandbox(event, sandbox_id) {
            continue;
        }
        match event.event.kind.as_str() {
            "sandbox.policy.denied" => append_sandbox_doctor_policy_finding(findings, event),
            "sandbox.resource.exhausted" => append_sandbox_doctor_resource_finding(findings, event),
            _ => {}
        }
    }
}

fn sandbox_doctor_event_matches_sandbox(
    event: &sinomo_core::events::StoredEvent,
    sandbox_id: Option<&crate::SandboxId>,
) -> bool {
    let Some(sandbox_id) = sandbox_id else {
        return true;
    };
    event.event.payload["sandbox_id"].as_str() == Some(sandbox_id.to_string().as_str())
}

fn append_sandbox_doctor_policy_finding(
    findings: &mut Vec<SandboxDoctorFinding>,
    event: &sinomo_core::events::StoredEvent,
) {
    let event_sandbox_id = event.event.payload["sandbox_id"].as_str();
    let scope = event_sandbox_id
        .map(|sandbox_id| format!("sandbox {sandbox_id}"))
        .unwrap_or_else(|| "cluster sandbox service".to_string());
    let phase = event.event.payload["phase"].as_str().unwrap_or("unknown");
    let code = event.event.payload["code"].as_str().unwrap_or("unknown");
    let detail = event.event.payload["detail"].as_str().unwrap_or("unknown");
    findings.push(SandboxDoctorFinding::warning(
        "recent-policy-denied",
        format!("recent sandbox policy denial in {scope} during {phase}: {code}: {detail}"),
    ));
}

fn append_sandbox_doctor_resource_finding(
    findings: &mut Vec<SandboxDoctorFinding>,
    event: &sinomo_core::events::StoredEvent,
) {
    let event_sandbox_id = event.event.payload["sandbox_id"]
        .as_str()
        .unwrap_or("unknown");
    let phase = event.event.payload["phase"].as_str().unwrap_or("unknown");
    let kind = event.event.payload["kind"].as_str().unwrap_or("unknown");
    let actual = event.event.payload["actual"]
        .as_u64()
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let limit = event.event.payload["limit"]
        .as_u64()
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    findings.push(SandboxDoctorFinding::warning(
        "recent-resource-exhausted",
        format!(
            "recent sandbox resource exhaustion in sandbox {event_sandbox_id} during {phase}: {kind} actual {actual}, limit {limit}"
        ),
    ));
}

impl SandboxDoctorFinding {
    fn info(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: SandboxDoctorSeverity::Info,
            code: code.into(),
            detail: detail.into(),
        }
    }

    fn warning(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: SandboxDoctorSeverity::Warning,
            code: code.into(),
            detail: detail.into(),
        }
    }

    fn error(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: SandboxDoctorSeverity::Error,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

fn print_sandbox_doctor_findings(findings: &[SandboxDoctorFinding]) {
    println!(
        "sandbox doctor: {}",
        if findings
            .iter()
            .all(|finding| finding.severity == SandboxDoctorSeverity::Info)
        {
            "ok"
        } else {
            "findings"
        }
    );
    if findings.is_empty() {
        println!("no findings");
        return;
    }
    for finding in findings {
        println!(
            "- [{}] {}: {}",
            sandbox_doctor_severity_label(&finding.severity),
            finding.code,
            finding.detail
        );
    }
}

fn sandbox_doctor_severity_label(severity: &SandboxDoctorSeverity) -> &'static str {
    match severity {
        SandboxDoctorSeverity::Info => "info",
        SandboxDoctorSeverity::Warning => "warning",
        SandboxDoctorSeverity::Error => "error",
    }
}

pub fn sandbox_policy(command: SandboxPolicyCliCommand) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match command {
        SandboxPolicyCliCommand::Show => {
            let (config, source) = load_sandbox_policy_config_for_node(&data_dir)?;
            print_sandbox_policy_config(&config, &source);
            Ok(())
        }
        SandboxPolicyCliCommand::Set(options) => {
            let mut config = load_sandbox_policy_config_for_node(&data_dir)?.0;
            apply_sandbox_policy_set_options(&mut config.policy, options);
            let path = write_sandbox_policy_config_for_node(&data_dir, &config)?;
            let source = if sandbox_policy_uses_cluster(&data_dir) {
                "cluster kv".to_string()
            } else {
                path.display().to_string()
            };
            println!("sandbox policy written: {}", path.display());
            print_sandbox_policy_config(&config, &source);
            Ok(())
        }
    }
}

pub fn sandbox_policy_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    if cli_force_local_mode() {
        return false;
    }
    sinomo_core::cluster::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
}

pub fn load_sandbox_policy_config_for_node(
    data_dir: &SinomoDataDir,
) -> Result<(crate::SandboxClusterPolicyConfig, String), CliError> {
    if sandbox_policy_uses_cluster(data_dir) {
        let paths = ClusterPaths::new(data_dir);
        let snapshot = sandbox_policy_cluster_snapshot(&paths)?;
        let config =
            crate::sandbox_policy_config_from_cluster_kv_snapshot(&snapshot)?.unwrap_or_default();
        return Ok((config, "cluster kv".to_string()));
    }
    load_local_sandbox_policy_config(data_dir)
}

pub fn write_sandbox_policy_config_for_node(
    data_dir: &SinomoDataDir,
    config: &crate::SandboxClusterPolicyConfig,
) -> Result<PathBuf, CliError> {
    if !sandbox_policy_uses_cluster(data_dir) {
        return write_local_sandbox_policy_config(data_dir, config);
    }
    let paths = ClusterPaths::new(data_dir);
    paths.ensure()?;
    let txn = crate::sandbox_policy_config_cluster_kv_txn(config)?;
    commit_sandbox_policy_cluster_request(data_dir, KvTxnRequest::generated(txn))?;
    Ok(paths.kv_paths().snapshot)
}

fn local_sandbox_policy_config_path(data_dir: &SinomoDataDir) -> PathBuf {
    sandbox_policy_config_definition()
        .map(|definition| definition.local_path(data_dir))
        .unwrap_or_else(|_| {
            sinomo_config::configs_dir(data_dir)
                .join("sandbox")
                .join("policy.json")
        })
}

fn load_local_sandbox_policy_config(
    data_dir: &SinomoDataDir,
) -> Result<(crate::SandboxClusterPolicyConfig, String), CliError> {
    let store = sandbox_policy_config_store()?;
    let resolution = store.get(data_dir, None, crate::SANDBOX_POLICY_CONFIG_KEY)?;
    let config: crate::SandboxClusterPolicyConfig = serde_json::from_value(resolution.value)
        .map_err(|source| CliError::Json {
            path: resolution.definition.key.to_string(),
            detail: source.to_string(),
        })?;
    crate::validate_sandbox_policy_config(&config)?;
    let source = match resolution.source {
        sinomo_config::ConfigValueSource::Default => "built-in default".to_string(),
        sinomo_config::ConfigValueSource::MigratedLocal => {
            format!(
                "{} (migrated)",
                resolution.definition.local_path(data_dir).display()
            )
        }
        _ => resolution
            .definition
            .local_path(data_dir)
            .display()
            .to_string(),
    };
    Ok((config, source))
}

fn write_local_sandbox_policy_config(
    data_dir: &SinomoDataDir,
    config: &crate::SandboxClusterPolicyConfig,
) -> Result<PathBuf, CliError> {
    crate::validate_sandbox_policy_config(config)?;
    let store = sandbox_policy_config_store()?;
    store.set_typed(
        data_dir,
        None,
        crate::SANDBOX_POLICY_CONFIG_KEY,
        config,
        sinomo_config::ConfigWriteTarget::Local,
    )?;
    Ok(local_sandbox_policy_config_path(data_dir))
}

fn sandbox_policy_config_definition() -> Result<sinomo_config::ConfigDefinition, CliError> {
    crate::sandbox_policy_config_definition().map_err(Into::into)
}

fn sandbox_policy_config_store() -> Result<sinomo_config::ConfigStore, CliError> {
    let registry =
        crate::register_config_definitions(sinomo_config::ConfigRegistry::builder())?.build();
    Ok(sinomo_config::ConfigStore::new(registry))
}

fn sandbox_policy_cluster_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    let host = current_sandbox_cli_host();
    if let Some(address) = host.default_main_daemon_address()? {
        return fetch_daemon_cluster_kv_authoritative_snapshot(&address);
    }
    if let Some(snapshot) = host.read_cluster_kv_local_snapshot_for_test(paths) {
        return snapshot;
    }
    #[cfg(test)]
    {
        return sinomo_core::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .map_err(SandboxCliError::Kv);
    }
    #[cfg(not(test))]
    {
        Err(CliError::DaemonNotRunning {
            detail: "clustered sandbox policy requires the local daemon".to_string(),
        })
    }
}

#[cfg(test)]
fn commit_sandbox_policy_cluster_request(
    data_dir: &SinomoDataDir,
    request: KvTxnRequest,
) -> Result<(), CliError> {
    let paths = ClusterPaths::new(data_dir);
    let snapshot = sinomo_core::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
        .unwrap_or_else(|_| {
            sinomo_core::kv::InProcessKvStore::new()
                .snapshot()
                .expect("snapshot")
        });
    let mut store =
        sinomo_core::kv::InProcessKvStore::restore(snapshot).map_err(SandboxCliError::Kv)?;
    let _: KvTxnResponse = store.txn(request.txn).map_err(SandboxCliError::Kv)?;
    sinomo_core::kv::write_cluster_kv_snapshot(
        &paths.kv_paths(),
        &store.snapshot().expect("snapshot"),
    )
    .map_err(SandboxCliError::Kv)?;
    Ok(())
}

#[cfg(not(test))]
fn commit_sandbox_policy_cluster_request(
    data_dir: &SinomoDataDir,
    request: KvTxnRequest,
) -> Result<(), CliError> {
    let host = current_sandbox_cli_host();
    if let Some(result) = host.commit_cluster_kv_request_for_test(data_dir, &request) {
        return require_sandbox_policy_cluster_commit(result?);
    }
    let Some(address) = host.default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "clustered sandbox policy writes require the local daemon".to_string(),
        });
    };
    let payload =
        sinomo_core::kv::cluster_kv_txn_rpc_payload(&request).map_err(SandboxCliError::Kv)?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response =
        host.send_daemon_rpc(&address, &request, SANDBOX_DAEMON_CLUSTER_KV_TXN_TIMEOUT)?;
    response.error_detail().map_err(CliError::from)?;
    let commit = sinomo_core::kv::decode_cluster_kv_txn_rpc_body(&response.body)
        .map_err(SandboxCliError::Kv)?;
    require_sandbox_policy_cluster_commit(commit)
}

#[cfg(not(test))]
fn require_sandbox_policy_cluster_commit(commit: ClusterKvTxnCommitResult) -> Result<(), CliError> {
    if commit.txn_response.is_none() {
        return Err(CliError::Parse {
            detail: "cluster KV returned a barrier result for a sandbox policy transaction"
                .to_string(),
        });
    }
    Ok(())
}

fn fetch_daemon_cluster_kv_authoritative_snapshot(address: &str) -> Result<KvSnapshot, CliError> {
    let payload = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()
        .map_err(SandboxCliError::Kv)?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = send_daemon_rpc(address, &request, SANDBOX_DAEMON_PROBE_TIMEOUT)?;
    response.error_detail().map_err(CliError::from)?;
    sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
        .map(|read| read.value)
        .map_err(SandboxCliError::Kv)
}

pub fn apply_sandbox_policy_set_options(
    policy: &mut crate::SandboxPolicy,
    options: SandboxPolicySetOptions,
) {
    if let Some(value) = options.allow_unsigned_toolchains {
        policy.allow_unsigned_toolchains = value;
    }
    if let Some(value) = options.allow_remote_local_owned_mounts {
        policy.allow_remote_local_owned_mounts = value;
    }
    policy
        .allowed_mount_prefixes
        .extend(options.allowed_mount_prefixes);
    if let Some(value) = options.max_memory_mb {
        policy.max_limits.memory_mb = value;
    }
    if let Some(value) = options.max_wall_time_ms {
        policy.max_limits.wall_time_ms = value;
    }
    if let Some(value) = options.max_output_bytes {
        policy.max_limits.max_output_bytes = value;
    }
    if let Some(value) = options.max_input_bytes {
        policy.max_limits.max_input_bytes = value;
    }
    if let Some(value) = options.max_workspace_bytes {
        policy.max_limits.max_workspace_bytes = value;
    }
    if let Some(value) = options.max_written_bytes_per_command {
        policy.max_limits.max_written_bytes_per_command = value;
    }
    if let Some(value) = options.max_file_count {
        policy.max_limits.max_file_count = value;
    }
    if let Some(value) = options.max_http_timeout_ms {
        policy.max_http_timeout_ms = value;
    }
    if let Some(value) = options.max_concurrent_commands_per_sandbox {
        policy.max_concurrent_commands_per_sandbox = value;
    }
    if let Some(value) = options.max_concurrent_sandboxes_per_node {
        policy.max_concurrent_sandboxes_per_node = value;
    }
    policy
        .allowed_command_capabilities
        .extend(options.allowed_capabilities);
    policy
        .allowed_toolchain_ids
        .extend(options.allowed_toolchains);
    policy
        .allowed_toolchain_provenance_prefixes
        .extend(options.allowed_toolchain_provenance_prefixes);
    policy
        .allowed_package_cache_prefixes
        .extend(options.allowed_package_cache_prefixes);
    policy.allowed_http_hosts.extend(options.allowed_http_hosts);
}

fn print_sandbox_policy_config(config: &crate::SandboxClusterPolicyConfig, source: &str) {
    let policy = &config.policy;
    println!("source: {source}");
    println!("key: {}", crate::SANDBOX_POLICY_CONFIG_KEY);
    println!("cluster key: {}", crate::SANDBOX_POLICY_CONFIG_KV_KEY);
    println!(
        "allow unsigned toolchains: {}",
        policy.allow_unsigned_toolchains
    );
    println!(
        "allow remote local-owned mounts: {}",
        policy.allow_remote_local_owned_mounts
    );
    println!("allowed mount prefixes:");
    for prefix in &policy.allowed_mount_prefixes {
        println!("  {prefix}");
    }
    println!("allowed command capabilities:");
    for capability in &policy.allowed_command_capabilities {
        println!("  {capability}");
    }
    println!("allowed toolchains:");
    for toolchain_id in &policy.allowed_toolchain_ids {
        println!("  {toolchain_id}");
    }
    println!("allowed toolchain provenance prefixes:");
    for prefix in &policy.allowed_toolchain_provenance_prefixes {
        println!("  {prefix}");
    }
    println!("allowed package cache prefixes:");
    for prefix in &policy.allowed_package_cache_prefixes {
        println!("  {prefix}");
    }
    println!("allowed http hosts:");
    for host in &policy.allowed_http_hosts {
        println!("  {host}");
    }
    println!("max memory mb: {}", policy.max_limits.memory_mb);
    println!(
        "max wall time ms: {}",
        format_sandbox_wall_time_ms(policy.max_limits.wall_time_ms)
    );
    println!(
        "max input bytes: {}",
        format_sandbox_size_limit(policy.max_limits.max_input_bytes)
    );
    println!(
        "max output bytes: {}",
        format_sandbox_size_limit(policy.max_limits.max_output_bytes)
    );
    println!(
        "max workspace bytes: {}",
        format_sandbox_size_limit(policy.max_limits.max_workspace_bytes)
    );
    println!(
        "max written bytes per command: {}",
        format_sandbox_size_limit(policy.max_limits.max_written_bytes_per_command)
    );
    println!(
        "max file count: {}",
        format_sandbox_size_limit(policy.max_limits.max_file_count)
    );
    println!("max http timeout ms: {}", policy.max_http_timeout_ms);
    println!(
        "max concurrent commands per sandbox: {}",
        policy.max_concurrent_commands_per_sandbox
    );
    println!(
        "max concurrent sandboxes per node: {}",
        policy.max_concurrent_sandboxes_per_node
    );
}

fn format_sandbox_wall_time_ms(value: u64) -> String {
    if value == crate::SANDBOX_UNLIMITED_WALL_TIME_MS {
        "unlimited".to_string()
    } else {
        value.to_string()
    }
}

fn format_sandbox_size_limit(value: u64) -> String {
    if value == crate::SANDBOX_UNLIMITED_SIZE_LIMIT {
        "unlimited".to_string()
    } else {
        value.to_string()
    }
}

pub fn sandbox_toolchain_add_command(
    options: SandboxToolchainAddCommandOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_add_command_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_add_command_with_daemon(options),
    }
}

pub fn sandbox_toolchain_busybox_plan() {
    print!("{}", sandbox_toolchain_busybox_plan_text());
}

pub fn sandbox_toolchain_build_tools_plan() {
    print!("{}", sandbox_toolchain_build_tools_plan_text());
}

pub fn sandbox_toolchain_bash_plan() {
    print!("{}", sandbox_toolchain_bash_plan_text());
}

pub fn sandbox_toolchain_bash_plan_text() -> String {
    let mut output = String::new();
    output.push_str("commands: ");
    output.push_str(crate::BASH_TOOLCHAIN_COMMAND);
    output.push_str(", ");
    output.push_str(crate::SH_TOOLCHAIN_COMMAND);
    output.push('\n');
    output.push_str("target ABI: wasix\n");
    output.push_str("recipe:\n");
    output.push_str(&format!("  {}\n", crate::BASH_TOOLCHAIN_RECIPE));
    output.push_str("status:\n");
    output.push_str(
        "  required for coding-task readiness; the maintained Bash WASIX package passes local and daemon-local SIFS verification when cluster policy allows child-process-exec plus wasix.unpinned-child-exec for sandbox-local script and WASM execution\n",
    );
    output.push_str("import:\n");
    output.push_str(&format!(
        "  sinomo sandbox toolchain import-package [--daemon] --into {} <bash-package.json>\n",
        crate::BUSYBOX_TOOLCHAIN_LEGACY_ID
    ));
    output.push_str("verification:\n");
    output.push_str(
        "  sinomo sandbox toolchain verify-bash <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str("fidelity cases:\n");
    for case in crate::BASH_TOOLCHAIN_FIDELITY_CASES {
        output.push_str(&format!("  {}: {}\n", case.name, case.command));
    }
    output
}

pub fn sandbox_toolchain_busybox_plan_text() -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "toolchain: {}",
        crate::BUSYBOX_TOOLCHAIN_LEGACY_ID
    ));
    output.push('\n');
    output.push_str("core applets:\n");
    for command in crate::BUSYBOX_TOOLCHAIN_COMMANDS {
        output.push_str(&format!("  {command}\n"));
    }
    output.push_str("fidelity cases:\n");
    for case in crate::BUSYBOX_TOOLCHAIN_FIDELITY_CASES {
        output.push_str(&format!("  {}: {}\n", case.name, case.command));
    }
    if !crate::SANDBOX_MVP_COMMAND_GAPS.is_empty() {
        output.push_str("known MVP gaps:\n");
        for gap in crate::SANDBOX_MVP_COMMAND_GAPS {
            output.push_str(&format!("  {}: {}\n", gap.command, gap.reason));
        }
    }
    output
}

pub fn sandbox_toolchain_build_tools_plan_text() -> String {
    let mut output = String::new();
    output.push_str("target doc:\n");
    output.push_str(&format!("  {}\n", crate::BUILD_TOOLS_TOOLCHAIN_PLAN));
    output.push_str("tracked build tool commands:\n");
    for target in crate::SANDBOX_RICH_TOOLCHAIN_TARGETS {
        if matches!(
            target.command,
            crate::TCC_TOOLCHAIN_COMMAND
                | crate::MAKE_TOOLCHAIN_COMMAND
                | crate::MAKEPKG_TOOLCHAIN_COMMAND
        ) {
            let status = match target.status {
                crate::SandboxRichToolchainStatus::Implemented => "implemented",
                crate::SandboxRichToolchainStatus::Required => "required",
                crate::SandboxRichToolchainStatus::Planned => "planned",
            };
            output.push_str(&format!(
                "  {} ({status}): {}\n",
                target.command, target.note
            ));
            if let Some(recipe) = target.recipe {
                output.push_str(&format!("    recipe: {recipe}\n"));
            }
        }
    }
    output.push_str("availability rule:\n");
    output.push_str(
        "  build tools stay absent until real WASI packages and fidelity suites exist; do not expose host fallbacks, fake compilers, fake make, or parser substitutes under these command names\n",
    );
    output.push_str("required gates:\n");
    output.push_str("  real upstream or maintained source build to WASI/WASIX\n");
    output.push_str("  generic import-package manifest with hashes and provenance\n");
    output.push_str("  local and daemon/SIFS verifier coverage for compile/build workflows\n");
    output.push_str(
        "  sinomo sandbox toolchain verify-make <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str(
        "  sinomo sandbox toolchain verify-makepkg <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str(
        "  sinomo sandbox toolchain verify-tcc <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str("  sandbox doctor reports missing implemented build tools as warnings\n");
    output
}

pub fn sandbox_toolchain_python_plan() {
    print!("{}", sandbox_toolchain_python_plan_text());
}

pub fn sandbox_toolchain_python_plan_text() -> String {
    let mut output = String::new();
    output.push_str("commands:\n");
    output.push_str(&format!("  {}\n", crate::PYTHON_TOOLCHAIN_COMMAND));
    output.push_str(&format!("  {}\n", crate::PIP_TOOLCHAIN_COMMAND));
    output.push_str("target ABI: wasix\n");
    output.push_str("recipe:\n");
    output.push_str(&format!("  {}\n", crate::PYTHON_TOOLCHAIN_RECIPE));
    output.push_str("status:\n");
    output.push_str(
        "  required for coding-task readiness; the maintained CPython WASIX package is the active POSIX/process gate for Python, pip, native-extension loading, numpy array execution, and matplotlib Agg rendering inside the sandbox\n",
    );
    output.push_str("import:\n");
    output.push_str(&format!(
        "  sinomo sandbox toolchain import-package [--daemon] --into {} <python-package.json>\n",
        crate::BUSYBOX_TOOLCHAIN_LEGACY_ID
    ));
    output.push_str("verification:\n");
    output.push_str(
        "  sinomo sandbox toolchain verify-python <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str(&format!("  {}\n", crate::PYTHON_WASIX_NATIVE_VERIFIER));
    output.push_str("fidelity cases:\n");
    for case in crate::PYTHON_TOOLCHAIN_FIDELITY_CASES
        .iter()
        .chain(crate::PIP_TOOLCHAIN_FIDELITY_CASES.iter())
    {
        output.push_str(&format!("  {}: {}\n", case.name, case.command));
    }
    output
}

pub fn sandbox_toolchain_verify_busybox(
    options: SandboxToolchainVerifyBusyBoxOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_busybox_with_stores(
            options,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_busybox_with_daemon(options),
    }
}

pub fn sandbox_toolchain_verify_busybox_with_stores(
    options: SandboxToolchainVerifyBusyBoxOptions,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = crate::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = options.allow_unsigned;
    match current_sandbox_cli_host().verify_busybox_toolchain_fidelity(
        store,
        toolchain_store,
        &options.sandbox_id,
        &node_id,
        &policy,
        crate::BUSYBOX_TOOLCHAIN_FIDELITY_CASES,
    ) {
        Ok(results) => {
            print_busybox_fidelity_results(&results);
            Ok(())
        }
        Err(crate::SandboxToolFidelityError::Failed { results, .. }) => {
            print_busybox_fidelity_results(&results);
            Err(CliError::Parse {
                detail: "BusyBox fidelity verification failed".to_string(),
            })
        }
        Err(crate::SandboxToolFidelityError::Command(error)) => Err(error.into()),
    }
}

pub fn sandbox_toolchain_verify_busybox_with_daemon(
    options: SandboxToolchainVerifyBusyBoxOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon BusyBox verification requires the local daemon".to_string(),
        });
    };
    let mut results = Vec::new();
    for case in crate::BUSYBOX_TOOLCHAIN_FIDELITY_CASES {
        let outcome = daemon_run_toolchain_command(
            &address,
            options.sandbox_id.clone(),
            case.command.to_string(),
            case.argv_vec(),
            case.stdin_bytes(),
            crate::SandboxResourceLimits::default().wall_time_ms,
        )?;
        let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
        let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
        let mut mismatch = None;
        if outcome.response.exit_status != case.expected_exit_status {
            mismatch = Some(format!(
                "expected exit {}, got {}",
                case.expected_exit_status, outcome.response.exit_status
            ));
        } else if stdout != case.expected_stdout {
            mismatch = Some(format!(
                "expected stdout {:?}, got {:?}",
                case.expected_stdout, stdout
            ));
        }
        results.push(crate::SandboxToolFidelityResult {
            case_name: case.name.to_string(),
            command: case.command.to_string(),
            passed: mismatch.is_none(),
            exit_status: outcome.response.exit_status,
            stdout,
            stderr,
            mismatch,
        });
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "BusyBox fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

fn print_busybox_fidelity_results(results: &[crate::SandboxToolFidelityResult]) {
    for result in results {
        let status = if result.passed { "ok" } else { "failed" };
        println!("{}: {} ({})", result.case_name, result.command, status);
        if let Some(mismatch) = &result.mismatch {
            println!("  {mismatch}");
        }
    }
}

pub fn sandbox_toolchain_verify_coding(
    options: SandboxToolchainVerifyCodingOptions,
) -> Result<(), CliError> {
    println!("BusyBox file/search fidelity:");
    sandbox_toolchain_verify_busybox(SandboxToolchainVerifyBusyBoxOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("Coding workflow fidelity:");
    match options.execution {
        SandboxMetadataExecution::Local => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            sandbox_toolchain_verify_coding_workflow_with_stores(
                options.clone(),
                &sandbox_store_from_data_dir(&data_dir),
                &sandbox_toolchain_store_from_data_dir(&data_dir),
            )
        }
        SandboxMetadataExecution::Daemon => {
            sandbox_toolchain_verify_coding_workflow_with_daemon(options.clone())
        }
    }?;
    if options.full {
        sandbox_toolchain_verify_full_coding_surface(options)?;
    }
    Ok(())
}

fn sandbox_toolchain_verify_full_coding_surface(
    options: SandboxToolchainVerifyCodingOptions,
) -> Result<(), CliError> {
    println!("Bash shell fidelity:");
    sandbox_toolchain_verify_bash(SandboxToolchainVerifyBashOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("Python and pip fidelity:");
    sandbox_toolchain_verify_python(SandboxToolchainVerifyPythonOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("GNU Make fidelity:");
    sandbox_toolchain_verify_make(SandboxToolchainVerifyMakeOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("TinyCC fidelity:");
    sandbox_toolchain_verify_tcc(SandboxToolchainVerifyTccOptions {
        sandbox_id: options.sandbox_id,
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })
}

fn sandbox_toolchain_verify_coding_workflow_with_stores(
    options: SandboxToolchainVerifyCodingOptions,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = crate::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = options.allow_unsigned;
    let mut results = Vec::new();
    for case in CODING_WORKFLOW_BUSYBOX_CASES {
        let outcome = current_sandbox_cli_host().run_wasix_toolchain_command(
            store,
            toolchain_store,
            &options.sandbox_id,
            case.command,
            case.argv_vec(),
            case.stdin_bytes(),
            crate::SandboxResourceLimits::default().wall_time_ms,
            &node_id,
            &policy,
            None,
        )?;
        results.push(sandbox_tool_fidelity_result_from_busybox_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "Coding workflow fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

fn sandbox_toolchain_verify_coding_workflow_with_daemon(
    options: SandboxToolchainVerifyCodingOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon coding verification requires the local daemon".to_string(),
        });
    };
    let mut results = Vec::new();
    for case in CODING_WORKFLOW_BUSYBOX_CASES {
        let outcome = daemon_run_toolchain_command(
            &address,
            options.sandbox_id.clone(),
            case.command.to_string(),
            case.argv_vec(),
            case.stdin_bytes(),
            crate::SandboxResourceLimits::default().wall_time_ms,
        )?;
        results.push(sandbox_tool_fidelity_result_from_busybox_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "Coding workflow fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

pub const CODING_WORKFLOW_BUSYBOX_CASES: &[crate::SandboxBusyBoxFidelityCase] = &[
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-cleanup-work-dir",
        command: "rm",
        argv: &["rm", "-rf", "/work/phase3"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-mkdir-work-dir",
        command: "mkdir",
        argv: &["mkdir", "/work/phase3"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-touch-file",
        command: "touch",
        argv: &["touch", "/work/phase3/file"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-printf-stdout",
        command: "printf",
        argv: &["printf", "hello"],
        stdin: None,
        expected_stdout: "hello",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-grep-stdin",
        command: "grep",
        argv: &["grep", "hell"],
        stdin: Some("hello\n"),
        expected_stdout: "hello\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-find-work-file",
        command: "find",
        argv: &["find", "/work/phase3", "-type", "f"],
        stdin: None,
        expected_stdout: "/work/phase3/file\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-diff-file-with-self",
        command: "diff",
        argv: &["diff", "/work/phase3/file", "/work/phase3/file"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-mkdir-src-dir",
        command: "mkdir",
        argv: &["mkdir", "/work/phase3/src"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-cp-work-file",
        command: "cp",
        argv: &["cp", "/work/phase3/file", "/work/phase3/src/copy"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-cmp-copy",
        command: "cmp",
        argv: &["cmp", "/work/phase3/file", "/work/phase3/src/copy"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-mv-copy",
        command: "mv",
        argv: &["mv", "/work/phase3/src/copy", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-test-renamed-file",
        command: "test",
        argv: &["test", "-f", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-chmod-renamed-file",
        command: "chmod",
        argv: &["chmod", "600", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-tar-create",
        command: "tar",
        argv: &[
            "tar",
            "-cf",
            "/work/phase3/archive.tar",
            "-C",
            "/work/phase3/src",
            "renamed",
        ],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-tar-list",
        command: "tar",
        argv: &["tar", "-tf", "/work/phase3/archive.tar"],
        stdin: None,
        expected_stdout: "renamed\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-sed-stdin",
        command: "sed",
        argv: &["sed", "s/beta/gamma/"],
        stdin: Some("alpha\nbeta\n"),
        expected_stdout: "alpha\ngamma\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-awk-stdin",
        command: "awk",
        argv: &["awk", "{sum += $1} END {print sum}"],
        stdin: Some("2\n3\n"),
        expected_stdout: "5\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-sort-stdin",
        command: "sort",
        argv: &["sort"],
        stdin: Some("b\na\n"),
        expected_stdout: "a\nb\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-wc-stdin",
        command: "wc",
        argv: &["wc", "-l"],
        stdin: Some("one\ntwo\n"),
        expected_stdout: "2\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-head-stdin",
        command: "head",
        argv: &["head", "-n", "1"],
        stdin: Some("first\nsecond\n"),
        expected_stdout: "first\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-tail-stdin",
        command: "tail",
        argv: &["tail", "-n", "1"],
        stdin: Some("first\nsecond\n"),
        expected_stdout: "second\n",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-rm-renamed-file",
        command: "rm",
        argv: &["rm", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    crate::SandboxBusyBoxFidelityCase {
        name: "phase3-rmdir-src-dir",
        command: "rmdir",
        argv: &["rmdir", "/work/phase3/src"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
];

fn sandbox_tool_fidelity_result_from_busybox_case(
    case: &crate::SandboxBusyBoxFidelityCase,
    exit_status: i32,
    stdout: &[u8],
    stderr: &[u8],
) -> crate::SandboxToolFidelityResult {
    sandbox_tool_fidelity_result_from_outcome(
        case.name,
        case.command,
        exit_status,
        stdout,
        stderr,
        case.expected_exit_status,
        case.expected_stdout,
    )
}

fn sandbox_tool_fidelity_result_from_outcome(
    case_name: impl Into<String>,
    command: impl Into<String>,
    exit_status: i32,
    stdout: &[u8],
    stderr: &[u8],
    expected_exit_status: i32,
    expected_stdout: &str,
) -> crate::SandboxToolFidelityResult {
    let stdout = String::from_utf8_lossy(stdout).to_string();
    let stderr = String::from_utf8_lossy(stderr).to_string();
    let mismatch = if exit_status != expected_exit_status {
        Some(format!(
            "expected exit {expected_exit_status}, got {exit_status}"
        ))
    } else if stdout != expected_stdout {
        Some(format!(
            "expected stdout {:?}, got {:?}",
            expected_stdout, stdout
        ))
    } else {
        None
    };
    crate::SandboxToolFidelityResult {
        case_name: case_name.into(),
        command: command.into(),
        passed: mismatch.is_none(),
        exit_status,
        stdout,
        stderr,
        mismatch,
    }
}

fn sandbox_tool_fidelity_result_from_command_case(
    case: &crate::SandboxCommandFidelityCase,
    exit_status: i32,
    stdout: &[u8],
    stderr: &[u8],
) -> crate::SandboxToolFidelityResult {
    let stdout = String::from_utf8_lossy(stdout).to_string();
    let stderr = String::from_utf8_lossy(stderr).to_string();
    let mismatch = if exit_status != case.expected_exit_status {
        Some(format!(
            "expected exit {}, got {exit_status}",
            case.expected_exit_status
        ))
    } else if let Some(expected) = case.expected_stdout {
        (stdout != expected).then(|| format!("expected stdout {:?}, got {:?}", expected, stdout))
    } else if let Some(expected) = case.expected_stdout_contains {
        (!stdout.contains(expected)).then(|| {
            format!(
                "expected stdout to contain {:?}, got {:?}",
                expected, stdout
            )
        })
    } else if let Some(expected) = case.expected_stderr_contains {
        (!stderr.contains(expected)).then(|| {
            format!(
                "expected stderr to contain {:?}, got {:?}",
                expected, stderr
            )
        })
    } else {
        None
    }
    .or_else(|| {
        case.expected_stderr_contains.and_then(|expected| {
            (!stderr.contains(expected)).then(|| {
                format!(
                    "expected stderr to contain {:?}, got {:?}",
                    expected, stderr
                )
            })
        })
    });
    crate::SandboxToolFidelityResult {
        case_name: case.name.to_string(),
        command: case.command.to_string(),
        passed: mismatch.is_none(),
        exit_status,
        stdout,
        stderr,
        mismatch,
    }
}

fn sandbox_toolchain_verify_command_cases_with_stores(
    sandbox_id: &crate::SandboxId,
    allow_unsigned: bool,
    allowed_capabilities: &[&str],
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
    cases: &[crate::SandboxCommandFidelityCase],
    failure_detail: &str,
) -> Result<(), CliError> {
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = crate::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = allow_unsigned;
    for capability in allowed_capabilities {
        policy
            .allowed_command_capabilities
            .insert((*capability).to_string());
    }
    let mut results = Vec::new();
    for case in cases {
        if !case.setup_files.is_empty() {
            let (_manifest, layout) = store.load(sandbox_id)?;
            let mut sandbox_fs = crate::LocalSandboxFileSystem::new(layout);
            for (path, contents) in case.setup_files {
                sandbox_fs.write(
                    &crate::SandboxPath::new((*path).to_string())?,
                    contents.as_bytes(),
                )?;
            }
        }
        let outcome = current_sandbox_cli_host().run_wasix_toolchain_command(
            store,
            toolchain_store,
            sandbox_id,
            case.command,
            case.argv_vec(),
            case.stdin_bytes(),
            case.timeout_ms(),
            &node_id,
            &policy,
            None,
        )?;
        results.push(sandbox_tool_fidelity_result_from_command_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: failure_detail.to_string(),
        });
    }
    Ok(())
}

fn sandbox_toolchain_verify_command_cases_with_daemon(
    sandbox_id: &crate::SandboxId,
    cases: &[crate::SandboxCommandFidelityCase],
    failure_detail: &str,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon command verification requires the local daemon".to_string(),
        });
    };
    sandbox_toolchain_verify_command_cases_with_daemon_address(
        &address,
        sandbox_id,
        cases,
        failure_detail,
    )
}

fn sandbox_toolchain_verify_command_cases_with_daemon_address(
    daemon_address: &str,
    sandbox_id: &crate::SandboxId,
    cases: &[crate::SandboxCommandFidelityCase],
    failure_detail: &str,
) -> Result<(), CliError> {
    let mut results = Vec::new();
    for case in cases {
        for (path, contents) in case.setup_files {
            daemon_sandbox_write_file_to_address(
                daemon_address,
                sandbox_id,
                crate::SandboxPath::new((*path).to_string())?,
                contents.as_bytes().to_vec(),
            )?;
        }
        let outcome = daemon_run_toolchain_command(
            daemon_address,
            sandbox_id.clone(),
            case.command.to_string(),
            case.argv_vec(),
            case.stdin_bytes(),
            case.timeout_ms().max(120_000),
        )?;
        results.push(sandbox_tool_fidelity_result_from_command_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: failure_detail.to_string(),
        });
    }
    Ok(())
}

pub fn sandbox_toolchain_verify_bash(
    options: SandboxToolchainVerifyBashOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[
                crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            ],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            crate::BASH_TOOLCHAIN_FIDELITY_CASES,
            "Bash fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            crate::BASH_TOOLCHAIN_FIDELITY_CASES,
            "Bash fidelity verification failed",
        ),
    }
}

pub fn sandbox_toolchain_verify_make(
    options: SandboxToolchainVerifyMakeOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[
                crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            ],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            crate::MAKE_TOOLCHAIN_FIDELITY_CASES,
            "GNU Make fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            crate::MAKE_TOOLCHAIN_FIDELITY_CASES,
            "GNU Make fidelity verification failed",
        ),
    }
}

pub fn sandbox_toolchain_verify_makepkg(
    options: SandboxToolchainVerifyMakepkgOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[
                crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            ],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            crate::MAKEPKG_TOOLCHAIN_FIDELITY_CASES,
            "makepkg fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            crate::MAKEPKG_TOOLCHAIN_FIDELITY_CASES,
            "makepkg fidelity verification failed",
        ),
    }
}

pub fn sandbox_toolchain_verify_tcc(
    options: SandboxToolchainVerifyTccOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            crate::TCC_TOOLCHAIN_FIDELITY_CASES,
            "TinyCC fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            crate::TCC_TOOLCHAIN_FIDELITY_CASES,
            "TinyCC fidelity verification failed",
        ),
    }
}

pub fn sandbox_toolchain_verify_python(
    options: SandboxToolchainVerifyPythonOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_toolchain_verify_command_cases_with_stores(
                &options.sandbox_id,
                options.allow_unsigned,
                &[
                    crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                    crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
                ],
                &sandbox_store_from_data_dir(&data_dir),
                &sandbox_toolchain_store_from_data_dir(&data_dir),
                crate::PYTHON_TOOLCHAIN_FIDELITY_CASES,
                "Python fidelity verification failed",
            )?;
            sandbox_toolchain_verify_command_cases_with_stores(
                &options.sandbox_id,
                options.allow_unsigned,
                &[
                    crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                    crate::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
                ],
                &sandbox_store_from_data_dir(&data_dir),
                &sandbox_toolchain_store_from_data_dir(&data_dir),
                crate::PIP_TOOLCHAIN_FIDELITY_CASES,
                "pip fidelity verification failed",
            )
        }
        SandboxMetadataExecution::Daemon => {
            let Some(address) = default_main_daemon_address()? else {
                return Err(CliError::DaemonNotRunning {
                    detail: "sandbox daemon command verification requires the local daemon"
                        .to_string(),
                });
            };
            sandbox_toolchain_verify_command_cases_with_daemon_address(
                &address,
                &options.sandbox_id,
                crate::PYTHON_TOOLCHAIN_FIDELITY_CASES,
                "Python fidelity verification failed",
            )?;
            sandbox_toolchain_verify_command_cases_with_daemon_address(
                &address,
                &options.sandbox_id,
                crate::PIP_TOOLCHAIN_FIDELITY_CASES,
                "pip fidelity verification failed",
            )
        }
    }
}

fn daemon_run_toolchain_command(
    daemon_address: &str,
    sandbox_id: crate::SandboxId,
    command_name: String,
    argv: Vec<String>,
    stdin: Option<Vec<u8>>,
    timeout_ms: u64,
) -> Result<crate::SandboxLocalCommandOutcome, CliError> {
    let request = daemon_run_toolchain_request(sandbox_id, command_name, argv, stdin, timeout_ms);
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_RUN_TOOLCHAIN_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(
        daemon_address,
        &rpc,
        sandbox_daemon_execution_rpc_timeout(timeout_ms),
    )?;
    response.decode_body().map_err(CliError::from)
}

fn sandbox_daemon_execution_rpc_timeout(timeout_ms: u64) -> std::time::Duration {
    let timeout_ms = if timeout_ms == 0 {
        15 * 60 * 1000
    } else {
        timeout_ms
    };
    std::time::Duration::from_millis(timeout_ms.saturating_add(10_000))
}

fn sandbox_daemon_metadata_rpc_connect_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(15 * 60)
}

pub fn sandbox_module_artifact(command: SandboxModuleArtifactCliCommand) -> Result<(), CliError> {
    match command {
        SandboxModuleArtifactCliCommand::Register(options) => {
            sandbox_module_artifact_register(options)
        }
    }
}

pub fn sandbox_module_artifact_register(
    options: SandboxModuleArtifactRegisterOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    sandbox_module_artifact_register_with_data_dir(options, &data_dir.root())
}

pub fn sandbox_module_artifact_register_with_data_dir(
    options: SandboxModuleArtifactRegisterOptions,
    data_dir_root: &Path,
) -> Result<(), CliError> {
    let bytes = std::fs::read(&options.source_path).map_err(|source| CliError::Io {
        path: options.source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    if options.component_model {
        validate_sandbox_component_artifact_bytes(&bytes)?;
    }
    let content_hash = crate::sandbox_module_hash(&bytes);
    let rollout_material = options.rollout_material.clone();
    let relative_path = sandbox_module_artifact_relative_path(&options.artifact_id);
    let artifact_path = data_dir_root.join(&relative_path);
    if let Some(parent) = artifact_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    std::fs::write(&artifact_path, &bytes).map_err(|source| CliError::Io {
        path: artifact_path.display().to_string(),
        detail: source.to_string(),
    })?;

    let mut registry =
        crate::read_sandbox_wasm_module_artifact_registry(data_dir_root)?.unwrap_or_default();
    registry
        .artifacts
        .retain(|artifact| artifact.artifact_id != options.artifact_id);
    registry.artifacts.push(crate::SandboxWasmModuleArtifact {
        artifact_id: options.artifact_id.clone(),
        wasm_path: relative_path.clone(),
        content_hash: content_hash.clone(),
        interface_version: options.interface_version.clone(),
        component_model: options.component_model,
        abi: options.abi,
        sandbox_id: options.sandbox_id,
        argv: options.argv,
        cwd: options.cwd,
        env: options.env,
        limits: options.limits,
        timeout_ms: options.timeout_ms,
    });
    let registry_path =
        crate::write_sandbox_wasm_module_artifact_registry(data_dir_root, &registry)?;
    let rollout_material_path = sandbox_module_artifact_write_rollout_material_if_requested(
        data_dir_root,
        &options.artifact_id,
        &artifact_path,
        &content_hash,
        rollout_material.as_ref(),
    )?;

    println!("artifact: {}", options.artifact_id);
    println!("path: {}", artifact_path.display());
    println!("registry: {}", registry_path.display());
    println!("hash: {content_hash}");
    if let Some(interface_version) = options.interface_version {
        println!("interface: {interface_version}");
    }
    println!("component-model: {}", options.component_model);
    if let Some(path) = rollout_material_path {
        println!("rollout-material: {}", path.display());
    }
    Ok(())
}

fn sandbox_module_artifact_write_rollout_material_if_requested(
    data_dir_root: &Path,
    artifact_id: &sinomo_module::ArtifactId,
    artifact_path: &Path,
    content_hash: &str,
    options: Option<&SandboxModuleArtifactRolloutMaterialOptions>,
) -> Result<Option<PathBuf>, CliError> {
    let Some(options) = options else {
        return Ok(None);
    };
    let bundle = read_sandbox_module_candidate_bundle_manifest(&options.candidate_bundle_path)?;
    let identity = sinomo_core::node::load_or_create_main_node_identity(data_dir_root)?;
    let candidate_bundle_ref = format!("candidate-bundle://{}", bundle.job_id);
    let registry_artifact_evidence_ref = format!("registry://artifact-evidence/{}", bundle.job_id);
    let sandbox_artifact_ref = format!("sandbox://module-artifact/{artifact_id}");
    let material_path =
        PathBuf::from(&bundle.bundle_path).join(sinomo_registry::RELEASE_MATERIAL_RELATIVE_PATH);
    let mut evidence_refs = options.evidence_refs.clone();
    evidence_refs.push(candidate_bundle_ref.clone());
    evidence_refs.push(registry_artifact_evidence_ref);
    evidence_refs.push(sandbox_artifact_ref.clone());
    evidence_refs.push(artifact_path.display().to_string());
    evidence_refs.sort();
    evidence_refs.dedup();

    let mut artifact = sinomo_core::ArtifactManifest {
        artifact_id: artifact_id.clone(),
        module_id: options.module_id.clone(),
        target_ring: sinomo_core::TrustRing::R3SandboxedModule,
        content_hash: content_hash.to_string(),
        signature: None,
        source_workspace: None,
        source_revision: None,
        module_manifest: sinomo_core::ModuleManifest::r3_wasm(
            options.module_id.clone(),
            options.module_version.clone(),
            options.module_owner.clone(),
        ),
        provenance: sinomo_core::ArtifactProvenance {
            produced_by: sinomo_core::ActorRef {
                kind: sinomo_core::ActorKind::Operator,
                id: "sinomo sandbox module-artifact register".to_string(),
            },
            source_event_id: None,
            build_log_artifact_id: None,
            attestations: evidence_refs.clone(),
        },
    };
    artifact.signature = Some(
        sinomo_module::sign_artifact_manifest_with_node_identity(&artifact, &identity).map_err(
            |source| CliError::Parse {
                detail: format!("failed to sign sandbox module artifact manifest: {source}"),
            },
        )?,
    );
    let material = sinomo_registry::VerifiedReleaseMaterialRecord {
        release_id: bundle.job_id.clone(),
        release_material_hash: bundle.bundle_content_hash.clone().unwrap_or_default(),
        artifact: artifact.clone(),
        policy_decision: sinomo_core::PolicyDecision {
            decision_id: options.policy_decision_id.clone().unwrap_or_else(|| {
                sinomo_core::Id::derived(
                    "sinomo.sandbox.module-artifact.policy-decision",
                    format!("{}:{artifact_id}", bundle.job_id),
                )
            }),
            target_ring: artifact.target_ring.clone(),
            target_artifact_id: artifact.artifact_id.clone(),
            action: sinomo_core::PolicyAction::Allow,
            capability_diff: sinomo_core::CapabilityDiff::default(),
            required_additional_evals: Vec::new(),
            max_rollout_weight_bps: Some(500),
            reason: "sandbox module artifact registration prepared low-risk rollout material"
                .to_string(),
        },
        rollout_plan: sinomo_core::RolloutPlan {
            rollout_plan_id: options.rollout_plan_id.clone().unwrap_or_else(|| {
                sinomo_core::Id::derived(
                    "sinomo.sandbox.module-artifact.rollout-plan",
                    format!("{}:{artifact_id}", bundle.job_id),
                )
            }),
            artifact_id: artifact.artifact_id.clone(),
            module_id: artifact.module_id.clone(),
            target_ring: artifact.target_ring.clone(),
            strategy: sinomo_core::RolloutStrategy::Shadow,
            target_nodes: Vec::new(),
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500],
            rollback_policy: sinomo_core::RollbackPolicy::default(),
        },
        active_artifact_id: options.active_artifact_id.clone(),
        evidence_refs,
        registered_at: OffsetDateTime::now_utc(),
    };
    if let Some(parent) = material_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(&material).map_err(|source| CliError::Json {
        path: material_path.display().to_string(),
        detail: source.to_string(),
    })?;
    std::fs::write(&material_path, content).map_err(|source| CliError::Io {
        path: material_path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(Some(material_path))
}

#[derive(Debug, serde::Deserialize)]
struct SandboxModuleCandidateBundleManifest {
    job_id: sinomo_core::Id,
    bundle_path: String,
    #[serde(default)]
    bundle_content_hash: Option<String>,
}

fn read_sandbox_module_candidate_bundle_manifest(
    path: &Path,
) -> Result<SandboxModuleCandidateBundleManifest, CliError> {
    let manifest_path = if path.is_dir() {
        path.join("manifest.json")
    } else {
        path.to_path_buf()
    };
    let content = std::fs::read_to_string(&manifest_path).map_err(|source| CliError::Io {
        path: manifest_path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: manifest_path.display().to_string(),
        detail: source.to_string(),
    })
}

fn sandbox_module_artifact_relative_path(artifact_id: &sinomo_module::ArtifactId) -> PathBuf {
    PathBuf::from("sandbox")
        .join("module-artifacts")
        .join(format!("{artifact_id}.wasm"))
}

fn validate_sandbox_component_artifact_bytes(bytes: &[u8]) -> Result<(), CliError> {
    current_sandbox_cli_host().validate_component_artifact_bytes(bytes)
}

pub fn sandbox_module_replay(command: SandboxModuleReplayCliCommand) -> Result<(), CliError> {
    match command {
        SandboxModuleReplayCliCommand::Run(options) => sandbox_module_replay_run(options),
    }
}

pub fn sandbox_module_replay_run(options: SandboxModuleReplayRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    sandbox_module_replay_run_with_data_dir(options, &data_dir.root())
}

pub fn sandbox_module_replay_run_with_data_dir(
    options: SandboxModuleReplayRunOptions,
    data_dir_root: &Path,
) -> Result<(), CliError> {
    let bytes = std::fs::read(&options.fixture_path).map_err(|source| CliError::Io {
        path: options.fixture_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let fixture: sinomo_module::ModuleReplayFixture =
        serde_json::from_slice(&bytes).map_err(|source| CliError::Json {
            path: options.fixture_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let report = current_sandbox_cli_host().run_module_replay_fixture(data_dir_root, &fixture)?;
    if let Some(parent) = options.report_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|source| CliError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
    }
    let report_json = serde_json::to_vec_pretty(&report).map_err(|source| CliError::Json {
        path: options.report_path.display().to_string(),
        detail: source.to_string(),
    })?;
    std::fs::write(&options.report_path, report_json).map_err(|source| CliError::Io {
        path: options.report_path.display().to_string(),
        detail: source.to_string(),
    })?;
    println!("fixture: {}", report.fixture_id);
    println!("module: {}", report.module_id);
    println!("cases: {}", report.cases.len());
    println!("missing-routes: {}", report.missing_route_count);
    println!("primary-errors: {}", report.primary_error_count);
    println!("shadow-errors: {}", report.shadow_error_count);
    println!("shadow-mismatches: {}", report.shadow_mismatch_count);
    println!("report: {}", options.report_path.display());
    Ok(())
}

pub fn daemon_run_toolchain_request(
    sandbox_id: crate::SandboxId,
    command_name: String,
    argv: Vec<String>,
    stdin: Option<Vec<u8>>,
    timeout_ms: u64,
) -> crate::SandboxRunToolchainRequest {
    crate::SandboxRunToolchainRequest {
        sandbox_id,
        command_name,
        argv,
        stdin,
        timeout_ms,
    }
}

pub fn sandbox_toolchain_import_busybox(
    options: SandboxToolchainImportBusyBoxOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_import_busybox_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_import_busybox_with_daemon(options),
    }
}

pub fn sandbox_toolchain_import_busybox_with_store(
    options: SandboxToolchainImportBusyBoxOptions,
    store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let spec = crate::SandboxBusyBoxToolchainSpec::new(
        options.version,
        options.provenance,
        OffsetDateTime::now_utc(),
    )?;
    let manifest = crate::install_busybox_toolchain_to_local_store(store, &spec, &module_bytes)?;
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("commands: {}", manifest.commands.len());
    println!("hash: {}", crate::sandbox_module_hash(&module_bytes));
    Ok(())
}

pub fn sandbox_toolchain_import_busybox_with_daemon(
    options: SandboxToolchainImportBusyBoxOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox BusyBox import requires the local daemon".to_string(),
        });
    };
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let request = crate::SandboxImportBusyBoxToolchainRequest {
        version: options.version,
        provenance: options.provenance,
        module_bytes,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_IMPORT_BUSYBOX_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(600))?;
    let response: crate::SandboxImportBusyBoxToolchainResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("commands: {}", response.manifest.commands.len());
    println!("hash: {}", response.module_hash);
    println!(
        "sifs manifest: {}",
        crate::sandbox_toolchain_manifest_path(&response.manifest.toolchain_id)
    );
    Ok(())
}

pub fn sandbox_toolchain_add_command_with_store(
    options: SandboxToolchainAddCommandOptions,
    store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let mut manifest = match store.load_manifest(&options.toolchain_id) {
        Ok((manifest, _layout)) => manifest,
        Err(crate::SandboxToolchainError::NotFound { .. }) => crate::SandboxToolchainManifest::new(
            options.toolchain_id.clone(),
            options.version.clone(),
            options.target_abi.clone(),
            options.provenance.clone(),
            OffsetDateTime::now_utc(),
        )?,
        Err(error) => return Err(error.into()),
    };
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    store.put_command_module_with_target_abi_capabilities_and_argv_prefix(
        &mut manifest,
        &options.command_name,
        &module_bytes,
        options.default_limits,
        Some(options.target_abi.clone()),
        options.required_capabilities,
        Vec::new(),
        BTreeMap::new(),
    )?;
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("command: {}", options.command_name);
    println!(
        "module: {}",
        crate::sandbox_toolchain_command_path(&manifest.toolchain_id, &options.command_name)
    );
    println!("hash: {}", crate::sandbox_module_hash(&module_bytes));
    Ok(())
}

pub fn sandbox_toolchain_add_command_with_daemon(
    options: SandboxToolchainAddCommandOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox toolchain command import requires the local daemon".to_string(),
        });
    };
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let request = crate::SandboxAddToolchainCommandRequest {
        toolchain_id: options.toolchain_id,
        version: options.version,
        target_abi: options.target_abi,
        provenance: options.provenance,
        command_name: options.command_name,
        module_bytes,
        required_capabilities: options.required_capabilities,
        default_limits: options.default_limits,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(
        &address,
        &rpc,
        sandbox_daemon_metadata_rpc_connect_timeout(),
    )?;
    let response: crate::SandboxAddToolchainCommandResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("commands: {}", response.manifest.commands.len());
    println!("hash: {}", response.module_hash);
    println!(
        "sifs manifest: {}",
        crate::sandbox_toolchain_manifest_path(&response.manifest.toolchain_id)
    );
    Ok(())
}

pub fn sandbox_toolchain_hash(options: SandboxToolchainHashOptions) -> Result<(), CliError> {
    let bytes = std::fs::read(&options.path).map_err(|source| CliError::Io {
        path: options.path.display().to_string(),
        detail: source.to_string(),
    })?;
    println!("{}", crate::sandbox_module_hash(&bytes));
    Ok(())
}

pub fn sandbox_toolchain_add_artifact(
    options: SandboxToolchainAddArtifactOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_add_artifact_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_add_artifact_with_daemon(options),
    }
}

pub fn sandbox_toolchain_add_artifact_with_store(
    options: SandboxToolchainAddArtifactOptions,
    store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let mut manifest = match store.load_manifest(&options.toolchain_id) {
        Ok((manifest, _layout)) => manifest,
        Err(crate::SandboxToolchainError::NotFound { .. }) => crate::SandboxToolchainManifest::new(
            options.toolchain_id.clone(),
            options.version.clone(),
            crate::SandboxWasiAbi::Wasix,
            options.provenance.clone(),
            OffsetDateTime::now_utc(),
        )?,
        Err(error) => return Err(error.into()),
    };
    let bytes = std::fs::read(&options.source_path).map_err(|source| CliError::Io {
        path: options.source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    store.put_artifact(&mut manifest, &options.relative_path, &bytes)?;
    let artifact = manifest.artifact(&options.relative_path)?;
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("artifact: {}", artifact.relative_path);
    println!("path: {}", artifact.sifs_path);
    println!("hash: {}", artifact.content_hash);
    println!("bytes: {}", artifact.byte_len);
    Ok(())
}

pub fn sandbox_toolchain_add_artifact_with_daemon(
    options: SandboxToolchainAddArtifactOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox toolchain artifact import requires the local daemon".to_string(),
        });
    };
    let bytes = std::fs::read(&options.source_path).map_err(|source| CliError::Io {
        path: options.source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let request = crate::SandboxAddToolchainArtifactRequest {
        toolchain_id: options.toolchain_id,
        version: options.version,
        provenance: options.provenance,
        relative_path: options.relative_path,
        bytes,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(
        &address,
        &rpc,
        sandbox_daemon_metadata_rpc_connect_timeout(),
    )?;
    let response: crate::SandboxAddToolchainArtifactResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("artifacts: {}", response.manifest.artifacts.len());
    println!("hash: {}", response.artifact_hash);
    println!(
        "sifs manifest: {}",
        crate::sandbox_toolchain_manifest_path(&response.manifest.toolchain_id)
    );
    Ok(())
}

pub fn sandbox_toolchain_import_package(
    options: SandboxToolchainImportPackageOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_import_package_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_import_package_with_daemon(options),
    }
}

pub fn sandbox_toolchain_verify_package(
    options: SandboxToolchainVerifyPackageOptions,
) -> Result<(), CliError> {
    let loaded = load_sandbox_toolchain_package(&options.package_path)?;
    println!("package: {}", options.package_path.display());
    println!("toolchain: {}", loaded.spec.toolchain_id);
    println!("version: {}", loaded.spec.version);
    println!("commands: {}", loaded.commands.len());
    for command in &loaded.commands {
        let target_abi = command
            .target_abi
            .as_ref()
            .unwrap_or(&loaded.spec.target_abi);
        println!("  command: {} ({target_abi:?})", command.name);
    }
    println!("artifacts: {}", loaded.artifacts.len());
    for artifact in &loaded.artifacts {
        println!("  artifact: {}", artifact.relative_path);
    }
    Ok(())
}

pub fn sandbox_toolchain_import_package_with_store(
    options: SandboxToolchainImportPackageOptions,
    store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let package = load_sandbox_toolchain_package(&options.package_path)?;
    let target_toolchain_id = options
        .target_toolchain_id
        .clone()
        .unwrap_or_else(|| package.spec.toolchain_id.clone());
    let mut manifest = match store.load_manifest(&target_toolchain_id) {
        Ok((manifest, _layout)) => manifest,
        Err(crate::SandboxToolchainError::NotFound { .. }) => crate::SandboxToolchainManifest::new(
            target_toolchain_id,
            package.spec.version.clone(),
            package.spec.target_abi.clone(),
            package.spec.provenance.clone(),
            OffsetDateTime::now_utc(),
        )?,
        Err(error) => return Err(error.into()),
    };
    validate_sandbox_toolchain_package_merge(&manifest, &package)?;
    for command in &package.commands {
        store.put_command_module_with_target_abi_capabilities_and_argv_prefix(
            &mut manifest,
            &command.name,
            &command.bytes,
            command.default_limits.clone(),
            Some(
                command
                    .target_abi
                    .clone()
                    .unwrap_or_else(|| package.spec.target_abi.clone()),
            ),
            command.required_capabilities.clone(),
            command.argv_prefix.clone(),
            command.default_env.clone(),
        )?;
        if let Some(exec_name) = &command.exec_name {
            manifest
                .commands
                .get_mut(&command.name)
                .expect("inserted command")
                .exec_name = Some(exec_name.clone());
            store.save_manifest(&manifest)?;
        }
    }
    for artifact in &package.artifacts {
        store.put_artifact(&mut manifest, &artifact.relative_path, &artifact.bytes)?;
    }
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("commands: {}", manifest.commands.len());
    println!("artifacts: {}", manifest.artifacts.len());
    Ok(())
}

fn validate_sandbox_toolchain_package_merge(
    manifest: &crate::SandboxToolchainManifest,
    package: &LoadedSandboxToolchainPackage,
) -> Result<(), CliError> {
    for command in &package.commands {
        if manifest.commands.contains_key(&command.name) {
            return Err(crate::SandboxToolchainError::DuplicateCommand {
                name: command.name.clone(),
            }
            .into());
        }
    }
    for artifact in &package.artifacts {
        if manifest.artifacts.contains_key(&artifact.relative_path) {
            return Err(crate::SandboxToolchainError::DuplicateArtifact {
                path: artifact.relative_path.clone(),
            }
            .into());
        }
    }
    Ok(())
}

pub fn sandbox_toolchain_import_package_with_daemon(
    options: SandboxToolchainImportPackageOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox toolchain package import requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxImportToolchainPackageFromPathRequest {
        package_path: options.package_path.display().to_string(),
        toolchain_id: options.target_toolchain_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(600))?;
    let response: crate::SandboxImportToolchainPackageResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("commands: {}", response.manifest.commands.len());
    println!("artifacts: {}", response.manifest.artifacts.len());
    println!("hash: {}", response.package_hash);
    Ok(())
}

struct LoadedSandboxToolchainPackage {
    spec: crate::SandboxToolchainPackageSpec,
    commands: Vec<LoadedSandboxToolchainPackageCommand>,
    artifacts: Vec<LoadedSandboxToolchainPackageArtifact>,
}

struct LoadedSandboxToolchainPackageCommand {
    name: String,
    bytes: Vec<u8>,
    target_abi: Option<crate::SandboxWasiAbi>,
    argv_prefix: Vec<String>,
    exec_name: Option<String>,
    default_env: BTreeMap<String, String>,
    required_capabilities: Vec<String>,
    default_limits: crate::SandboxResourceLimits,
}

struct LoadedSandboxToolchainPackageArtifact {
    relative_path: String,
    bytes: Vec<u8>,
}

fn load_sandbox_toolchain_package(
    package_path: &Path,
) -> Result<LoadedSandboxToolchainPackage, CliError> {
    let (spec, package_dir) = read_sandbox_toolchain_package_spec(package_path)?;
    let mut command_names = BTreeSet::new();
    let mut commands = Vec::new();
    for command in &spec.commands {
        if !command_names.insert(command.name.clone()) {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate sandbox toolchain package command `{}`",
                    command.name
                ),
            });
        }
        let module_path = sandbox_toolchain_package_file_path(&package_dir, &command.module)?;
        let module_bytes = std::fs::read(&module_path).map_err(|source| CliError::Io {
            path: module_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_toolchain_package_hash(&module_path, &module_bytes, &command.content_hash)?;
        crate::SandboxToolchainCommand::new(
            &spec.toolchain_id,
            &command.name,
            &module_bytes,
            command.default_limits.clone(),
        )?;
        if let Some(exec_name) = &command.exec_name {
            validate_sandbox_toolchain_package_exec_name(&command.name, exec_name)?;
        }
        for (index, arg) in command.argv_prefix.iter().enumerate() {
            if arg.contains('\0') {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package command `{}` argv_prefix[{index}] contains an embedded NUL byte",
                        command.name
                    ),
                });
            }
        }
        for capability in &command.required_capabilities {
            validate_sandbox_command_capability(
                capability,
                "sandbox toolchain command capability",
            )?;
        }
        for (name, value) in &command.default_env {
            if !crate::sandbox_env_name_is_valid(name) {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package command `{}` has invalid default environment variable name `{name}`",
                        command.name
                    ),
                });
            }
            if value.contains('\0') {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package command `{}` default environment variable `{name}` contains an embedded NUL byte",
                        command.name
                    ),
                });
            }
        }
        commands.push(LoadedSandboxToolchainPackageCommand {
            name: command.name.clone(),
            bytes: module_bytes,
            target_abi: command.target_abi.clone(),
            argv_prefix: command.argv_prefix.clone(),
            exec_name: command.exec_name.clone(),
            default_env: command.default_env.clone(),
            required_capabilities: command.required_capabilities.clone(),
            default_limits: command.default_limits.clone(),
        });
    }

    let mut artifact_paths = BTreeSet::new();
    let mut artifacts = Vec::new();
    for artifact in &spec.artifacts {
        if !artifact_paths.insert(artifact.relative_path.clone()) {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate sandbox toolchain package artifact `{}`",
                    artifact.relative_path
                ),
            });
        }
        let source_path = sandbox_toolchain_package_file_path(&package_dir, &artifact.file)?;
        let bytes = std::fs::read(&source_path).map_err(|source| CliError::Io {
            path: source_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_toolchain_package_hash(&source_path, &bytes, &artifact.content_hash)?;
        crate::SandboxToolchainArtifact::new(&spec.toolchain_id, &artifact.relative_path, &bytes)?;
        artifacts.push(LoadedSandboxToolchainPackageArtifact {
            relative_path: artifact.relative_path.clone(),
            bytes,
        });
    }

    Ok(LoadedSandboxToolchainPackage {
        spec,
        commands,
        artifacts,
    })
}

fn validate_sandbox_toolchain_package_exec_name(
    command_name: &str,
    exec_name: &str,
) -> Result<(), CliError> {
    if exec_name.is_empty() {
        return Err(CliError::Parse {
            detail: format!(
                "sandbox toolchain package command `{command_name}` exec_name must be non-empty"
            ),
        });
    }
    if exec_name.contains('\0') {
        return Err(CliError::Parse {
            detail: format!(
                "sandbox toolchain package command `{command_name}` exec_name contains an embedded NUL byte"
            ),
        });
    }
    if let Some(path) = exec_name.strip_prefix('/') {
        let mut parts = Vec::new();
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    if parts.pop().is_none() {
                        return Err(CliError::Parse {
                            detail: format!(
                                "sandbox toolchain package command `{command_name}` exec_name escapes the sandbox root"
                            ),
                        });
                    }
                }
                part => parts.push(part),
            }
        }
        if parts.is_empty() {
            return Err(CliError::Parse {
                detail: format!(
                    "sandbox toolchain package command `{command_name}` exec_name must name a file"
                ),
            });
        }
        return Ok(());
    }
    let toolchain_id = crate::SandboxToolchainId::from_legacy_named("exec_name_validation")
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
    crate::SandboxToolchainCommand::new(
        &toolchain_id,
        exec_name,
        &[],
        crate::SandboxResourceLimits::default(),
    )?;
    Ok(())
}

fn read_sandbox_toolchain_package_spec(
    package_path: &Path,
) -> Result<(crate::SandboxToolchainPackageSpec, PathBuf), CliError> {
    let bytes = std::fs::read(package_path).map_err(|source| CliError::Io {
        path: package_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let spec =
        serde_json::from_slice::<crate::SandboxToolchainPackageSpec>(&bytes).map_err(|source| {
            CliError::Parse {
                detail: format!(
                    "invalid sandbox toolchain package manifest {}: {source}",
                    package_path.display()
                ),
            }
        })?;
    let package_dir = package_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok((spec, package_dir))
}

fn sandbox_toolchain_package_file_path(
    package_dir: &Path,
    relative_path: &str,
) -> Result<PathBuf, CliError> {
    let path = Path::new(relative_path);
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(CliError::Parse {
            detail: format!(
                "sandbox toolchain package file path `{relative_path}` must be relative"
            ),
        });
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package file path `{relative_path}` cannot escape the package directory"
                    ),
                });
            }
        }
    }
    Ok(package_dir.join(path))
}

fn verify_sandbox_toolchain_package_hash(
    path: &Path,
    bytes: &[u8],
    expected_hash: &str,
) -> Result<(), CliError> {
    let actual_hash = crate::sandbox_module_hash(bytes);
    if actual_hash == expected_hash {
        return Ok(());
    }
    Err(CliError::Parse {
        detail: format!(
            "sandbox toolchain package file {} hash mismatch: expected {expected_hash}, actual {actual_hash}",
            path.display()
        ),
    })
}

pub fn sandbox_create_with_store(
    options: SandboxCreateOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.create(crate::SandboxCreateInstanceRequest {
        sandbox_id: options.sandbox_id,
        toolchain_id: options.toolchain_id,
        temp_policy: options.temp_policy,
        memory_mb: options.memory_mb,
        wall_time_ms: options.wall_time_ms,
        max_input_bytes: options.max_input_bytes,
        max_output_bytes: options.max_output_bytes,
        max_workspace_bytes: options.max_workspace_bytes,
        max_written_bytes_per_command: options.max_written_bytes_per_command,
        max_file_count: options.max_file_count,
        mounts: options.mounts,
    })?;
    let manifest = response.manifest;
    // The backend returns only the manifest; recover the on-disk layout for the local summary.
    let layout = store.layout(&manifest.sandbox_id);

    println!("sandbox created: {}", manifest.sandbox_id);
    println!("workspace: {}", manifest.workspace_root);
    println!("local root: {}", layout.root.display());
    println!("manifest: {}", layout.manifest.display());
    println!("toolchain: {}", manifest.toolchain_id);
    println!(
        "tmp policy: {}",
        sandbox_temp_policy_label(&manifest.temp_policy)
    );
    if !manifest.mounts.is_empty() {
        println!("mounts:");
        for mount in &manifest.mounts {
            println!(
                "  {} -> {} ({})",
                mount.source_sifs_path,
                mount.mount_path,
                sandbox_mount_mode_label(&mount.mode)
            );
        }
    }
    Ok(())
}

pub fn sandbox_create_with_daemon(options: SandboxCreateOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon create requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxCreateInstanceRequest {
        sandbox_id: options.sandbox_id,
        toolchain_id: options.toolchain_id,
        temp_policy: options.temp_policy,
        memory_mb: options.memory_mb,
        wall_time_ms: options.wall_time_ms,
        max_input_bytes: options.max_input_bytes,
        max_output_bytes: options.max_output_bytes,
        max_workspace_bytes: options.max_workspace_bytes,
        max_written_bytes_per_command: options.max_written_bytes_per_command,
        max_file_count: options.max_file_count,
        mounts: options.mounts,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_CREATE_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(60))?;
    let response: crate::SandboxCreateInstanceResponse =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_manifest_summary(&response.manifest, None);
    Ok(())
}

pub fn sandbox_run_with_store(
    options: SandboxRunOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    sandbox_run_with_stores(
        options,
        store,
        &crate::LocalSandboxToolchainStore::new(std::env::temp_dir()),
    )
}

pub fn sandbox_run_with_stores(
    options: SandboxRunOptions,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    sandbox_run_with_stores_and_stdin(options, None, store, toolchain_store)
}

pub fn sandbox_process_plan_with_stores(
    options: SandboxProcessPlanOptions,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let plan = read_sandbox_process_plan_file(&options.plan_path)?;
    if sandbox_simple_native_for_execution(&options.execution)? {
        return sandbox_process_plan_simple_native_with_stores(
            options,
            &plan,
            store,
            toolchain_store,
        );
    }
    if let Some(runner) = sandbox_container_runner_for_execution(&options.execution)? {
        let outcome = runner.run_process_plan_with_output_sink(
            store,
            &options.sandbox_id,
            &plan,
            options.timeout_ms,
            Some(sandbox_cli_streaming_output_sink()),
        )?;
        return print_sandbox_process_plan_outcome(&outcome, false);
    }
    sandbox_process_plan_wasix_with_stores(options, &plan, store, toolchain_store)
}

fn sandbox_process_plan_wasix_with_stores(
    options: SandboxProcessPlanOptions,
    plan: &crate::SandboxProcessPlan,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    let mut policy = crate::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = current_sandbox_cli_host().run_wasix_process_plan(
        store,
        toolchain_store,
        &options.sandbox_id,
        plan,
        options.timeout_ms,
        &execution_node_id,
        &policy,
        Some(sandbox_cli_streaming_output_sink()),
    )?;
    print_sandbox_process_plan_outcome(&outcome, false)
}

fn sandbox_process_plan_simple_native_with_stores(
    options: SandboxProcessPlanOptions,
    plan: &crate::SandboxProcessPlan,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    let mut policy = crate::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = current_sandbox_cli_host().run_simple_native_process_plan(
        store,
        toolchain_store,
        &options.sandbox_id,
        plan,
        options.timeout_ms,
        &execution_node_id,
        &policy,
        Some(sandbox_cli_streaming_output_sink()),
    )?;
    print_sandbox_process_plan_outcome(&outcome, false)
}

pub fn sandbox_process_plan_with_daemon(
    options: SandboxProcessPlanOptions,
) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon execution requires the local daemon".to_string(),
        });
    };
    let plan = read_sandbox_process_plan_file(&options.plan_path)?;
    let request = crate::SandboxRunProcessPlanRequest {
        sandbox_id: options.sandbox_id,
        plan,
        timeout_ms: options.timeout_ms,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let timeout = sandbox_daemon_execution_rpc_timeout(options.timeout_ms);
    let mut final_outcome = None;
    send_daemon_rpc_stream(&address, &rpc, timeout, |response| {
        if response.route != crate::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE {
            return Err(CliError::Parse {
                detail: format!(
                    "unexpected sandbox process-plan stream route {}",
                    response.route
                ),
            });
        }
        let frame: crate::SandboxRunProcessPlanStreamFrame =
            response.decode_body().map_err(CliError::from)?;
        match frame {
            crate::SandboxRunProcessPlanStreamFrame::Output { chunk } => {
                write_sandbox_output_chunk(&chunk).map_err(|source| CliError::Network {
                    address: "stdout".to_string(),
                    detail: source.to_string(),
                })?;
                Ok(true)
            }
            crate::SandboxRunProcessPlanStreamFrame::Complete { outcome } => {
                final_outcome = Some(outcome);
                Ok(false)
            }
            crate::SandboxRunProcessPlanStreamFrame::Error { detail } => {
                Err(CliError::Parse { detail })
            }
        }
    })?;
    let outcome = final_outcome.ok_or_else(|| CliError::Parse {
        detail: "sandbox process-plan stream ended without final outcome".to_string(),
    })?;
    print_sandbox_process_plan_outcome(&outcome, false)
}

fn read_sandbox_process_plan_file(plan_path: &Path) -> Result<crate::SandboxProcessPlan, CliError> {
    let bytes = std::fs::read(plan_path).map_err(|source| CliError::Io {
        path: plan_path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_slice(&bytes).map_err(|source| CliError::Parse {
        detail: format!(
            "invalid sandbox process plan {}: {source}",
            plan_path.display()
        ),
    })
}

pub fn sandbox_shell_with_stores(
    options: SandboxShellOptions,
    stdin: Vec<u8>,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    sandbox_run_with_stores_and_stdin(
        SandboxRunOptions {
            sandbox_id: options.sandbox_id,
            target: SandboxRunTarget::ToolchainCommand("sh".to_string()),
            argv: vec!["sh".to_string()],
            timeout_ms: options.timeout_ms,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: options.execution,
        },
        Some(stdin),
        store,
        toolchain_store,
    )
}

pub fn sandbox_shell_with_daemon(
    options: SandboxShellOptions,
    stdin: Vec<u8>,
) -> Result<(), CliError> {
    sandbox_run_with_daemon_and_stdin(
        SandboxRunOptions {
            sandbox_id: options.sandbox_id,
            target: SandboxRunTarget::ToolchainCommand("sh".to_string()),
            argv: vec!["sh".to_string()],
            timeout_ms: options.timeout_ms,
            stdin: false,
            allow_unsigned: false,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Daemon,
        },
        Some(stdin),
    )
}

pub fn sandbox_run_with_stores_and_stdin(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    if sandbox_simple_native_for_execution(&options.execution)? {
        return sandbox_run_simple_native_with_stores_and_stdin(
            options,
            stdin,
            store,
            toolchain_store,
        );
    }
    if let Some(runner) = sandbox_container_runner_for_execution(&options.execution)? {
        return sandbox_run_container_with_store(options, stdin, store, runner);
    }
    sandbox_run_wasix_with_stores_and_stdin(options, stdin, store, toolchain_store)
}

fn sandbox_simple_native_for_execution(execution: &SandboxRunExecution) -> Result<bool, CliError> {
    match execution {
        SandboxRunExecution::SimpleNative => Ok(true),
        SandboxRunExecution::Local => sandbox_simple_native_requested_from_env(),
        SandboxRunExecution::Daemon | SandboxRunExecution::Container(_) => Ok(false),
    }
}

fn sandbox_simple_native_requested_from_env() -> Result<bool, CliError> {
    match std::env::var(crate::SINOMO_SANDBOX_BACKEND_ENV) {
        Ok(value) => Ok(value.trim() == "simple-native"),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(_)) => Err(CliError::Parse {
            detail: format!(
                "invalid non-unicode sandbox backend environment variable `{}`",
                crate::SINOMO_SANDBOX_BACKEND_ENV
            ),
        }),
    }
}

fn sandbox_container_runner_for_execution(
    execution: &SandboxRunExecution,
) -> Result<Option<crate::SandboxContainerCommandRunner>, CliError> {
    match execution {
        SandboxRunExecution::Container(config) => Ok(Some(
            crate::SandboxContainerCommandRunner::new(config.clone()),
        )),
        SandboxRunExecution::Local => {
            let config = crate::SandboxContainerBackendConfig::from_env().map_err(|source| {
                CliError::Parse {
                    detail: source.to_string(),
                }
            })?;
            Ok(config.map(crate::SandboxContainerCommandRunner::new))
        }
        SandboxRunExecution::Daemon | SandboxRunExecution::SimpleNative => Ok(None),
    }
}

fn sandbox_run_container_with_store(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
    store: &crate::LocalSandboxInstanceStore,
    runner: crate::SandboxContainerCommandRunner,
) -> Result<(), CliError> {
    let SandboxRunTarget::ToolchainCommand(command_name) = &options.target else {
        return Err(CliError::Parse {
            detail: "container sandbox execution requires an image command, not --wasm".to_string(),
        });
    };
    let outcome = runner.run_toolchain_command_with_stdin_and_output_sink(
        store,
        &options.sandbox_id,
        command_name,
        options.argv,
        stdin,
        options.timeout_ms,
        Some(sandbox_cli_streaming_output_sink()),
    )?;
    print_sandbox_command_outcome(&outcome, false)
}

fn sandbox_run_simple_native_with_stores_and_stdin(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let SandboxRunTarget::ToolchainCommand(command_name) = &options.target else {
        return Err(CliError::Parse {
            detail: "simple-native sandbox execution requires a host command, not --wasm"
                .to_string(),
        });
    };
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    let mut policy = crate::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let request = crate::SandboxRunToolchainRequest {
        sandbox_id: options.sandbox_id.clone(),
        command_name: command_name.clone(),
        argv: options.argv,
        stdin,
        timeout_ms: options.timeout_ms,
    };
    let outcome = current_sandbox_cli_host().run_simple_native_toolchain_command(
        store,
        toolchain_store,
        &request.sandbox_id,
        &request.command_name,
        request.argv,
        request.stdin,
        request.timeout_ms,
        &execution_node_id,
        &policy,
        Some(sandbox_cli_streaming_output_sink()),
    )?;
    print_sandbox_command_outcome(&outcome, false)
}

fn sandbox_run_wasix_with_stores_and_stdin(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
    store: &crate::LocalSandboxInstanceStore,
    toolchain_store: &crate::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let (manifest, _layout) = store.load(&options.sandbox_id)?;
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    crate::SandboxPolicy::default()
        .validate_manifest(&manifest, &execution_node_id)
        .map_err(|error| CliError::SandboxPolicyDenied {
            detail: error.to_string(),
        })?;
    match &options.target {
        SandboxRunTarget::WasmModule(module_path) => {
            let wasm = std::fs::read(module_path).map_err(|source| CliError::Io {
                path: module_path.display().to_string(),
                detail: source.to_string(),
            })?;
            let timeout_ms = if options.timeout_ms == 0 {
                crate::SandboxResourceLimits::default().wall_time_ms
            } else {
                options.timeout_ms
            };
            let outcome = current_sandbox_cli_host().run_wasix_module_with_stdin(
                store,
                toolchain_store,
                &options.sandbox_id,
                wasm,
                options.argv,
                stdin,
                timeout_ms,
                Some(sandbox_cli_streaming_output_sink()),
            )?;
            print_sandbox_command_outcome(&outcome, false)
        }
        SandboxRunTarget::ToolchainCommand(command_name) => {
            let mut policy = crate::SandboxPolicy::default();
            policy.allow_unsigned_toolchains = options.allow_unsigned;
            for capability in &options.allowed_capabilities {
                policy
                    .allowed_command_capabilities
                    .insert(capability.clone());
            }
            let request = crate::SandboxRunToolchainRequest {
                sandbox_id: options.sandbox_id.clone(),
                command_name: command_name.clone(),
                argv: options.argv,
                stdin,
                timeout_ms: options.timeout_ms,
            };
            let outcome = current_sandbox_cli_host().run_wasix_toolchain_command(
                store,
                toolchain_store,
                &request.sandbox_id,
                &request.command_name,
                request.argv,
                request.stdin,
                request.timeout_ms,
                &execution_node_id,
                &policy,
                Some(sandbox_cli_streaming_output_sink()),
            )?;
            print_sandbox_command_outcome(&outcome, false)
        }
    }
}

fn sandbox_cli_streaming_output_sink() -> std::sync::Arc<dyn crate::SandboxOutputSink> {
    std::sync::Arc::new(|chunk: crate::SandboxOutputChunk| match chunk.stream {
        crate::SandboxOutputStream::Stdout => {
            let _ = write_cli_stdout_bytes(&chunk.bytes);
        }
        crate::SandboxOutputStream::Stderr => {
            let _ = write_cli_stderr_bytes(&chunk.bytes);
        }
    })
}

pub fn sandbox_run_with_daemon_and_stdin(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
) -> Result<(), CliError> {
    let SandboxRunTarget::ToolchainCommand(command_name) = &options.target else {
        return Err(CliError::Parse {
            detail: "sandbox daemon execution requires a pinned toolchain command".to_string(),
        });
    };
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon execution requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxRunToolchainRequest {
        sandbox_id: options.sandbox_id,
        command_name: command_name.clone(),
        argv: options.argv,
        stdin,
        timeout_ms: options.timeout_ms,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let timeout = sandbox_daemon_execution_rpc_timeout(options.timeout_ms);
    let mut final_outcome = None;
    send_daemon_rpc_stream(&address, &rpc, timeout, |response| {
        if response.route != crate::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE {
            return Err(CliError::Parse {
                detail: format!("unexpected sandbox stream route {}", response.route),
            });
        }
        let frame: crate::SandboxRunToolchainStreamFrame =
            response.decode_body().map_err(CliError::from)?;
        match frame {
            crate::SandboxRunToolchainStreamFrame::Output { chunk } => {
                write_sandbox_output_chunk(&chunk).map_err(|source| CliError::Network {
                    address: "stdout".to_string(),
                    detail: source.to_string(),
                })?;
                Ok(true)
            }
            crate::SandboxRunToolchainStreamFrame::Complete { outcome } => {
                final_outcome = Some(outcome);
                Ok(false)
            }
            crate::SandboxRunToolchainStreamFrame::Error { detail } => {
                Err(CliError::Parse { detail })
            }
        }
    })?;
    let outcome = final_outcome.ok_or_else(|| CliError::Parse {
        detail: "sandbox stream ended without final outcome".to_string(),
    })?;
    print_sandbox_command_outcome(&outcome, false)
}

fn write_sandbox_output_chunk(chunk: &crate::SandboxOutputChunk) -> Result<(), CliError> {
    match chunk.stream {
        crate::SandboxOutputStream::Stdout => {
            write_cli_stdout_bytes(&chunk.bytes).map_err(CliError::stdio)
        }
        crate::SandboxOutputStream::Stderr => {
            write_cli_stderr_bytes(&chunk.bytes).map_err(CliError::stdio)
        }
    }
}

fn print_sandbox_command_outcome(
    outcome: &crate::SandboxLocalCommandOutcome,
    replay_output: bool,
) -> Result<(), CliError> {
    if replay_output {
        for chunk in &outcome.output_chunks {
            match chunk.stream {
                crate::SandboxOutputStream::Stdout => {
                    write_cli_stdout_bytes(&chunk.bytes).map_err(CliError::stdio)?;
                }
                crate::SandboxOutputStream::Stderr => {
                    write_cli_stderr_bytes(&chunk.bytes).map_err(CliError::stdio)?;
                }
            }
        }
    }

    ensure_sandbox_stream_line_endings(&outcome.output_chunks)?;

    println!("sandbox command: {}", outcome.response.command_id);
    println!("exit status: {}", outcome.response.exit_status);
    println!("stdout: {}", outcome.response.stdout_ref);
    println!("stderr: {}", outcome.response.stderr_ref);
    if !outcome.response.changed_paths.is_empty() {
        println!("changed paths:");
        for path in &outcome.response.changed_paths {
            println!("  {path}");
        }
    }
    sandbox_command_status_result(
        outcome.response.command_id.as_str(),
        outcome.response.exit_status,
        Some(outcome.response.stderr_ref.as_str()),
    )
}

fn ensure_sandbox_stream_line_endings(
    chunks: &[crate::SandboxOutputChunk],
) -> Result<(), CliError> {
    let stdout_needs_newline = chunks
        .iter()
        .rev()
        .find(|chunk| chunk.stream == crate::SandboxOutputStream::Stdout)
        .is_some_and(|chunk| !chunk.bytes.is_empty() && !chunk.bytes.ends_with(b"\n"));
    let stderr_needs_newline = chunks
        .iter()
        .rev()
        .find(|chunk| chunk.stream == crate::SandboxOutputStream::Stderr)
        .is_some_and(|chunk| !chunk.bytes.is_empty() && !chunk.bytes.ends_with(b"\n"));
    if stdout_needs_newline {
        write_cli_stdout_bytes(b"\n").map_err(CliError::stdio)?;
    }
    if stderr_needs_newline {
        write_cli_stderr_bytes(b"\n").map_err(CliError::stdio)?;
    }
    Ok(())
}

fn print_sandbox_process_plan_outcome(
    outcome: &crate::SandboxProcessPlanOutcome,
    replay_output: bool,
) -> Result<(), CliError> {
    if replay_output {
        write_cli_stdout_bytes(&outcome.stdout).map_err(CliError::stdio)?;
        write_cli_stderr_bytes(&outcome.stderr).map_err(CliError::stdio)?;
    }
    println!(
        "sandbox process plan steps: {}",
        outcome.step_outcomes.len()
    );
    println!("exit status: {}", outcome.exit_status);
    if let Some(last) = outcome.step_outcomes.last() {
        println!("final command: {}", last.response.command_id);
        println!("stdout: {}", last.response.stdout_ref);
        println!("stderr: {}", last.response.stderr_ref);
    }
    sandbox_command_status_result("sandbox process plan", outcome.exit_status, None)
}

fn sandbox_command_status_result(
    command: &str,
    exit_status: i32,
    stderr_ref: Option<&str>,
) -> Result<(), CliError> {
    if exit_status == 0 {
        return Ok(());
    }
    Err(CliError::Process {
        command: command.to_string(),
        status: exit_status,
        stderr: stderr_ref
            .map(|path| format!("sandbox command failed; stderr log: {path}"))
            .unwrap_or_else(|| "sandbox command failed".to_string()),
    })
}

pub fn sandbox_status_with_store(
    options: SandboxStatusOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.status(crate::SandboxStatusRequest {
        sandbox_id: options.sandbox_id,
    })?;
    // The backend returns only the manifest; recover the on-disk layout for the local summary
    // (the daemon path has no layout and passes None).
    let layout = store.layout(&response.manifest.sandbox_id);
    print_sandbox_manifest_summary(&response.manifest, Some(&layout));
    Ok(())
}

pub fn sandbox_status_with_daemon(options: SandboxStatusOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon status requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxStatusRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_STATUS_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(10))?;
    let response: crate::SandboxStatusResponse = response.decode_body().map_err(CliError::from)?;
    print_sandbox_manifest_summary(&response.manifest, None);
    Ok(())
}

pub fn sandbox_config_with_store(
    options: SandboxConfigOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    validate_sandbox_config_env_options(&options)?;
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.update(crate::SandboxUpdateInstanceRequest {
        sandbox_id: options.sandbox_id,
        cwd: options.cwd,
        mounts: None,
        env_set: options.env_set,
        env_unset: options.env_unset,
    })?;
    print_sandbox_manifest_summary(
        &response.manifest,
        Some(&store.layout(&response.manifest.sandbox_id)),
    );
    Ok(())
}

pub fn sandbox_config_with_daemon(options: SandboxConfigOptions) -> Result<(), CliError> {
    validate_sandbox_config_env_options(&options)?;
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon config requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxUpdateInstanceRequest {
        sandbox_id: options.sandbox_id,
        cwd: options.cwd,
        mounts: None,
        env_set: options.env_set,
        env_unset: options.env_unset,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_UPDATE_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(10))?;
    let response: crate::SandboxUpdateInstanceResponse =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_manifest_summary(&response.manifest, None);
    Ok(())
}

fn validate_sandbox_config_env_options(options: &SandboxConfigOptions) -> Result<(), CliError> {
    for (key, value) in &options.env_set {
        validate_sandbox_env_name(key)?;
        validate_sandbox_env_value(key, value)?;
    }
    for key in &options.env_unset {
        validate_sandbox_env_name(key)?;
    }
    Ok(())
}

fn print_sandbox_manifest_summary(
    manifest: &crate::SandboxInstanceManifest,
    layout: Option<&crate::LocalSandboxInstanceLayout>,
) {
    println!("sandbox: {}", manifest.sandbox_id);
    println!("workspace: {}", manifest.workspace_root);
    if let Some(layout) = layout {
        println!("local root: {}", layout.root.display());
        println!("manifest: {}", layout.manifest.display());
    } else {
        println!(
            "sifs manifest: {}",
            crate::sandbox_manifest_path(&manifest.sandbox_id)
        );
    }
    println!("toolchain: {}", manifest.toolchain_id);
    println!("cwd: {}", manifest.cwd);
    println!("env vars: {}", manifest.env.len());
    println!(
        "tmp policy: {}",
        sandbox_temp_policy_label(&manifest.temp_policy)
    );
    println!("memory mb: {}", manifest.limits.memory_mb);
    println!("wall time ms: {}", manifest.limits.wall_time_ms);
    println!("mounts: {}", manifest.mounts.len());
    println!("command history: {}", manifest.command_history.len());
    if !manifest.command_history.is_empty() {
        println!("recent commands:");
        for command_id in manifest.command_history.iter().rev().take(5).rev() {
            println!("  {command_id}");
        }
    }
}

pub fn sandbox_logs_with_store(
    options: SandboxLogsOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (manifest, layout) = store.load(&options.sandbox_id)?;
    let mut sandbox_fs = crate::LocalSandboxFileSystem::new(layout);
    match options.command_id {
        Some(command_id) => {
            let stdout_path = crate::sandbox_command_stdout_log_path(&command_id)?;
            let stderr_path = crate::sandbox_command_stderr_log_path(&command_id)?;
            let record = crate::load_command_record(&mut sandbox_fs, &command_id)?;
            print_sandbox_command_record_summary(&record);
            let stdout = sandbox_fs.read(&stdout_path, None)?;
            let stderr = sandbox_fs.read(&stderr_path, None)?;
            print!("{}", String::from_utf8_lossy(&stdout));
            eprint!("{}", String::from_utf8_lossy(&stderr));
        }
        None => {
            if manifest.command_history.is_empty() {
                for entry in
                    sandbox_fs.list(&crate::SandboxPath::new("/logs").expect("static path"))?
                {
                    println!("{}", entry.path);
                }
            } else {
                print_sandbox_command_history(&mut sandbox_fs, &manifest.command_history)?;
            }
        }
    }
    Ok(())
}

pub fn sandbox_logs_with_daemon(options: SandboxLogsOptions) -> Result<(), CliError> {
    match options.command_id {
        Some(command_id) => {
            let stdout_path = crate::sandbox_command_stdout_log_path(&command_id)?;
            let stderr_path = crate::sandbox_command_stderr_log_path(&command_id)?;
            let record_path = crate::sandbox_command_record_log_path(&command_id)?;
            let record_bytes = daemon_sandbox_read_file(&options.sandbox_id, record_path, None)?;
            let record = crate::decode_command_record(&record_bytes)?;
            print_sandbox_command_record_summary(&record);
            let stdout = daemon_sandbox_read_file(&options.sandbox_id, stdout_path, None)?;
            let stderr = daemon_sandbox_read_file(&options.sandbox_id, stderr_path, None)?;
            print!("{}", String::from_utf8_lossy(&stdout));
            eprint!("{}", String::from_utf8_lossy(&stderr));
        }
        None => {
            let manifest = daemon_sandbox_status(&options.sandbox_id)?;
            if manifest.command_history.is_empty() {
                for entry in daemon_sandbox_list(
                    &options.sandbox_id,
                    crate::SandboxPath::new("/logs").expect("static path"),
                )? {
                    println!("{}", entry.path);
                }
            } else {
                for command_id in &manifest.command_history {
                    let record_path = crate::sandbox_command_record_log_path(command_id)?;
                    let record_bytes =
                        daemon_sandbox_read_file(&options.sandbox_id, record_path, None)?;
                    let record = crate::decode_command_record(&record_bytes)?;
                    print_sandbox_command_history_line(&record);
                }
            }
        }
    }
    Ok(())
}

fn print_sandbox_command_history<F: crate::SandboxFileSystem>(
    sandbox_fs: &mut F,
    command_history: &[crate::SandboxCommandId],
) -> Result<(), CliError> {
    for command_id in command_history {
        let record = crate::load_command_record(sandbox_fs, command_id)?;
        print_sandbox_command_history_line(&record);
    }
    Ok(())
}

fn print_sandbox_command_history_line(record: &crate::SandboxCommandRecord) {
    let exit_status = record
        .response
        .as_ref()
        .map(|response| response.exit_status.to_string())
        .unwrap_or_else(|| "-".to_string());
    println!(
        "{}\t{}\texit={}\t{}",
        record.command_id,
        sandbox_command_record_status_label(&record.status),
        exit_status,
        record.argv.join(" ")
    );
}

pub fn sandbox_ls_with_store(
    options: SandboxLsOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.list(crate::SandboxListRequest {
        sandbox_id: options.sandbox_id,
        path: options.path,
    })?;
    for entry in response.entries {
        let kind = if entry.metadata.is_dir {
            "dir "
        } else {
            "file"
        };
        println!("{kind}\t{}\t{}", entry.metadata.len, entry.path);
    }
    Ok(())
}

pub fn sandbox_ls_with_daemon(options: SandboxLsOptions) -> Result<(), CliError> {
    for entry in daemon_sandbox_list(&options.sandbox_id, options.path)? {
        let kind = if entry.metadata.is_dir {
            "dir "
        } else {
            "file"
        };
        println!("{kind}\t{}\t{}", entry.metadata.len, entry.path);
    }
    Ok(())
}

pub fn sandbox_cat_with_store(
    options: SandboxCatOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.read(crate::SandboxReadRequest {
        sandbox_id: options.sandbox_id,
        path: options.path,
        range: options.range,
    })?;
    write_cli_stdout_bytes(&response.bytes).map_err(CliError::stdio)
}

pub fn sandbox_cat_with_daemon(options: SandboxCatOptions) -> Result<(), CliError> {
    let bytes = daemon_sandbox_read_file(&options.sandbox_id, options.path, options.range)?;
    write_cli_stdout_bytes(&bytes).map_err(CliError::stdio)
}

pub fn sandbox_write_with_store(
    options: SandboxWriteOptions,
    bytes: Vec<u8>,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut event_store = sinomo_core::events::InMemoryEventStore::new();
    sandbox_write_with_store_and_event_store(options, bytes, store, &mut event_store)
}

pub fn sandbox_write_with_store_and_event_store<S>(
    options: SandboxWriteOptions,
    bytes: Vec<u8>,
    store: &crate::LocalSandboxInstanceStore,
    event_store: &mut S,
) -> Result<(), CliError>
where
    S: EventStore,
{
    // Route the validate+write through the shared backend; the local event emission stays
    // here, keyed off the returned error type (QuotaExceeded -> resource-exhausted;
    // ReadOnly -> write-denial, via the helper), matching the prior split-step behavior.
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let sandbox_id = options.sandbox_id.clone();
    let path = options.path.clone();
    let byte_len = bytes.len() as u64;
    match backend.write(crate::SandboxWriteRequest {
        sandbox_id: options.sandbox_id,
        path: options.path,
        bytes,
    }) {
        Ok(_response) => {
            if !crate::sandbox_path_is_virtual_device_sink(&path) {
                let event = crate::sandbox_file_written_event(
                    NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
                        detail: format!("invalid sandbox CLI node id: {source}"),
                    })?,
                    &sandbox_id,
                    &path,
                    byte_len,
                );
                event_store.append(event)?;
            }
            println!("wrote {byte_len} bytes to {path}");
            Ok(())
        }
        Err(crate::SandboxBackendError::Fs(error)) => {
            if let crate::SandboxFsError::QuotaExceeded {
                kind,
                actual,
                limit,
            } = &error
            {
                let event = crate::sandbox_resource_exhausted_event(
                    NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
                        detail: format!("invalid sandbox CLI node id: {source}"),
                    })?,
                    &sandbox_id,
                    "sandbox-write",
                    kind.clone(),
                    *actual,
                    *limit,
                );
                event_store.append(event)?;
            }
            append_local_sandbox_write_denial_event(event_store, &sandbox_id, &path, &error)?;
            Err(error.into())
        }
        Err(other) => Err(other.into()),
    }
}

fn append_local_sandbox_write_denial_event<S>(
    event_store: &mut S,
    sandbox_id: &crate::SandboxId,
    request_path: &crate::SandboxPath,
    error: &crate::SandboxFsError,
) -> Result<(), CliError>
where
    S: EventStore,
{
    let crate::SandboxFsError::ReadOnly { path } = error else {
        return Ok(());
    };
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let denial = crate::SandboxPolicyDenial::new(
        "mount-read-only-denied",
        format!("write to read-only sandbox path {path} denied for request path {request_path}"),
    );
    event_store.append(crate::sandbox_policy_denied_event(
        node_id.clone(),
        Some(sandbox_id),
        "sandbox-write",
        &denial,
    ))?;
    event_store.append(crate::sandbox_mount_denied_event(
        node_id,
        Some(sandbox_id),
        "sandbox-write",
        &denial,
    ))?;
    Ok(())
}

pub fn sandbox_write_with_daemon(
    options: SandboxWriteOptions,
    bytes: Vec<u8>,
) -> Result<(), CliError> {
    let response = daemon_sandbox_write_file(&options.sandbox_id, options.path, bytes)?;
    println!(
        "wrote {} bytes to {}",
        response.bytes_written, response.path
    );
    Ok(())
}

fn daemon_sandbox_list(
    sandbox_id: &crate::SandboxId,
    path: crate::SandboxPath,
) -> Result<Vec<crate::SandboxDirEntry>, CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon list requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxListRequest {
        sandbox_id: sandbox_id.clone(),
        path,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_LIST_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(10))?;
    let response: crate::SandboxListResponse = response.decode_body().map_err(CliError::from)?;
    Ok(response.entries)
}

fn daemon_sandbox_status(
    sandbox_id: &crate::SandboxId,
) -> Result<crate::SandboxInstanceManifest, CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon status requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxStatusRequest {
        sandbox_id: sandbox_id.clone(),
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_STATUS_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(10))?;
    let response: crate::SandboxStatusResponse = response.decode_body().map_err(CliError::from)?;
    Ok(response.manifest)
}

fn daemon_sandbox_read_file(
    sandbox_id: &crate::SandboxId,
    path: crate::SandboxPath,
    range: Option<crate::ByteRange>,
) -> Result<Vec<u8>, CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon read requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxReadRequest {
        sandbox_id: sandbox_id.clone(),
        path,
        range,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_READ_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(10))?;
    let response: crate::SandboxReadResponse = response.decode_body().map_err(CliError::from)?;
    Ok(response.bytes)
}

fn daemon_sandbox_write_file(
    sandbox_id: &crate::SandboxId,
    path: crate::SandboxPath,
    bytes: Vec<u8>,
) -> Result<crate::SandboxWriteResponse, CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon write requires the local daemon".to_string(),
        });
    };
    daemon_sandbox_write_file_to_address(&address, sandbox_id, path, bytes)
}

fn daemon_sandbox_write_file_to_address(
    daemon_address: &str,
    sandbox_id: &crate::SandboxId,
    path: crate::SandboxPath,
    bytes: Vec<u8>,
) -> Result<crate::SandboxWriteResponse, CliError> {
    let request = crate::SandboxWriteRequest {
        sandbox_id: sandbox_id.clone(),
        path,
        bytes,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_WRITE_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(daemon_address, &rpc, std::time::Duration::from_secs(60))?;
    response.decode_body().map_err(CliError::from)
}

pub fn sandbox_fetch_with_store_transport_and_event_store<T, S>(
    options: SandboxFetchOptions,
    store: &crate::LocalSandboxInstanceStore,
    policy: &crate::SandboxPolicy,
    transport: &T,
    event_store: &mut S,
) -> Result<crate::SandboxHttpFetchFileResult, CliError>
where
    T: crate::SandboxHttpTransport,
    S: EventStore,
{
    let (manifest, layout) = store.load(&options.sandbox_id)?;
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    let mut request = crate::SandboxHttpRequest::get(options.url);
    request.timeout_ms = options.timeout_ms;
    request.max_response_bytes = options.max_response_bytes;
    request.headers = options.headers;
    if let Err(denial) = policy.validate_manifest(&manifest, &execution_node_id) {
        let event = crate::sandbox_policy_denied_event(
            execution_node_id.clone(),
            Some(&manifest.sandbox_id),
            "sandbox-fetch",
            &denial,
        );
        event_store.append(event)?;
        return Err(CliError::SandboxPolicyDenied {
            detail: denial.to_string(),
        });
    }
    if let Err(denial) = policy.validate_http_request(&request) {
        let event = crate::sandbox_policy_denied_event(
            execution_node_id.clone(),
            Some(&manifest.sandbox_id),
            "sandbox-fetch",
            &denial,
        );
        event_store.append(event)?;
        return Err(CliError::SandboxPolicyDenied {
            detail: denial.to_string(),
        });
    }
    let mut sandbox_fs = crate::LocalSandboxFileSystem::new(layout);
    let command_id = crate::SandboxCommandId::generate();
    let result = match crate::sandbox_http_fetch_to_file(
        policy,
        transport,
        &mut sandbox_fs,
        manifest.sandbox_id.clone(),
        command_id.clone(),
        &request,
        &options.output_path,
        &manifest.limits,
    ) {
        Ok(result) => result,
        Err(error) => {
            if let crate::SandboxRecordedHttpFetchError::Fs(
                crate::SandboxFsError::QuotaExceeded {
                    kind,
                    actual,
                    limit,
                },
            ) = &error
            {
                let event = crate::sandbox_resource_exhausted_event(
                    execution_node_id.clone(),
                    &manifest.sandbox_id,
                    "sandbox-fetch",
                    kind.clone(),
                    *actual,
                    *limit,
                );
                event_store.append(event)?;
            }
            if let Some(record) =
                sandbox_http_exchange_record_for_command(&sandbox_fs, &command_id)?
            {
                let event = crate::sandbox_http_exchange_event(execution_node_id, &record);
                event_store.append(event)?;
            }
            return Err(error.into());
        }
    };
    let file_event = crate::sandbox_file_written_event_with_context(
        execution_node_id.clone(),
        &manifest.sandbox_id,
        &options.output_path,
        result.response_bytes,
        "sandbox-fetch",
        Some(&result.record.command_id),
        Some(&result.request_id),
    );
    event_store.append(file_event)?;
    let event = crate::sandbox_http_exchange_event(execution_node_id, &result.record);
    event_store.append(event)?;
    print_sandbox_http_fetch_file_result(&result);
    Ok(result)
}

fn print_sandbox_http_fetch_file_result(result: &crate::SandboxHttpFetchFileResult) {
    println!("request: {}", result.request_id);
    println!("status: {}", result.status);
    println!("bytes: {}", result.response_bytes);
    println!("output: {}", result.output_path);
    println!(
        "audit: {}",
        crate::sandbox_http_exchange_record_path(
            &result.record.sandbox_id,
            &result.record.command_id,
            &result.request_id,
        )
    );
}

fn sandbox_http_exchange_record_for_command<F>(
    sandbox_fs: &F,
    command_id: &crate::SandboxCommandId,
) -> Result<Option<crate::SandboxHttpExchangeRecord>, CliError>
where
    F: crate::SandboxFileSystem,
{
    let logs_path = crate::SandboxPath::new("/logs")?;
    let mut entries = sandbox_fs.list(&logs_path)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let prefix = format!("/logs/{command_id}.");
    for entry in entries {
        let path = entry.path;
        let path_text = path.as_str();
        if path_text.starts_with(&prefix) && path_text.ends_with(".http.sobj") {
            let bytes = sandbox_fs.read(&path, None)?;
            return Ok(Some(crate::decode_http_exchange_record(&bytes)?));
        }
    }
    Ok(None)
}

struct DaemonMediatedSandboxHttpTransport {
    address: String,
}

impl crate::SandboxHttpTransport for DaemonMediatedSandboxHttpTransport {
    fn fetch(
        &self,
        request: &crate::SandboxHttpRequest,
    ) -> Result<crate::SandboxHttpResponse, crate::SandboxNetworkError> {
        let rpc = network::ObjectRpcRequest::encode_body(
            crate::SANDBOX_HTTP_FETCH_ROUTE,
            crate::SANDBOX_RPC_CATEGORY,
            request,
        )
        .map_err(|source| crate::SandboxNetworkError::Transport {
            detail: source.to_string(),
        })?;
        let timeout = std::time::Duration::from_millis(request.timeout_ms.saturating_add(1_000));
        let response = send_daemon_rpc(&self.address, &rpc, timeout).map_err(|source| {
            crate::SandboxNetworkError::Transport {
                detail: source.to_string(),
            }
        })?;
        response
            .decode_body()
            .map_err(|source| crate::SandboxNetworkError::Transport {
                detail: source.to_string(),
            })
    }
}

fn print_sandbox_command_record_summary(record: &crate::SandboxCommandRecord) {
    println!("command: {}", record.command_id);
    println!(
        "status: {}",
        sandbox_command_record_status_label(&record.status)
    );
    println!("workspace: {}", record.workspace_root);
    println!("toolchain: {}", record.toolchain_id);
    println!("cwd: {}", record.cwd);
    println!("argv: {}", record.argv.join(" "));
    if !record.env.is_empty() {
        let env_keys = record.env.keys().cloned().collect::<Vec<_>>().join(", ");
        println!("env keys: {env_keys}");
    }
    if let Some(response) = &record.response {
        println!("exit status: {}", response.exit_status);
        println!("stdout: {}", response.stdout_ref);
        println!("stderr: {}", response.stderr_ref);
        if response.resource_usage.written_bytes == crate::SANDBOX_RESOURCE_USAGE_UNAVAILABLE {
            println!("written bytes: unavailable (host writes are not fully observable)");
        } else {
            println!("written bytes: {}", response.resource_usage.written_bytes);
        }
        if !response.changed_paths.is_empty() {
            println!("changed paths:");
            for path in &response.changed_paths {
                println!("  {path}");
            }
        }
    }
    if let Some(error) = &record.error {
        println!("error: {error}");
    }
    println!("--- stdout ---");
}

fn sandbox_command_record_status_label(status: &crate::SandboxCommandRecordStatus) -> &'static str {
    match status {
        crate::SandboxCommandRecordStatus::Completed => "completed",
        crate::SandboxCommandRecordStatus::Failed => "failed",
        crate::SandboxCommandRecordStatus::Cancelled => "cancelled",
        crate::SandboxCommandRecordStatus::ResourceExhausted => "resource-exhausted",
    }
}

pub fn sandbox_checkpoint_with_store(
    options: SandboxCheckpointOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.checkpoint(crate::SandboxCheckpointRequest {
        sandbox_id: options.sandbox_id,
    })?;
    println!("checkpoint: {}", response.checkpoint.checkpoint_id);
    println!("sandbox: {}", response.checkpoint.sandbox_id);
    println!("manifest: {}", response.checkpoint.manifest_ref);
    Ok(())
}

pub fn sandbox_checkpoint_with_daemon(options: SandboxCheckpointOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon checkpoint requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxCheckpointRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_CHECKPOINT_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(
        &address,
        &rpc,
        sandbox_daemon_metadata_rpc_connect_timeout(),
    )?;
    let response: crate::SandboxCheckpointResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("checkpoint: {}", response.checkpoint.checkpoint_id);
    println!("sandbox: {}", response.checkpoint.sandbox_id);
    println!("manifest: {}", response.checkpoint.manifest_ref);
    Ok(())
}

pub fn sandbox_resume_with_store(
    options: SandboxResumeOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let checkpoint_id = options.checkpoint_id.clone();
    let response = backend.resume(crate::SandboxResumeRequest {
        sandbox_id: options.sandbox_id,
        checkpoint_id: options.checkpoint_id,
    })?;
    println!("sandbox resumed: {}", response.manifest.sandbox_id);
    println!("checkpoint: {checkpoint_id}");
    println!("workspace: {}", response.manifest.workspace_root);
    Ok(())
}

pub fn sandbox_resume_with_daemon(options: SandboxResumeOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon resume requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxResumeRequest {
        sandbox_id: options.sandbox_id,
        checkpoint_id: options.checkpoint_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_RESUME_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(
        &address,
        &rpc,
        sandbox_daemon_metadata_rpc_connect_timeout(),
    )?;
    let response: crate::SandboxResumeResponse = response.decode_body().map_err(CliError::from)?;
    println!("sandbox resumed: {}", response.manifest.sandbox_id);
    println!("checkpoint: {}", request.checkpoint_id);
    println!("workspace: {}", response.manifest.workspace_root);
    Ok(())
}

pub fn sandbox_reboot_with_store(
    options: SandboxRebootOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.reboot(crate::SandboxRebootRequest {
        sandbox_id: options.sandbox_id,
    })?;
    println!("sandbox rebooted: {}", response.manifest.sandbox_id);
    println!("workspace: {}", response.manifest.workspace_root);
    println!("tmp cleared: {}", response.cleared_tmp);
    Ok(())
}

pub fn sandbox_reboot_with_daemon(options: SandboxRebootOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon reboot requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxRebootRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_REBOOT_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(30))?;
    let response: crate::SandboxRebootResponse = response.decode_body().map_err(CliError::from)?;
    println!("sandbox rebooted: {}", response.manifest.sandbox_id);
    println!("workspace: {}", response.manifest.workspace_root);
    println!("tmp cleared: {}", response.cleared_tmp);
    Ok(())
}

pub fn sandbox_destroy_with_store(
    options: SandboxDestroyOptions,
    store: &crate::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut backend = LocalSandboxBackend::with_store(store.clone());
    let response = backend.destroy(crate::SandboxDestroyRequest {
        sandbox_id: options.sandbox_id,
    })?;
    println!("sandbox destroyed: {}", response.sandbox_id);
    Ok(())
}

pub fn sandbox_destroy_with_daemon(options: SandboxDestroyOptions) -> Result<(), CliError> {
    let Some(address) = default_main_daemon_address()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon destroy requires the local daemon".to_string(),
        });
    };
    let request = crate::SandboxDestroyRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        crate::SANDBOX_DESTROY_ROUTE,
        crate::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = send_daemon_rpc(&address, &rpc, std::time::Duration::from_secs(30))?;
    let response: crate::SandboxDestroyResponse = response.decode_body().map_err(CliError::from)?;
    println!("sandbox destroyed: {}", response.sandbox_id);
    Ok(())
}

pub fn sandbox_store_from_data_dir(data_dir: &SinomoDataDir) -> crate::LocalSandboxInstanceStore {
    crate::LocalSandboxInstanceStore::new(data_dir.root().join("sandbox").join("runtime"))
}

pub fn sandbox_toolchain_store_from_data_dir(
    data_dir: &SinomoDataDir,
) -> crate::LocalSandboxToolchainStore {
    crate::LocalSandboxToolchainStore::new(data_dir.root().join("sandbox").join("cache"))
}

fn parse_module_artifact_id(value: &str) -> Result<sinomo_module::ArtifactId, CliError> {
    sinomo_module::ArtifactId::new(value.to_string())
        .or_else(|_| sinomo_module::ArtifactId::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid module artifact id `{value}`: {source}"),
        })
}

fn parse_module_id(value: &str) -> Result<sinomo_module::ModuleId, CliError> {
    sinomo_module::ModuleId::new(value.to_string())
        .or_else(|_| sinomo_module::ModuleId::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid module id `{value}`: {source}"),
        })
}

fn parse_sinomo_id(value: &str, label: &str) -> Result<sinomo_core::Id, CliError> {
    sinomo_core::Id::new(value.to_string())
        .or_else(|_| sinomo_core::Id::from_legacy_named(value))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid {label} id `{value}`: {source}"),
        })
}

fn parse_sandbox_id(value: &str) -> Result<crate::SandboxId, CliError> {
    crate::SandboxId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox id `{value}`: {source}"),
    })
}

fn parse_sandbox_command_id(value: &str) -> Result<crate::SandboxCommandId, CliError> {
    crate::SandboxCommandId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox command id `{value}`: {source}"),
    })
}

fn parse_sandbox_checkpoint_id(value: &str) -> Result<crate::SandboxCheckpointId, CliError> {
    crate::SandboxCheckpointId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox checkpoint id `{value}`: {source}"),
    })
}

fn parse_sandbox_path(value: &str) -> Result<crate::SandboxPath, CliError> {
    crate::SandboxPath::new(value.to_string()).map_err(CliError::from)
}

fn parse_sandbox_env_assignment(value: &str) -> Result<(String, String), CliError> {
    let Some((key, env_value)) = value.split_once('=') else {
        return Err(CliError::Parse {
            detail: format!("sandbox env assignment `{value}` must use NAME=VALUE"),
        });
    };
    validate_sandbox_env_name(key)?;
    validate_sandbox_env_value(key, env_value)?;
    Ok((key.to_string(), env_value.to_string()))
}

fn validate_sandbox_env_name(value: &str) -> Result<(), CliError> {
    if !crate::sandbox_env_name_is_valid(value) {
        return Err(CliError::Parse {
            detail: format!("invalid sandbox env name `{value}`"),
        });
    }
    Ok(())
}

fn validate_sandbox_env_value(name: &str, value: &str) -> Result<(), CliError> {
    if value.contains('\0') {
        return Err(CliError::Parse {
            detail: format!("sandbox env `{name}` contains an embedded NUL byte"),
        });
    }
    Ok(())
}

fn validate_sandbox_argv_value(value: &str, context: &str) -> Result<(), CliError> {
    if value.contains('\0') {
        return Err(CliError::Parse {
            detail: format!("{context} contains an embedded NUL byte"),
        });
    }
    Ok(())
}

fn validate_sandbox_command_capability(value: &str, context: &str) -> Result<(), CliError> {
    if value.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!("{context} must be non-empty"),
        });
    }
    if value.contains('\0') {
        return Err(CliError::Parse {
            detail: format!("{context} `{value}` contains an embedded NUL byte"),
        });
    }
    Ok(())
}

fn parse_sandbox_mount(value: &str) -> Result<crate::SandboxMount, CliError> {
    let parts: Vec<&str> = value.split(':').collect();
    match parts.as_slice() {
        [source_sifs_path, mount_path] => Ok(crate::SandboxMount {
            source_sifs_path: parse_sifs_mount_prefix(source_sifs_path)?,
            mount_path: parse_sandbox_path(mount_path)?,
            mode: crate::SandboxMountMode::ReadWrite,
        }),
        [source_sifs_path, mount_path, mode] => Ok(crate::SandboxMount {
            source_sifs_path: parse_sifs_mount_prefix(source_sifs_path)?,
            mount_path: parse_sandbox_path(mount_path)?,
            mode: parse_sandbox_mount_mode(mode)?,
        }),
        _ => Err(CliError::Parse {
            detail:
                "sandbox mount must use <sifs-path>:<mount-path> or <sifs-path>:<mount-path>:ro|rw"
                    .to_string(),
        }),
    }
}

fn parse_sifs_mount_prefix(value: &str) -> Result<String, CliError> {
    let trimmed = value.trim();
    if !trimmed.starts_with('/')
        || trimmed.contains('\0')
        || trimmed.contains("//")
        || trimmed.contains("/../")
        || trimmed.ends_with("/..")
    {
        return Err(CliError::Parse {
            detail: format!("invalid SIFS path `{value}`"),
        });
    }
    if trimmed == "/" {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", trimmed.trim_matches('/')))
    }
}

fn parse_http_host(value: &str) -> Result<String, CliError> {
    let host = value.trim().to_ascii_lowercase();
    if host.is_empty()
        || host.contains('\0')
        || host.contains('/')
        || host.contains(':')
        || host.contains('@')
        || host.chars().any(char::is_whitespace)
    {
        return Err(CliError::Parse {
            detail: format!("invalid HTTP host `{value}`"),
        });
    }
    Ok(host)
}

fn parse_http_header(value: &str) -> Result<(String, String), CliError> {
    let Some((name, header_value)) = value.split_once(':') else {
        return Err(CliError::Parse {
            detail: format!("invalid HTTP header `{value}`; expected Name: value"),
        });
    };
    let name = name.trim();
    if name.is_empty()
        || name.contains('/')
        || name.contains('@')
        || name.chars().any(char::is_whitespace)
        || name.chars().any(|ch| ch.is_control())
    {
        return Err(CliError::Parse {
            detail: format!("invalid HTTP header name `{name}`"),
        });
    }
    Ok((name.to_string(), header_value.trim().to_string()))
}

fn parse_sandbox_mount_mode(value: &str) -> Result<crate::SandboxMountMode, CliError> {
    match value {
        "ro" | "read-only" => Ok(crate::SandboxMountMode::ReadOnly),
        "rw" | "read-write" => Ok(crate::SandboxMountMode::ReadWrite),
        _ => Err(CliError::Parse {
            detail: format!("invalid sandbox mount mode `{value}`; expected ro or rw"),
        }),
    }
}

fn parse_toolchain_id(value: &str) -> Result<crate::SandboxToolchainId, CliError> {
    crate::SandboxToolchainId::new(value.to_string()).or_else(|new_error| {
        crate::SandboxToolchainId::from_legacy_named(value).map_err(
            |legacy_error| CliError::Parse {
                detail: format!(
                    "invalid sandbox toolchain id `{value}`: {new_error}; legacy name parse failed: {legacy_error}"
                ),
            },
        )
    })
}

fn parse_sandbox_temp_policy(value: &str) -> Result<crate::SandboxTempPolicy, CliError> {
    match value {
        "clear" | "clear-on-session-start" => Ok(crate::SandboxTempPolicy::ClearOnSessionStart),
        "retain" | "retain-across-sessions" => Ok(crate::SandboxTempPolicy::RetainAcrossSessions),
        _ => Err(CliError::Parse {
            detail: format!("invalid sandbox tmp policy `{value}`; expected clear or retain"),
        }),
    }
}

fn sandbox_temp_policy_label(policy: &crate::SandboxTempPolicy) -> &'static str {
    match policy {
        crate::SandboxTempPolicy::ClearOnSessionStart => "clear-on-session-start",
        crate::SandboxTempPolicy::RetainAcrossSessions => "retain-across-sessions",
    }
}

fn sandbox_mount_mode_label(mode: &crate::SandboxMountMode) -> &'static str {
    match mode {
        crate::SandboxMountMode::ReadOnly => "ro",
        crate::SandboxMountMode::ReadWrite => "rw",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct AuthoritativeSnapshotHost {
        fail_authority: bool,
    }

    impl SandboxCliHost for AuthoritativeSnapshotHost {
        fn default_main_daemon_address(&self) -> Result<Option<String>, SandboxCliError> {
            Ok(Some("127.0.0.1:21352".to_string()))
        }

        fn send_daemon_rpc(
            &self,
            _address: &str,
            request: &network::ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, SandboxCliError> {
            assert_eq!(
                request.route,
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE
            );
            assert_eq!(request.category, sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY);
            sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_request_body(
                &request.body,
            )
            .expect("authoritative snapshot marker request");
            if self.fail_authority {
                return Err(SandboxCliError::Network {
                    address: request.route.clone(),
                    detail: "quorum unavailable".to_string(),
                });
            }

            let snapshot = sinomo_core::kv::InProcessKvStore::new()
                .snapshot()
                .expect("snapshot");
            let response = sinomo_core::kv::KvAuthoritativeRead {
                authority: sinomo_core::kv::KvAuthorityProof {
                    term: 3,
                    commit_index: 7,
                    revision: snapshot.revision,
                    basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                        voter_acks: 2,
                        required_quorum: 2,
                    },
                },
                value: snapshot,
            };
            let payload =
                sinomo_core::kv::cluster_kv_authoritative_snapshot_response_rpc_payload(&response)
                    .expect("authoritative snapshot response");
            Ok(ObjectRpcResponse::from_encoded_body(
                payload.route,
                payload.body,
            ))
        }

        fn read_cluster_kv_local_snapshot_for_test(
            &self,
            _paths: &ClusterPaths,
        ) -> Option<Result<KvSnapshot, SandboxCliError>> {
            panic!("an authority failure must not fall back to a local snapshot")
        }
    }

    #[test]
    fn sandbox_policy_reads_use_the_authoritative_snapshot_route() {
        let host = Arc::new(AuthoritativeSnapshotHost {
            fail_authority: false,
        });
        let snapshot = with_sandbox_cli_host(host, || {
            fetch_daemon_cluster_kv_authoritative_snapshot("127.0.0.1:21352")
        })
        .expect("authoritative snapshot");

        assert_eq!(snapshot.revision, sinomo_core::kv::KvRevision::ZERO);
    }

    #[test]
    fn sandbox_policy_authority_failure_does_not_fall_back_to_local_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-sandbox-policy-authority-failure");
        let paths = ClusterPaths::new(&SinomoDataDir::new(root.path()));
        let host = Arc::new(AuthoritativeSnapshotHost {
            fail_authority: true,
        });

        let error = with_sandbox_cli_host(host, || sandbox_policy_cluster_snapshot(&paths))
            .expect_err("authority failure");

        assert!(matches!(
            error,
            SandboxCliError::Network { detail, .. } if detail == "quorum unavailable"
        ));
    }

    #[test]
    fn sandbox_help_exposes_lifecycle_and_id_requirements() {
        let help = sandbox_cli_help();

        assert!(
            help.lines
                .iter()
                .any(|line| line.contains("<lowercase-ulid>"))
        );
        for command in ["checkpoint", "resume", "reboot", "destroy"] {
            assert!(
                help.lines.iter().any(|line| line.contains(command)),
                "sandbox help is missing lifecycle command {command}"
            );
        }
    }

    #[test]
    fn sandbox_doctor_reports_coding_toolchain_readiness() {
        let root = sinomo_testdir::TestDir::new("sinomo-sandbox-doctor-coding");
        let store = crate::LocalSandboxInstanceStore::new(root.join("runtime"));
        let toolchain_store = crate::LocalSandboxToolchainStore::new(root.join("cache"));
        let sandbox_id = crate::SandboxId::generate();
        let toolchain_id = crate::SandboxToolchainId::generate();
        let mut toolchain = crate::SandboxToolchainManifest::new(
            toolchain_id.clone(),
            "coding-base-test",
            crate::SandboxWasiAbi::Wasix,
            "unit test",
            OffsetDateTime::now_utc(),
        )
        .expect("toolchain");
        toolchain_store
            .put_command_module(
                &mut toolchain,
                "grep",
                br#"
                (module
                  (import "wasi_snapshot_preview1" "fd_write"
                    (func $fd_write (param i32 i32 i32 i32) (result i32)))
                  (memory (export "memory") 1 128)
                  (func (export "_start")))
                "#,
                crate::SandboxResourceLimits::default(),
            )
            .expect("grep");
        sandbox_create_with_store(
            SandboxCreateOptions {
                sandbox_id: Some(sandbox_id.clone()),
                toolchain_id,
                temp_policy: crate::SandboxTempPolicy::ClearOnSessionStart,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                execution: SandboxMetadataExecution::Local,
            },
            &store,
        )
        .expect("create");

        let findings = sandbox_doctor_findings_with_stores(
            SandboxDoctorOptions {
                sandbox_id: Some(sandbox_id),
            },
            &store,
            &toolchain_store,
            &crate::SandboxPolicy::default(),
        )
        .expect("doctor");
        let coding_findings: Vec<_> = findings
            .iter()
            .filter(|finding| finding.code == "coding-command-missing")
            .collect();
        assert_eq!(
            coding_findings.len(),
            crate::BUSYBOX_TOOLCHAIN_COMMANDS.len() - 1
        );
        let required_rich_targets = crate::SANDBOX_RICH_TOOLCHAIN_TARGETS
            .iter()
            .filter(|target| matches!(target.status, crate::SandboxRichToolchainStatus::Required))
            .count();
        let required_toolchain_findings: Vec<_> = findings
            .iter()
            .filter(|finding| finding.code == "required-toolchain-command-missing")
            .collect();
        assert_eq!(required_toolchain_findings.len(), required_rich_targets);
        for target in crate::SANDBOX_RICH_TOOLCHAIN_TARGETS
            .iter()
            .filter(|target| matches!(target.status, crate::SandboxRichToolchainStatus::Required))
        {
            assert!(
                required_toolchain_findings.iter().any(|finding| {
                    finding.detail.contains(&format!("`{}`", target.command))
                        && finding.severity == SandboxDoctorSeverity::Error
                }),
                "doctor should treat missing {} as an error",
                target.command
            );
        }
        assert!(
            !coding_findings
                .iter()
                .any(|finding| finding.detail.contains("`grep`"))
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.code == "ambient-wasi-network-disabled")
        );
    }

    #[cfg(not(feature = "wasix-runtime"))]
    #[test]
    fn sandbox_doctor_rejects_shell_when_process_substrate_is_missing() {
        let root = sinomo_testdir::TestDir::new("sinomo-sandbox-doctor-shell-substrate");
        let store = crate::LocalSandboxInstanceStore::new(root.join("runtime"));
        let toolchain_store = crate::LocalSandboxToolchainStore::new(root.join("cache"));
        let sandbox_id = crate::SandboxId::generate();
        let toolchain_id = crate::SandboxToolchainId::generate();
        let mut toolchain = crate::SandboxToolchainManifest::new(
            toolchain_id.clone(),
            "shell-substrate-test",
            crate::SandboxWasiAbi::Wasix,
            "unit test",
            OffsetDateTime::now_utc(),
        )
        .expect("toolchain");
        toolchain_store
            .put_command_module_with_capabilities(
                &mut toolchain,
                "sh",
                br#"
                (module
                  (memory (export "memory") 1 128)
                  (func (export "_start")))
                "#,
                crate::SandboxResourceLimits::default(),
                vec![crate::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string()],
            )
            .expect("sh");
        sandbox_create_with_store(
            SandboxCreateOptions {
                sandbox_id: Some(sandbox_id.clone()),
                toolchain_id,
                temp_policy: crate::SandboxTempPolicy::ClearOnSessionStart,
                memory_mb: None,
                wall_time_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
                max_workspace_bytes: None,
                max_written_bytes_per_command: None,
                max_file_count: None,
                mounts: Vec::new(),
                execution: SandboxMetadataExecution::Local,
            },
            &store,
        )
        .expect("create");

        let findings = sandbox_doctor_findings_with_stores(
            SandboxDoctorOptions {
                sandbox_id: Some(sandbox_id),
            },
            &store,
            &toolchain_store,
            &crate::SandboxPolicy::default(),
        )
        .expect("doctor");
        assert!(findings.iter().any(|finding| {
            finding.code == "shell-process-substrate-missing"
                && finding.severity == SandboxDoctorSeverity::Error
                && finding.detail.contains("child-process-exec")
        }));
        assert!(
            !findings
                .iter()
                .any(|finding| finding.code == "shell-command-missing")
        );
    }
}
