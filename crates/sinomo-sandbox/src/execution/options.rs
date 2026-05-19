use crate::internal::*;

#[derive(Clone, Debug)]
pub struct SandboxCancellation {
    cancelled: Arc<AtomicBool>,
}

impl SandboxCancellation {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl Default for SandboxCancellation {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxWasiPreopen {
    pub host_path: PathBuf,
    pub guest_path: SandboxPath,
    pub mode: SandboxMountMode,
}

pub trait SandboxOutputSink: Send + Sync {
    fn output_chunk(&self, chunk: SandboxOutputChunk);
}

impl<F> SandboxOutputSink for F
where
    F: Fn(SandboxOutputChunk) + Send + Sync,
{
    fn output_chunk(&self, chunk: SandboxOutputChunk) {
        self(chunk)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxWasixCommandOverride {
    pub name: String,
    pub argv_prefix: Vec<String>,
    pub exec_name: Option<String>,
}

#[cfg(feature = "wasix-runtime")]
#[derive(Clone, Default)]
pub(crate) struct SandboxWasixPipeOptions {
    pub stdin_pipe: Option<virtual_fs::ArcFile<virtual_fs::Pipe>>,
    pub stdout_pipe: Option<virtual_fs::Pipe>,
}

#[derive(Clone)]
pub struct SandboxWasiRunOptions {
    pub preopened_dirs: Vec<SandboxWasiPreopen>,
    pub working_directory_host_path: Option<PathBuf>,
    pub metadata_host_path: Option<PathBuf>,
    #[cfg(feature = "wasix-runtime")]
    pub wasix_stdin_pipe: Option<virtual_fs::ArcFile<virtual_fs::Pipe>>,
    #[cfg(feature = "wasix-runtime")]
    pub wasix_stdout_pipe: Option<virtual_fs::Pipe>,
    pub enable_virtual_devices: bool,
    pub zero_device_bytes: usize,
    pub memory_limit_bytes: usize,
    pub fuel: u64,
    pub input_limit_bytes: usize,
    pub output_limit_bytes: usize,
    pub workspace_limit_bytes: u64,
    pub file_count_limit: u64,
    pub cancellation: SandboxCancellation,
    pub output_sink: Option<Arc<dyn SandboxOutputSink>>,
    pub allow_preview1_pinned_subprocesses: bool,
    pub wasix_pinned_child_exec_only: bool,
    pub wasix_instruction_metering: bool,
    pub wasix_command_overrides: Vec<SandboxWasixCommandOverride>,
}

impl fmt::Debug for SandboxWasiRunOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SandboxWasiRunOptions")
            .field("preopened_dirs", &self.preopened_dirs)
            .field(
                "working_directory_host_path",
                &self.working_directory_host_path,
            )
            .field("metadata_host_path", &self.metadata_host_path)
            .field(
                "wasix_stdin_pipe",
                #[cfg(feature = "wasix-runtime")]
                &self.wasix_stdin_pipe.as_ref().map(|_| "<pipe>"),
                #[cfg(not(feature = "wasix-runtime"))]
                &Option::<&str>::None,
            )
            .field(
                "wasix_stdout_pipe",
                #[cfg(feature = "wasix-runtime")]
                &self.wasix_stdout_pipe.as_ref().map(|_| "<pipe>"),
                #[cfg(not(feature = "wasix-runtime"))]
                &Option::<&str>::None,
            )
            .field("enable_virtual_devices", &self.enable_virtual_devices)
            .field("zero_device_bytes", &self.zero_device_bytes)
            .field("memory_limit_bytes", &self.memory_limit_bytes)
            .field("fuel", &self.fuel)
            .field("input_limit_bytes", &self.input_limit_bytes)
            .field("output_limit_bytes", &self.output_limit_bytes)
            .field("workspace_limit_bytes", &self.workspace_limit_bytes)
            .field("file_count_limit", &self.file_count_limit)
            .field("cancellation", &self.cancellation)
            .field("output_sink", &self.output_sink.as_ref().map(|_| "<sink>"))
            .field(
                "allow_preview1_pinned_subprocesses",
                &self.allow_preview1_pinned_subprocesses,
            )
            .field(
                "wasix_pinned_child_exec_only",
                &self.wasix_pinned_child_exec_only,
            )
            .field(
                "wasix_instruction_metering",
                &self.wasix_instruction_metering,
            )
            .field("wasix_command_overrides", &self.wasix_command_overrides)
            .finish()
    }
}

impl Default for SandboxWasiRunOptions {
    fn default() -> Self {
        let limits = SandboxResourceLimits::default();
        Self {
            preopened_dirs: Vec::new(),
            working_directory_host_path: None,
            metadata_host_path: None,
            #[cfg(feature = "wasix-runtime")]
            wasix_stdin_pipe: None,
            #[cfg(feature = "wasix-runtime")]
            wasix_stdout_pipe: None,
            enable_virtual_devices: false,
            zero_device_bytes: SANDBOX_WASI_ZERO_DEVICE_BYTES,
            memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024) as usize,
            fuel: u64::MAX,
            input_limit_bytes: limits.max_input_bytes as usize,
            output_limit_bytes: limits.max_output_bytes as usize,
            workspace_limit_bytes: limits.max_workspace_bytes,
            file_count_limit: limits.max_file_count,
            cancellation: SandboxCancellation::new(),
            output_sink: None,
            allow_preview1_pinned_subprocesses: false,
            wasix_pinned_child_exec_only: true,
            wasix_instruction_metering: true,
            wasix_command_overrides: Vec::new(),
        }
    }
}

impl SandboxWasiRunOptions {
    pub fn with_output_sink(mut self, output_sink: Option<Arc<dyn SandboxOutputSink>>) -> Self {
        self.output_sink = output_sink;
        self
    }
}

pub fn local_staged_sandbox_wasi_options(
    manifest: &SandboxInstanceManifest,
    layout: &LocalSandboxInstanceLayout,
) -> SandboxWasiRunOptions {
    local_staged_sandbox_wasi_options_with_limits(layout, &manifest.limits)
}

pub fn local_staged_sandbox_wasi_options_with_limits(
    layout: &LocalSandboxInstanceLayout,
    limits: &SandboxResourceLimits,
) -> SandboxWasiRunOptions {
    local_staged_sandbox_wasi_options_with_limits_and_preopens(layout, limits, Vec::new())
}

pub fn local_staged_sandbox_wasi_options_with_limits_and_preopens(
    layout: &LocalSandboxInstanceLayout,
    limits: &SandboxResourceLimits,
    extra_preopens: Vec<SandboxWasiPreopen>,
) -> SandboxWasiRunOptions {
    let mut preopened_dirs = vec![
        local_staged_sandbox_preopen(&layout.root_dir, "/root", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.home, "/home", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.tmp, "/tmp", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.work, "/work", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.logs, "/logs", SandboxMountMode::ReadWrite),
        local_staged_sandbox_preopen(&layout.artifacts, "/artifacts", SandboxMountMode::ReadWrite),
    ];
    preopened_dirs.extend(extra_preopens);
    SandboxWasiRunOptions {
        preopened_dirs,
        metadata_host_path: Some(layout.metadata.clone()),
        enable_virtual_devices: true,
        memory_limit_bytes: limits.memory_mb.saturating_mul(1024 * 1024) as usize,
        input_limit_bytes: limits.max_input_bytes as usize,
        output_limit_bytes: limits.max_output_bytes as usize,
        workspace_limit_bytes: limits.max_workspace_bytes,
        file_count_limit: limits.max_file_count,
        ..SandboxWasiRunOptions::default()
    }
}

fn local_staged_sandbox_preopen(
    host_path: &Path,
    guest_path: &str,
    mode: SandboxMountMode,
) -> SandboxWasiPreopen {
    SandboxWasiPreopen {
        host_path: host_path.to_path_buf(),
        guest_path: SandboxPath::new(guest_path).expect("static sandbox path"),
        mode,
    }
}
