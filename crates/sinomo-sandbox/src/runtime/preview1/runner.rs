use super::extensions::add_sinomo_sandbox_wasi_extensions;
use super::output::spawn_output_watcher;
use super::state::LocalWasiState;
use crate::{
    LocalWasiVirtualDeviceDir, SANDBOX_UNLIMITED_WALL_TIME_MS, SandboxCommandId,
    SandboxCommandRequest, SandboxCommandResponse, SandboxLocalCommandOutcome, SandboxMountMode,
    SandboxOutputStream, SandboxPath, SandboxResourceUsage, SandboxWasiError,
    SandboxWasiRunOptions, sandbox_command_stderr_path, sandbox_command_stdout_path,
    sandbox_output_chunks, validate_wasi_request,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct LocalWasiCommandRunner {
    engine: wasmtime::Engine,
}

impl LocalWasiCommandRunner {
    pub fn new() -> Result<Self, SandboxWasiError> {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = wasmtime::Engine::new(&config).map_err(SandboxWasiError::from_wasmtime)?;
        Ok(Self { engine })
    }

    pub fn run_module(
        &self,
        wasm: impl AsRef<[u8]>,
        request: SandboxCommandRequest,
        options: SandboxWasiRunOptions,
    ) -> Result<SandboxLocalCommandOutcome, SandboxWasiError> {
        validate_wasi_request(&request)?;
        if options.cancellation.is_cancelled() {
            return Err(SandboxWasiError::Cancelled);
        }

        let wasm = Arc::<[u8]>::from(wasm.as_ref().to_vec());
        let module = wasmtime::Module::new(&self.engine, wasm.as_ref())
            .map_err(SandboxWasiError::from_wasmtime)?;
        let pipe_capacity = options.output_limit_bytes.saturating_add(1);
        let stdout = wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(pipe_capacity);
        let stderr = wasmtime_wasi::p2::pipe::MemoryOutputPipe::new(pipe_capacity);
        let command_id = SandboxCommandId::generate();

        let mut wasi = wasmtime_wasi::WasiCtxBuilder::new();
        wasi.allow_blocking_current_thread(true);
        wasi.allow_ip_name_lookup(false);
        wasi.allow_tcp(false);
        wasi.allow_udp(false);
        wasi.socket_addr_check(|_, _| Box::pin(async { false }));
        wasi.args(&request.argv);
        for (key, value) in &request.env {
            if key == "PWD" {
                continue;
            }
            wasi.env(key, value);
        }
        if !request.env.iter().any(|(key, _)| key == "PATH") {
            wasi.env("PATH", "/bin:/usr/bin");
        }
        if !request.env.iter().any(|(key, _)| key == "HOME") {
            wasi.env("HOME", "/home/sinomo");
        }
        if !request.env.iter().any(|(key, _)| key == "USER") {
            wasi.env("USER", "sinomo");
        }
        if !request.env.iter().any(|(key, _)| key == "LOGNAME") {
            wasi.env("LOGNAME", "sinomo");
        }
        if !request.env.iter().any(|(key, _)| key == "SHELL") {
            wasi.env("SHELL", "/bin/sh");
        }
        if !request.env.iter().any(|(key, _)| key == "TMPDIR") {
            wasi.env("TMPDIR", "/tmp");
        }
        if !request.env.iter().any(|(key, _)| key == "XDG_CACHE_HOME") {
            wasi.env("XDG_CACHE_HOME", "/home/sinomo/.cache");
        }
        if !request.env.iter().any(|(key, _)| key == "XDG_CONFIG_HOME") {
            wasi.env("XDG_CONFIG_HOME", "/home/sinomo/.config");
        }
        if !request.env.iter().any(|(key, _)| key == "XDG_DATA_HOME") {
            wasi.env("XDG_DATA_HOME", "/home/sinomo/.local/share");
        }
        if !request.env.iter().any(|(key, _)| key == "LANG") {
            wasi.env("LANG", "C.UTF-8");
        }
        if !request.env.iter().any(|(key, _)| key == "LC_CTYPE") {
            wasi.env("LC_CTYPE", "C.UTF-8");
        }
        if !request.env.iter().any(|(key, _)| key == "TZ") {
            wasi.env("TZ", "UTC");
        }
        if !request.env.iter().any(|(key, _)| key == "TERM") {
            wasi.env("TERM", "dumb");
        }
        wasi.env("PWD", request.cwd.as_str());
        if let Some(stdin) = &request.stdin
            && stdin.len() > options.input_limit_bytes
        {
            return Err(SandboxWasiError::InputLimitExceeded {
                limit: options.input_limit_bytes,
            });
        }
        let stdin = request
            .stdin
            .clone()
            .map(wasmtime_wasi::p2::pipe::MemoryInputPipe::new);
        if let Some(stdin) = stdin.clone() {
            wasi.stdin(stdin);
        }
        wasi.stdout(stdout.clone());
        wasi.stderr(stderr.clone());
        if let Some(host_path) = &options.working_directory_host_path {
            wasi.preopened_dir(
                host_path,
                ".",
                wasmtime_wasi::DirPerms::all(),
                wasmtime_wasi::FilePerms::all(),
            )
            .map_err(SandboxWasiError::from_wasmtime)?;
        }
        for preopen in &options.preopened_dirs {
            let (dir_perms, file_perms) = match preopen.mode {
                SandboxMountMode::ReadOnly => (
                    wasmtime_wasi::DirPerms::READ,
                    wasmtime_wasi::FilePerms::READ,
                ),
                SandboxMountMode::ReadWrite => (
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                ),
            };
            wasi.preopened_dir(
                &preopen.host_path,
                guest_path_for_wasi(&preopen.guest_path),
                dir_perms,
                file_perms,
            )
            .map_err(SandboxWasiError::from_wasmtime)?;
        }
        let virtual_devices = if options.enable_virtual_devices {
            let mut proc_env = request
                .env
                .iter()
                .filter(|(key, _)| key.as_str() != "PWD")
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            proc_env.push(("PWD".to_string(), request.cwd.as_str().to_string()));
            let devices = LocalWasiVirtualDeviceDir::create(
                options.zero_device_bytes,
                &request.argv,
                &proc_env,
                &options
                    .preopened_dirs
                    .iter()
                    .map(|preopen| (preopen.guest_path.clone(), preopen.mode.clone()))
                    .collect::<Vec<_>>(),
            )?;
            wasi.preopened_dir(
                devices.path(),
                "dev",
                wasmtime_wasi::DirPerms::READ,
                wasmtime_wasi::FilePerms::READ | wasmtime_wasi::FilePerms::WRITE,
            )
            .map_err(SandboxWasiError::from_wasmtime)?;
            wasi.preopened_dir(
                devices.proc_path(),
                "proc",
                wasmtime_wasi::DirPerms::READ,
                wasmtime_wasi::FilePerms::READ,
            )
            .map_err(SandboxWasiError::from_wasmtime)?;
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/etc")
            {
                wasi.preopened_dir(
                    devices.etc_path(),
                    "etc",
                    wasmtime_wasi::DirPerms::READ,
                    wasmtime_wasi::FilePerms::READ,
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/sys")
            {
                wasi.preopened_dir(
                    devices.sys_path(),
                    "sys",
                    wasmtime_wasi::DirPerms::READ,
                    wasmtime_wasi::FilePerms::READ,
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/run")
            {
                wasi.preopened_dir(
                    devices.run_path(),
                    "run",
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/var")
            {
                wasi.preopened_dir(
                    devices.var_path(),
                    "var",
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/tmp")
            {
                wasi.preopened_dir(
                    devices.tmp_path(),
                    "tmp",
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/home")
            {
                wasi.preopened_dir(
                    devices.home_path(),
                    "home",
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/root")
            {
                wasi.preopened_dir(
                    devices.root_home_path(),
                    "root",
                    wasmtime_wasi::DirPerms::all(),
                    wasmtime_wasi::FilePerms::all(),
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/usr")
            {
                wasi.preopened_dir(
                    devices.usr_path(),
                    "usr",
                    wasmtime_wasi::DirPerms::READ,
                    wasmtime_wasi::FilePerms::READ,
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            if !options
                .preopened_dirs
                .iter()
                .any(|preopen| preopen.guest_path.as_str() == "/opt")
            {
                wasi.preopened_dir(
                    devices.opt_path(),
                    "opt",
                    wasmtime_wasi::DirPerms::READ,
                    wasmtime_wasi::FilePerms::READ,
                )
                .map_err(SandboxWasiError::from_wasmtime)?;
            }
            Some(devices)
        } else {
            None
        };
        let virtual_device_host_path = virtual_devices
            .as_ref()
            .map(|devices| devices.path().to_path_buf());
        let child_output_limit_exceeded = Arc::new(AtomicBool::new(false));
        let child_input_limit_exceeded = Arc::new(AtomicBool::new(false));

        let mut store = wasmtime::Store::new(
            &self.engine,
            LocalWasiState {
                wasi: wasi.build_p1(),
                limits: wasmtime::StoreLimitsBuilder::new()
                    .memory_size(options.memory_limit_bytes)
                    .build(),
                stdin,
                working_directory_host_path: options.working_directory_host_path.clone(),
                preopened_dirs: options.preopened_dirs.clone(),
                virtual_device_host_path,
                cwd: request.cwd.clone(),
                env: request.env.clone(),
                engine: self.engine.clone(),
                stdout: stdout.clone(),
                stderr: stderr.clone(),
                input_limit_bytes: options.input_limit_bytes,
                child_input_limit_exceeded: child_input_limit_exceeded.clone(),
                output_limit_bytes: options.output_limit_bytes,
                child_output_limit_exceeded: child_output_limit_exceeded.clone(),
                memory_limit_bytes: options.memory_limit_bytes,
                fuel: options.fuel,
                fuel_remaining: Arc::new(AtomicU64::new(options.fuel)),
                child_depth: 0,
                allow_pinned_subprocesses: options.allow_preview1_pinned_subprocesses,
            },
        );
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(options.fuel)
            .map_err(SandboxWasiError::from_wasmtime)?;
        store.set_epoch_deadline(1);

        let mut linker = wasmtime::Linker::new(&self.engine);
        wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut LocalWasiState| {
            &mut state.wasi
        })
        .map_err(SandboxWasiError::from_wasmtime)?;
        add_sinomo_sandbox_wasi_extensions(&mut linker).map_err(SandboxWasiError::from_wasmtime)?;

        let done = Arc::new(AtomicBool::new(false));
        let timed_out = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let output_watcher = options.output_sink.clone().map(|sink| {
            spawn_output_watcher(
                request.sandbox_id.clone(),
                command_id.clone(),
                stdout.clone(),
                stderr.clone(),
                done.clone(),
                sink,
            )
        });
        let watcher = spawn_epoch_watcher(
            self.engine.clone(),
            request.timeout_ms,
            done.clone(),
            timed_out.clone(),
            cancelled.clone(),
            options.cancellation.clone(),
        );

        let started = Instant::now();
        let exit_status = match run_wasi_start(&mut store, &linker, &module) {
            Ok(status) => Ok(status),
            Err(error) => {
                if cancelled.load(Ordering::SeqCst) || options.cancellation.is_cancelled() {
                    Err(SandboxWasiError::Cancelled)
                } else if timed_out.load(Ordering::SeqCst) {
                    Err(SandboxWasiError::TimedOut {
                        timeout_ms: request.timeout_ms,
                    })
                } else if output_pipe_limit_exceeded(&error) {
                    Err(SandboxWasiError::OutputLimitExceeded {
                        limit: options.output_limit_bytes,
                    })
                } else if memory_limit_exceeded(&error) {
                    Err(SandboxWasiError::MemoryLimitExceeded {
                        limit: options.memory_limit_bytes,
                    })
                } else {
                    Err(SandboxWasiError::from_wasmtime(error))
                }
            }
        };
        done.store(true, Ordering::SeqCst);
        let _ = watcher.join();
        if let Some(output_watcher) = output_watcher {
            let _ = output_watcher.join();
        }
        drop(virtual_devices);
        if cancelled.load(Ordering::SeqCst) || options.cancellation.is_cancelled() {
            return Err(SandboxWasiError::Cancelled);
        }
        if timed_out.load(Ordering::SeqCst) {
            return Err(SandboxWasiError::TimedOut {
                timeout_ms: request.timeout_ms,
            });
        }
        if child_input_limit_exceeded.load(Ordering::SeqCst) {
            return Err(SandboxWasiError::InputLimitExceeded {
                limit: options.input_limit_bytes,
            });
        }
        if child_output_limit_exceeded.load(Ordering::SeqCst) {
            return Err(SandboxWasiError::OutputLimitExceeded {
                limit: options.output_limit_bytes,
            });
        }
        let exit_status = exit_status?;

        let stdout = stdout.contents().to_vec();
        let stderr = stderr.contents().to_vec();
        if stdout.len() > options.output_limit_bytes || stderr.len() > options.output_limit_bytes {
            return Err(SandboxWasiError::OutputLimitExceeded {
                limit: options.output_limit_bytes,
            });
        }
        let stdout_ref = sandbox_command_stdout_path(&request.sandbox_id, &command_id);
        let stderr_ref = sandbox_command_stderr_path(&request.sandbox_id, &command_id);
        let mut output_chunks = sandbox_output_chunks(
            &request.sandbox_id,
            &command_id,
            SandboxOutputStream::Stdout,
            &stdout,
        );
        output_chunks.extend(sandbox_output_chunks(
            &request.sandbox_id,
            &command_id,
            SandboxOutputStream::Stderr,
            &stderr,
        ));

        Ok(SandboxLocalCommandOutcome {
            response: SandboxCommandResponse {
                command_id,
                exit_status,
                stdout_ref,
                stderr_ref,
                changed_paths: Vec::new(),
                resource_usage: SandboxResourceUsage {
                    wall_time_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    peak_memory_bytes: 0,
                    stdout_bytes: stdout.len() as u64,
                    stderr_bytes: stderr.len() as u64,
                    written_bytes: 0,
                },
            },
            stdout,
            stderr,
            output_chunks,
        })
    }
}

pub(super) fn guest_path_for_wasi(path: &SandboxPath) -> String {
    if path.as_str() == "/" {
        ".".to_string()
    } else {
        path.as_str().trim_start_matches('/').to_string()
    }
}

fn spawn_epoch_watcher(
    engine: wasmtime::Engine,
    timeout_ms: u64,
    done: Arc<AtomicBool>,
    timed_out: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    cancellation: crate::SandboxCancellation,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let started = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        while !done.load(Ordering::SeqCst) {
            if cancellation.is_cancelled() {
                cancelled.store(true, Ordering::SeqCst);
                engine.increment_epoch();
                return;
            }
            if timeout_ms != SANDBOX_UNLIMITED_WALL_TIME_MS && started.elapsed() >= timeout {
                timed_out.store(true, Ordering::SeqCst);
                engine.increment_epoch();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    })
}

pub(super) fn run_wasi_start(
    store: &mut wasmtime::Store<LocalWasiState>,
    linker: &wasmtime::Linker<LocalWasiState>,
    module: &wasmtime::Module,
) -> Result<i32, wasmtime::Error> {
    let instance = linker.instantiate(&mut *store, module)?;
    let start = instance.get_typed_func::<(), ()>(&mut *store, "_start")?;
    match start.call(&mut *store, ()) {
        Ok(()) => Ok(0),
        Err(error) => match error.downcast::<wasmtime_wasi::I32Exit>() {
            Ok(exit) => Ok(exit.0),
            Err(error) => Err(error),
        },
    }
}

pub(super) fn output_pipe_limit_exceeded(error: &wasmtime::Error) -> bool {
    let message = error.to_string();
    message.contains("MemoryOutputPipe")
        || (message.contains("output") && message.contains("limit"))
        || (message.contains("pipe") && message.contains("capacity"))
}

fn memory_limit_exceeded(error: &wasmtime::Error) -> bool {
    let message = error.to_string();
    message.contains("memory allocation")
        || message.contains("out of memory")
        || message.contains("memory limit")
        || message.contains("heap")
}
