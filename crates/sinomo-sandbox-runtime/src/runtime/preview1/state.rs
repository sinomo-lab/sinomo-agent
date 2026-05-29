use crate::{SandboxPath, SandboxWasiPreopen};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

pub(super) struct LocalWasiState {
    pub(super) wasi: wasmtime_wasi::p1::WasiP1Ctx,
    pub(super) limits: wasmtime::StoreLimits,
    pub(super) stdin: Option<wasmtime_wasi::p2::pipe::MemoryInputPipe>,
    pub(super) working_directory_host_path: Option<PathBuf>,
    pub(super) preopened_dirs: Vec<SandboxWasiPreopen>,
    pub(super) virtual_device_host_path: Option<PathBuf>,
    pub(super) cwd: SandboxPath,
    pub(super) env: BTreeMap<String, String>,
    pub(super) engine: wasmtime::Engine,
    pub(super) stdout: wasmtime_wasi::p2::pipe::MemoryOutputPipe,
    pub(super) stderr: wasmtime_wasi::p2::pipe::MemoryOutputPipe,
    pub(super) input_limit_bytes: usize,
    pub(super) child_input_limit_exceeded: Arc<AtomicBool>,
    pub(super) output_limit_bytes: usize,
    pub(super) child_output_limit_exceeded: Arc<AtomicBool>,
    pub(super) memory_limit_bytes: usize,
    pub(super) fuel: u64,
    pub(super) fuel_remaining: Arc<AtomicU64>,
    pub(super) child_depth: usize,
    pub(super) allow_pinned_subprocesses: bool,
}
