#![cfg_attr(
    not(all(
        feature = "cpu-profiler",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )),
    allow(dead_code)
)]

use crate::ObserveError;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

pub const OBSERVE_CPU_PROFILE_ARTIFACT_ROOT: &str = "observe/profiles";
pub const OBSERVE_CPU_PROFILE_DEFAULT_DURATION_SECONDS: u64 = 10;
pub const OBSERVE_CPU_PROFILE_MIN_DURATION_SECONDS: u64 = 1;
pub const OBSERVE_CPU_PROFILE_MAX_DURATION_SECONDS: u64 = 30;
pub const OBSERVE_CPU_PROFILE_DEFAULT_FREQUENCY_HZ: u32 = 250;
pub const OBSERVE_CPU_PROFILE_MIN_FREQUENCY_HZ: u32 = 10;
pub const OBSERVE_CPU_PROFILE_MAX_FREQUENCY_HZ: u32 = 500;
pub const OBSERVE_CPU_PROFILE_MAX_ARTIFACT_BYTES: usize = 32 * 1024 * 1024;

const CPU_PROFILE_FILE_NAME: &str = "cpu.pprof.pb";
const CPU_PROFILE_MANIFEST_FILE_NAME: &str = "manifest.json";
const CPU_PROFILE_EXCLUDED_LIBRARIES: &[&str] = &[
    "libc.so",
    "libgcc_s",
    "libpthread",
    "linux-vdso",
    "ld-linux",
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveCpuProfileAction {
    Start,
    Stop,
    #[default]
    Status,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveCpuProfileState {
    Unsupported,
    Idle,
    Starting,
    Running,
    Finalizing,
    Complete,
    Failed,
}

impl ObserveCpuProfileState {
    fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Finalizing)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObserveCpuProfileRequest {
    #[serde(default)]
    pub action: ObserveCpuProfileAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frequency_hz: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObserveCpuProfileResponse {
    pub supported: bool,
    pub state: ObserveCpuProfileState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frequency_hz: Option<u32>,
    #[serde(
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub finished_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_relative_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_relative_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ObserveCpuProfileResponse {
    fn initial() -> Self {
        if cpu_profiler_supported() {
            Self {
                supported: true,
                state: ObserveCpuProfileState::Idle,
                capture_id: None,
                duration_seconds: None,
                frequency_hz: None,
                started_at: None,
                finished_at: None,
                sample_count: None,
                artifact_bytes: None,
                artifact_relative_path: None,
                manifest_relative_path: None,
                error: None,
            }
        } else {
            Self {
                supported: false,
                state: ObserveCpuProfileState::Unsupported,
                capture_id: None,
                duration_seconds: None,
                frequency_hz: None,
                started_at: None,
                finished_at: None,
                sample_count: None,
                artifact_bytes: None,
                artifact_relative_path: None,
                manifest_relative_path: None,
                error: Some(cpu_profiler_unavailable_reason().to_string()),
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ObserveCpuProfileManifest {
    protocol: &'static str,
    backend: &'static str,
    process_id: u32,
    sampling_excluded_libraries: Vec<String>,
    response: ObserveCpuProfileResponse,
}

#[derive(Debug)]
struct CpuProfileInner {
    response: ObserveCpuProfileResponse,
    generation: u64,
    stop_requested: bool,
}

#[derive(Debug)]
struct CpuProfileManager {
    inner: Mutex<CpuProfileInner>,
    wake: Condvar,
    sequence: AtomicU64,
}

impl CpuProfileManager {
    fn new() -> Self {
        Self {
            inner: Mutex::new(CpuProfileInner {
                response: ObserveCpuProfileResponse::initial(),
                generation: 0,
                stop_requested: false,
            }),
            wake: Condvar::new(),
            sequence: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, CpuProfileInner>, ObserveError> {
        self.inner.lock().map_err(|_| ObserveError::Runtime {
            detail: "CPU profiler state lock is poisoned".to_string(),
        })
    }

    fn snapshot(&self) -> Result<ObserveCpuProfileResponse, ObserveError> {
        Ok(self.lock()?.response.clone())
    }

    fn next_capture_id(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!(
            "cpu-{}-{}-{sequence}",
            OffsetDateTime::now_utc().unix_timestamp_nanos(),
            std::process::id()
        )
    }
}

#[derive(Clone, Debug)]
struct CpuProfileConfig {
    capture_id: String,
    duration_seconds: u64,
    frequency_hz: u32,
}

static CPU_PROFILE_MANAGER: OnceLock<CpuProfileManager> = OnceLock::new();

fn cpu_profile_manager() -> &'static CpuProfileManager {
    CPU_PROFILE_MANAGER.get_or_init(CpuProfileManager::new)
}

/// Controls one bounded process-wide CPU capture.
///
/// Captures are intentionally asynchronous: `start` returns after the worker
/// is launched, while `status` reports finalization and artifact paths. The
/// worker itself is the watchdog and always stops no later than the requested
/// duration, even if the caller disconnects.
pub fn handle_observe_cpu_profile(
    data_dir_root: impl AsRef<Path>,
    request: ObserveCpuProfileRequest,
) -> Result<ObserveCpuProfileResponse, ObserveError> {
    validate_action_fields(&request)?;
    match request.action {
        ObserveCpuProfileAction::Status => cpu_profile_manager().snapshot(),
        ObserveCpuProfileAction::Stop => stop_cpu_profile(request.capture_id.as_deref()),
        ObserveCpuProfileAction::Start => start_cpu_profile(data_dir_root.as_ref(), request),
    }
}

pub fn observe_cpu_profile_snapshot() -> Result<ObserveCpuProfileResponse, ObserveError> {
    cpu_profile_manager().snapshot()
}

fn validate_action_fields(request: &ObserveCpuProfileRequest) -> Result<(), ObserveError> {
    if let Some(capture_id) = request.capture_id.as_deref() {
        validate_capture_id(capture_id)?;
    }
    if request.action != ObserveCpuProfileAction::Start
        && (request.duration_seconds.is_some() || request.frequency_hz.is_some())
    {
        return Err(ObserveError::Parse {
            detail: "CPU profile duration and frequency are only valid for start".to_string(),
        });
    }
    Ok(())
}

fn validate_capture_id(capture_id: &str) -> Result<(), ObserveError> {
    let valid = !capture_id.is_empty()
        && capture_id.len() <= 64
        && !capture_id.starts_with('.')
        && capture_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(ObserveError::Parse {
            detail: "CPU profile capture id must be 1-64 ASCII letters, digits, '.', '_', or '-', and cannot start with '.'"
                .to_string(),
        })
    }
}

fn start_cpu_profile(
    data_dir_root: &Path,
    request: ObserveCpuProfileRequest,
) -> Result<ObserveCpuProfileResponse, ObserveError> {
    let manager = cpu_profile_manager();
    let duration_seconds = request
        .duration_seconds
        .unwrap_or(OBSERVE_CPU_PROFILE_DEFAULT_DURATION_SECONDS);
    if !(OBSERVE_CPU_PROFILE_MIN_DURATION_SECONDS..=OBSERVE_CPU_PROFILE_MAX_DURATION_SECONDS)
        .contains(&duration_seconds)
    {
        return Err(ObserveError::Parse {
            detail: format!(
                "CPU profile duration must be {}-{} seconds",
                OBSERVE_CPU_PROFILE_MIN_DURATION_SECONDS, OBSERVE_CPU_PROFILE_MAX_DURATION_SECONDS
            ),
        });
    }
    let frequency_hz = request
        .frequency_hz
        .unwrap_or(OBSERVE_CPU_PROFILE_DEFAULT_FREQUENCY_HZ);
    if !(OBSERVE_CPU_PROFILE_MIN_FREQUENCY_HZ..=OBSERVE_CPU_PROFILE_MAX_FREQUENCY_HZ)
        .contains(&frequency_hz)
    {
        return Err(ObserveError::Parse {
            detail: format!(
                "CPU profile frequency must be {}-{} Hz",
                OBSERVE_CPU_PROFILE_MIN_FREQUENCY_HZ, OBSERVE_CPU_PROFILE_MAX_FREQUENCY_HZ
            ),
        });
    }
    let capture_id = request
        .capture_id
        .unwrap_or_else(|| manager.next_capture_id());
    validate_capture_id(&capture_id)?;
    let config = CpuProfileConfig {
        capture_id,
        duration_seconds,
        frequency_hz,
    };
    if !cpu_profiler_supported() {
        return manager.snapshot();
    }

    let generation = {
        let mut inner = manager.lock()?;
        if inner.response.state.is_active() {
            return Err(ObserveError::Runtime {
                detail: format!(
                    "CPU profile capture `{}` is already {}",
                    inner.response.capture_id.as_deref().unwrap_or("unknown"),
                    cpu_profile_state_name(inner.response.state)
                ),
            });
        }
        inner.generation = inner.generation.wrapping_add(1);
        inner.stop_requested = false;
        inner.response = ObserveCpuProfileResponse {
            supported: true,
            state: ObserveCpuProfileState::Starting,
            capture_id: Some(config.capture_id.clone()),
            duration_seconds: Some(duration_seconds),
            frequency_hz: Some(frequency_hz),
            started_at: None,
            finished_at: None,
            sample_count: None,
            artifact_bytes: None,
            artifact_relative_path: None,
            manifest_relative_path: None,
            error: None,
        };
        inner.generation
    };

    let data_dir_root = data_dir_root.to_path_buf();
    if let Err(source) = std::thread::Builder::new()
        .name("sinomo-cpu-profile".to_string())
        .stack_size(1024 * 1024)
        .spawn(move || run_cpu_profile_worker(data_dir_root, config, generation))
    {
        mark_profile_failed(
            generation,
            format!("failed to spawn profiler worker: {source}"),
        );
        return Err(ObserveError::Runtime {
            detail: format!("failed to spawn CPU profiler worker: {source}"),
        });
    }
    manager.snapshot()
}

fn stop_cpu_profile(
    expected_capture_id: Option<&str>,
) -> Result<ObserveCpuProfileResponse, ObserveError> {
    let manager = cpu_profile_manager();
    let mut inner = manager.lock()?;
    if let Some(expected_capture_id) = expected_capture_id
        && inner.response.capture_id.as_deref() != Some(expected_capture_id)
    {
        return Err(ObserveError::Runtime {
            detail: format!(
                "CPU profile capture mismatch: active is `{}`",
                inner.response.capture_id.as_deref().unwrap_or("none")
            ),
        });
    }
    if matches!(
        inner.response.state,
        ObserveCpuProfileState::Starting | ObserveCpuProfileState::Running
    ) {
        inner.stop_requested = true;
        manager.wake.notify_all();
    }
    Ok(inner.response.clone())
}

#[cfg(all(
    feature = "cpu-profiler",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn run_cpu_profile_worker(data_dir_root: PathBuf, config: CpuProfileConfig, generation: u64) {
    if let Err(error) = run_cpu_profile_capture(&data_dir_root, &config, generation) {
        mark_profile_failed(generation, error.to_string());
    }
}

#[cfg(not(all(
    feature = "cpu-profiler",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
fn run_cpu_profile_worker(_data_dir_root: PathBuf, _config: CpuProfileConfig, generation: u64) {
    mark_profile_failed(generation, cpu_profiler_unavailable_reason().to_string());
}

#[cfg(all(
    feature = "cpu-profiler",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn run_cpu_profile_capture(
    data_dir_root: &Path,
    config: &CpuProfileConfig,
    generation: u64,
) -> Result<(), ObserveError> {
    use pprof::protos::Message;

    let capture_dir = prepare_capture_directory(data_dir_root, &config.capture_id)?;
    // Sampling inside allocator/loader locks can deadlock the signal unwinder
    // and then every allocating daemon worker. Follow pprof's blocklist contract;
    // record this sampling boundary in the artifact rather than hiding it.
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(config.frequency_hz as i32)
        .blocklist(CPU_PROFILE_EXCLUDED_LIBRARIES)
        .build();
    let deadline = Instant::now() + Duration::from_secs(config.duration_seconds);
    // pprof warms up a resolved backtrace before starting its timer. Sampling
    // uses raw framehop addresses, so it need not retain that DWARF/image cache.
    backtrace::clear_symbol_cache();
    let guard = guard.map_err(|source| ObserveError::Runtime {
        detail: format!("failed to start CPU profiler: {source}"),
    })?;
    mark_profile_running(generation)?;
    wait_for_capture_expiry(generation, deadline)?;
    let unresolved = stop_cpu_profile_sampling(guard)?;
    mark_profile_finalizing(generation)?;

    let mut report = pprof::Report {
        data: Default::default(),
        timing: unresolved.timing,
    };
    for (frames, count) in unresolved.data {
        *report.data.entry(pprof::Frames::from(frames)).or_default() += count;
    }
    // Frames own their resolved names and locations. Release symbolization's
    // image mappings and DWARF data before encoding, including later failures.
    backtrace::clear_symbol_cache();
    let profile = report.pprof().map_err(|source| ObserveError::Runtime {
        detail: format!("failed to encode CPU profile report: {source}"),
    })?;

    let sample_count = profile
        .sample
        .iter()
        .filter_map(|sample| sample.value.first())
        .filter_map(|count| u64::try_from(*count).ok())
        .sum::<u64>();
    let artifact_bytes = profile.encoded_len();
    if artifact_bytes > OBSERVE_CPU_PROFILE_MAX_ARTIFACT_BYTES {
        return Err(ObserveError::Runtime {
            detail: format!(
                "CPU profile artifact is {artifact_bytes} bytes, above the {} byte limit",
                OBSERVE_CPU_PROFILE_MAX_ARTIFACT_BYTES
            ),
        });
    }
    let mut encoded = Vec::with_capacity(artifact_bytes);
    profile
        .encode(&mut encoded)
        .map_err(|source| ObserveError::Runtime {
            detail: format!("failed to serialize CPU profile report: {source}"),
        })?;

    let artifact_relative_path = profile_relative_path(&config.capture_id, CPU_PROFILE_FILE_NAME);
    let manifest_relative_path =
        profile_relative_path(&config.capture_id, CPU_PROFILE_MANIFEST_FILE_NAME);
    atomic_write(&capture_dir.join(CPU_PROFILE_FILE_NAME), &encoded)?;

    let finished_at = OffsetDateTime::now_utc();
    let response = {
        let manager = cpu_profile_manager();
        let inner = manager.lock()?;
        if inner.generation != generation {
            return Err(ObserveError::Runtime {
                detail: "CPU profile generation changed during finalization".to_string(),
            });
        }
        let mut response = inner.response.clone();
        response.state = ObserveCpuProfileState::Complete;
        response.finished_at = Some(finished_at);
        response.sample_count = Some(sample_count);
        response.artifact_bytes = Some(encoded.len() as u64);
        response.artifact_relative_path = Some(artifact_relative_path);
        response.manifest_relative_path = Some(manifest_relative_path);
        response
    };
    let manifest = ObserveCpuProfileManifest {
        protocol: "sinomo.observe.cpu-profile.v1",
        backend: "pprof-framehop",
        process_id: std::process::id(),
        sampling_excluded_libraries: CPU_PROFILE_EXCLUDED_LIBRARIES
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
        response: response.clone(),
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|source| ObserveError::Json {
            path: capture_dir
                .join(CPU_PROFILE_MANIFEST_FILE_NAME)
                .display()
                .to_string(),
            detail: source.to_string(),
        })?;
    atomic_write(
        &capture_dir.join(CPU_PROFILE_MANIFEST_FILE_NAME),
        &manifest_bytes,
    )?;

    let manager = cpu_profile_manager();
    let mut inner = manager.lock()?;
    if inner.generation == generation {
        inner.response = response;
        inner.stop_requested = false;
    }
    Ok(())
}

#[cfg(all(
    feature = "cpu-profiler",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn stop_cpu_profile_sampling(
    guard: pprof::ProfilerGuard<'_>,
) -> Result<pprof::UnresolvedReport, ObserveError> {
    // Dropping the guard clears pprof's collector. Detach the raw samples first,
    // then stop its signal timer before symbol resolution and serialization.
    let report = guard.report().build_unresolved();
    drop(guard);
    report.map_err(|source| ObserveError::Runtime {
        detail: format!("failed to build CPU profile report: {source}"),
    })
}

fn mark_profile_running(generation: u64) -> Result<(), ObserveError> {
    let manager = cpu_profile_manager();
    let mut inner = manager.lock()?;
    ensure_generation(&inner, generation)?;
    inner.response.state = ObserveCpuProfileState::Running;
    inner.response.started_at = Some(OffsetDateTime::now_utc());
    Ok(())
}

fn wait_for_capture_expiry(generation: u64, deadline: Instant) -> Result<(), ObserveError> {
    let manager = cpu_profile_manager();
    let mut inner = manager.lock()?;
    loop {
        ensure_generation(&inner, generation)?;
        if inner.stop_requested {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        let (next_inner, timeout) = manager
            .wake
            .wait_timeout(inner, deadline.saturating_duration_since(now))
            .map_err(|_| ObserveError::Runtime {
                detail: "CPU profiler watchdog lock is poisoned".to_string(),
            })?;
        inner = next_inner;
        if timeout.timed_out() {
            return Ok(());
        }
    }
}

fn mark_profile_finalizing(generation: u64) -> Result<(), ObserveError> {
    let manager = cpu_profile_manager();
    let mut inner = manager.lock()?;
    ensure_generation(&inner, generation)?;
    inner.response.state = ObserveCpuProfileState::Finalizing;
    Ok(())
}

fn mark_profile_failed(generation: u64, detail: String) {
    let manager = cpu_profile_manager();
    let Ok(mut inner) = manager.inner.lock() else {
        return;
    };
    if inner.generation == generation {
        inner.response.state = ObserveCpuProfileState::Failed;
        inner.response.finished_at = Some(OffsetDateTime::now_utc());
        inner.response.error = Some(detail);
        inner.stop_requested = false;
    }
}

fn ensure_generation(inner: &CpuProfileInner, generation: u64) -> Result<(), ObserveError> {
    if inner.generation == generation {
        Ok(())
    } else {
        Err(ObserveError::Runtime {
            detail: "CPU profile generation changed".to_string(),
        })
    }
}

fn prepare_capture_directory(
    data_dir_root: &Path,
    capture_id: &str,
) -> Result<PathBuf, ObserveError> {
    let artifact_root = data_dir_root.join(OBSERVE_CPU_PROFILE_ARTIFACT_ROOT);
    fs::create_dir_all(&artifact_root).map_err(|source| io_error(&artifact_root, source))?;
    let capture_dir = artifact_root.join(capture_id);
    fs::create_dir(&capture_dir).map_err(|source| io_error(&capture_dir, source))?;
    Ok(capture_dir)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ObserveError> {
    let temporary_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("artifact")
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .map_err(|source| io_error(&temporary_path, source))?;
        file.write_all(bytes)
            .map_err(|source| io_error(&temporary_path, source))?;
        file.sync_all()
            .map_err(|source| io_error(&temporary_path, source))?;
        fs::rename(&temporary_path, path).map_err(|source| io_error(path, source))?;
        File::open(path.parent().unwrap_or_else(|| Path::new(".")))
            .and_then(|directory| directory.sync_all())
            .map_err(|source| io_error(path, source))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn profile_relative_path(capture_id: &str, file_name: &str) -> String {
    format!("{OBSERVE_CPU_PROFILE_ARTIFACT_ROOT}/{capture_id}/{file_name}")
}

fn io_error(path: &Path, source: std::io::Error) -> ObserveError {
    ObserveError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

fn cpu_profile_state_name(state: ObserveCpuProfileState) -> &'static str {
    match state {
        ObserveCpuProfileState::Unsupported => "unsupported",
        ObserveCpuProfileState::Idle => "idle",
        ObserveCpuProfileState::Starting => "starting",
        ObserveCpuProfileState::Running => "running",
        ObserveCpuProfileState::Finalizing => "finalizing",
        ObserveCpuProfileState::Complete => "complete",
        ObserveCpuProfileState::Failed => "failed",
    }
}

const fn cpu_profiler_supported() -> bool {
    cfg!(all(
        feature = "cpu-profiler",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))
}

const fn cpu_profiler_unavailable_reason() -> &'static str {
    if cfg!(not(feature = "cpu-profiler")) {
        "CPU profiling is disabled; rebuild sinomo-observe with the `cpu-profiler` feature"
    } else {
        "CPU profiling is supported only on Linux x86_64 and aarch64"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(
        feature = "cpu-profiler",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[test]
    fn cpu_profile_allocation_stress_finishes_in_a_bounded_child() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        if std::env::var_os("SINOMO_TEST_CPU_PROFILE_CHILD").is_none() {
            for _ in 0..2 {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "profile::tests::cpu_profile_allocation_stress_finishes_in_a_bounded_child",
                        "--nocapture",
                    ])
                    .env("SINOMO_TEST_CPU_PROFILE_CHILD", "1")
                    .spawn()
                    .expect("profile test child");
                let deadline = Instant::now() + Duration::from_secs(15);
                loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        assert!(status.success(), "CPU profile child failed");
                        break;
                    }
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("CPU profiling blocked allocation or finalization");
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            return;
        }
        let root = sinomo_testdir::TestDir::new("observe-profile-allocation");
        let running = Arc::new(AtomicBool::new(true));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let running = Arc::clone(&running);
                std::thread::spawn(move || {
                    while running.load(Ordering::Relaxed) {
                        let bytes = vec![7u8; 16384];
                        std::hint::black_box(bytes.iter().map(|v| u64::from(*v)).sum::<u64>());
                    }
                })
            })
            .collect();
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(99)
            .blocklist(CPU_PROFILE_EXCLUDED_LIBRARIES)
            .build()
            .expect("start detached CPU profile");
        std::thread::sleep(Duration::from_millis(100));
        let detached = stop_cpu_profile_sampling(guard).expect("detach CPU samples");
        let samples = detached.data.values().sum::<isize>();
        assert!(samples > 0, "busy workers must produce CPU samples");
        let next_guard = pprof::ProfilerGuardBuilder::default()
            .blocklist(CPU_PROFILE_EXCLUDED_LIBRARIES)
            .build()
            .expect("sampling stopped before the detached report is resolved");
        drop(next_guard);
        assert_eq!(detached.data.values().sum::<isize>(), samples);
        drop(detached);
        handle_observe_cpu_profile(
            &root,
            ObserveCpuProfileRequest {
                action: ObserveCpuProfileAction::Start,
                capture_id: Some("allocation-stress".into()),
                duration_seconds: Some(1),
                frequency_hz: Some(99),
            },
        )
        .expect("start CPU profile");
        loop {
            let response =
                handle_observe_cpu_profile(&root, ObserveCpuProfileRequest::default()).unwrap();
            if response.state == ObserveCpuProfileState::Complete {
                break;
            }
            assert_ne!(
                response.state,
                ObserveCpuProfileState::Failed,
                "{:?}",
                response.error
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        running.store(false, Ordering::Relaxed);
        for worker in workers {
            worker.join().unwrap();
        }
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(
                root.join(OBSERVE_CPU_PROFILE_ARTIFACT_ROOT)
                    .join("allocation-stress")
                    .join(CPU_PROFILE_MANIFEST_FILE_NAME),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            manifest["sampling_excluded_libraries"],
            serde_json::json!(CPU_PROFILE_EXCLUDED_LIBRARIES)
        );
        use pprof::protos::Message;
        let bytes = fs::read(
            root.join(OBSERVE_CPU_PROFILE_ARTIFACT_ROOT)
                .join("allocation-stress")
                .join(CPU_PROFILE_FILE_NAME),
        )
        .unwrap();
        let profile = pprof::protos::Profile::decode(bytes.as_slice()).unwrap();
        assert!(
            profile
                .sample
                .iter()
                .any(|sample| { sample.value.first().is_some_and(|count| *count > 0) })
        );
        assert!(
            profile.function.iter().any(|function| {
                usize::try_from(function.name)
                    .ok()
                    .and_then(|index| profile.string_table.get(index))
                    .is_some_and(|name| name.contains("cpu_profile_allocation_stress"))
            }),
            "profile must retain resolved application symbols after releasing the cache"
        );
    }

    #[test]
    fn capture_ids_and_bounds_are_validated_before_start() {
        let root = sinomo_testdir::TestDir::new("observe-profile-validation");
        let invalid_id = handle_observe_cpu_profile(
            &root,
            ObserveCpuProfileRequest {
                action: ObserveCpuProfileAction::Start,
                capture_id: Some("../escape".to_string()),
                ..ObserveCpuProfileRequest::default()
            },
        )
        .expect_err("capture path traversal must fail");
        assert!(matches!(invalid_id, ObserveError::Parse { .. }));

        let invalid_duration = handle_observe_cpu_profile(
            &root,
            ObserveCpuProfileRequest {
                action: ObserveCpuProfileAction::Start,
                duration_seconds: Some(OBSERVE_CPU_PROFILE_MAX_DURATION_SECONDS + 1),
                ..ObserveCpuProfileRequest::default()
            },
        )
        .expect_err("unbounded duration must fail");
        assert!(matches!(invalid_duration, ObserveError::Parse { .. }));

        let invalid_frequency = handle_observe_cpu_profile(
            &root,
            ObserveCpuProfileRequest {
                action: ObserveCpuProfileAction::Start,
                frequency_hz: Some(OBSERVE_CPU_PROFILE_MAX_FREQUENCY_HZ + 1),
                ..ObserveCpuProfileRequest::default()
            },
        )
        .expect_err("unbounded frequency must fail");
        assert!(matches!(invalid_frequency, ObserveError::Parse { .. }));
    }

    #[test]
    fn status_fields_reject_capture_configuration() {
        let root = sinomo_testdir::TestDir::new("observe-profile-status-fields");
        let error = handle_observe_cpu_profile(
            &root,
            ObserveCpuProfileRequest {
                action: ObserveCpuProfileAction::Status,
                duration_seconds: Some(1),
                ..ObserveCpuProfileRequest::default()
            },
        )
        .expect_err("status duration must fail");
        assert!(matches!(error, ObserveError::Parse { .. }));
    }

    #[cfg(not(all(
        feature = "cpu-profiler",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )))]
    #[test]
    fn disabled_build_reports_unsupported_without_creating_artifacts() {
        let root = sinomo_testdir::TestDir::new("observe-profile-disabled");
        let response = handle_observe_cpu_profile(
            &root,
            ObserveCpuProfileRequest {
                action: ObserveCpuProfileAction::Start,
                duration_seconds: Some(1),
                frequency_hz: Some(10),
                ..ObserveCpuProfileRequest::default()
            },
        )
        .expect("unsupported response");
        assert!(!response.supported);
        assert_eq!(response.state, ObserveCpuProfileState::Unsupported);
        assert!(!root.join(OBSERVE_CPU_PROFILE_ARTIFACT_ROOT).exists());
    }
}
