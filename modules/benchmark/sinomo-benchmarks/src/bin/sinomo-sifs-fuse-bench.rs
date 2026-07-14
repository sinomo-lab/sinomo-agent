use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_ITERATIONS: usize = 32;
const DEFAULT_LARGE_FILE_MIB: usize = 8;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_CACHE_TTL_MS: u64 = 0;
const MAX_ITERATIONS: usize = 1024;
const MAX_LARGE_FILE_MIB: usize = 64;
const MAX_CONCURRENCY: usize = 64;

fn main() -> ExitCode {
    let config = match Config::parse(env::args().skip(1)) {
        Ok(ParseResult::Help) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(ParseResult::Run(config)) => config,
        Err(detail) => {
            eprintln!("sinomo-sifs-fuse-bench: {detail}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    let mut report = BenchmarkReport::new(config.clone());
    run_platform(&config, &mut report);
    report.finish();

    let json = match serde_json::to_string_pretty(&report) {
        Ok(json) => json,
        Err(source) => {
            eprintln!("sinomo-sifs-fuse-bench: failed to encode report: {source}");
            return ExitCode::from(2);
        }
    };
    if let Some(path) = &config.json_out {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            && let Err(source) = std::fs::create_dir_all(parent)
        {
            eprintln!(
                "sinomo-sifs-fuse-bench: failed to create report directory {}: {source}",
                parent.display()
            );
            return ExitCode::from(2);
        }
        if let Err(source) = std::fs::write(path, format!("{json}\n")) {
            eprintln!(
                "sinomo-sifs-fuse-bench: failed to write report {}: {source}",
                path.display()
            );
            return ExitCode::from(2);
        }
    }
    println!("{json}");
    eprintln!(
        "sinomo-sifs-fuse-bench: {} ({} checks, {} measurements)",
        report.status.label(),
        report.checks.len(),
        report.measurements.len()
    );
    ExitCode::from(report.exit_code())
}

const USAGE: &str = r#"Usage:
  sinomo-sifs-fuse-bench --root SIFS_PATH [options]

Required:
  --root PATH                    Existing caller-writable SIFS subtree.
                                 Defaults to SINOMO_SIFS_FUSE_BENCH_ROOT.

Selection and output:
  --suite full|correctness|performance
                                 Workload selection (default: full).
  --data-dir PATH                Sinomo data directory (default: SINOMO_DATA_DIR).
  --json-out PATH                Also write the JSON report to PATH.
  --report-only                  Return zero for product/threshold failures.
  --require-fuse                 Treat unavailable FUSE support as exit code 3.
  --keep-fixtures                Preserve the unique SIFS run directory for debugging.

Tuning:
  --iterations N                 Metadata and random-I/O samples (default: 32, max: 1024).
  --large-file-mib N             Large-file and sequential-I/O size (default: 8).
                                 Maximum: 64 MiB.
  --concurrency N                Parallel create workers (default: 4, max: 64).
  --cache-ttl-ms N               FUSE attr/entry cache TTL (default: 0).

Optional performance gates (zero disables the gate):
  --max-create-p95-ms N
  --min-sequential-write-mib-s N  Open-file data throughput (create excluded).
  --min-sequential-read-mib-s N
  --min-parallel-create-speedup N
  --min-random-iops N

Exit codes:
  0 pass, ordinary capability skip, or --report-only
  1 correctness, threshold, or cleanup failure
  2 invalid arguments or harness/report error
  3 required FUSE capability unavailable
"#;

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum Suite {
    Full,
    Correctness,
    Performance,
}

impl Suite {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "full" => Ok(Self::Full),
            "correctness" => Ok(Self::Correctness),
            "performance" => Ok(Self::Performance),
            _ => Err(format!(
                "invalid --suite value {raw:?}; expected full, correctness, or performance"
            )),
        }
    }

    fn runs_correctness(self) -> bool {
        matches!(self, Self::Full | Self::Correctness)
    }

    fn runs_performance(self) -> bool {
        matches!(self, Self::Full | Self::Performance)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
struct Config {
    root: String,
    data_dir: Option<PathBuf>,
    suite: Suite,
    json_out: Option<PathBuf>,
    iterations: usize,
    large_file_mib: usize,
    concurrency: usize,
    cache_ttl_ms: u64,
    max_create_p95_ms: f64,
    min_sequential_write_mib_s: f64,
    min_sequential_read_mib_s: f64,
    min_parallel_create_speedup: f64,
    min_random_iops: f64,
    report_only: bool,
    require_fuse: bool,
    keep_fixtures: bool,
}

#[derive(Debug)]
enum ParseResult {
    Help,
    Run(Config),
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<ParseResult, String> {
        Self::parse_with_environment(
            args,
            env::var("SINOMO_SIFS_FUSE_BENCH_ROOT").ok(),
            env::var_os("SINOMO_DATA_DIR").map(PathBuf::from),
        )
    }

    fn parse_with_environment(
        args: impl IntoIterator<Item = String>,
        environment_root: Option<String>,
        environment_data_dir: Option<PathBuf>,
    ) -> Result<ParseResult, String> {
        let mut root = environment_root;
        let mut data_dir = environment_data_dir;
        let mut suite = Suite::Full;
        let mut json_out = None;
        let mut iterations = DEFAULT_ITERATIONS;
        let mut large_file_mib = DEFAULT_LARGE_FILE_MIB;
        let mut concurrency = DEFAULT_CONCURRENCY;
        let mut cache_ttl_ms = DEFAULT_CACHE_TTL_MS;
        let mut max_create_p95_ms = 0.0;
        let mut min_sequential_write_mib_s = 0.0;
        let mut min_sequential_read_mib_s = 0.0;
        let mut min_parallel_create_speedup = 0.0;
        let mut min_random_iops = 0.0;
        let mut report_only = false;
        let mut require_fuse = false;
        let mut keep_fixtures = false;

        let args = args.into_iter().collect::<Vec<_>>();
        let mut index = 0;
        while index < args.len() {
            let flag = &args[index];
            let mut value = || -> Result<&str, String> {
                index += 1;
                args.get(index)
                    .map(String::as_str)
                    .ok_or_else(|| format!("{flag} requires a value"))
            };
            match flag.as_str() {
                "-h" | "--help" => return Ok(ParseResult::Help),
                "--root" => root = Some(value()?.to_string()),
                "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
                "--suite" => suite = Suite::parse(value()?)?,
                "--json-out" => json_out = Some(PathBuf::from(value()?)),
                "--iterations" => iterations = parse_bounded_usize(flag, value()?, MAX_ITERATIONS)?,
                "--large-file-mib" => {
                    large_file_mib = parse_bounded_usize(flag, value()?, MAX_LARGE_FILE_MIB)?
                }
                "--concurrency" => {
                    concurrency = parse_bounded_usize(flag, value()?, MAX_CONCURRENCY)?
                }
                "--cache-ttl-ms" => cache_ttl_ms = parse_u64(flag, value()?)?,
                "--max-create-p95-ms" => max_create_p95_ms = parse_nonnegative_f64(flag, value()?)?,
                "--min-sequential-write-mib-s" => {
                    min_sequential_write_mib_s = parse_nonnegative_f64(flag, value()?)?
                }
                "--min-sequential-read-mib-s" => {
                    min_sequential_read_mib_s = parse_nonnegative_f64(flag, value()?)?
                }
                "--min-parallel-create-speedup" => {
                    min_parallel_create_speedup = parse_nonnegative_f64(flag, value()?)?
                }
                "--min-random-iops" => min_random_iops = parse_nonnegative_f64(flag, value()?)?,
                "--report-only" => report_only = true,
                "--require-fuse" => require_fuse = true,
                "--keep-fixtures" => keep_fixtures = true,
                _ => return Err(format!("unknown argument {flag:?}")),
            }
            index += 1;
        }

        let root = root
            .filter(|root| !root.trim().is_empty())
            .ok_or_else(|| "--root is required (or set SINOMO_SIFS_FUSE_BENCH_ROOT)".to_string())?;
        validate_sifs_root(&root)?;
        Ok(ParseResult::Run(Self {
            root,
            data_dir,
            suite,
            json_out,
            iterations,
            large_file_mib,
            concurrency,
            cache_ttl_ms,
            max_create_p95_ms,
            min_sequential_write_mib_s,
            min_sequential_read_mib_s,
            min_parallel_create_speedup,
            min_random_iops,
            report_only,
            require_fuse,
            keep_fixtures,
        }))
    }

    fn large_file_bytes(&self) -> usize {
        self.large_file_mib * 1024 * 1024
    }
}

fn parse_bounded_usize(flag: &str, raw: &str, maximum: usize) -> Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|source| format!("invalid {flag} value {raw:?}: {source}"))?;
    if value == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    if value > maximum {
        return Err(format!("{flag} must not exceed {maximum}"));
    }
    Ok(value)
}

fn parse_u64(flag: &str, raw: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|source| format!("invalid {flag} value {raw:?}: {source}"))
}

fn parse_nonnegative_f64(flag: &str, raw: &str) -> Result<f64, String> {
    let value = raw
        .parse::<f64>()
        .map_err(|source| format!("invalid {flag} value {raw:?}: {source}"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("{flag} must be a finite non-negative number"));
    }
    Ok(value)
}

fn validate_sifs_root(root: &str) -> Result<(), String> {
    if !root.starts_with('/') {
        return Err("--root must be an absolute SIFS path".to_string());
    }
    if root
        .split('/')
        .any(|component| matches!(component, "." | ".."))
    {
        return Err("--root must not contain . or .. path components".to_string());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum ReportStatus {
    Running,
    Passed,
    Failed,
    Skipped,
    Error,
}

impl ReportStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum CheckStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Clone, Debug, Serialize)]
struct CheckResult {
    name: String,
    status: CheckStatus,
    elapsed_us: u64,
    detail: String,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkEnvironment {
    package_version: String,
    git_commit: Option<String>,
    git_dirty: Option<bool>,
    os: String,
    arch: String,
    process_id: u32,
    resolved_data_dir: Option<PathBuf>,
    cluster_id: Option<String>,
    node_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkReport {
    schema_version: u32,
    status: ReportStatus,
    started_at_unix_ms: u64,
    duration_ms: u64,
    config: Config,
    environment: BenchmarkEnvironment,
    run_sifs_paths: Vec<String>,
    checks: Vec<CheckResult>,
    measurements: BTreeMap<String, serde_json::Value>,
    required_capability_missing: bool,
    harness_errors: Vec<String>,
    #[serde(skip)]
    started: std::time::Instant,
}

impl BenchmarkReport {
    fn new(config: Config) -> Self {
        let resolved_data_dir = resolved_data_dir(&config);
        let (cluster_id, node_id) = resolved_data_dir
            .as_deref()
            .and_then(cluster_identity)
            .map(|(cluster_id, node_id)| (Some(cluster_id), Some(node_id)))
            .unwrap_or_default();
        Self {
            schema_version: 1,
            status: ReportStatus::Running,
            started_at_unix_ms: unix_millis(),
            duration_ms: 0,
            config,
            environment: BenchmarkEnvironment {
                package_version: env!("CARGO_PKG_VERSION").to_string(),
                git_commit: option_env!("SINOMO_GIT_COMMIT").map(str::to_string),
                git_dirty: option_env!("SINOMO_GIT_DIRTY").and_then(|value| value.parse().ok()),
                os: env::consts::OS.to_string(),
                arch: env::consts::ARCH.to_string(),
                process_id: std::process::id(),
                resolved_data_dir,
                cluster_id,
                node_id,
            },
            run_sifs_paths: Vec::new(),
            checks: Vec::new(),
            measurements: BTreeMap::new(),
            required_capability_missing: false,
            harness_errors: Vec::new(),
            started: std::time::Instant::now(),
        }
    }

    fn check(
        &mut self,
        name: impl Into<String>,
        status: CheckStatus,
        elapsed: Duration,
        detail: impl Into<String>,
    ) {
        self.checks.push(CheckResult {
            name: name.into(),
            status,
            elapsed_us: duration_us(elapsed),
            detail: detail.into(),
        });
    }

    fn pass(&mut self, name: impl Into<String>, elapsed: Duration, detail: impl Into<String>) {
        self.check(name, CheckStatus::Passed, elapsed, detail);
    }

    fn fail(&mut self, name: impl Into<String>, elapsed: Duration, detail: impl Into<String>) {
        self.check(name, CheckStatus::Failed, elapsed, detail);
    }

    fn skip(&mut self, name: impl Into<String>, elapsed: Duration, detail: impl Into<String>) {
        self.check(name, CheckStatus::Skipped, elapsed, detail);
    }

    fn metric(&mut self, name: impl Into<String>, value: impl Serialize) {
        let name = name.into();
        match serde_json::to_value(value) {
            Ok(value) => {
                self.measurements.insert(name, value);
            }
            Err(source) => self.harness_errors.push(format!(
                "failed to serialize measurement {name:?}: {source}"
            )),
        }
    }

    fn finish(&mut self) {
        self.duration_ms = self
            .started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        self.status = if !self.harness_errors.is_empty() {
            ReportStatus::Error
        } else if self
            .checks
            .iter()
            .any(|check| check.status == CheckStatus::Failed)
        {
            ReportStatus::Failed
        } else if self
            .checks
            .iter()
            .any(|check| check.status == CheckStatus::Passed)
        {
            ReportStatus::Passed
        } else {
            ReportStatus::Skipped
        };
    }

    fn exit_code(&self) -> u8 {
        if self.required_capability_missing {
            return 3;
        }
        match self.status {
            ReportStatus::Passed => 0,
            ReportStatus::Failed if self.config.report_only => 0,
            ReportStatus::Failed => 1,
            ReportStatus::Skipped => 0,
            ReportStatus::Error | ReportStatus::Running => 2,
        }
    }
}

fn resolved_data_dir(config: &Config) -> Option<PathBuf> {
    config.data_dir.clone().or_else(|| {
        sinomo_runtime::local::SinomoDataDir::from_env_or_home()
            .ok()
            .map(|data_dir| data_dir.root())
    })
}

fn cluster_identity(data_dir: &std::path::Path) -> Option<(String, String)> {
    let data_dir = sinomo_runtime::local::SinomoDataDir::new(data_dir);
    let paths = sinomo_runtime::network::ClusterPaths::new(&data_dir);
    let state = sinomo_runtime::cluster::read_cluster_state_without_kv(&paths).ok()?;
    Some((
        state.cluster_id.to_string(),
        state.local_node_id.to_string(),
    ))
}

#[derive(Clone, Debug, Serialize, PartialEq)]
struct LatencyMetrics {
    count: usize,
    elapsed_us: u64,
    min_us: u64,
    mean_us: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    ops_per_second: f64,
}

impl LatencyMetrics {
    fn from_samples(samples: &[Duration], elapsed: Duration) -> Self {
        let mut micros = samples
            .iter()
            .map(|sample| duration_us(*sample))
            .collect::<Vec<_>>();
        micros.sort_unstable();
        let sum = micros.iter().copied().map(u128::from).sum::<u128>();
        let mean_us = if micros.is_empty() {
            0
        } else {
            (sum / micros.len() as u128).try_into().unwrap_or(u64::MAX)
        };
        Self {
            count: micros.len(),
            elapsed_us: duration_us(elapsed),
            min_us: micros.first().copied().unwrap_or(0),
            mean_us,
            p50_us: percentile(&micros, 50),
            p95_us: percentile(&micros, 95),
            p99_us: percentile(&micros, 99),
            max_us: micros.last().copied().unwrap_or(0),
            ops_per_second: rate_per_second(micros.len(), elapsed),
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
struct ThroughputMetrics {
    bytes: usize,
    elapsed_us: u64,
    mib_per_second: f64,
}

impl ThroughputMetrics {
    fn new(bytes: usize, elapsed: Duration) -> Self {
        let seconds = elapsed.as_secs_f64();
        Self {
            bytes,
            elapsed_us: duration_us(elapsed),
            mib_per_second: if seconds > 0.0 {
                bytes as f64 / (1024.0 * 1024.0) / seconds
            } else {
                0.0
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
struct ScalingMetrics {
    operations: usize,
    workers: usize,
    sequential_elapsed_us: u64,
    parallel_elapsed_us: u64,
    sequential_ops_per_second: f64,
    parallel_ops_per_second: f64,
    speedup: f64,
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn rate_per_second(count: usize, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds > 0.0 {
        count as f64 / seconds
    } else {
        0.0
    }
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(not(unix))]
fn run_platform(config: &Config, report: &mut BenchmarkReport) {
    report.required_capability_missing = config.require_fuse;
    report.skip(
        "environment.fuse",
        Duration::ZERO,
        "SIFS FUSE benchmarks require a Unix build with kernel FUSE support",
    );
}

#[cfg(unix)]
fn run_platform(config: &Config, report: &mut BenchmarkReport) {
    unix_runner::run(config, report);
}

#[cfg(unix)]
mod unix_runner {
    use super::*;
    use sinomo_dfs_fuse::{
        DfsFuseError, DfsFuseMountHandle, DfsFuseMountOptions, mount_sifs_background,
    };
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Instant;

    const SMALL_BYTES: usize = 4096;
    const ONE_MIB: usize = 1024 * 1024;
    const LARGE_WRITE_CHUNK: usize = ONE_MIB;
    const RANDOM_FILE_BYTES: usize = 64 * 1024;
    const MOUNT_CLEANUP_ATTEMPTS: usize = 100;
    const MOUNT_CLEANUP_POLL: Duration = Duration::from_millis(20);
    static NEXT_NONCE: AtomicU64 = AtomicU64::new(1);

    pub(super) fn run(config: &Config, report: &mut BenchmarkReport) {
        if config.suite.runs_correctness() {
            eprintln!("sinomo-sifs-fuse-bench: running correctness suite");
            run_correctness(config, report);
        }
        if config.suite.runs_performance() {
            eprintln!("sinomo-sifs-fuse-bench: running performance suite");
            run_performance(config, report);
        }
    }

    struct FuseMount {
        handle: Option<DfsFuseMountHandle>,
        mount_point: PathBuf,
    }

    impl FuseMount {
        fn root(&self) -> &Path {
            self.handle.as_ref().expect("active mount").mount_root()
        }

        fn close(mut self) -> Result<(), String> {
            drop(self.handle.take());
            remove_mount_point(&self.mount_point)
        }
    }

    impl Drop for FuseMount {
        fn drop(&mut self) {
            drop(self.handle.take());
            let _ = remove_mount_point(&self.mount_point);
        }
    }

    fn remove_mount_point(path: &Path) -> Result<(), String> {
        let mut last_error = None;
        for _ in 0..MOUNT_CLEANUP_ATTEMPTS {
            match fs::remove_dir(path) {
                Ok(()) => return Ok(()),
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(source) => last_error = Some(source),
            }
            thread::sleep(MOUNT_CLEANUP_POLL);
        }
        Err(format!(
            "failed to remove host mount point {} after unmount: {}",
            path.display(),
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown error".to_string())
        ))
    }

    fn open_mount(
        config: &Config,
        report: &mut BenchmarkReport,
        check_name: &str,
        read_only: bool,
        initial: bool,
    ) -> Option<FuseMount> {
        let mount_point = unique_host_path("sinomo-sifs-fuse-bench-mount");
        let mut options =
            DfsFuseMountOptions::new(&mount_point).with_root_path(config.root.clone());
        if let Some(data_dir) = resolved_data_dir(config) {
            options = options.with_data_dir(data_dir);
        }
        options.read_only = read_only;
        options.foreground = false;
        options.mount_name = format!("sinomo-sifs-bench-{}", std::process::id());
        options.attr_ttl = Duration::from_millis(config.cache_ttl_ms);
        options.entry_ttl = Duration::from_millis(config.cache_ttl_ms);

        let started = Instant::now();
        match mount_sifs_background(options) {
            Ok(handle) => {
                let elapsed = started.elapsed();
                if !handle.is_active() {
                    report.fail(
                        check_name,
                        elapsed,
                        "mount returned an inactive background session",
                    );
                    drop(handle);
                    let _ = remove_mount_point(&mount_point);
                    return None;
                }
                report.pass(
                    check_name,
                    elapsed,
                    format!(
                        "mounted {} at {}",
                        config.root,
                        handle.mount_root().display()
                    ),
                );
                record_mount_startup(report, elapsed);
                Some(FuseMount {
                    handle: Some(handle),
                    mount_point,
                })
            }
            Err(error) => {
                let elapsed = started.elapsed();
                let capability = fuse_capability_error(&error);
                if initial && capability && !config.require_fuse {
                    report.skip(check_name, elapsed, error.to_string());
                } else {
                    report.fail(check_name, elapsed, error.to_string());
                }
                if capability && config.require_fuse {
                    report.required_capability_missing = true;
                }
                if let Err(detail) = remove_mount_point(&mount_point) {
                    report.fail(format!("{check_name}.host-cleanup"), Duration::ZERO, detail);
                }
                None
            }
        }
    }

    fn record_mount_startup(report: &mut BenchmarkReport, elapsed: Duration) {
        let samples_us = {
            let entry = report
                .measurements
                .entry("fuse.mount_startup_samples_us".to_string())
                .or_insert_with(|| serde_json::Value::Array(Vec::new()));
            if let Some(samples) = entry.as_array_mut() {
                samples.push(serde_json::Value::from(duration_us(elapsed)));
                samples
                    .iter()
                    .filter_map(serde_json::Value::as_u64)
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        if !samples_us.is_empty() {
            let samples = samples_us
                .iter()
                .copied()
                .map(Duration::from_micros)
                .collect::<Vec<_>>();
            let total = samples
                .iter()
                .copied()
                .fold(Duration::ZERO, Duration::saturating_add);
            report.metric(
                "fuse.mount_startup",
                LatencyMetrics::from_samples(&samples, total),
            );
        }
    }

    fn fuse_capability_error(error: &DfsFuseError) -> bool {
        match error {
            DfsFuseError::Unsupported => true,
            DfsFuseError::Mount { detail, .. } => {
                detail.contains("/dev/fuse")
                    || detail.contains("fusermount")
                    || detail.contains("Operation not permitted")
                    || detail.contains("Permission denied")
            }
            _ => false,
        }
    }

    fn unique_host_path(prefix: &str) -> PathBuf {
        let nonce = NEXT_NONCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        env::temp_dir().join(format!("{prefix}-{}-{nanos}-{nonce}", std::process::id()))
    }

    fn workspace_name(suite: &str) -> String {
        let nonce = NEXT_NONCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("fuse-bench-{suite}-{}-{nanos}-{nonce}", std::process::id())
    }

    pub(super) fn run_sifs_path(root: &str, child: &str) -> String {
        if root == "/" {
            format!("/{child}")
        } else {
            format!("{}/{child}", root.trim_end_matches('/'))
        }
    }

    fn close_mount(report: &mut BenchmarkReport, name: &str, mount: FuseMount) {
        let started = Instant::now();
        match mount.close() {
            Ok(()) => report.pass(
                name,
                started.elapsed(),
                "unmounted and removed host mount point",
            ),
            Err(detail) => report.fail(name, started.elapsed(), detail),
        }
    }

    fn run_check(
        report: &mut BenchmarkReport,
        name: &str,
        operation: impl FnOnce() -> Result<String, String>,
    ) {
        let started = Instant::now();
        match operation() {
            Ok(detail) => report.pass(name, started.elapsed(), detail),
            Err(detail) => report.fail(name, started.elapsed(), detail),
        }
    }

    fn run_correctness(config: &Config, report: &mut BenchmarkReport) {
        let Some(mount) = open_mount(config, report, "correctness.mount.initial", false, true)
        else {
            return;
        };
        let name = workspace_name("correctness");
        report
            .run_sifs_paths
            .push(run_sifs_path(&config.root, &name));
        let workspace = mount.root().join(&name);
        let create_started = Instant::now();
        if let Err(source) = fs::create_dir(&workspace) {
            report.fail(
                "correctness.workspace.create",
                create_started.elapsed(),
                io_error("create benchmark workspace", &workspace, source),
            );
            close_mount(report, "correctness.mount.initial.unmount", mount);
            return;
        }
        report.pass(
            "correctness.workspace.create",
            create_started.elapsed(),
            format!("created {}", run_sifs_path(&config.root, &name)),
        );

        basic_posix_checks(report, &workspace);
        link_checks(report, &workspace);
        sync_checks(report, &workspace);
        lock_checks(report, &workspace);
        persistence_writes(config, report, &workspace);
        persistence_reads(config, report, &workspace, "immediate");

        close_mount(report, "correctness.mount.first.unmount", mount);

        if let Some(remount) = open_mount(
            config,
            report,
            "correctness.mount.post-remount",
            false,
            false,
        ) {
            let remounted_workspace = remount.root().join(&name);
            persistence_reads(config, report, &remounted_workspace, "post-remount");
            close_mount(report, "correctness.mount.post-remount.unmount", remount);
        }

        if let Some(read_only_mount) =
            open_mount(config, report, "correctness.mount.read-only", true, false)
        {
            let read_only_workspace = read_only_mount.root().join(&name);
            read_only_checks(report, &read_only_workspace);
            close_mount(
                report,
                "correctness.mount.read-only.unmount",
                read_only_mount,
            );
        }

        cleanup_sifs_workspace(config, report, &name, "correctness");
    }

    fn basic_posix_checks(report: &mut BenchmarkReport, workspace: &Path) {
        let roundtrip = workspace.join("roundtrip.bin");
        let expected = payload(SMALL_BYTES, 11);
        run_check(report, "correctness.posix.create-write-read", || {
            fs::write(&roundtrip, &expected)
                .map_err(|source| io_error("write", &roundtrip, source))?;
            validate_file(&roundtrip, expected.len(), 11)
        });
        run_check(report, "correctness.posix.stat", || {
            let metadata =
                fs::metadata(&roundtrip).map_err(|source| io_error("stat", &roundtrip, source))?;
            if metadata.len() != SMALL_BYTES as u64 {
                return Err(format!(
                    "{} reported {} bytes, expected {SMALL_BYTES}",
                    roundtrip.display(),
                    metadata.len()
                ));
            }
            Ok(format!(
                "size={} mode={:o}",
                metadata.len(),
                metadata.mode() & 0o777
            ))
        });

        let truncate = workspace.join("truncate.bin");
        run_check(report, "correctness.posix.truncate", || {
            fs::write(&truncate, b"abcdefgh")
                .map_err(|source| io_error("write", &truncate, source))?;
            OpenOptions::new()
                .write(true)
                .open(&truncate)
                .and_then(|file| file.set_len(3))
                .map_err(|source| io_error("truncate", &truncate, source))?;
            let actual =
                fs::read(&truncate).map_err(|source| io_error("read", &truncate, source))?;
            if actual != b"abc" {
                return Err(format!("truncate content mismatch: {actual:?}"));
            }
            Ok("truncated 8 bytes to 3 bytes".to_string())
        });

        let chmod = workspace.join("chmod.bin");
        run_check(report, "correctness.posix.chmod", || {
            fs::write(&chmod, b"mode").map_err(|source| io_error("write", &chmod, source))?;
            fs::set_permissions(&chmod, fs::Permissions::from_mode(0o640))
                .map_err(|source| io_error("chmod", &chmod, source))?;
            let mode = fs::metadata(&chmod)
                .map_err(|source| io_error("stat", &chmod, source))?
                .mode()
                & 0o777;
            if mode != 0o640 {
                return Err(format!("chmod reported mode {mode:o}, expected 640"));
            }
            Ok("mode=640".to_string())
        });

        let rename_from = workspace.join("rename-from.bin");
        let rename_to = workspace.join("rename-to.bin");
        run_check(report, "correctness.posix.rename-unlink", || {
            fs::write(&rename_from, b"rename")
                .map_err(|source| io_error("write", &rename_from, source))?;
            fs::rename(&rename_from, &rename_to)
                .map_err(|source| io_error("rename", &rename_to, source))?;
            if rename_from
                .try_exists()
                .map_err(|source| io_error("check renamed source", &rename_from, source))?
                || fs::read(&rename_to).ok().as_deref() != Some(b"rename")
            {
                return Err("renamed file visibility or content mismatch".to_string());
            }
            fs::remove_file(&rename_to).map_err(|source| io_error("unlink", &rename_to, source))?;
            if rename_to
                .try_exists()
                .map_err(|source| io_error("check unlinked target", &rename_to, source))?
            {
                return Err("unlinked file still exists".to_string());
            }
            Ok("rename and unlink preserved expected visibility".to_string())
        });
    }

    fn link_checks(report: &mut BenchmarkReport, workspace: &Path) {
        let original = workspace.join("link-original.bin");
        let hard = workspace.join("link-hard.bin");
        let symbolic = workspace.join("link-symbolic.bin");
        run_check(report, "correctness.posix.append-hardlink-symlink", || {
            fs::write(&original, b"alpha")
                .map_err(|source| io_error("write", &original, source))?;
            OpenOptions::new()
                .append(true)
                .open(&original)
                .and_then(|mut file| file.write_all(b"-beta"))
                .map_err(|source| io_error("append", &original, source))?;
            fs::hard_link(&original, &hard)
                .map_err(|source| io_error("hard link", &hard, source))?;
            symlink(
                original
                    .file_name()
                    .expect("benchmark file always has a name"),
                &symbolic,
            )
            .map_err(|source| io_error("symbolic link", &symbolic, source))?;
            let expected = b"alpha-beta";
            for path in [&original, &hard, &symbolic] {
                let actual = fs::read(path).map_err(|source| io_error("read", path, source))?;
                if actual != expected {
                    return Err(format!("link content mismatch at {}", path.display()));
                }
            }
            let original_metadata =
                fs::metadata(&original).map_err(|source| io_error("stat", &original, source))?;
            let hard_metadata =
                fs::metadata(&hard).map_err(|source| io_error("stat", &hard, source))?;
            if original_metadata.ino() != hard_metadata.ino() {
                return Err(format!(
                    "hard-linked names have different inode numbers: {} != {}",
                    original_metadata.ino(),
                    hard_metadata.ino()
                ));
            }
            if original_metadata.nlink() != 2 || hard_metadata.nlink() != 2 {
                return Err(format!(
                    "hard-link count mismatch: original={} linked={}",
                    original_metadata.nlink(),
                    hard_metadata.nlink()
                ));
            }
            Ok(format!(
                "content matched through append and links; ino={} nlink=2",
                original_metadata.ino()
            ))
        });
    }

    fn sync_checks(report: &mut BenchmarkReport, workspace: &Path) {
        let sync_all_path = workspace.join("sync-all.bin");
        run_check(report, "correctness.posix.sync-all", || {
            let mut file = File::create(&sync_all_path)
                .map_err(|source| io_error("create", &sync_all_path, source))?;
            file.write_all(&payload(SMALL_BYTES, 21))
                .map_err(|source| io_error("write", &sync_all_path, source))?;
            file.sync_all()
                .map_err(|source| io_error("sync_all", &sync_all_path, source))?;
            Ok("sync_all completed".to_string())
        });

        let sync_data_path = workspace.join("sync-data.bin");
        run_check(report, "correctness.posix.sync-data", || {
            let mut file = File::create(&sync_data_path)
                .map_err(|source| io_error("create", &sync_data_path, source))?;
            file.write_all(&payload(SMALL_BYTES, 22))
                .map_err(|source| io_error("write", &sync_data_path, source))?;
            file.sync_data()
                .map_err(|source| io_error("sync_data", &sync_data_path, source))?;
            Ok("sync_data completed".to_string())
        });
    }

    fn lock_checks(report: &mut BenchmarkReport, workspace: &Path) {
        #[cfg(target_os = "linux")]
        {
            let path = workspace.join("ofd-lock.bin");
            run_check(report, "correctness.posix.ofd-whole-file-lock", || {
                fs::write(&path, b"lock").map_err(|source| io_error("write", &path, source))?;
                let first = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(|source| io_error("open", &path, source))?;
                let second = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(|source| io_error("open", &path, source))?;
                set_ofd_lock(first.as_raw_fd(), libc::F_WRLCK, 0, 0)
                    .map_err(|source| io_error("fcntl lock", &path, source))?;
                match set_ofd_lock(second.as_raw_fd(), libc::F_WRLCK, 0, 0) {
                    Ok(()) => {
                        let _ = set_ofd_lock(second.as_raw_fd(), libc::F_UNLCK, 0, 0);
                        let _ = set_ofd_lock(first.as_raw_fd(), libc::F_UNLCK, 0, 0);
                        return Err("contending whole-file OFD lock unexpectedly succeeded".into());
                    }
                    Err(source)
                        if matches!(
                            source.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::EACCES)
                        ) => {}
                    Err(source) => {
                        let _ = set_ofd_lock(first.as_raw_fd(), libc::F_UNLCK, 0, 0);
                        return Err(io_error("contending fcntl lock", &path, source));
                    }
                }
                set_ofd_lock(first.as_raw_fd(), libc::F_UNLCK, 0, 0)
                    .map_err(|source| io_error("fcntl unlock", &path, source))?;
                set_ofd_lock(second.as_raw_fd(), libc::F_WRLCK, 0, 0)
                    .map_err(|source| io_error("fcntl relock", &path, source))?;
                set_ofd_lock(second.as_raw_fd(), libc::F_UNLCK, 0, 0)
                    .map_err(|source| io_error("fcntl final unlock", &path, source))?;
                Ok("whole-file OFD lock, contention, unlock, and reacquire passed".to_string())
            });

            run_check(
                report,
                "correctness.posix.ofd-partial-lock-rejected",
                || {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                        .map_err(|source| io_error("open", &path, source))?;
                    match set_ofd_lock(file.as_raw_fd(), libc::F_WRLCK, 1, 1) {
                        Err(source) if source.raw_os_error() == Some(libc::EOPNOTSUPP) => {
                            Ok("partial-range OFD lock rejected with EOPNOTSUPP".to_string())
                        }
                        Err(source) => Err(format!(
                            "partial-range OFD lock returned {source}, expected EOPNOTSUPP"
                        )),
                        Ok(()) => {
                            let _ = set_ofd_lock(file.as_raw_fd(), libc::F_UNLCK, 1, 1);
                            Err("partial-range OFD lock unexpectedly succeeded".to_string())
                        }
                    }
                },
            );
        }
        #[cfg(not(target_os = "linux"))]
        report.skip(
            "correctness.posix.ofd-locks",
            Duration::ZERO,
            "Linux F_OFD_SETLK is unavailable on this Unix platform",
        );
    }

    #[cfg(target_os = "linux")]
    fn set_ofd_lock(
        fd: std::os::fd::RawFd,
        kind: libc::c_int,
        start: i64,
        len: i64,
    ) -> std::io::Result<()> {
        let lock = libc::flock {
            l_type: kind as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: start,
            l_len: len,
            l_pid: 0,
        };
        let result = unsafe { libc::fcntl(fd, libc::F_OFD_SETLK, &lock) };
        if result == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn persistence_writes(config: &Config, report: &mut BenchmarkReport, workspace: &Path) {
        run_check(report, "correctness.persistence.small-files.write", || {
            for index in 0..12 {
                let path = workspace.join(format!("durable-small-{index:02}.bin"));
                let bytes = payload(SMALL_BYTES, index as u8);
                write_chunked(&path, &bytes, SMALL_BYTES)
                    .map_err(|source| io_error("write", &path, source))?;
            }
            Ok("wrote 12 x 4 KiB files".to_string())
        });

        write_persistence_file(
            report,
            workspace,
            "durable-1mib.bin",
            ONE_MIB,
            ONE_MIB,
            31,
            "correctness.persistence.write_1mib",
        );
        write_persistence_file(
            report,
            workspace,
            "durable-large.bin",
            config.large_file_bytes(),
            LARGE_WRITE_CHUNK,
            32,
            "correctness.persistence.write_large",
        );
    }

    fn write_persistence_file(
        report: &mut BenchmarkReport,
        workspace: &Path,
        file_name: &str,
        bytes: usize,
        chunk: usize,
        seed: u8,
        check_name: &str,
    ) {
        let path = workspace.join(file_name);
        let expected = payload(bytes, seed);
        let started = Instant::now();
        match write_chunked(&path, &expected, chunk) {
            Ok(()) => {
                let elapsed = started.elapsed();
                report.pass(
                    check_name,
                    elapsed,
                    format!("wrote {bytes} bytes in chunks of {chunk}"),
                );
                report.metric(
                    format!("{check_name}.throughput"),
                    ThroughputMetrics::new(bytes, elapsed),
                );
            }
            Err(source) => report.fail(
                check_name,
                started.elapsed(),
                io_error("write", &path, source),
            ),
        }
    }

    fn persistence_reads(
        config: &Config,
        report: &mut BenchmarkReport,
        workspace: &Path,
        phase: &str,
    ) {
        run_check(
            report,
            &format!("correctness.persistence.{phase}.small-files"),
            || {
                for index in 0..12 {
                    validate_file(
                        &workspace.join(format!("durable-small-{index:02}.bin")),
                        SMALL_BYTES,
                        index as u8,
                    )?;
                }
                Ok("validated 12 x 4 KiB checksums".to_string())
            },
        );
        run_check(
            report,
            &format!("correctness.persistence.{phase}.read_1mib"),
            || validate_file(&workspace.join("durable-1mib.bin"), ONE_MIB, 31),
        );
        run_check(
            report,
            &format!("correctness.persistence.{phase}.read_large"),
            || {
                validate_file(
                    &workspace.join("durable-large.bin"),
                    config.large_file_bytes(),
                    32,
                )
            },
        );
    }

    fn read_only_checks(report: &mut BenchmarkReport, workspace: &Path) {
        run_check(report, "correctness.read-only.read", || {
            validate_file(&workspace.join("durable-small-00.bin"), SMALL_BYTES, 0)
        });
        let mutation = workspace.join("read-only-mutation.bin");
        run_check(
            report,
            "correctness.read-only.mutation-denied",
            || match File::create(&mutation) {
                Err(source) if source.raw_os_error() == Some(libc::EROFS) => {
                    Ok("create denied with EROFS".to_string())
                }
                Err(source) => Err(format!(
                    "read-only create returned {source}, expected EROFS"
                )),
                Ok(_) => {
                    let _ = fs::remove_file(&mutation);
                    Err("write unexpectedly succeeded on read-only mount".to_string())
                }
            },
        );
    }

    fn cleanup_sifs_workspace(
        config: &Config,
        report: &mut BenchmarkReport,
        name: &str,
        suite: &str,
    ) {
        if config.keep_fixtures {
            report.skip(
                format!("{suite}.cleanup"),
                Duration::ZERO,
                format!(
                    "fixture preserved by --keep-fixtures at {}",
                    run_sifs_path(&config.root, name)
                ),
            );
            return;
        }
        let Some(mount) = open_mount(
            config,
            report,
            &format!("{suite}.cleanup.mount"),
            false,
            false,
        ) else {
            report.fail(
                format!("{suite}.cleanup"),
                Duration::ZERO,
                format!(
                    "could not remount to remove {}",
                    run_sifs_path(&config.root, name)
                ),
            );
            return;
        };
        let workspace = mount.root().join(name);
        remove_workspace(
            report,
            &format!("{suite}.cleanup"),
            &workspace,
            &run_sifs_path(&config.root, name),
        );
        close_mount(report, &format!("{suite}.cleanup.unmount"), mount);
    }

    fn remove_workspace(
        report: &mut BenchmarkReport,
        check_name: &str,
        workspace: &Path,
        sifs_path: &str,
    ) {
        let started = Instant::now();
        match fs::remove_dir_all(workspace) {
            Ok(()) => match workspace.try_exists() {
                Ok(false) => report.pass(
                    check_name,
                    started.elapsed(),
                    format!("removed {sifs_path}"),
                ),
                Ok(true) => report.fail(
                    check_name,
                    started.elapsed(),
                    format!("{} still exists after cleanup", workspace.display()),
                ),
                Err(source) => report.fail(
                    check_name,
                    started.elapsed(),
                    io_error("verify removed benchmark workspace", workspace, source),
                ),
            },
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => report.pass(
                check_name,
                started.elapsed(),
                "workspace was already absent",
            ),
            Err(source) => report.fail(
                check_name,
                started.elapsed(),
                io_error("remove benchmark workspace", workspace, source),
            ),
        }
    }

    #[derive(Default)]
    struct PerformanceResults {
        create: Option<LatencyMetrics>,
        random: Option<LatencyMetrics>,
        sequential_write_open_fd: Option<ThroughputMetrics>,
        sequential_read_hot: Option<ThroughputMetrics>,
        parallel_create_scaling: Option<ScalingMetrics>,
    }

    fn run_performance(config: &Config, report: &mut BenchmarkReport) {
        let Some(mount) = open_mount(config, report, "performance.mount", false, true) else {
            return;
        };
        let name = workspace_name("performance");
        report
            .run_sifs_paths
            .push(run_sifs_path(&config.root, &name));
        let workspace = mount.root().join(&name);
        if let Err(source) = fs::create_dir(&workspace) {
            report.fail(
                "performance.workspace.create",
                Duration::ZERO,
                io_error("create benchmark workspace", &workspace, source),
            );
            close_mount(report, "performance.mount.unmount", mount);
            return;
        }
        report.pass(
            "performance.workspace.create",
            Duration::ZERO,
            format!("created {}", run_sifs_path(&config.root, &name)),
        );
        let results = performance_workload("fuse", &workspace, config, report);
        apply_performance_gates(config, report, &results);
        close_mount(report, "performance.mount.unmount", mount);

        let host_workspace = unique_host_path("sinomo-sifs-fuse-host-baseline");
        match fs::create_dir(&host_workspace) {
            Ok(()) => {
                let _ = performance_workload("host", &host_workspace, config, report);
                if let Err(source) = fs::remove_dir_all(&host_workspace) {
                    report.fail(
                        "performance.host.cleanup",
                        Duration::ZERO,
                        io_error("remove host baseline workspace", &host_workspace, source),
                    );
                } else {
                    report.pass(
                        "performance.host.cleanup",
                        Duration::ZERO,
                        "removed host baseline workspace",
                    );
                }
            }
            Err(source) => report.fail(
                "performance.host.workspace",
                Duration::ZERO,
                io_error("create host baseline workspace", &host_workspace, source),
            ),
        }

        performance_post_remount_read_and_cleanup(config, report, &name);
    }

    fn performance_post_remount_read_and_cleanup(
        config: &Config,
        report: &mut BenchmarkReport,
        name: &str,
    ) {
        let Some(mount) = open_mount(
            config,
            report,
            "performance.post-remount.mount",
            false,
            false,
        ) else {
            report.fail(
                "performance.cleanup",
                Duration::ZERO,
                format!(
                    "could not remount to verify or remove {}",
                    run_sifs_path(&config.root, name)
                ),
            );
            return;
        };
        let workspace = mount.root().join(name);
        let path = workspace.join("fuse-sequential.bin");
        let expected = payload(config.large_file_bytes(), 43);
        let started = Instant::now();
        match fs::read(&path) {
            Ok(actual) if actual == expected => {
                let elapsed = started.elapsed();
                report.metric(
                    "fuse.sequential_read_post_remount",
                    ThroughputMetrics::new(actual.len(), elapsed),
                );
                report.pass(
                    "performance.post-remount.read",
                    elapsed,
                    format!(
                        "validated {} bytes blake3:{}",
                        actual.len(),
                        checksum(&actual)
                    ),
                );
            }
            Ok(actual) => record_measurement_error(
                report,
                "fuse.sequential_read_post_remount",
                format!(
                    "post-remount checksum mismatch: expected {} actual {}",
                    checksum(&expected),
                    checksum(&actual)
                ),
            ),
            Err(source) => record_measurement_error(
                report,
                "fuse.sequential_read_post_remount",
                io_error("post-remount sequential read", &path, source),
            ),
        }

        if config.keep_fixtures {
            report.skip(
                "performance.cleanup",
                Duration::ZERO,
                format!(
                    "fixture preserved by --keep-fixtures at {}",
                    run_sifs_path(&config.root, name)
                ),
            );
        } else {
            remove_workspace(
                report,
                "performance.cleanup",
                &workspace,
                &run_sifs_path(&config.root, name),
            );
        }
        close_mount(report, "performance.post-remount.unmount", mount);
    }

    fn performance_workload(
        prefix: &str,
        root: &Path,
        config: &Config,
        report: &mut BenchmarkReport,
    ) -> PerformanceResults {
        let mut results = PerformanceResults::default();
        let empty_prefix = format!("{prefix}-empty");
        let create = measure_repeated(config.iterations, |index| {
            File::create(root.join(format!("{empty_prefix}-{index:04}"))).map(|_| ())
        });
        results.create = record_latency(report, &format!("{prefix}.empty_create"), create);

        if config.cache_ttl_ms > 0 {
            thread::sleep(Duration::from_millis(
                config.cache_ttl_ms.saturating_add(50),
            ));
        }
        record_latency(
            report,
            &format!("{prefix}.stat"),
            measure_repeated(config.iterations, |index| {
                fs::metadata(root.join(format!("{empty_prefix}-{index:04}"))).map(|_| ())
            }),
        );
        record_latency(
            report,
            &format!("{prefix}.readdir"),
            measure_repeated(config.iterations, |_| {
                fs::read_dir(root)?
                    .collect::<Result<Vec<_>, std::io::Error>>()
                    .map(|_| ())
            }),
        );
        record_latency(
            report,
            &format!("{prefix}.rename"),
            measure_repeated(config.iterations, |index| {
                fs::rename(
                    root.join(format!("{empty_prefix}-{index:04}")),
                    root.join(format!("{prefix}-renamed-{index:04}")),
                )
            }),
        );
        record_latency(
            report,
            &format!("{prefix}.unlink"),
            measure_repeated(config.iterations, |index| {
                fs::remove_file(root.join(format!("{prefix}-renamed-{index:04}")))
            }),
        );

        let small_payload = payload(SMALL_BYTES, 41);
        record_latency(
            report,
            &format!("{prefix}.create_write_close_4k"),
            measure_repeated(config.iterations, |index| {
                let path = root.join(format!("{prefix}-small-{index:04}.bin"));
                let mut file = File::create(path)?;
                file.write_all(&small_payload)
            }),
        );

        let random_path = root.join(format!("{prefix}-random.bin"));
        match fs::write(&random_path, vec![0_u8; RANDOM_FILE_BYTES]) {
            Ok(()) => {
                let random_payload = payload(SMALL_BYTES, 42);
                let random_result = (|| {
                    let mut file = OpenOptions::new().write(true).open(&random_path)?;
                    measure_repeated(config.iterations, |index| {
                        let slots = RANDOM_FILE_BYTES / SMALL_BYTES;
                        let offset = ((index * 17) % slots) * SMALL_BYTES;
                        file.seek(SeekFrom::Start(offset as u64))?;
                        file.write_all(&random_payload)
                    })
                })();
                results.random = record_latency(
                    report,
                    &format!("{prefix}.random_overwrite_4k_open_fd"),
                    random_result,
                );
            }
            Err(source) => record_measurement_error(
                report,
                &format!("{prefix}.random_overwrite_4k_open_fd"),
                io_error("initialize random-I/O file", &random_path, source),
            ),
        }

        let sequential_path = root.join(format!("{prefix}-sequential.bin"));
        let sequential_payload = payload(config.large_file_bytes(), 43);
        let create_started = Instant::now();
        match File::create(&sequential_path) {
            Ok(mut file) => {
                let write_started = Instant::now();
                match write_chunks(&mut file, &sequential_payload, LARGE_WRITE_CHUNK) {
                    Ok(()) => {
                        let open_fd_metrics = ThroughputMetrics::new(
                            config.large_file_bytes(),
                            write_started.elapsed(),
                        );
                        report.metric(
                            format!("{prefix}.sequential_write_open_fd"),
                            &open_fd_metrics,
                        );
                        drop(file);
                        let end_to_end_metrics = ThroughputMetrics::new(
                            config.large_file_bytes(),
                            create_started.elapsed(),
                        );
                        report.metric(format!("{prefix}.sequential_write"), &end_to_end_metrics);
                        results.sequential_write_open_fd = Some(open_fd_metrics);
                    }
                    Err(source) => record_measurement_error(
                        report,
                        &format!("{prefix}.sequential_write_open_fd"),
                        io_error("sequential write", &sequential_path, source),
                    ),
                }
            }
            Err(source) => record_measurement_error(
                report,
                &format!("{prefix}.sequential_write"),
                io_error("create sequential-write file", &sequential_path, source),
            ),
        }
        let read_started = Instant::now();
        match fs::read(&sequential_path) {
            Ok(actual) if actual == sequential_payload => {
                let metrics = ThroughputMetrics::new(actual.len(), read_started.elapsed());
                report.metric(format!("{prefix}.sequential_read_hot"), &metrics);
                results.sequential_read_hot = Some(metrics);
            }
            Ok(actual) => record_measurement_error(
                report,
                &format!("{prefix}.sequential_read_hot"),
                format!(
                    "sequential read checksum mismatch: expected {} actual {}",
                    checksum(&sequential_payload),
                    checksum(&actual)
                ),
            ),
            Err(source) => record_measurement_error(
                report,
                &format!("{prefix}.sequential_read_hot"),
                io_error("sequential read", &sequential_path, source),
            ),
        }

        match measure_scaling(root, prefix, config.iterations, config.concurrency) {
            Ok(metrics) => {
                report.metric(format!("{prefix}.parallel_create_scaling"), &metrics);
                results.parallel_create_scaling = Some(metrics);
            }
            Err(detail) => record_measurement_error(
                report,
                &format!("{prefix}.parallel_create_scaling"),
                detail,
            ),
        }
        results
    }

    fn measure_repeated(
        count: usize,
        mut operation: impl FnMut(usize) -> std::io::Result<()>,
    ) -> std::io::Result<LatencyMetrics> {
        let measured_started = Instant::now();
        let mut samples = Vec::with_capacity(count);
        for index in 0..count {
            let started = Instant::now();
            operation(index)?;
            samples.push(started.elapsed());
        }
        Ok(LatencyMetrics::from_samples(
            &samples,
            measured_started.elapsed(),
        ))
    }

    fn record_latency(
        report: &mut BenchmarkReport,
        name: &str,
        result: std::io::Result<LatencyMetrics>,
    ) -> Option<LatencyMetrics> {
        match result {
            Ok(metrics) => {
                report.metric(name, &metrics);
                Some(metrics)
            }
            Err(source) => {
                record_measurement_error(report, name, source.to_string());
                None
            }
        }
    }

    fn record_measurement_error(report: &mut BenchmarkReport, name: &str, detail: String) {
        report.fail(format!("measurement.{name}"), Duration::ZERO, detail);
    }

    fn measure_scaling(
        root: &Path,
        prefix: &str,
        operations: usize,
        workers: usize,
    ) -> Result<ScalingMetrics, String> {
        let sequential_started = Instant::now();
        for index in 0..operations {
            File::create(root.join(format!("{prefix}-scale-sequential-{index:04}")))
                .map_err(|source| source.to_string())?;
        }
        let sequential_elapsed = sequential_started.elapsed();

        let ready = Arc::new(Barrier::new(workers + 1));
        let start = Arc::new(Barrier::new(workers + 1));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let parallel_started = thread::scope(|scope| {
            for worker in 0..workers {
                let ready = Arc::clone(&ready);
                let start = Arc::clone(&start);
                let errors = Arc::clone(&errors);
                scope.spawn(move || {
                    ready.wait();
                    start.wait();
                    for index in (worker..operations).step_by(workers) {
                        let path =
                            root.join(format!("{prefix}-scale-parallel-{worker}-{index:04}"));
                        if let Err(source) = File::create(&path) {
                            errors
                                .lock()
                                .expect("parallel error lock")
                                .push(format!("{}: {source}", path.display()));
                            break;
                        }
                    }
                });
            }
            ready.wait();
            let parallel_started = Instant::now();
            start.wait();
            parallel_started
        });
        let parallel_elapsed = parallel_started.elapsed();
        let errors = errors.lock().expect("parallel error lock");
        if !errors.is_empty() {
            return Err(errors.join("; "));
        }
        let sequential_rate = rate_per_second(operations, sequential_elapsed);
        let parallel_rate = rate_per_second(operations, parallel_elapsed);
        Ok(ScalingMetrics {
            operations,
            workers,
            sequential_elapsed_us: duration_us(sequential_elapsed),
            parallel_elapsed_us: duration_us(parallel_elapsed),
            sequential_ops_per_second: sequential_rate,
            parallel_ops_per_second: parallel_rate,
            speedup: if sequential_rate > 0.0 {
                parallel_rate / sequential_rate
            } else {
                0.0
            },
        })
    }

    fn apply_performance_gates(
        config: &Config,
        report: &mut BenchmarkReport,
        results: &PerformanceResults,
    ) {
        if config.max_create_p95_ms > 0.0 {
            let started = Instant::now();
            match &results.create {
                Some(metrics) if metrics.p95_us as f64 / 1000.0 <= config.max_create_p95_ms => {
                    report.pass(
                        "threshold.empty-create-p95",
                        started.elapsed(),
                        format!(
                            "{:.3}ms <= {:.3}ms",
                            metrics.p95_us as f64 / 1000.0,
                            config.max_create_p95_ms
                        ),
                    );
                }
                Some(metrics) => report.fail(
                    "threshold.empty-create-p95",
                    started.elapsed(),
                    format!(
                        "{:.3}ms > {:.3}ms",
                        metrics.p95_us as f64 / 1000.0,
                        config.max_create_p95_ms
                    ),
                ),
                None => report.fail(
                    "threshold.empty-create-p95",
                    started.elapsed(),
                    "empty-create measurement unavailable",
                ),
            }
        }
        if config.min_sequential_write_mib_s > 0.0 {
            let started = Instant::now();
            match &results.sequential_write_open_fd {
                Some(metrics) if metrics.mib_per_second >= config.min_sequential_write_mib_s => {
                    report.pass(
                        "threshold.sequential-write",
                        started.elapsed(),
                        format!(
                            "{:.3} MiB/s >= {:.3} MiB/s",
                            metrics.mib_per_second, config.min_sequential_write_mib_s
                        ),
                    );
                }
                Some(metrics) => report.fail(
                    "threshold.sequential-write",
                    started.elapsed(),
                    format!(
                        "{:.3} MiB/s < {:.3} MiB/s",
                        metrics.mib_per_second, config.min_sequential_write_mib_s
                    ),
                ),
                None => report.fail(
                    "threshold.sequential-write",
                    started.elapsed(),
                    "open-file sequential-write measurement unavailable",
                ),
            }
        }
        if config.min_sequential_read_mib_s > 0.0 {
            let started = Instant::now();
            match &results.sequential_read_hot {
                Some(metrics) if metrics.mib_per_second >= config.min_sequential_read_mib_s => {
                    report.pass(
                        "threshold.sequential-read",
                        started.elapsed(),
                        format!(
                            "{:.3} MiB/s >= {:.3} MiB/s",
                            metrics.mib_per_second, config.min_sequential_read_mib_s
                        ),
                    );
                }
                Some(metrics) => report.fail(
                    "threshold.sequential-read",
                    started.elapsed(),
                    format!(
                        "{:.3} MiB/s < {:.3} MiB/s",
                        metrics.mib_per_second, config.min_sequential_read_mib_s
                    ),
                ),
                None => report.fail(
                    "threshold.sequential-read",
                    started.elapsed(),
                    "hot sequential-read measurement unavailable",
                ),
            }
        }
        if config.min_parallel_create_speedup > 0.0 {
            let started = Instant::now();
            match &results.parallel_create_scaling {
                Some(metrics) if metrics.speedup >= config.min_parallel_create_speedup => {
                    report.pass(
                        "threshold.parallel-create-speedup",
                        started.elapsed(),
                        format!(
                            "{:.3}x >= {:.3}x",
                            metrics.speedup, config.min_parallel_create_speedup
                        ),
                    );
                }
                Some(metrics) => report.fail(
                    "threshold.parallel-create-speedup",
                    started.elapsed(),
                    format!(
                        "{:.3}x < {:.3}x",
                        metrics.speedup, config.min_parallel_create_speedup
                    ),
                ),
                None => report.fail(
                    "threshold.parallel-create-speedup",
                    started.elapsed(),
                    "parallel-create scaling measurement unavailable",
                ),
            }
        }
        if config.min_random_iops > 0.0 {
            let started = Instant::now();
            match &results.random {
                Some(metrics) if metrics.ops_per_second >= config.min_random_iops => report.pass(
                    "threshold.random-iops",
                    started.elapsed(),
                    format!(
                        "{:.3} IOPS >= {:.3} IOPS",
                        metrics.ops_per_second, config.min_random_iops
                    ),
                ),
                Some(metrics) => report.fail(
                    "threshold.random-iops",
                    started.elapsed(),
                    format!(
                        "{:.3} IOPS < {:.3} IOPS",
                        metrics.ops_per_second, config.min_random_iops
                    ),
                ),
                None => report.fail(
                    "threshold.random-iops",
                    started.elapsed(),
                    "random-overwrite measurement unavailable",
                ),
            }
        }
    }

    fn write_chunks(file: &mut File, bytes: &[u8], chunk: usize) -> std::io::Result<()> {
        for part in bytes.chunks(chunk.max(1)) {
            file.write_all(part)?;
        }
        file.flush()
    }

    fn write_chunked(path: &Path, bytes: &[u8], chunk: usize) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        write_chunks(&mut file, bytes, chunk)
    }

    fn validate_file(path: &Path, expected_len: usize, seed: u8) -> Result<String, String> {
        let mut file = File::open(path).map_err(|source| io_error("open", path, source))?;
        let mut actual = Vec::new();
        file.read_to_end(&mut actual)
            .map_err(|source| io_error("read", path, source))?;
        let expected = payload(expected_len, seed);
        if actual != expected {
            return Err(format!(
                "{} checksum mismatch: expected {} actual {} (expected_bytes={} actual_bytes={})",
                path.display(),
                checksum(&expected),
                checksum(&actual),
                expected.len(),
                actual.len()
            ));
        }
        Ok(format!(
            "validated {} bytes blake3:{}",
            actual.len(),
            checksum(&actual)
        ))
    }

    fn io_error(operation: &str, path: &Path, source: std::io::Error) -> String {
        let errno = source
            .raw_os_error()
            .map(|errno| format!(" errno={errno}"))
            .unwrap_or_default();
        format!("{operation} {} failed: {source}{errno}", path.display())
    }

    #[cfg(test)]
    mod performance_gate_tests {
        use super::*;

        fn threshold_config(read_mib_s: &str, parallel_speedup: &str) -> Config {
            match Config::parse_with_environment(
                [
                    "--root".to_string(),
                    "/sinomo/agents/test/home".to_string(),
                    "--min-sequential-read-mib-s".to_string(),
                    read_mib_s.to_string(),
                    "--min-parallel-create-speedup".to_string(),
                    parallel_speedup.to_string(),
                ],
                None,
                None,
            )
            .expect("threshold arguments")
            {
                ParseResult::Run(config) => config,
                ParseResult::Help => panic!("unexpected help"),
            }
        }

        #[test]
        fn sequential_read_and_parallel_scaling_floors_are_enforced() {
            let config = threshold_config("2", "1.5");
            let mut report = BenchmarkReport::new(config.clone());
            let results = PerformanceResults {
                sequential_read_hot: Some(ThroughputMetrics::new(
                    1024 * 1024,
                    Duration::from_secs(1),
                )),
                parallel_create_scaling: Some(ScalingMetrics {
                    operations: 32,
                    workers: 4,
                    sequential_elapsed_us: 100,
                    parallel_elapsed_us: 80,
                    sequential_ops_per_second: 320.0,
                    parallel_ops_per_second: 400.0,
                    speedup: 1.25,
                }),
                ..PerformanceResults::default()
            };

            apply_performance_gates(&config, &mut report, &results);

            assert!(report.checks.iter().any(|check| {
                check.name == "threshold.sequential-read" && check.status == CheckStatus::Failed
            }));
            assert!(report.checks.iter().any(|check| {
                check.name == "threshold.parallel-create-speedup"
                    && check.status == CheckStatus::Failed
            }));
        }
    }
}

fn payload(bytes: usize, seed: u8) -> Vec<u8> {
    (0..bytes)
        .map(|index| {
            let mixed = index
                .wrapping_mul(31)
                .wrapping_add(usize::from(seed).wrapping_mul(17));
            (mixed % 251) as u8
        })
        .collect()
}

fn checksum(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(args: &[&str]) -> Config {
        match Config::parse_with_environment(args.iter().map(|arg| (*arg).to_string()), None, None)
            .expect("arguments should parse")
        {
            ParseResult::Run(config) => config,
            ParseResult::Help => panic!("unexpected help"),
        }
    }

    #[test]
    fn parser_requires_absolute_safe_root() {
        assert!(
            Config::parse_with_environment(Vec::<String>::new(), None, None)
                .unwrap_err()
                .contains("--root is required")
        );
        assert!(
            Config::parse_with_environment(
                ["--root".to_string(), "relative".to_string()],
                None,
                None,
            )
            .unwrap_err()
            .contains("absolute")
        );
        assert!(
            Config::parse_with_environment(
                ["--root".to_string(), "/safe/../unsafe".to_string()],
                None,
                None,
            )
            .unwrap_err()
            .contains("must not contain")
        );
    }

    #[test]
    fn parser_applies_tuning_and_thresholds() {
        let config = parsed(&[
            "--root",
            "/sinomo/agents/test/home",
            "--suite",
            "performance",
            "--iterations",
            "12",
            "--large-file-mib",
            "4",
            "--concurrency",
            "3",
            "--cache-ttl-ms",
            "50",
            "--max-create-p95-ms",
            "100",
            "--min-sequential-write-mib-s",
            "0.25",
            "--min-sequential-read-mib-s",
            "0.75",
            "--min-parallel-create-speedup",
            "1.5",
            "--min-random-iops",
            "0.5",
            "--report-only",
        ]);
        assert_eq!(config.suite, Suite::Performance);
        assert_eq!(config.iterations, 12);
        assert_eq!(config.large_file_bytes(), 4 * 1024 * 1024);
        assert_eq!(config.concurrency, 3);
        assert_eq!(config.cache_ttl_ms, 50);
        assert_eq!(config.max_create_p95_ms, 100.0);
        assert_eq!(config.min_sequential_write_mib_s, 0.25);
        assert_eq!(config.min_sequential_read_mib_s, 0.75);
        assert_eq!(config.min_parallel_create_speedup, 1.5);
        assert_eq!(config.min_random_iops, 0.5);
        assert!(config.report_only);
    }

    #[test]
    fn parser_bounds_resource_consuming_options() {
        for (flag, value) in [
            ("--iterations", "1025"),
            ("--large-file-mib", "65"),
            ("--concurrency", "65"),
        ] {
            let error = Config::parse_with_environment(
                [
                    "--root".to_string(),
                    "/sinomo/agents/test/home".to_string(),
                    flag.to_string(),
                    value.to_string(),
                ],
                None,
                None,
            )
            .unwrap_err();
            assert!(error.contains("must not exceed"), "{error}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn generated_run_path_is_a_child_and_never_the_supplied_root() {
        let root = "/sinomo/agents/test/home";
        let generated = unix_runner::run_sifs_path(root, "fuse-bench-123");
        assert_eq!(generated, "/sinomo/agents/test/home/fuse-bench-123");
        assert_ne!(generated, root);
        assert_eq!(
            unix_runner::run_sifs_path("/", "fuse-bench-123"),
            "/fuse-bench-123"
        );
    }

    #[test]
    fn latency_metrics_use_nearest_rank_percentiles() {
        let samples = [1_u64, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(Duration::from_micros);
        let metrics = LatencyMetrics::from_samples(&samples, Duration::from_micros(100));
        assert_eq!(metrics.count, 10);
        assert_eq!(metrics.min_us, 1);
        assert_eq!(metrics.mean_us, 5);
        assert_eq!(metrics.p50_us, 5);
        assert_eq!(metrics.p95_us, 10);
        assert_eq!(metrics.p99_us, 10);
        assert_eq!(metrics.max_us, 10);
        assert_eq!(metrics.ops_per_second, 100_000.0);
    }

    #[test]
    fn payload_and_checksum_are_deterministic() {
        let first = payload(4096, 7);
        let second = payload(4096, 7);
        assert_eq!(first, second);
        assert_ne!(first, payload(4096, 8));
        assert_eq!(checksum(&first), checksum(&second));
    }

    #[test]
    fn report_aggregation_and_report_only_exit_are_stable() {
        let mut report = BenchmarkReport::new(parsed(&[
            "--root",
            "/sinomo/agents/test/home",
            "--report-only",
        ]));
        report.pass("pass", Duration::from_millis(1), "ok");
        report.fail("fail", Duration::from_millis(1), "bad");
        report.metric(
            "latency",
            LatencyMetrics::from_samples(&[Duration::from_micros(10)], Duration::from_micros(10)),
        );
        report.finish();
        assert_eq!(report.status, ReportStatus::Failed);
        assert_eq!(report.exit_code(), 0);
        let encoded = serde_json::to_value(&report).expect("report serialization");
        assert_eq!(encoded["status"], "failed");
        assert_eq!(encoded["measurements"]["latency"]["p50_us"], 10);
    }

    #[test]
    fn required_capability_exit_is_not_hidden_by_report_only() {
        let mut report = BenchmarkReport::new(parsed(&[
            "--root",
            "/sinomo/agents/test/home",
            "--require-fuse",
            "--report-only",
        ]));
        report.required_capability_missing = true;
        report.fail("mount", Duration::ZERO, "/dev/fuse is unavailable");
        report.finish();
        assert_eq!(report.status, ReportStatus::Failed);
        assert_eq!(report.exit_code(), 3);
    }
}
