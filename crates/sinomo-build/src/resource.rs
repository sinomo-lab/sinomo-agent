use crate::{BuildError, BuildPaths, BuildPlacementRequirements, BuildResourceSnapshot, Result};
use std::fs;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildExecutionPolicy {
    pub cargo_jobs: u16,
    pub placement: BuildPlacementRequirements,
}

impl BuildExecutionPolicy {
    pub fn for_source_bytes(resources: &BuildResourceSnapshot, source_bytes: u64) -> Result<Self> {
        let placement = BuildPlacementRequirements::for_source_bytes(source_bytes);
        if !placement.is_satisfied_by(resources) {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "build host has insufficient resources: {} MiB memory and {} MiB disk available; requires at least {} MiB memory and {} MiB disk",
                    resources.memory_available_mb.unwrap_or_default(),
                    resources.disk_available_mb.unwrap_or_default(),
                    placement.minimum_memory_available_mb.unwrap_or_default(),
                    placement.minimum_disk_available_mb.unwrap_or_default(),
                ),
            });
        }
        Ok(Self {
            // Placement and admission handle resource availability. Once a
            // build is admitted, Cargo uses every core available to the host.
            cargo_jobs: resources.cpu_threads.max(1),
            placement,
        })
    }
}

pub fn sample_build_resources(
    paths: &BuildPaths,
    active_jobs: u16,
    max_jobs: u16,
    queue_depth: u32,
) -> BuildResourceSnapshot {
    let cpu_threads = std::thread::available_parallelism()
        .map(|count| u16::try_from(count.get()).unwrap_or(u16::MAX))
        .unwrap_or(1);
    BuildResourceSnapshot {
        cpu_threads,
        memory_total_mb: host_memory_mb("MemTotal:"),
        memory_available_mb: host_memory_mb("MemAvailable:"),
        cpu_load_milli: host_cpu_load_milli(cpu_threads),
        disk_total_mb: fs2::total_space(&paths.root).ok().map(bytes_to_mb),
        disk_available_mb: fs2::available_space(&paths.root).ok().map(bytes_to_mb),
        active_jobs,
        max_jobs,
        queue_depth,
    }
}

fn bytes_to_mb(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}

fn host_memory_mb(label: &str) -> Option<u64> {
    let contents = fs::read_to_string("/proc/meminfo").ok()?;
    let kib = contents.lines().find_map(|line| {
        let value = line.strip_prefix(label)?.trim();
        value.split_whitespace().next()?.parse::<u64>().ok()
    })?;
    Some(kib / 1024)
}

fn host_cpu_load_milli(cpu_threads: u16) -> Option<u16> {
    let one_minute = fs::read_to_string("/proc/loadavg")
        .ok()?
        .split_whitespace()
        .next()?
        .parse::<f64>()
        .ok()?;
    let normalized = (one_minute / f64::from(cpu_threads.max(1)) * 1_000.0)
        .round()
        .clamp(0.0, f64::from(u16::MAX));
    Some(normalized as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_policy_uses_all_cores_under_memory_and_load_pressure() {
        let mut resources = BuildResourceSnapshot {
            cpu_threads: 32,
            memory_total_mb: Some(64 * 1_024),
            memory_available_mb: Some(2_048),
            cpu_load_milli: Some(2_000),
            disk_total_mb: Some(128 * 1_024),
            disk_available_mb: Some(64 * 1_024),
            active_jobs: 0,
            max_jobs: 1,
            queue_depth: 0,
        };

        let policy = BuildExecutionPolicy::for_source_bytes(&resources, 1024).expect("policy");

        assert_eq!(policy.cargo_jobs, 32);

        resources.memory_available_mb = None;
        resources.cpu_load_milli = None;
        let policy = BuildExecutionPolicy::for_source_bytes(&resources, 1024).expect("policy");
        assert_eq!(policy.cargo_jobs, 32);
    }

    #[test]
    fn execution_policy_rejects_known_low_disk() {
        let resources = BuildResourceSnapshot {
            cpu_threads: 4,
            memory_total_mb: Some(8 * 1_024),
            memory_available_mb: Some(4 * 1_024),
            cpu_load_milli: Some(0),
            disk_total_mb: Some(8 * 1_024),
            disk_available_mb: Some(512),
            active_jobs: 0,
            max_jobs: 1,
            queue_depth: 0,
        };

        assert!(BuildExecutionPolicy::for_source_bytes(&resources, 1024).is_err());
    }
}
