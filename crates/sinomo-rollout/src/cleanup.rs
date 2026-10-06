//! Rollout-owned retention at operation boundaries. Replay fences are durable;
//! large replay payloads and process logs have explicit owners and retention.
use crate::{RolloutError, SelfRolloutPaths, read_object};
use sinomo_core::storage::scratch::{remove_inactive_scratch_directory, remove_owned_scratch_path};
use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    time::{Duration, SystemTime},
};

pub const ROLLOUT_ARTIFACT_GENERATIONS: usize = 4;
pub const ROLLOUT_LOG_GENERATIONS: usize = 16;
const TRANSFER_GRACE: Duration = Duration::from_secs(24 * 60 * 60);

fn entries(path: &Path) -> Result<Vec<fs::DirEntry>, RolloutError> {
    match fs::read_dir(path) {
        Ok(entries) => entries
            .map(|entry| entry.map_err(|error| io_error(path, error)))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
        Err(error) => Err(io_error(path, error)),
    }
}

fn digest_name(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit())
}
fn old(path: &Path, grace: Duration) -> bool {
    fs::symlink_metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| SystemTime::now().duration_since(t).ok())
        .is_some_and(|age| age >= grace)
}
fn io_error(path: &Path, error: std::io::Error) -> RolloutError {
    RolloutError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

/// Caller holds the coordinator lock. Keep all unfinished intents, the newest
/// four settled plans, and the latest pointer even when it names an older plan.
pub fn prune_rollout_coordinator_artifacts(paths: &SelfRolloutPaths) -> Result<(), RolloutError> {
    let mut settled = Vec::new();
    let mut keep = Vec::new();
    let latest_path = paths.root.join("coordinator/latest.sobj");
    let latest: Option<String> = if latest_path.exists() {
        Some(read_object(&latest_path)?)
    } else {
        None
    };
    if let Some(id) = &latest {
        crate::load_rollout_coordinator_plan(paths, Some(id))?;
    }
    for entry in entries(&paths.root.join("coordinator"))? {
        if !entry
            .file_type()
            .map_err(|e| io_error(&entry.path(), e))?
            .is_file()
        {
            continue;
        }
        let path = entry.path();
        if path.extension().is_some_and(|s| s == "sobj")
            && path.file_name().is_some_and(|s| s != "latest.sobj")
        {
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let plan = crate::load_rollout_coordinator_plan(paths, Some(id))?;
            if latest.as_deref() == Some(id) {
                keep.push(plan.clone());
            }
            if !plan.complete() && !plan.cancelled {
                keep.push(plan);
            } else {
                settled.push(plan);
            }
        }
    }
    settled.sort_by(|a, b| b.id.cmp(&a.id));
    keep.extend(settled.into_iter().take(ROLLOUT_ARTIFACT_GENERATIONS));
    let protected = keep
        .iter()
        .flat_map(|p| &p.targets)
        .flat_map(|t| t.manifest.artifacts())
        .map(|a| {
            a.blake3
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        })
        .collect::<BTreeSet<_>>();
    for entry in entries(&paths.root.join("coordinator/artifacts"))? {
        if !entry
            .file_type()
            .map_err(|e| io_error(&entry.path(), e))?
            .is_file()
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if digest_name(&name) && !protected.contains(&name) {
            remove_owned_scratch_path(&entry.path()).map_err(|e| io_error(&entry.path(), e))?;
        } else if name.ends_with(".part") && old(&entry.path(), TRANSFER_GRACE) {
            remove_owned_scratch_path(&entry.path()).map_err(|e| io_error(&entry.path(), e))?;
        }
    }
    // These are transfer scratch, not the builder's immutable artifact cache.
    // New callers use leased download directories. Legacy key directories and
    // bundle caches are only swept after a full transfer grace period.
    for entry in entries(&paths.root.join("staged/build-artifacts"))? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if (name.starts_with("download-") || digest_name(&name))
            && old(&entry.path(), TRANSFER_GRACE)
        {
            remove_inactive_scratch_directory(&entry.path())
                .map_err(|e| io_error(&entry.path(), e))?;
        }
    }
    for entry in entries(&paths.root.join("staged/build-sources"))? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry
            .file_type()
            .map_err(|e| io_error(&entry.path(), e))?
            .is_file()
            && name.strip_suffix(".bundle").is_some_and(digest_name)
            && old(&entry.path(), TRANSFER_GRACE)
        {
            remove_owned_scratch_path(&entry.path()).map_err(|e| io_error(&entry.path(), e))?;
        }
    }
    Ok(())
}

pub fn reclaim_rollout_transfers(paths: &SelfRolloutPaths) -> Result<(), RolloutError> {
    if crate::rollout_main_activation_path(paths).exists()
        || crate::rollout_shadow_finalize_path(paths).exists()
    {
        return Ok(());
    }
    let lock_path = paths.root.join("activation.lock");
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|e| io_error(&lock_path, e))?;
    if fs2::FileExt::try_lock_exclusive(&lock).is_err() {
        return Ok(());
    }
    // Activation may have started between the first check and lock acquisition.
    if crate::rollout_main_activation_path(paths).exists()
        || crate::rollout_shadow_finalize_path(paths).exists()
    {
        return Ok(());
    }
    crate::binary_ingress::reclaim_upload_payloads(paths, TRANSFER_GRACE)?;
    for entry in entries(&paths.root.join("staged"))? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry
            .file_type()
            .map_err(|e| io_error(&entry.path(), e))?
            .is_file()
            && name.starts_with("source-")
            && name.ends_with(".bundle")
            && old(&entry.path(), TRANSFER_GRACE)
        {
            remove_owned_scratch_path(&entry.path()).map_err(|e| io_error(&entry.path(), e))?;
        }
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProcessLogs {
    paths: Vec<String>,
}

pub fn record_rollout_process_logs(
    paths: &SelfRolloutPaths,
    pid: u32,
    stdout: &Path,
    stderr: &Path,
) -> Result<(), RolloutError> {
    let logs = paths.root.join("logs");
    let names = [stdout, stderr]
        .iter()
        .map(|p| {
            if p.parent() != Some(logs.as_path()) {
                return Err(RolloutError::InvalidArtifact {
                    detail: "process log is outside rollout logs".into(),
                });
            }
            p.file_name()
                .and_then(|s| s.to_str())
                .map(str::to_string)
                .ok_or_else(|| RolloutError::InvalidArtifact {
                    detail: "invalid process log name".into(),
                })
        })
        .collect::<Result<_, _>>()?;
    crate::write_object(
        &logs.join(format!("process-{pid}.sobj")),
        &ProcessLogs { paths: names },
    )
}

/// Call only after both daemons are healthy and no replacement can begin yet.
/// Missing live-process ownership records disable log pruning for older hosts.
pub fn prune_rollout_process_logs(
    paths: &SelfRolloutPaths,
    live_pids: &[u32],
) -> Result<(), RolloutError> {
    let logs = paths.root.join("logs");
    let mut protected = BTreeSet::new();
    for pid in live_pids {
        let path = logs.join(format!("process-{pid}.sobj"));
        if !path.exists() {
            return Ok(());
        }
        let record: ProcessLogs = read_object(&path)?;
        for name in record.paths {
            if Path::new(&name).file_name().and_then(|s| s.to_str()) != Some(name.as_str()) {
                return Err(RolloutError::InvalidArtifact {
                    detail: "invalid retained process log name".into(),
                });
            }
            protected.insert(name);
        }
    }
    let mut generations =
        std::collections::BTreeMap::<String, (SystemTime, Vec<std::path::PathBuf>)>::new();
    for entry in entries(&logs)? {
        if !entry
            .file_type()
            .map_err(|e| io_error(&entry.path(), e))?
            .is_file()
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("candidate-main-") && !name.starts_with("shadow-supervisor-") {
            continue;
        }
        let Some(base) = [".stdout.log", ".stderr.log", ".diagnostics.json"]
            .iter()
            .find_map(|suffix| name.strip_suffix(suffix))
        else {
            continue;
        };
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .map_err(|e| io_error(&entry.path(), e))?;
        let group = generations
            .entry(base.to_string())
            .or_insert((modified, vec![]));
        group.0 = group.0.max(modified);
        group.1.push(entry.path());
    }
    let mut generations = generations.into_values().collect::<Vec<_>>();
    generations.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    for (_, files) in generations.into_iter().skip(ROLLOUT_LOG_GENERATIONS) {
        if files.iter().any(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| protected.contains(s))
        }) {
            continue;
        }
        for path in files {
            remove_owned_scratch_path(&path).map_err(|e| io_error(&path, e))?;
        }
    }
    for entry in entries(&logs)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(pid) = name
            .strip_prefix("process-")
            .and_then(|s| s.strip_suffix(".sobj"))
            .and_then(|s| s.parse::<u32>().ok())
            && !live_pids.contains(&pid)
            && entry
                .file_type()
                .map_err(|e| io_error(&entry.path(), e))?
                .is_file()
        {
            remove_owned_scratch_path(&entry.path()).map_err(|e| io_error(&entry.path(), e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(
        paths: &SelfRolloutPaths,
        root: &Path,
        generation: usize,
        verified: bool,
    ) -> crate::RolloutCoordinatorPlan {
        let binary = root.join("candidate");
        fs::write(&binary, format!("binary-{generation}")).unwrap();
        let manifest = crate::prepare_cluster_rollout_binary_manifest(
            "source",
            &binary,
            &[],
            "0.0.0.0",
            21353,
            21354,
            "request",
        )
        .unwrap();
        let artifact =
            crate::cache_rollout_coordinator_artifact(paths, &binary, &manifest.binary).unwrap();
        crate::RolloutCoordinatorPlan {
            schema_version: 1,
            cancelled: false,
            id: format!("rollout-{generation:04}"),
            cluster_id: sinomo_core::Id::generate(),
            source_repo: root.into(),
            source_revision: None,
            targets: vec![crate::RolloutCoordinatorTarget {
                node_id: sinomo_core::Id::generate(),
                address: "node:21353".into(),
                endpoint: None,
                restart_endpoints: vec![],
                was_voter: false,
                artifacts: vec![crate::RolloutArtifactSource {
                    artifact_id: "binary".into(),
                    path: artifact,
                }],
                manifest,
                submitted: true,
                verified,
            }],
        }
    }

    #[test]
    fn repeated_settled_rollouts_bound_payloads_but_preserve_unfinished_replay() {
        let root = sinomo_testdir::TestDir::new("rollout-coordinator-retention");
        let paths = SelfRolloutPaths::new(&root);
        crate::ensure_rollout_layout(&paths).unwrap();
        let _lock = crate::lock_rollout_coordinator(&paths).unwrap();
        let unfinished = plan(&paths, &root, 0, false);
        crate::save_rollout_coordinator_plan(&paths, &unfinished).unwrap();
        for generation in 1..=12 {
            let settled = plan(&paths, &root, generation, true);
            crate::save_rollout_coordinator_plan(&paths, &settled).unwrap();
            assert!(unfinished.targets[0].artifacts[0].path.exists());
            assert!(settled.targets[0].artifacts[0].path.exists());
            assert!(
                entries(&paths.root.join("coordinator/artifacts"))
                    .unwrap()
                    .len()
                    <= ROLLOUT_ARTIFACT_GENERATIONS + 1
            );
        }
        assert_eq!(
            crate::load_rollout_coordinator_plan(&paths, Some(&unfinished.id))
                .unwrap()
                .targets[0]
                .manifest,
            unfinished.targets[0].manifest
        );
        // An explicit older latest pointer is also a retained replay input.
        crate::save_rollout_coordinator_plan(&paths, &unfinished).unwrap();
        assert!(unfinished.targets[0].artifacts[0].path.exists());
    }

    #[test]
    fn process_logs_keep_live_files_even_when_they_are_older_than_retention() {
        let root = sinomo_testdir::TestDir::new("rollout-log-retention");
        let paths = SelfRolloutPaths::new(&root);
        let logs = paths.root.join("logs");
        fs::create_dir_all(&logs).unwrap();
        let main_out = logs.join("candidate-main-original.stdout.log");
        let main_err = logs.join("candidate-main-original.stderr.log");
        fs::write(&main_out, b"main").unwrap();
        fs::write(&main_err, []).unwrap();
        record_rollout_process_logs(&paths, 1, &main_out, &main_err).unwrap();
        let unrelated = logs.join("operator.notes");
        fs::write(&unrelated, b"keep").unwrap();
        for generation in 2..=40 {
            let stdout = logs.join(format!("shadow-supervisor-{generation}.stdout.log"));
            let stderr = logs.join(format!("shadow-supervisor-{generation}.stderr.log"));
            fs::write(&stdout, b"shadow").unwrap();
            fs::write(&stderr, []).unwrap();
            record_rollout_process_logs(&paths, generation, &stdout, &stderr).unwrap();
            prune_rollout_process_logs(&paths, &[1, generation]).unwrap();
            assert!(main_out.exists());
            assert!(main_err.exists());
            assert!(stdout.exists());
            assert!(stderr.exists());
        }
        assert!(unrelated.exists());
        assert!(entries(&logs).unwrap().len() <= (ROLLOUT_LOG_GENERATIONS + 1) * 2 + 3);
    }

    #[test]
    fn missing_live_log_ownership_and_symlinks_fail_closed() {
        let root = sinomo_testdir::TestDir::new("rollout-old-log-owner");
        let paths = SelfRolloutPaths::new(&root);
        let logs = paths.root.join("logs");
        fs::create_dir_all(&logs).unwrap();
        for generation in 0..30 {
            fs::write(
                logs.join(format!("candidate-main-{generation}.stdout.log")),
                b"keep",
            )
            .unwrap();
        }
        prune_rollout_process_logs(&paths, &[123]).unwrap();
        assert_eq!(entries(&logs).unwrap().len(), 30);
        #[cfg(unix)]
        {
            let outside = root.join("external");
            fs::write(&outside, b"outside").unwrap();
            let artifacts = paths.root.join("coordinator/artifacts");
            fs::create_dir_all(&artifacts).unwrap();
            std::os::unix::fs::symlink(&outside, artifacts.join("a".repeat(64))).unwrap();
            prune_rollout_coordinator_artifacts(&paths).unwrap();
            assert_eq!(fs::read(outside).unwrap(), b"outside");
        }
    }
}
