//! Build-owned cleanup. Triggered by startup and completed work, never idle polling.
use crate::{BuildArtifactSetManifest, BuildJobId, BuildJobStatus, BuildPaths};
use sinomo_core::{network::read_object_file, storage::scratch::remove_owned_scratch_path};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

pub const BUILD_CACHE_GENERATIONS: usize = 4;
pub const BUILD_TRANSFER_GRACE: Duration = Duration::from_secs(24 * 60 * 60);

pub(crate) fn cleanup_job_scratch(paths: &BuildPaths, job: &BuildJobId) {
    for path in [
        paths.job_work_dir(job),
        paths.job_source_bundle(job),
        paths.job_source_bundle(job).with_extension("partial"),
    ] {
        if let Err(error) = remove_owned_scratch_path(&path) {
            tracing::warn!(path = %path.display(), %error, "could not remove terminal build scratch");
        }
    }
}

/// Keep cache hits/downloads alive across multi-artifact transfers. Capability
/// discovery does not touch this marker or turn idle reads into writes.
pub(crate) fn touch_cache(paths: &BuildPaths, key: &crate::BuildKey) -> std::io::Result<()> {
    let path = paths.cache_entry_dir(key).join("last-used");
    fs::write(path, [])
}

pub(crate) fn maintain(paths: &BuildPaths) {
    if let Err(error) = prune_at(paths, SystemTime::now(), BUILD_TRANSFER_GRACE) {
        tracing::warn!(path = %paths.root.display(), %error, "could not reclaim obsolete build files");
    }
}

fn regular_entries(path: &Path) -> std::io::Result<Vec<fs::DirEntry>> {
    match fs::read_dir(path) {
        Ok(entries) => entries.collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
        Err(error) => Err(error),
    }
}

fn old(path: &Path, now: SystemTime, grace: Duration) -> bool {
    fs::symlink_metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| now.duration_since(t).ok())
        .is_some_and(|age| age >= grace)
}

fn digest_name(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

fn prune_at(paths: &BuildPaths, now: SystemTime, grace: Duration) -> std::io::Result<()> {
    // Shared by local builds, main/shadow servers, and readers. A maintenance
    // pass never races another pass, and cache-entry locks fence publication.
    fs::create_dir_all(&paths.cache)?;
    let maintenance = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(paths.cache.join("retention.lock"))?;
    let Ok(_maintenance) = crate::manager::BuildPathLock::try_lock_file(maintenance) else {
        return Ok(());
    };
    let mut protected = BTreeSet::new();
    let mut source_hashes = BTreeSet::new();
    let mut terminal_jobs = Vec::new();
    for entry in regular_entries(&paths.jobs)? {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let status: BuildJobStatus = match read_object_file(&entry.path().join("status.sobj")) {
            Ok(status) => status,
            // Unknown state is not proof of quiescence: retain all caches.
            Err(_)
                if !entry.path().join("status.sobj").exists() && old(&entry.path(), now, grace) =>
            {
                sinomo_core::storage::scratch::remove_inactive_scratch_directory(&entry.path())?;
                continue;
            }
            Err(_) => return Ok(()),
        };
        if entry.file_name().to_str() != Some(status.job_id.as_str()) {
            return Ok(());
        }
        if !status.state.is_terminal() || !old(&entry.path().join("status.sobj"), now, grace) {
            protected.insert(status.key.path_key().to_string());
            if let Ok(plan) = read_object_file::<crate::BuildPlan>(&entry.path().join("plan.sobj"))
            {
                source_hashes.insert(
                    plan.source
                        .bundle_hash
                        .trim_start_matches("blake3:")
                        .to_string(),
                );
            }
        }
        if status.state.is_terminal() {
            cleanup_job_scratch(paths, &status.job_id);
            terminal_jobs.push((status.created_at, entry.path()));
        }
    }
    let mut groups =
        BTreeMap::<String, Vec<(SystemTime, PathBuf, BuildArtifactSetManifest)>>::new();
    for entry in regular_entries(&paths.cache)? {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some((key, job)) = name.split_once(".next-") {
            if digest_name(key)
                && BuildJobId::new(job.to_string()).is_ok()
                && !protected.contains(key)
                && old(&entry.path(), now, grace)
            {
                let lock = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(paths.cache.join(format!("{key}.lock")))?;
                if let Ok(_lease) = crate::manager::BuildPathLock::try_lock_file(lock) {
                    remove_owned_scratch_path(&entry.path())?;
                }
            }
            continue;
        }
        if !digest_name(&name) {
            continue;
        }
        let manifest: BuildArtifactSetManifest =
            match read_object_file::<BuildArtifactSetManifest>(&entry.path().join("manifest.sobj"))
            {
                Ok(manifest)
                    if manifest.validate().is_ok()
                        && manifest.provenance.build_key.path_key() == name =>
                {
                    manifest
                }
                _ => continue,
            };
        let used = fs::metadata(entry.path().join("last-used"))
            .or_else(|_| fs::metadata(entry.path().join("manifest.sobj")))?
            .modified()?;
        let input = &manifest.provenance.input;
        let group = serde_json::to_string(&(&input.target, &input.profile, &input.outputs))
            .map_err(std::io::Error::other)?;
        groups
            .entry(group)
            .or_default()
            .push((used, entry.path(), manifest));
    }
    for entries in groups.values_mut() {
        entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        for (index, (_, path, manifest)) in entries.iter().enumerate() {
            let name = path.file_name().unwrap().to_string_lossy();
            let recent = !old(&path.join("last-used"), now, grace)
                && path.join("last-used").exists()
                || !old(&path.join("manifest.sobj"), now, grace);
            if index < BUILD_CACHE_GENERATIONS || protected.contains(name.as_ref()) || recent {
                source_hashes.insert(
                    manifest
                        .provenance
                        .source
                        .bundle_hash
                        .trim_start_matches("blake3:")
                        .to_string(),
                );
                continue;
            }
            let lock = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path.with_extension("lock"))?;
            let Ok(_lease) = crate::manager::BuildPathLock::try_lock_file(lock) else {
                continue;
            };
            // A reader may have refreshed its transfer grace while this pass
            // was collecting manifests. Recheck after acquiring its lock.
            if path.join("last-used").exists() && !old(&path.join("last-used"), now, grace) {
                continue;
            }
            remove_owned_scratch_path(path)?;
        }
    }
    for entry in regular_entries(&paths.sources)? {
        if entry.file_type()?.is_dir()
            && entry.file_name().to_string_lossy().starts_with("prepare-")
            && old(&entry.path(), now, grace)
        {
            sinomo_core::storage::scratch::remove_inactive_scratch_directory(&entry.path())?;
            continue;
        }
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".source-")
            && name.ends_with(".bundle")
            && old(&entry.path(), now, grace)
        {
            remove_owned_scratch_path(&entry.path())?;
            continue;
        }
        let Some(hash) = name.strip_suffix(".bundle") else {
            continue;
        };
        if digest_name(hash) && !source_hashes.contains(hash) && old(&entry.path(), now, grace) {
            let marker = entry.path().with_extension("last-used");
            let lock = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(entry.path().with_extension("lock"))?;
            if fs2::FileExt::try_lock_exclusive(&lock).is_err()
                || (marker.exists() && !old(&marker, now, grace))
            {
                continue;
            }
            remove_owned_scratch_path(&entry.path())?;
            remove_owned_scratch_path(&marker)?;
        }
    }
    for entry in regular_entries(&paths.checkouts)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type()?.is_dir()
            || name.len() != 16
            || !name.bytes().all(|b| b.is_ascii_hexdigit())
            || !old(&entry.path(), now, grace)
        {
            continue;
        }
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(entry.path().with_extension("lock"))?;
        if fs2::FileExt::try_lock_exclusive(&lock).is_ok() {
            remove_owned_scratch_path(&entry.path())?;
        }
    }
    // Retain recent pollable records plus the latest 64 terminal jobs.
    terminal_jobs.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    for (_, path) in terminal_jobs.into_iter().skip(64) {
        if old(&path.join("status.sobj"), now, grace) {
            remove_owned_scratch_path(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BuildArtifactManifest, BuildJobState, BuildKeyInput, BuildOutputRequest, BuildPlan,
        BuildProvenance, BuildSource, BuildTarget, build_content_hash,
    };
    use sinomo_core::network::write_object_file;

    fn cache(paths: &BuildPaths, generation: u64) -> BuildPlan {
        let bytes = format!("source-{generation}");
        let hash = build_content_hash(bytes.as_bytes());
        let revision = format!("{generation:040x}");
        let plan = BuildPlan::new(
            BuildSource {
                label: "test-source".into(),
                revision: Some(revision.clone()),
                identity_hash: None,
                bundle_hash: hash.clone(),
                bundle_bytes: bytes.len() as u64,
            },
            BuildKeyInput {
                source_hash: hash,
                source_revision: Some(revision),
                lockfile_hash: build_content_hash(b"lock"),
                target: BuildTarget::new("linux", "x86_64")
                    .unwrap()
                    .with_rust_target("x86_64-unknown-linux-gnu")
                    .unwrap(),
                profile: "release".into(),
                features: vec![],
                toolchain_fingerprint: "test".into(),
                outputs: vec![BuildOutputRequest::binary("sinomo", "sinomo")],
            },
        )
        .unwrap();
        let root = paths.cache_artifacts_dir(&plan.key);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("sinomo"), b"binary").unwrap();
        let provenance = BuildProvenance::from_plan(&plan).unwrap();
        let artifact = BuildArtifactManifest::binary_from_file(
            "binary",
            "sinomo",
            "sinomo",
            provenance.clone(),
            &root,
        )
        .unwrap();
        let manifest = BuildArtifactSetManifest::new(provenance, vec![artifact]).unwrap();
        write_object_file(&paths.cache_manifest(&plan.key), &manifest).unwrap();
        fs::File::options()
            .write(true)
            .open(paths.cache_manifest(&plan.key))
            .unwrap()
            .set_times(
                fs::FileTimes::new()
                    .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(generation)),
            )
            .unwrap();
        plan
    }

    fn job(
        paths: &BuildPaths,
        plan: &BuildPlan,
        state: BuildJobState,
        generation: i64,
    ) -> BuildJobId {
        let id = BuildJobId::generate();
        write_object_file(&paths.job_plan(&id), plan).unwrap();
        let when = time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(generation);
        write_object_file(
            &paths.job_status(&id),
            &BuildJobStatus {
                job_id: id.clone(),
                key: plan.key.clone(),
                state,
                queue_position: None,
                detail: "test".into(),
                artifact_set: None,
                created_at: when,
                started_at: None,
                finished_at: state.is_terminal().then_some(when),
            },
        )
        .unwrap();
        fs::write(paths.job_source_bundle(&id), b"source").unwrap();
        fs::create_dir_all(paths.job_work_dir(&id)).unwrap();
        fs::write(paths.job_work_dir(&id).join("output"), b"output").unwrap();
        id
    }

    #[test]
    fn repeated_terminal_builds_bound_cache_and_jobs_and_leave_no_scratch() {
        let root = sinomo_testdir::TestDir::new("build-retention-repeat");
        let paths = BuildPaths::new(&root);
        paths.ensure_layout().unwrap();
        for generation in 1..=70 {
            let plan = cache(&paths, generation);
            let id = job(
                &paths,
                &plan,
                if generation % 2 == 0 {
                    BuildJobState::Succeeded
                } else {
                    BuildJobState::Failed
                },
                generation as i64,
            );
            prune_at(&paths, SystemTime::now(), Duration::ZERO).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while paths.job_source_bundle(&id).exists() && std::time::Instant::now() < deadline {
                // A concurrent fork can temporarily inherit the maintenance
                // descriptor. Cleanup retries only after that owner releases it.
                std::thread::sleep(Duration::from_millis(5));
                prune_at(&paths, SystemTime::now(), Duration::ZERO).unwrap();
            }
            assert!(!paths.job_source_bundle(&id).exists());
            assert!(!paths.job_work_dir(&id).exists());
            let caches = regular_entries(&paths.cache)
                .unwrap()
                .into_iter()
                .filter(|e| e.file_type().unwrap().is_dir())
                .count();
            assert_eq!(caches, (generation as usize).min(BUILD_CACHE_GENERATIONS));
        }
        assert_eq!(regular_entries(&paths.jobs).unwrap().len(), 64);
        assert!(regular_entries(&paths.work).unwrap().is_empty());
    }

    #[test]
    fn crashed_cache_publication_is_reclaimed_only_after_its_writer_releases_the_lock() {
        let root = sinomo_testdir::TestDir::new("build-retention-interrupted-publish");
        let paths = BuildPaths::new(&root);
        paths.ensure_layout().unwrap();
        let plan = cache(&paths, 1);
        let staging = paths
            .cache_entry_dir(&plan.key)
            .with_extension(format!("next-{}", BuildJobId::generate()));
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("partial"), b"partial").unwrap();
        let lease = crate::manager::BuildPathLock::acquire(
            &paths.cache_entry_dir(&plan.key).with_extension("lock"),
        )
        .unwrap();
        prune_at(&paths, SystemTime::now(), Duration::ZERO).unwrap();
        assert!(staging.exists());
        drop(lease);
        prune_at(&paths, SystemTime::now(), Duration::ZERO).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while staging.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
            prune_at(&paths, SystemTime::now(), Duration::ZERO).unwrap();
        }
        assert!(!staging.exists());
        assert!(paths.cache_manifest(&plan.key).exists());
    }

    #[test]
    fn active_jobs_download_grace_and_cache_leases_survive_pruning() {
        let root = sinomo_testdir::TestDir::new("build-retention-pins");
        let paths = BuildPaths::new(&root);
        paths.ensure_layout().unwrap();
        let plans = (1..=9).map(|g| cache(&paths, g)).collect::<Vec<_>>();
        let id = job(&paths, &plans[0], BuildJobState::Running, 1);
        touch_cache(&paths, &plans[1].key).unwrap();
        let lease = crate::manager::BuildPathLock::acquire(
            &paths.cache_entry_dir(&plans[2].key).with_extension("lock"),
        )
        .unwrap();
        prune_at(&paths, SystemTime::now(), BUILD_TRANSFER_GRACE).unwrap();
        for index in [0, 1, 2, 6, 7, 8] {
            assert!(paths.cache_manifest(&plans[index].key).exists());
        }
        assert!(!paths.cache_manifest(&plans[3].key).exists());
        assert!(!paths.cache_manifest(&plans[4].key).exists());
        assert!(paths.job_source_bundle(&id).exists());
        assert!(paths.job_work_dir(&id).exists());
        drop(lease);
        prune_at(&paths, SystemTime::now(), BUILD_TRANSFER_GRACE).unwrap();
        assert!(!paths.cache_manifest(&plans[2].key).exists());
    }
}
