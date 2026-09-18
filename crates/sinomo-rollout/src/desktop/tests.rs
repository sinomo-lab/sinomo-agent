use super::*;

pub(super) fn bundle(root: &Path, version: &str) -> PathBuf {
    let directory = root.join(version);
    fs::create_dir_all(&directory).unwrap();
    let names: Vec<_> = DESKTOP_BINARY_STEMS
        .into_iter()
        .map(crate::executable_name)
        .collect();
    for name in &names {
        fs::write(directory.join(name), format!("{name}-{version}")).unwrap();
    }
    write_bundle_manifest(&directory, version.into(), &names).unwrap();
    directory
}

#[test]
fn stage_requires_complete_verified_compatible_bundle_and_preserves_active_state() {
    let root = sinomo_testdir::TestDir::new("desktop-verified-stage");
    let releases = DesktopReleases::new(root.join("data"));
    let old = bundle(&root, "old");
    let current = releases.install_initial(&old).unwrap();
    let candidate = bundle(&root, "new");
    fs::write(
        candidate.join(crate::executable_name("sinomo-app")),
        "tampered",
    )
    .unwrap();
    assert!(
        releases
            .stage(&candidate)
            .unwrap_err()
            .contains("length mismatch")
    );
    assert_eq!(releases.status().unwrap(), current);
    let mut manifest = read_manifest(&old).unwrap();
    manifest.target_os = "not-this-os".into();
    atomic_json(&old.join(APP_BUNDLE_MANIFEST), &manifest).unwrap();
    assert!(releases.stage(&old).unwrap_err().contains("platform"));
    assert_eq!(releases.status().unwrap(), current);
}

#[test]
fn activation_requires_current_launch_and_only_commits_after_restoration() {
    let root = sinomo_testdir::TestDir::new("desktop-ready-transaction");
    let releases = DesktopReleases::new(root.join("data"));
    let old = releases
        .install_initial(&bundle(&root, "old"))
        .unwrap()
        .active_release
        .unwrap();
    let launch = releases.begin_launch(false).unwrap();
    releases
        .acknowledge_ready(AppUpdateReadyRequest {
            launch_token: launch.clone(),
            restoration_schema: 1,
        })
        .unwrap();
    let candidate = releases
        .stage(&bundle(&root, "new"))
        .unwrap()
        .candidate_release
        .unwrap();
    let request = AppUpdateRestartRequest {
        candidate_release: candidate.clone(),
        launch_token: launch.clone(),
    };
    assert!(
        releases
            .request_restart(AppUpdateRestartRequest {
                launch_token: "wrong".into(),
                ..request.clone()
            })
            .is_err()
    );
    releases.request_restart(request.clone()).unwrap();
    // Lost acknowledgement: the same launch/candidate intent remains retryable without
    // starting another activation or losing the previous release.
    releases.request_restart(request).unwrap();
    let new_launch = releases.begin_launch(true).unwrap();
    assert_eq!(
        releases.status().unwrap().active_release.as_deref(),
        Some(old.as_str())
    );
    assert!(
        releases
            .acknowledge_ready(AppUpdateReadyRequest {
                launch_token: launch,
                restoration_schema: 1
            })
            .is_err()
    );
    let current = releases
        .acknowledge_ready(AppUpdateReadyRequest {
            launch_token: new_launch,
            restoration_schema: 1,
        })
        .unwrap();
    assert_eq!(current.phase, AppUpdatePhase::Current);
    assert_eq!(current.active_release, Some(candidate));
    assert_eq!(current.previous_release, Some(old));
    assert!(current.candidate_release.is_none());
    assert_eq!(
        DesktopReleases::new(root.join("data")).status().unwrap(),
        current
    );
}

#[test]
fn interrupted_candidate_keeps_previous_bundle_and_rejects_late_readiness() {
    let root = sinomo_testdir::TestDir::new("desktop-rollback");
    let releases = DesktopReleases::new(root.join("data"));
    let active = releases
        .install_initial(&bundle(&root, "old"))
        .unwrap()
        .active_release;
    let token = releases.begin_launch(false).unwrap();
    let workspace = root.join("data").join(APP_WORKSPACE_SNAPSHOT_PATH);
    fs::create_dir_all(workspace.parent().unwrap()).unwrap();
    fs::write(&workspace, b"saved drafts and exact event cursor").unwrap();
    let candidate = releases
        .stage(&bundle(&root, "new"))
        .unwrap()
        .candidate_release
        .unwrap();
    releases
        .request_restart(AppUpdateRestartRequest {
            candidate_release: candidate,
            launch_token: token,
        })
        .unwrap();
    let failed_token = releases.begin_launch(true).unwrap();
    fs::write(
        &workspace,
        b"candidate wrote an incompatible presentation state",
    )
    .unwrap();
    releases.rollback("readiness timed out".into()).unwrap();
    assert_eq!(
        fs::read(&workspace).unwrap(),
        b"saved drafts and exact event cursor"
    );
    assert_eq!(releases.status().unwrap().active_release, active);
    assert_eq!(releases.status().unwrap().phase, AppUpdatePhase::RolledBack);
    assert!(
        releases
            .acknowledge_ready(AppUpdateReadyRequest {
                launch_token: failed_token,
                restoration_schema: 1
            })
            .is_err()
    );
}

#[test]
fn bundle_paths_cannot_escape_or_alias_and_live_stage_cannot_replace_pending_update() {
    let root = sinomo_testdir::TestDir::new("desktop-paths");
    let candidate = bundle(&root, "new");
    let base = read_manifest(&candidate).unwrap();
    for path in [
        "../escape",
        "/absolute",
        "a/../b",
        "a//b",
        "a/./b",
        "C:stream",
        "a\\b",
        "bad.",
    ] {
        let mut manifest = base.clone();
        manifest.files[0].path = path.into();
        assert!(validate_manifest(&manifest).is_err(), "{path}");
    }
    let mut duplicate = base.clone();
    duplicate.files.push(base.files[0].clone());
    assert!(validate_manifest(&duplicate).is_err());
    let releases = DesktopReleases::new(root.join("data"));
    let id = releases
        .stage(&candidate)
        .unwrap()
        .candidate_release
        .unwrap();
    let token = releases.begin_launch(false).unwrap();
    releases
        .request_restart(AppUpdateRestartRequest {
            candidate_release: id,
            launch_token: token,
        })
        .unwrap();
    let pending = releases.status().unwrap();
    assert!(releases.stage(&bundle(&root, "other")).is_err());
    assert_eq!(releases.status().unwrap(), pending);
}

#[cfg(unix)]
#[test]
fn bundle_rejects_symlinks_even_when_the_target_bytes_match() {
    let root = sinomo_testdir::TestDir::new("desktop-symlink");
    let directory = bundle(&root, "new");
    let file = directory.join(crate::executable_name("sinomo-app"));
    let other = root.join("elsewhere");
    fs::rename(&file, &other).unwrap();
    std::os::unix::fs::symlink(&other, &file).unwrap();
    assert!(
        DesktopReleases::new(root.join("data"))
            .stage(&directory)
            .unwrap_err()
            .contains("symlink")
    );
}
