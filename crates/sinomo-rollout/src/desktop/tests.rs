use super::*;
use sinomo_core::local::SinomoDataDir;

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
fn repeated_desktop_staging_and_activation_reclaim_releases_and_workspace_backups() {
    let root = sinomo_testdir::TestDir::new("desktop-retention-repeat");
    let releases = DesktopReleases::new(root.join("data"));
    let initial = bundle(&root, "initial");
    let first = releases
        .install_initial(&initial)
        .unwrap()
        .active_release
        .unwrap();
    let token = releases.begin_launch(false).unwrap();
    releases
        .acknowledge_ready(AppUpdateReadyRequest {
            launch_token: token.clone(),
            restoration_schema: 1,
        })
        .unwrap();
    let workspace = releases.workspace_path().unwrap();
    fs::create_dir_all(workspace.parent().unwrap()).unwrap();
    fs::write(&workspace, b"drafts and durable cursor").unwrap();
    for generation in 1..=12 {
        let source = bundle(&root, &format!("release-{generation}"));
        let staged = releases.stage(&source).unwrap();
        let launch = releases.read().unwrap().launch_token.unwrap();
        releases
            .request_restart(AppUpdateRestartRequest {
                launch_token: launch,
                candidate_release: staged.candidate_release.unwrap(),
            })
            .unwrap();
        let token = releases.begin_launch(true).unwrap();
        let backup = releases.workspace_backup_path(&token).unwrap();
        assert!(backup.exists());
        releases
            .acknowledge_ready(AppUpdateReadyRequest {
                launch_token: token,
                restoration_schema: 1,
            })
            .unwrap();
        // Maintenance defers behind a publisher (or a descriptor inherited by
        // concurrent fork-based tests). Retry at a later operation boundary.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while backup.exists() && std::time::Instant::now() < deadline {
            releases.prune_best_effort();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(!backup.exists());
        let status = releases.status().unwrap();
        assert!(
            releases
                .release_dir(status.active_release.as_ref().unwrap())
                .unwrap()
                .exists()
        );
        assert!(
            releases
                .release_dir(status.previous_release.as_ref().unwrap())
                .unwrap()
                .exists()
        );
        assert!(
            fs::read_dir(releases.root.join("releases"))
                .unwrap()
                .count()
                <= crate::ROLLOUT_ARTIFACT_GENERATIONS + 3
        );
    }
    assert!(!releases.release_dir(&first).unwrap().exists());
    assert_eq!(fs::read(workspace).unwrap(), b"drafts and durable cursor");
}

#[test]
fn desktop_retention_keeps_rollback_workspace_and_defers_for_live_publisher() {
    let root = sinomo_testdir::TestDir::new("desktop-retention-live");
    let releases = DesktopReleases::new(root.join("data"));
    releases.install_initial(&bundle(&root, "old")).unwrap();
    let token = releases.begin_launch(false).unwrap();
    let workspace = releases.workspace_path().unwrap();
    fs::create_dir_all(workspace.parent().unwrap()).unwrap();
    fs::write(workspace, b"restore").unwrap();
    let candidate = releases
        .stage(&bundle(&root, "candidate"))
        .unwrap()
        .candidate_release
        .unwrap();
    releases
        .request_restart(AppUpdateRestartRequest {
            launch_token: token,
            candidate_release: candidate.clone(),
        })
        .unwrap();
    let launching = releases.begin_launch(true).unwrap();
    let backup = releases.workspace_backup_path(&launching).unwrap();
    releases.prune_best_effort();
    assert!(backup.exists());
    assert!(releases.release_dir(&candidate).unwrap().exists());
    let _publisher = releases.lock("stage.lock").unwrap();
    releases.prune_best_effort(); // try-lock must return without waiting on this publisher
    assert!(backup.exists());
}

#[test]
fn installed_legacy_bundle_uses_bootstrap_cli_but_cannot_be_staged_again() {
    let root = sinomo_testdir::TestDir::new("desktop-installed-legacy");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let releases = DesktopReleases::new(data_dir.root());
    let directory = bundle(&root, "legacy");
    let mut manifest = read_manifest(&directory).unwrap();
    let cli_name = crate::executable_name("sinomo");
    let legacy_name = crate::executable_name("sinomo-app-daemon");
    let entry = manifest
        .files
        .iter_mut()
        .find(|file| file.path == cli_name)
        .unwrap();
    fs::rename(directory.join(&cli_name), directory.join(&legacy_name)).unwrap();
    entry.path = legacy_name.clone();
    atomic_json(&directory.join(APP_BUNDLE_MANIFEST), &manifest).unwrap();
    assert!(
        releases
            .stage(&directory)
            .unwrap_err()
            .contains("missing sinomo")
    );

    // Reconstruct a release installed by the previous version without changing the new
    // staging policy. Its manifest identity and every historical file remain mandatory.
    let id = release_id(&manifest).unwrap();
    let installed = releases.release_dir(&id).unwrap();
    fs::create_dir_all(installed.parent().unwrap()).unwrap();
    fs::rename(directory, &installed).unwrap();
    releases
        .change(|state| {
            state.status.active_release = Some(id);
            Ok(())
        })
        .unwrap();
    let bootstrap = data_dir
        .root()
        .join("bin")
        .join(crate::executable_name("sinomo-app"));
    fs::create_dir_all(bootstrap.parent().unwrap()).unwrap();
    fs::write(&bootstrap, b"installed bootstrap").unwrap();
    let error = supervise_app(&data_dir, &bootstrap, &[], |cli| {
        assert!(
            cli.is_none(),
            "legacy helpers must not be selected as CLI hosts"
        );
        Err::<(), _>("verified legacy launch".into())
    })
    .unwrap_err();
    assert_eq!(error, "verified legacy launch");

    fs::write(installed.join(legacy_name), b"tampered old helper").unwrap();
    let error = supervise_app(&data_dir, &bootstrap, &[], |_| -> Result<()> {
        panic!("tampered legacy bundle must fail before runtime startup")
    })
    .unwrap_err();
    assert!(error.contains("mismatch"));
}

#[test]
fn explicit_launch_ignores_invalid_installed_activation_state() {
    let root = sinomo_testdir::TestDir::new("desktop-explicit-launch");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let releases = DesktopReleases::new(data_dir.root());
    releases.ensure_root().unwrap();
    let state_path = releases.root.join("state.json");
    let state = b"invalid installed activation state";
    fs::write(&state_path, state).unwrap();
    for directory in ["target/debug", "downloaded build"] {
        let app = root
            .join(directory)
            .join(crate::executable_name("sinomo-app"));
        fs::create_dir_all(app.parent().unwrap()).unwrap();
        fs::write(&app, b"explicit executable").unwrap();
        let error = supervise_app(&data_dir, &app, &[], |cli| {
            assert!(
                cli.is_none(),
                "explicit launches must use their caller's CLI"
            );
            Err::<(), _>("explicit runtime selected".into())
        })
        .unwrap_err();
        assert_eq!(error, "explicit runtime selected");
        assert_eq!(fs::read(&state_path).unwrap(), state);
    }
}

#[test]
fn installed_launchers_and_images_still_require_a_verified_active_bundle() {
    let root = sinomo_testdir::TestDir::new("desktop-managed-launch");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let releases = DesktopReleases::new(data_dir.root());
    let active = releases
        .install_initial(&bundle(&root, "installed"))
        .unwrap()
        .active_release
        .unwrap();
    let directory = releases.release_dir(&active).unwrap();
    let app_name = crate::executable_name("sinomo-app");
    let launcher = data_dir.root().join("bin").join(&app_name);
    fs::create_dir_all(launcher.parent().unwrap()).unwrap();
    fs::write(&launcher, b"stable launcher").unwrap();
    let cli = directory.join(crate::executable_name("sinomo"));
    for app in [&launcher, &directory.join(&app_name)] {
        let error = supervise_app(&data_dir, app, &[], |selected| {
            assert_eq!(selected, Some(cli.as_path()));
            Err::<(), _>("managed runtime selected".into())
        })
        .unwrap_err();
        assert_eq!(error, "managed runtime selected");
    }
    fs::write(directory.join(app_name), b"tampered installed image").unwrap();
    let error = supervise_app(&data_dir, &launcher, &[], |_| -> Result<()> {
        panic!("a managed launch must verify the bundle before starting its runtime")
    })
    .unwrap_err();
    assert!(error.contains("mismatch"));
}

#[test]
fn desktop_bundle_requires_and_verifies_the_cli_host() {
    let root = sinomo_testdir::TestDir::new("desktop-cli-bundle");
    let directory = bundle(&root, "cli-host");
    let manifest = read_manifest(&directory).unwrap();
    verify_bundle(&directory, &manifest).unwrap();
    let cli = crate::executable_name("sinomo");
    let mut missing_cli = manifest.clone();
    missing_cli.files.retain(|file| file.path != cli);
    assert!(
        validate_manifest(&missing_cli)
            .unwrap_err()
            .contains(&format!("missing {cli}"))
    );
    fs::write(directory.join(cli), b"tampered CLI").unwrap();
    assert!(verify_bundle(&directory, &manifest).is_err());
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
