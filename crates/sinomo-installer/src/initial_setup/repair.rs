//! Offline replacement of installation-owned runtime files. Application stores,
//! managed source and node-local consent are outside the repair plan.
use super::*;
use sinomo_core::storage::scratch::ScratchDirectory;

pub(super) struct RepairPlan {
    _payload: crate::bundle::PreparedPayload,
    _scratch: ScratchDirectory,
    files: Vec<(PathBuf, PathBuf)>,
}

impl RepairPlan {
    pub(super) fn prepare(
        executable: &Path,
        desktop_source: Option<&Path>,
        root: &Path,
        data: &Path,
    ) -> Result<Self> {
        let bundled_cli = desktop_source.map(|source| source.join(binary("sinomo")));
        let executable = bundled_cli.as_deref().unwrap_or(executable);
        let package = executable.parent().ok_or("no repair package directory")?;
        // Repair never substitutes the retained bootstrap record for incoming
        // bytes, and never requires the installed runtime/data to be readable.
        let payload = crate::bundle::prepare_repair(package, data, executable)?;
        let scratch = ScratchDirectory::create(&env::temp_dir().join("sinomo-setup"), "repair")?;
        let stage = scratch.path();
        let cli = stage.join(binary("sinomo"));
        copy_executable(executable, &cli)?;
        let mut files = vec![
            (cli.clone(), data.join("bin").join(binary("sinomo"))),
            (cli, data.join("bin").join(binary("sinomo-shadow"))),
        ];
        files.extend(payload.stage_repair(data, &stage.join("modules"))?);
        if let Some(source) = desktop_source {
            let manifest = desktop::read_manifest(source)?;
            desktop::verify_bundle(source, &manifest)?;
            // Let the rollout owner produce an immutable bundle and its initial
            // selection in isolation. Only package-owned files are published;
            // workspace snapshots and existing application data are never read.
            let desktop_data = stage.join("desktop");
            let releases = desktop::DesktopReleases::new(&desktop_data);
            let status = releases.install_initial(source)?;
            let id = status
                .active_release
                .ok_or("repair desktop selection missing")?;
            let release = releases.release_dir(&id)?;
            let destination = desktop::DesktopReleases::new(data).release_dir(&id)?;
            for file in &manifest.files {
                files.push((release.join(&file.path), destination.join(&file.path)));
                if desktop::is_desktop_payload_file(&file.path) {
                    files.push((release.join(&file.path), data.join("bin").join(&file.path)));
                }
            }
            files.push((
                release.join("desktop-release.json"),
                destination.join("desktop-release.json"),
            ));
            files.push((
                release.join("desktop-release.json"),
                data.join("bin/desktop-release.json"),
            ));
            files.push((
                desktop_data.join("rollout/desktop/state.json"),
                data.join("rollout/desktop/state.json"),
            ));
            let setup = source.join(binary("sinomo-setup"));
            if setup.is_file() {
                let staged = stage.join(binary("sinomo-setup"));
                copy_executable(&setup, &staged)?;
                #[cfg(target_os = "linux")]
                let destination = data.join("bin").join(binary("sinomo-setup"));
                #[cfg(not(target_os = "linux"))]
                let destination = root.join(binary("sinomo-setup"));
                files.push((staged, destination));
            }
        }
        #[cfg(target_os = "linux")]
        let _ = root;
        Ok(Self {
            _payload: payload,
            _scratch: scratch,
            files,
        })
    }

    pub(super) fn apply(&self, data: &Path) -> Result<()> {
        for (source, destination) in &self.files {
            fs::create_dir_all(destination.parent().ok_or("no runtime directory")?)?;
            match fs::remove_file(destination) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(
                        format!("could not replace {}: {error}", destination.display()).into(),
                    );
                }
            }
            fs::copy(source, destination).map_err(|error| {
                format!("could not overwrite {}: {error}", destination.display())
            })?;
        }
        let paths = sinomo_rollout::SelfRolloutPaths::new(data);
        // Explicit repair supersedes pending local runtime activations.
        for pending in [
            sinomo_rollout::rollout_main_activation_path(&paths),
            sinomo_rollout::rollout_shadow_finalize_path(&paths),
        ] {
            sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(&pending)?;
        }
        // Orphaned native pre-mutation intents would otherwise restore the
        // previous loader references on startup, undoing the explicit repair.
        let native = sinomo_rollout::rollout_native_dylib_loader_rollback_dir(&paths);
        match fs::read_dir(native) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if entry.file_type()?.is_file() {
                        fs::remove_file(entry.path())?;
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(root: &Path, library: bool) -> PathBuf {
        use crate::bundle::{PAYLOAD, PayloadLibrary, PayloadManifest, PayloadModule, hash};
        fs::create_dir_all(root).unwrap();
        let cli = root.join(binary("sinomo"));
        fs::write(&cli, b"older packaged CLI").unwrap();
        fs::write(
            root.join("source.tar"),
            b"source is retained, never imported by repair",
        )
        .unwrap();
        fs::write(root.join("module.bin"), b"unselected module").unwrap();
        fs::write(root.join("runtime.bin"), b"packaged library").unwrap();
        let manifest = PayloadManifest {
            schema: 1,
            os: env::consts::OS.into(),
            arch: env::consts::ARCH.into(),
            revision: "older".into(),
            source_hash: hash(&root.join("source.tar")).unwrap(),
            cli_hash: hash(&cli).unwrap(),
            modules: vec![PayloadModule {
                file: "module.bin".into(),
                hash: hash(&root.join("module.bin")).unwrap(),
                metadata: sinomo_build::NativeDylibArtifactMetadata::new(
                    sinomo_module::ModuleId::from_legacy_named("module.repair").unwrap(),
                    "v1",
                    "repair",
                    "module.bin",
                    "fixture",
                ),
            }],
            libraries: if library {
                vec![PayloadLibrary {
                    file: "runtime.bin".into(),
                    hash: hash(&root.join("runtime.bin")).unwrap(),
                }]
            } else {
                Vec::new()
            },
        };
        fs::write(
            root.join("initial-payload.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let mut tar = tar::Builder::new(fs::File::create(root.join(PAYLOAD)).unwrap());
        for (file, name) in [
            ("source.tar", "source.tar"),
            ("module.bin", "module.bin"),
            ("initial-payload.json", "initial-payload.json"),
        ] {
            tar.append_path_with_name(root.join(file), name).unwrap();
        }
        if library {
            tar.append_path_with_name(root.join("runtime.bin"), "lib/runtime.bin")
                .unwrap();
        }
        tar.finish().unwrap();
        cli
    }

    #[test]
    fn repair_validates_incoming_cli_without_reading_retained_data_format() {
        let fixture = sinomo_testdir::TestDir::new("repair-preflight");
        let data = fixture.join("data");
        fs::create_dir_all(data.join("config")).unwrap();
        fs::write(
            data.join("config/initial-payload.json"),
            b"future incompatible data format",
        )
        .unwrap();
        let cli = package(&fixture.join("package"), false);
        crate::bundle::prepare_repair(cli.parent().unwrap(), &data, &cli).unwrap();
        fs::write(&cli, b"tampered CLI").unwrap();
        assert!(
            crate::bundle::prepare_repair(cli.parent().unwrap(), &data, &cli)
                .err()
                .unwrap()
                .to_string()
                .contains("CLI does not match")
        );
        assert_eq!(
            fs::read(data.join("config/initial-payload.json")).unwrap(),
            b"future incompatible data format"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "isolated subprocess fixture"]
    fn offline_repair_fixture() {
        let Some(fixture) = env::var_os("SINOMO_REPAIR_FIXTURE") else {
            return;
        };
        let fixture = PathBuf::from(fixture);
        let root = fixture.join("setup");
        let data = fixture.join("data");
        fs::create_dir_all(data.join("bin")).unwrap();
        for name in ["sinomo", "sinomo-shadow"] {
            fs::write(data.join("bin").join(name), b"installed runtime").unwrap();
        }
        let options = services::Options {
            start: false,
            startup: Some(false),
            ..services::Options::default()
        };
        let cli = package(&fixture.join("package"), false);
        install(
            &cli,
            None,
            &root,
            &data,
            &IntegrationOptions {
                add_to_path: Some(true),
                ..IntegrationOptions::default()
            },
            &options,
        )
        .unwrap();
        let config = configuration(&root).unwrap();
        let stores = [
            "config/node-permissions.sobj",
            "kv/raft/data",
            "configs/app/workspace.json",
            "source/sinomo/.git/HEAD",
            "source/sinomo/Cargo.lock",
            "config/initial-payload.json",
        ];
        for path in stores {
            let path = data.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"future application data, unchanged").unwrap();
        }
        fs::remove_file(data.join("bin/sinomo")).unwrap();
        fs::write(data.join("bin/sinomo-shadow"), b"broken executable").unwrap();
        assert!(
            install(
                &cli,
                None,
                &root,
                &data,
                &IntegrationOptions::default(),
                &options
            )
            .is_err()
        );
        let paths = sinomo_rollout::SelfRolloutPaths::new(&data);
        let pending = sinomo_rollout::rollout_main_activation_path(&paths);
        fs::write(&pending, b"original uncertain activation").unwrap();
        let native_pending = sinomo_rollout::rollout_native_dylib_loader_rollback_dir(&paths)
            .join("future-intent.sobj");
        fs::create_dir_all(native_pending.parent().unwrap()).unwrap();
        fs::write(&native_pending, b"unreadable native activation intent").unwrap();
        let payload = cli.parent().unwrap().join(crate::bundle::PAYLOAD);
        let saved = payload.with_extension("saved");
        fs::rename(&payload, &saved).unwrap();
        assert!(
            repair(
                &cli,
                None,
                &root,
                &data,
                &IntegrationOptions::default(),
                &options
            )
            .is_err()
        );
        assert!(!data.join(local::REMOVAL_FENCE).exists());
        assert!(pending.exists());
        fs::rename(saved, payload).unwrap();
        repair(
            &cli,
            None,
            &root,
            &data,
            &IntegrationOptions::default(),
            &options,
        )
        .unwrap();
        for name in ["sinomo", "sinomo-shadow"] {
            assert_eq!(
                fs::read(data.join("bin").join(name)).unwrap(),
                b"older packaged CLI"
            );
        }
        assert_eq!(configuration(&root).unwrap(), config);
        assert!(!pending.exists());
        assert!(!data.join("rollout/installer-repairs").exists());
        assert!(!native_pending.exists());
        assert!(!root.join("sinomo-setup-recovery.json").exists());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("backup")
        }));
        assert!(fs::read_dir(data.join("bin")).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("previous")
        }));
        for path in stores {
            assert_eq!(
                fs::read(data.join(path)).unwrap(),
                b"future application data, unchanged"
            );
        }
        // Repair overwrites directly. A later copy error leaves completed
        // overwrites in place and the node stopped; rerunning repair can finish.
        fs::write(data.join("bin/sinomo"), b"old runtime").unwrap();
        fs::create_dir_all(data.join("bin/lib/runtime.bin")).unwrap();
        let failing = package(&fixture.join("bad-publication"), true);
        assert!(
            repair(
                &failing,
                None,
                &root,
                &data,
                &IntegrationOptions::default(),
                &options
            )
            .is_err()
        );
        assert_eq!(
            fs::read(data.join("bin/sinomo")).unwrap(),
            b"older packaged CLI"
        );
        assert!(data.join(local::REMOVAL_FENCE).exists());
        for path in stores {
            assert_eq!(
                fs::read(data.join(path)).unwrap(),
                b"future application data, unchanged"
            );
        }
        // Readiness failure follows replacement. Incompatible older
        // executables remain installed instead of silently reverting the repair.
        let error = repair(
            &cli,
            None,
            &root,
            &data,
            &IntegrationOptions::default(),
            &services::Options {
                start: true,
                ..options.clone()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("Packaged binaries repaired and data retained"),
            "{error}"
        );
        assert!(error.contains("Downgrades are not supported"));
        assert_eq!(
            fs::read(data.join("bin/sinomo")).unwrap(),
            b"older packaged CLI"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn offline_repair_preserves_data_and_keeps_completed_overwrites() {
        let fixture = sinomo_testdir::TestDir::new("offline-repair");
        let home = fixture.join("home");
        fs::create_dir_all(&home).unwrap();
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "initial_setup::repair::tests::offline_repair_fixture",
                "--nocapture",
            ])
            .env("SINOMO_REPAIR_FIXTURE", fixture.path())
            .env("HOME", home)
            .env("XDG_CONFIG_HOME", fixture.join("config"))
            .env("XDG_DATA_HOME", fixture.join("xdg-data"))
            .env(
                "DBUS_SESSION_BUS_ADDRESS",
                format!("unix:path={}", fixture.join("no-bus").display()),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
