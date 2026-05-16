use super::*;

#[test]
fn sandbox_toolchain_manifest_records_command_hashes_and_paths() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "sinomo-maintained test build",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let command = SandboxToolchainCommand::new(
        &toolchain_id,
        "cat",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");

    assert_eq!(
        sandbox_toolchain_manifest_path(&toolchain_id),
        format!("/services/sandbox/toolchains/{toolchain_id}/manifest.sobj")
    );
    assert_eq!(
        command.module_path,
        format!("/services/sandbox/toolchains/{toolchain_id}/bin/cat.wasm")
    );
    assert_eq!(command.content_hash, sandbox_module_hash(b"wasm bytes"));
    assert_eq!(
        command.effective_target_abi(&manifest),
        SandboxWasiAbi::Preview1
    );
    let mut wasix_command = SandboxToolchainCommand::new(
        &toolchain_id,
        "bash",
        b"wasix bytes",
        SandboxResourceLimits::default(),
    )
    .expect("wasix command");
    wasix_command.target_abi = Some(SandboxWasiAbi::Wasix);
    assert_eq!(
        wasix_command.effective_target_abi(&manifest),
        SandboxWasiAbi::Wasix
    );
    manifest.insert_command(command).expect("insert");
    manifest
        .insert_command(wasix_command)
        .expect("insert wasix");
    let artifact =
        SandboxToolchainArtifact::new(&toolchain_id, "lib/python/os.py", b"support bytes")
            .expect("artifact");
    assert_eq!(
        artifact.sifs_path,
        format!("/services/sandbox/toolchains/{toolchain_id}/lib/python/os.py")
    );
    assert_eq!(artifact.content_hash, sandbox_module_hash(b"support bytes"));
    assert_eq!(artifact.byte_len, b"support bytes".len() as u64);
    manifest
        .insert_artifact(artifact.clone())
        .expect("insert artifact");
    assert_eq!(
        manifest.artifact("lib/python/os.py").expect("artifact"),
        &artifact
    );
    assert!(matches!(
        SandboxToolchainArtifact::new(&toolchain_id, "../escape", b"bytes")
            .expect_err("path escape"),
        SandboxToolchainError::InvalidArtifactPath { .. }
    ));
    assert!(matches!(
        manifest
            .insert_artifact(
                SandboxToolchainArtifact::new(&toolchain_id, "lib/python/os.py", b"other")
                    .expect("duplicate artifact")
            )
            .expect_err("duplicate artifact"),
        SandboxToolchainError::DuplicateArtifact { .. }
    ));
    assert!(matches!(
        manifest
            .insert_command(
                SandboxToolchainCommand::new(
                    &toolchain_id,
                    "cat",
                    b"other bytes",
                    SandboxResourceLimits::default(),
                )
                .expect("duplicate command")
            )
            .expect_err("duplicate"),
        SandboxToolchainError::DuplicateCommand { .. }
    ));
}

#[test]
fn sandbox_toolchain_command_names_reject_path_escape() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    for name in ["", ".", "..", "../cat", "bin/cat", "cat\\evil", "cat evil"] {
        assert!(matches!(
            SandboxToolchainCommand::new(
                &toolchain_id,
                name,
                b"wasm bytes",
                SandboxResourceLimits::default(),
            )
            .expect_err("invalid"),
            SandboxToolchainError::InvalidCommandName { .. }
        ));
    }
}

#[test]
fn local_sandbox_toolchain_store_persists_manifest_and_verifies_modules() {
    let cache_root = unique_test_dir("sinomo-sandbox-toolchain-store");
    let store = LocalSandboxToolchainStore::new(&cache_root);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "sinomo-maintained test build",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");

    let mut command_limits = SandboxResourceLimits::default();
    command_limits.max_input_bytes = 2048;
    command_limits.max_output_bytes = 4096;
    let layout = store
        .put_command_module(&mut manifest, "echo", b"echo wasm bytes", command_limits)
        .expect("put command");

    assert!(layout.manifest.exists());
    assert!(layout.command_module("echo").exists());
    let (loaded, loaded_layout) = store.load_manifest(&toolchain_id).expect("load");
    assert_eq!(loaded, manifest);
    assert_eq!(loaded_layout, layout);
    assert_eq!(
        store
            .load_command_module(&loaded, "echo")
            .expect("load command"),
        b"echo wasm bytes"
    );
    let loaded_command = loaded.command("echo").expect("echo command");
    assert_eq!(loaded_command.default_limits.max_input_bytes, 2048);
    assert_eq!(loaded_command.default_limits.max_output_bytes, 4096);
    let layout = store
        .put_artifact(&mut manifest, "lib/python/os.py", b"support bytes")
        .expect("put artifact");
    let (loaded, _) = store.load_manifest(&toolchain_id).expect("reload");
    assert_eq!(
        store
            .load_artifact(&loaded, "lib/python/os.py")
            .expect("load artifact"),
        b"support bytes"
    );

    fs::write(layout.command_module("echo"), b"tampered").expect("tamper");
    assert!(matches!(
        store
            .load_command_module(&loaded, "echo")
            .expect_err("hash mismatch"),
        SandboxToolchainError::InvalidManifest { .. }
    ));
    fs::remove_dir_all(&cache_root).ok();
}

#[test]
fn busybox_toolchain_spec_registers_core_applets_locally() {
    let cache_root = unique_test_dir("sinomo-sandbox-busybox-local");
    let store = LocalSandboxToolchainStore::new(&cache_root);
    let spec = SandboxBusyBoxToolchainSpec::new(
        "busybox-wasi-test",
        "unit test build",
        OffsetDateTime::now_utc(),
    )
    .expect("spec");

    let manifest = install_busybox_toolchain_to_local_store(&store, &spec, b"busybox module")
        .expect("install");

    assert_eq!(manifest.commands.len(), BUSYBOX_TOOLCHAIN_COMMANDS.len());
    for command in ["cat", "grep", "find", "printf", "sha256sum"] {
        assert_eq!(
            store
                .load_command_module(&manifest, command)
                .expect("module"),
            b"busybox module"
        );
    }
    fs::remove_dir_all(cache_root).ok();
}

#[test]
fn busybox_fidelity_cases_reference_declared_core_applets() {
    let mut covered_commands = BTreeSet::new();
    let mut case_names = BTreeSet::new();
    for case in BUSYBOX_TOOLCHAIN_FIDELITY_CASES {
        assert!(
            case_names.insert(case.name),
            "duplicate BusyBox fidelity case name {}",
            case.name
        );
        assert!(
            BUSYBOX_TOOLCHAIN_COMMANDS.contains(&case.command),
            "fidelity case {} references undeclared command {}",
            case.name,
            case.command
        );
        assert_eq!(case.argv.first().copied(), Some(case.command));
        covered_commands.insert(case.command);
    }
    for command in BUSYBOX_TOOLCHAIN_COMMANDS {
        assert!(
            covered_commands.contains(command),
            "declared BusyBox command {command} has no fidelity case"
        );
    }
}

#[test]
fn sandbox_mvp_command_list_tracks_busybox_subset_and_known_gaps() {
    let mvp_commands: BTreeSet<_> = SANDBOX_MVP_COMMANDS.iter().copied().collect();
    assert_eq!(
        mvp_commands.len(),
        SANDBOX_MVP_COMMANDS.len(),
        "duplicate MVP command declaration"
    );
    for command in BUSYBOX_TOOLCHAIN_COMMANDS {
        assert!(
            mvp_commands.contains(command),
            "declared BusyBox command {command} is not tracked in the MVP command list"
        );
    }
    for pending in ["sh"] {
        assert!(
            mvp_commands.contains(pending),
            "known pending MVP command {pending} is missing from the MVP command list"
        );
        assert!(
            !BUSYBOX_TOOLCHAIN_COMMANDS.contains(&pending),
            "pending MVP command {pending} should not be exposed before fidelity is real"
        );
        assert!(
            sandbox_mvp_command_gap(pending).is_some(),
            "pending MVP command {pending} should explain why it is not exposed yet"
        );
    }
    assert_eq!(SANDBOX_MVP_COMMAND_GAPS.len(), 1);
    let gap_commands: BTreeSet<_> = SANDBOX_MVP_COMMAND_GAPS
        .iter()
        .map(|gap| gap.command)
        .collect();
    assert_eq!(gap_commands.len(), SANDBOX_MVP_COMMAND_GAPS.len());
    assert!(
        !sandbox_runtime_supports_child_processes(),
        "child processes should not be marked supported before forked pipelines are real"
    );
    let process_capabilities = sandbox_runtime_process_capabilities();
    assert_eq!(process_capabilities.len(), 2);
    assert!(
        process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY
                && capability.supported
                && capability.detail.contains("pinned /bin")
        }),
        "preview1 should report the bounded pinned subprocess bridge separately from full POSIX child-process support"
    );
    assert!(
        process_capabilities.iter().any(|capability| {
            capability.capability == "child-process-exec"
                && !capability.supported
                && capability.detail.contains("lacks POSIX fork")
        }),
        "full child-process-exec should remain unsupported for preview1"
    );
    let preview1_budget_capabilities =
        sandbox_runtime_budget_capabilities_for_abi(&SandboxWasiAbi::Preview1);
    assert_eq!(preview1_budget_capabilities.len(), 5);
    assert!(
        preview1_budget_capabilities
            .iter()
            .all(|capability| capability.supported),
        "preview1 runner should report timeout, cancellation, input, memory, and output-limit enforcement"
    );
    assert!(
        preview1_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "output-limit"),
        "preview1 runner should report output limit enforcement"
    );
    assert!(
        preview1_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "input-limit" && capability.supported),
        "preview1 runner should report stdin input limit enforcement"
    );
    assert!(
        preview1_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "memory-limit" && capability.supported),
        "preview1 runner should report memory limit enforcement"
    );
    let wasix_budget_capabilities =
        sandbox_runtime_budget_capabilities_for_abi(&SandboxWasiAbi::Wasix);
    let wasix_process_capabilities =
        sandbox_runtime_process_capabilities_for_abi(&SandboxWasiAbi::Wasix);
    assert_eq!(wasix_process_capabilities.len(), 3);
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == "child-process-exec"
                && capability.supported
                && capability.detail.contains("pinned toolchain")
        }),
        "compiled WASIX runner should report available process spawning through the staged toolchain path"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY
                && capability.supported
                && capability.detail.contains("pinned /bin")
        }),
        "WASIX runner should report strict pinned child-exec containment support"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                && capability.supported
                && capability.detail.contains("sandbox-mounted paths")
        }),
        "WASIX runner should report sandbox-local unpinned child exec support separately"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY
                && !capability.supported
        }),
        "non-WASIX builds should not claim strict pinned child-exec containment"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                && !capability.supported
        }),
        "non-WASIX builds should not claim sandbox-local unpinned child exec support"
    );
    assert_eq!(wasix_budget_capabilities.len(), 9);
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "task-limit" && capability.supported),
        "compiled WASIX runner should report the configured process/thread fanout guard"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "output-limit"
                && capability.supported
                && capability.detail.contains("reject guest writes")),
        "compiled WASIX runner should report streaming output limit enforcement"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "task-limit" && !capability.supported),
        "non-WASIX builds should not claim the process/thread fanout guard"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "output-limit" && !capability.supported),
        "non-WASIX builds should not claim the streaming output limit guard"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "input-limit"
                && capability.supported
                && capability.detail.contains("stdin")),
        "compiled WASIX runner should report stdin input limit enforcement"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "input-limit" && !capability.supported),
        "non-WASIX builds should not claim the stdin input limit guard"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "memory-limit"
                && capability.supported
                && capability.detail.contains("unbounded")
                && capability.detail.contains("Wasmer tunables")),
        "WASIX runner should report static linear-memory budget enforcement"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "instruction-budget"
                && capability.supported
                && capability.detail.contains("Wasmer metering")),
        "WASIX runner should report deterministic instruction metering"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(
                |capability| capability.capability == "instruction-budget" && !capability.supported
            ),
        "non-WASIX builds should not claim instruction metering"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities.iter().any(|capability| {
            capability.capability == "process-tree-timeout"
                && capability.supported
                && capability.detail.contains("SIGKILL")
        }),
        "compiled WASIX runner should report process-tree timeout signaling separately from exact engine wall-time interruption"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities.iter().any(|capability| {
            capability.capability == "process-tree-cancellation"
                && capability.supported
                && capability.detail.contains("SIGKILL")
        }),
        "compiled WASIX runner should report process-tree cancellation signaling separately from immediate non-yielding cancellation"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities.iter().any(|capability| {
            capability.capability == "process-tree-timeout" && !capability.supported
        }),
        "non-WASIX builds should not claim process-tree timeout signaling"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities.iter().any(|capability| {
            capability.capability == "process-tree-cancellation" && !capability.supported
        }),
        "non-WASIX builds should not claim process-tree cancellation signaling"
    );
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "memory-limit" && !capability.supported),
        "non-WASIX builds should not claim memory limit enforcement"
    );
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "wall-timeout" && !capability.supported),
        "WASIX runner should be honest about missing deterministic timeout interruption"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "wall-timeout"
                && capability.detail.contains("instruction metering")),
        "compiled WASIX runner should report the remaining exact engine wall-time interruption gap"
    );
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "cancellation" && !capability.supported),
        "WASIX runner should be honest about missing in-flight cancellation"
    );
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "cancellation"
                && capability
                    .detail
                    .contains("cannot yet be cancelled immediately")),
        "compiled WASIX runner should report the remaining immediate non-yielding cancellation gap"
    );
    assert_eq!(
        SANDBOX_MVP_VIRTUAL_DEVICES,
        ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"]
    );
    assert!(
        sandbox_runtime_supports_mvp_virtual_devices(),
        "the WASI runner should expose the planned virtual devices without host /dev"
    );
    let device_capabilities = sandbox_runtime_mvp_virtual_device_capabilities();
    assert_eq!(device_capabilities.len(), SANDBOX_MVP_VIRTUAL_DEVICES.len());
    assert!(
        device_capabilities
            .iter()
            .all(|capability| capability.supported
                && capability.detail.contains("never maps host device paths")),
        "runtime device support should explain the isolation boundary"
    );
    assert!(
        sandbox_runtime_disables_ambient_network(),
        "WASI commands must not get ambient network access; use mediated sandbox fetch paths"
    );
    assert!(sandbox_env_name_is_valid("RUST_LOG"));
    assert!(sandbox_env_name_is_valid("_SINOMO"));
    for invalid in ["", "1BAD", "BAD-NAME", "BAD=VALUE"] {
        assert!(
            !sandbox_env_name_is_valid(invalid),
            "invalid env name should be rejected: {invalid:?}"
        );
    }
}

#[test]
fn virtual_device_writes_do_not_consume_workspace_write_quota() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_local_fs_dev_quota");
    let mut fs = LocalSandboxFileSystem::new(layout);
    let null = SandboxPath::new("/dev/null").expect("null");
    let limits = SandboxResourceLimits {
        max_written_bytes_per_command: 1,
        max_workspace_bytes: 1,
        max_file_count: 0,
        ..SandboxResourceLimits::default()
    };

    validate_sandbox_write_resource_limits(&fs, &null, 128, &limits)
        .expect("virtual device write bypasses workspace quotas");
    fs.write(&null, b"discarded").expect("write null");
    assert!(matches!(
        validate_sandbox_write_resource_limits(
            &fs,
            &SandboxPath::new("/work/out.txt").expect("work"),
            2,
            &limits,
        ),
        Err(SandboxFsError::QuotaExceeded { .. })
    ));

    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn busybox_wasi_config_fragment_enables_declared_applets() {
    for command in BUSYBOX_TOOLCHAIN_COMMANDS {
        let config = busybox_applet_config_symbol(command).unwrap_or_else(|| {
            panic!("declared BusyBox command {command} has no config symbol mapping")
        });
        assert!(
            BUSYBOX_WASI_CONFIG_FRAGMENT.contains(config),
            "BusyBox fragment does not enable declared applet {command} with {config}"
        );
    }
}

#[test]
fn quickjs_toolchain_metadata_and_fidelity_cases_are_declared() {
    SandboxToolchainId::new(QUICKJS_TOOLCHAIN_ID.to_string()).expect("valid quickjs id");
    assert_eq!(QUICKJS_TOOLCHAIN_COMMAND, "qjs");
    assert!(QUICKJS_TOOLCHAIN_VERSION.contains("quickjs-"));
    assert!(QUICKJS_TOOLCHAIN_RECIPE.ends_with("build-qjs-wasi.sh"));
    assert!(QUICKJS_TOOLCHAIN_PACKAGE_MANIFEST.ends_with("package.json"));
    assert!(!QUICKJS_TOOLCHAIN_FIDELITY_CASES.is_empty());
    for case in QUICKJS_TOOLCHAIN_FIDELITY_CASES {
        assert_eq!(case.argv.first().copied(), Some(QUICKJS_TOOLCHAIN_COMMAND));
        for (path, _contents) in case.setup_files {
            SandboxPath::new(*path).expect("valid setup file path");
        }
    }
    assert!(
        QUICKJS_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
            case.expected_exit_status != 0 && case.expected_stderr_contains.is_some()
        })
    );
}

#[test]
fn required_bash_python_pip_and_make_fidelity_cases_are_declared() {
    assert!(!BASH_TOOLCHAIN_FIDELITY_CASES.is_empty());
    assert!(BASH_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "bash-pipe-external-grep"
            && case.command == BASH_TOOLCHAIN_COMMAND
            && case.expected_exit_status == 0
    }));
    assert!(
        BASH_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| { case.name == "bash-set-e-stops" && case.expected_exit_status != 0 })
    );
    assert!(BASH_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "bash-descriptor-duplication" && case.expected_stderr_contains.is_none()
    }));
    assert!(BASH_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "sh-pipe-external-grep"
            && case.command == SH_TOOLCHAIN_COMMAND
            && case.expected_exit_status == 0
    }));

    assert!(!PYTHON_TOOLCHAIN_FIDELITY_CASES.is_empty());
    assert!(
        PYTHON_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "python-json-stdlib")
    );
    assert!(
        PYTHON_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "python-run-work-script" && !case.setup_files.is_empty())
    );

    assert!(!PIP_TOOLCHAIN_FIDELITY_CASES.is_empty());
    assert!(
        PIP_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "pip-wheel-local-pure-package"
                && !case.setup_files.is_empty()
                && case.timeout_ms() > SandboxResourceLimits::default().wall_time_ms)
    );
    assert!(
        PIP_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "pip-install-local-pure-package"
                && case.setup_files.is_empty()
                && case.timeout_ms() > SandboxResourceLimits::default().wall_time_ms)
    );
    assert!(PIP_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| case.name
        == "pip-install-local-simple-index"
        && case.command == PIP_TOOLCHAIN_COMMAND
        && !case.setup_files.is_empty()
        && case.timeout_ms() > SandboxResourceLimits::default().wall_time_ms));
    assert!(PIP_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| case.name
        == "python-import-simple-index-installed-package"
        && case.expected_stdout == Some("installed package\n")));
    assert!(
        PIP_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.command == PIP_TOOLCHAIN_COMMAND)
    );

    assert!(!MAKE_TOOLCHAIN_FIDELITY_CASES.is_empty());
    assert!(MAKE_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "make-version"
            && case.command == MAKE_TOOLCHAIN_COMMAND
            && case.expected_stdout_contains == Some("GNU Make 4.4.1")
    }));
    assert!(MAKE_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "make-recipe-pinned-shell-exec"
            && !case.setup_files.is_empty()
            && case.expected_stdout == Some("sinomo-make\n")
    }));
    assert!(
        MAKE_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "make-failed-recipe-status"
                && case.expected_exit_status != 0
                && case.expected_stderr_contains == Some("Error 1"))
    );

    assert!(!TCC_TOOLCHAIN_FIDELITY_CASES.is_empty());
    assert!(TCC_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "tcc-compile-object"
            && case.command == TCC_TOOLCHAIN_COMMAND
            && !case.setup_files.is_empty()
            && case.expected_exit_status == 0
    }));
    assert!(
        TCC_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "tcc-invalid-c-diagnostics"
                && case.expected_exit_status != 0
                && case.expected_stderr_contains == Some("error"))
    );

    for case in BASH_TOOLCHAIN_FIDELITY_CASES
        .iter()
        .chain(PYTHON_TOOLCHAIN_FIDELITY_CASES.iter())
        .chain(PIP_TOOLCHAIN_FIDELITY_CASES.iter())
        .chain(MAKE_TOOLCHAIN_FIDELITY_CASES.iter())
        .chain(TCC_TOOLCHAIN_FIDELITY_CASES.iter())
    {
        assert_eq!(case.argv.first().copied(), Some(case.command));
        for (path, _contents) in case.setup_files {
            SandboxPath::new(*path).expect("valid setup file path");
        }
    }
}

#[test]
fn rich_toolchain_targets_track_implemented_and_planned_packages() {
    let commands: BTreeSet<_> = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .map(|target| target.command)
        .collect();
    assert_eq!(commands.len(), SANDBOX_RICH_TOOLCHAIN_TARGETS.len());
    let quickjs = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .find(|target| target.command == QUICKJS_TOOLCHAIN_COMMAND)
        .expect("quickjs target");
    assert_eq!(quickjs.status, SandboxRichToolchainStatus::Implemented);
    assert_eq!(quickjs.recipe, Some(QUICKJS_TOOLCHAIN_RECIPE));
    for required in [
        BASH_TOOLCHAIN_COMMAND,
        PYTHON_TOOLCHAIN_COMMAND,
        PIP_TOOLCHAIN_COMMAND,
    ] {
        let target = SANDBOX_RICH_TOOLCHAIN_TARGETS
            .iter()
            .find(|target| target.command == required)
            .unwrap_or_else(|| panic!("{required} target"));
        assert_eq!(target.status, SandboxRichToolchainStatus::Required);
        assert!(target.recipe.is_some());
    }
    let python = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .find(|target| target.command == PYTHON_TOOLCHAIN_COMMAND)
        .expect("python target");
    assert_eq!(python.recipe, Some(PYTHON_TOOLCHAIN_RECIPE));
    assert!(PYTHON_TOOLCHAIN_RECIPE.ends_with("build-cpython-wasix.sh"));
    assert!(PYTHON_WASIX_NATIVE_VERIFIER.ends_with("verify-python-wasix-native.sh"));
    assert!(python.note.contains("CPython WASIX"));
    assert!(python.note.contains("numpy"));
    assert!(python.note.contains("matplotlib"));

    let pip = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .find(|target| target.command == PIP_TOOLCHAIN_COMMAND)
        .expect("pip target");
    assert_eq!(pip.recipe, Some(PYTHON_TOOLCHAIN_RECIPE));
    assert!(pip.note.contains("native-extension"));
    assert!(pip.note.contains("numpy"));
    assert!(pip.note.contains("matplotlib"));

    let make = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .find(|target| target.command == MAKE_TOOLCHAIN_COMMAND)
        .expect("make target");
    assert_eq!(make.status, SandboxRichToolchainStatus::Implemented);
    assert_eq!(make.recipe, Some(MAKE_TOOLCHAIN_RECIPE));
    assert!(make.note.contains("verify-build-tools"));
    assert!(make.note.contains("daemon-local"));

    let tcc = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .find(|target| target.command == TCC_TOOLCHAIN_COMMAND)
        .expect("tcc target");
    assert_eq!(tcc.status, SandboxRichToolchainStatus::Implemented);
    assert_eq!(tcc.recipe, Some(TCC_TOOLCHAIN_RECIPE));
    assert!(tcc.note.contains("compiler fidelity"));
}

#[test]
fn busybox_toolchain_spec_registers_core_applets_in_sifs() {
    let runtime_root = unique_test_dir("sinomo-sandbox-busybox-sifs");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_busybox_sifs").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxToolchainStore::new(dfs);
    let spec = SandboxBusyBoxToolchainSpec::new(
        "busybox-wasi-test",
        "unit test build",
        OffsetDateTime::now_utc(),
    )
    .expect("spec");

    let manifest = install_busybox_toolchain_to_sifs_store(&mut store, &spec, b"busybox module")
        .expect("install");

    assert_eq!(manifest.commands.len(), BUSYBOX_TOOLCHAIN_COMMANDS.len());
    for command in ["cat", "grep", "find", "printf", "sha256sum"] {
        assert_eq!(
            store
                .load_command_module(&manifest, command)
                .expect("module"),
            b"busybox module"
        );
    }
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_toolchain_store_persists_manifest_and_verifies_modules() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-toolchain-store");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_toolchain").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxToolchainStore::new(dfs);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "sinomo-maintained test build",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");

    let mut command_limits = SandboxResourceLimits::default();
    command_limits.max_input_bytes = 2048;
    command_limits.max_output_bytes = 4096;
    store
        .put_command_module(&mut manifest, "cat", b"wasm bytes", command_limits)
        .expect("put command");

    let loaded = store.load_manifest(&toolchain_id).expect("load manifest");
    assert_eq!(loaded, manifest);
    assert_eq!(
        store
            .load_command_module(&loaded, "cat")
            .expect("load command"),
        b"wasm bytes"
    );
    let loaded_command = loaded.command("cat").expect("cat command");
    assert_eq!(loaded_command.default_limits.max_input_bytes, 2048);
    assert_eq!(loaded_command.default_limits.max_output_bytes, 4096);
    store
        .put_artifact(&mut manifest, "share/python/stdlib.zip", b"support archive")
        .expect("put artifact");
    let loaded = store.load_manifest(&toolchain_id).expect("reload manifest");
    assert_eq!(
        store
            .load_artifact(&loaded, "share/python/stdlib.zip")
            .expect("load artifact"),
        b"support archive"
    );
    let dfs = store.into_inner();
    assert!(
        !dfs.posix_read_file(&sandbox_toolchain_manifest_path(&toolchain_id))
            .expect("manifest exists")
            .is_empty()
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_toolchain_store_can_replicate_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-toolchain-replica");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_toolchain_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_toolchain_replica").expect("replica");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(runtime_root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    owner_dfs
        .bootstrap_distributed_sifs()
        .expect("owner bootstrap");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_replicated").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "replicated-1.0",
        SandboxWasiAbi::Preview1,
        "unit test build",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mut owner_store = SifsSandboxToolchainStore::new(owner_dfs)
        .with_replica_candidates(vec![owner, replica.clone()]);

    owner_store
        .put_command_module(
            &mut manifest,
            "cat",
            b"replicated wasm",
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    owner_store
        .put_artifact(&mut manifest, "share/cat/help.txt", b"help")
        .expect("put artifact");
    replica_dfs.process_incoming().expect("replica content");

    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let replica_store = SifsSandboxToolchainStore::new(replica_dfs);
    let loaded = replica_store
        .load_manifest(&toolchain_id)
        .expect("replica manifest");

    assert_eq!(
        replica_store
            .load_command_module(&loaded, "cat")
            .expect("replica command"),
        b"replicated wasm"
    );
    assert_eq!(
        replica_store
            .load_artifact(&loaded, "share/cat/help.txt")
            .expect("replica artifact"),
        b"help"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_sandbox_toolchain_store_rejects_tampered_module() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-toolchain-tamper");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_toolchain_tamper").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxToolchainStore::new(dfs);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "sinomo-maintained test build",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    store
        .put_command_module(
            &mut manifest,
            "cat",
            b"wasm bytes",
            SandboxResourceLimits::default(),
        )
        .expect("put command");

    let mut dfs = store.into_inner();
    dfs.posix_write_file(
        &sandbox_toolchain_command_path(&toolchain_id, "cat"),
        b"tampered bytes",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("tamper");
    let store = SifsSandboxToolchainStore::new(dfs);

    assert!(matches!(
        store
            .load_command_module(&manifest, "cat")
            .expect_err("hash mismatch"),
        SandboxToolchainError::InvalidManifest { .. }
    ));
    fs::remove_dir_all(runtime_root).ok();
}
