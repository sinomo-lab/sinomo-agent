use super::*;

#[test]
fn sandbox_toolchain_manifest_records_command_hashes_and_paths() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
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
        SandboxWasiAbi::Wasix
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
        SandboxWasiAbi::Wasix,
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

    assert_eq!(manifest.target_abi, SandboxWasiAbi::Wasix);
    assert_eq!(manifest.commands.len(), BUSYBOX_TOOLCHAIN_COMMANDS.len());
    for command in ["cat", "grep", "find", "printf", "sha256sum", "sh"] {
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
    assert!(
        BUSYBOX_TOOLCHAIN_COMMANDS.contains(&"sh"),
        "BusyBox shell should be exposed as part of the default toolchain"
    );
    assert!(
        sandbox_mvp_command_gap("sh").is_none(),
        "sh should no longer be tracked as a pending MVP gap"
    );
    assert_eq!(SANDBOX_MVP_COMMAND_GAPS.len(), 0);
    let gap_commands: BTreeSet<_> = SANDBOX_MVP_COMMAND_GAPS
        .iter()
        .map(|gap| gap.command)
        .collect();
    assert_eq!(gap_commands.len(), SANDBOX_MVP_COMMAND_GAPS.len());
    assert!(
        sandbox_runtime_supports_child_processes(),
        "the default WASIX runtime should report child-process execution support"
    );
    let process_capabilities = sandbox_runtime_process_capabilities();
    assert_eq!(process_capabilities.len(), 4);
    assert!(
        process_capabilities.iter().any(|capability| {
            capability.capability == "child-process-exec"
                && capability.supported
                && capability.detail.contains("pinned toolchain")
        }),
        "default process capabilities should describe WASIX child-process support"
    );
    assert!(
        process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY
                && capability.supported
                && capability.detail.contains("pinned /bin")
        }),
        "default process capabilities should describe WASIX pinned child-exec containment"
    );
    assert!(
        process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY
                && capability.supported
                && capability.detail.contains("asyncify")
        }),
        "default process capabilities should describe WASIX asyncified fork support"
    );
    let wasix_process_capabilities =
        sandbox_runtime_process_capabilities_for_abi(&SandboxWasiAbi::Wasix);
    assert_eq!(wasix_process_capabilities.len(), 4);
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY
                && capability.supported
                && capability.detail.contains("pinned toolchain")
        }),
        "wasix should report child-process support through the staged toolchain path"
    );
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                && capability.supported
                && capability.detail.contains("sandbox-mounted paths")
        }),
        "wasix should report sandbox-local child exec support separately"
    );
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY
                && capability.supported
                && capability.detail.contains("asyncify")
        }),
        "wasix should report asyncified fork support separately"
    );
    let wasix_budget_capabilities =
        sandbox_runtime_budget_capabilities_for_abi(&SandboxWasiAbi::Wasix);
    assert_eq!(wasix_budget_capabilities.len(), 9);
    for capability in [
        "task-limit",
        "output-limit",
        "input-limit",
        "memory-limit",
        "instruction-budget",
        "process-tree-timeout",
        "process-tree-cancellation",
    ] {
        assert!(
            wasix_budget_capabilities
                .iter()
                .any(|reported| reported.capability == capability && reported.supported),
            "wasix runner should report {capability} enforcement"
        );
    }
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "output-limit"),
        "wasix runner should report output limit enforcement"
    );
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "input-limit" && capability.supported),
        "wasix runner should report stdin input limit enforcement"
    );
    assert!(
        wasix_budget_capabilities
            .iter()
            .any(|capability| capability.capability == "memory-limit" && capability.supported),
        "wasix runner should report memory limit enforcement"
    );
    let wasix_budget_capabilities =
        sandbox_runtime_budget_capabilities_for_abi(&SandboxWasiAbi::Wasix);
    assert_eq!(
        sandbox_runtime_budget_capabilities(),
        wasix_budget_capabilities,
        "default runtime budget capability facade should describe WASIX"
    );
    let wasix_process_capabilities =
        sandbox_runtime_process_capabilities_for_abi(&SandboxWasiAbi::Wasix);
    assert_eq!(wasix_process_capabilities.len(), 4);
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
    #[cfg(feature = "wasix-runtime")]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY
                && capability.supported
                && capability.detail.contains("proc_fork")
                && capability.detail.contains("asyncify")
        }),
        "WASIX runner should report asyncified fork support separately from child exec"
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
    #[cfg(not(feature = "wasix-runtime"))]
    assert!(
        wasix_process_capabilities.iter().any(|capability| {
            capability.capability == SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY
                && !capability.supported
        }),
        "non-WASIX builds should not claim asyncified fork support"
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
                    .contains("exact engine-level immediate cancellation")),
        "compiled WASIX runner should report the remaining immediate non-yielding cancellation gap"
    );
    assert!(SANDBOX_MVP_VIRTUAL_DEVICES.contains(&"/dev/null"));
    assert!(SANDBOX_MVP_VIRTUAL_DEVICES.contains(&"/dev/zero"));
    assert!(SANDBOX_MVP_VIRTUAL_DEVICES.contains(&"/dev/random"));
    assert!(SANDBOX_MVP_VIRTUAL_DEVICES.contains(&"/dev/urandom"));
    assert!(SANDBOX_MVP_VIRTUAL_DEVICES.contains(&"/dev/fd"));
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

    validate_sandbox_write_resource_limits(&mut fs, &null, 128, &limits)
        .expect("virtual device write bypasses workspace quotas");
    fs.write(&null, b"discarded").expect("write null");
    assert!(matches!(
        validate_sandbox_write_resource_limits(
            &mut fs,
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
        case.name == "bash-compgen-wordlist" && case.expected_stdout == Some("beta\n")
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
                && case.timeout_ms() > 30_000)
    );
    assert!(
        PIP_TOOLCHAIN_FIDELITY_CASES
            .iter()
            .any(|case| case.name == "pip-install-local-pure-package"
                && case.setup_files.is_empty()
                && case.timeout_ms() > 30_000)
    );
    assert!(PIP_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| case.name
        == "pip-install-local-simple-index"
        && case.command == PIP_TOOLCHAIN_COMMAND
        && !case.setup_files.is_empty()
        && case.timeout_ms() > 30_000));
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

    assert!(!MAKEPKG_TOOLCHAIN_FIDELITY_CASES.is_empty());
    assert!(MAKEPKG_TOOLCHAIN_FIDELITY_CASES.iter().any(|case| {
        case.name == "makepkg-version"
            && case.command == MAKEPKG_TOOLCHAIN_COMMAND
            && case.expected_stdout_contains == Some("makepkg (pacman) 7.1.0")
    }));

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
        .chain(MAKEPKG_TOOLCHAIN_FIDELITY_CASES.iter())
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

    let makepkg = SANDBOX_RICH_TOOLCHAIN_TARGETS
        .iter()
        .find(|target| target.command == MAKEPKG_TOOLCHAIN_COMMAND)
        .expect("makepkg target");
    assert_eq!(makepkg.status, SandboxRichToolchainStatus::Required);
    assert_eq!(makepkg.recipe, Some(MAKEPKG_TOOLCHAIN_RECIPE));
    assert!(makepkg.note.contains("upstream pacman"));
    assert!(makepkg.note.contains("compgen"));

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
    let node_id = NodeId::from_legacy_named("node_busybox_sifs").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "busybox_toolchain_spec_registers_core_applets_in_sifs",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let mut store = SifsSandboxToolchainStore::new(dfs);
    let spec = SandboxBusyBoxToolchainSpec::new(
        "busybox-wasi-test",
        "unit test build",
        OffsetDateTime::now_utc(),
    )
    .expect("spec");

    let manifest = install_busybox_toolchain_to_sifs_store(&mut store, &spec, b"busybox module")
        .expect("install");

    assert_eq!(manifest.target_abi, SandboxWasiAbi::Wasix);
    assert_eq!(manifest.commands.len(), BUSYBOX_TOOLCHAIN_COMMANDS.len());
    for command in ["cat", "grep", "find", "printf", "sha256sum", "sh"] {
        assert_eq!(
            store
                .load_command_module(&manifest, command)
                .expect("module"),
            b"busybox module"
        );
    }

    drop(store);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_toolchain_store_persists_manifest_and_verifies_modules() {
    let node_id = NodeId::from_legacy_named("node_sifs_toolchain").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_toolchain_store_persists_manifest_and_verifies_modules",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let mut store = SifsSandboxToolchainStore::new(dfs);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
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
    let mut dfs = store.into_inner();
    assert!(
        !dfs.posix_read_file_distributed(&sandbox_toolchain_manifest_path(&toolchain_id))
            .expect("manifest exists")
            .is_empty()
    );

    drop(dfs);
    native.close().expect("normal native DFS shutdown");
}

#[test]
fn sifs_sandbox_toolchain_store_can_replicate_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-toolchain-replica");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_toolchain_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_toolchain_replica").expect("replica");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
        SandboxWasiAbi::Wasix,
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
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxToolchainStore::new(replica_dfs);
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
    let node_id = NodeId::from_legacy_named("node_sifs_toolchain_tamper").expect("node");
    let (native, mut dfs) = sinomo_runtime::dfs::test_support::DfsTestRuntime::start_single_node(
        "sifs_sandbox_toolchain_store_rejects_tampered_module",
        node_id.clone(),
    )
    .expect("normal native DFS startup");
    let mut store = SifsSandboxToolchainStore::new(dfs);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
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
    let mut store = SifsSandboxToolchainStore::new(dfs);

    assert!(matches!(
        store
            .load_command_module(&manifest, "cat")
            .expect_err("hash mismatch"),
        SandboxToolchainError::InvalidManifest { .. }
    ));

    drop(store);
    native.close().expect("normal native DFS shutdown");
}
