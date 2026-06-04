use crate::internal::*;
use sinomo_kv::InProcessKvStore;

#[test]
fn sandbox_policy_denied_event_reports_denial_shape() {
    let node_id = NodeId::from_legacy_named("node_policy_denied_event").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_policy_denied_event").expect("sandbox");
    let denial = SandboxPolicyDenial::new("mount-source-denied", "outside allowed prefixes");
    let event = sandbox_policy_denied_event(
        node_id.clone(),
        Some(&sandbox_id),
        "validate-mounts",
        &denial,
    );

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.policy");
    assert_eq!(event.kind.as_str(), "sandbox.policy.denied");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        event.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(event.payload["phase"], "validate-mounts");
    assert_eq!(event.payload["code"], "mount-source-denied");
    assert_eq!(event.payload["detail"], "outside allowed prefixes");
}

#[test]
fn sandbox_policy_validates_active_execution_counts() {
    let mut policy = SandboxPolicy {
        max_concurrent_commands_per_sandbox: 1,
        max_concurrent_sandboxes_per_node: 2,
        ..SandboxPolicy::default()
    };
    let sandbox_id = SandboxId::from_legacy_named("sandbox_active_counts").expect("sandbox");

    policy
        .validate_active_execution_counts(&sandbox_id, 1, 2)
        .expect("within limits");
    assert_eq!(
        policy
            .validate_active_execution_counts(&sandbox_id, 2, 2)
            .expect_err("same sandbox limit")
            .code,
        "concurrent-commands-denied"
    );
    assert_eq!(
        policy
            .validate_active_execution_counts(&sandbox_id, 1, 3)
            .expect_err("node sandbox limit")
            .code,
        "concurrent-sandboxes-denied"
    );

    policy.max_concurrent_commands_per_sandbox = 0;
    assert_eq!(
        policy
            .validate_active_execution_counts(&sandbox_id, 1, 1)
            .expect_err("zero denies commands")
            .code,
        "concurrent-commands-denied"
    );
}

#[test]
fn sandbox_policy_denies_remote_local_owned_mount_by_default() {
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        ..SandboxPolicy::default()
    };
    let mount = SandboxMount {
        source_sifs_path: "/sinomo/nodes/node_other/files".to_string(),
        mount_path: SandboxPath::new("/host").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    };

    let error = policy
        .validate_mounts(
            &[mount],
            &NodeId::from_legacy_named("node_local").expect("node"),
        )
        .expect_err("remote mount denied");

    assert_eq!(error.code, "mount-local-owned-remote-denied");
}

#[test]
fn sandbox_policy_allows_local_owned_mount_on_owner_node() {
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        ..SandboxPolicy::default()
    };
    let node_id = NodeId::from_legacy_named("node_owner").expect("node");
    let mount = SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{node_id}/files"),
        mount_path: SandboxPath::new("/host").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    };

    policy
        .validate_mounts(&[mount], &node_id)
        .expect("owner mount allowed");
}

#[test]
fn sandbox_policy_denies_mount_source_outside_allowed_prefixes() {
    let policy = SandboxPolicy::default();
    let mount = SandboxMount {
        source_sifs_path: "/users/alice".to_string(),
        mount_path: SandboxPath::new("/users").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    };

    let error = policy
        .validate_mounts(
            &[mount],
            &NodeId::from_legacy_named("node_local").expect("node"),
        )
        .expect_err("source denied");

    assert_eq!(error.code, "mount-source-denied");
}

#[test]
fn sandbox_policy_denies_duplicate_mount_paths() {
    let policy = SandboxPolicy::default();
    let mounts = vec![
        SandboxMount {
            source_sifs_path: "/services/sandbox/a".to_string(),
            mount_path: SandboxPath::new("/work").expect("mount path"),
            mode: SandboxMountMode::ReadWrite,
        },
        SandboxMount {
            source_sifs_path: "/services/sandbox/b".to_string(),
            mount_path: SandboxPath::new("/work").expect("mount path"),
            mode: SandboxMountMode::ReadOnly,
        },
    ];

    let error = policy
        .validate_mounts(
            &mounts,
            &NodeId::from_legacy_named("node_local").expect("node"),
        )
        .expect_err("duplicate denied");

    assert_eq!(error.code, "mount-duplicate");
}

#[test]
fn sandbox_policy_denies_command_capability_without_allowlist() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "curl",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec!["network.http".to_string()];

    let error = SandboxPolicy::default()
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect_err("capability denied");

    assert_eq!(error.code, "command-capability-denied");
}

#[test]
fn sandbox_policy_allows_command_capability_when_listed() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "curl",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec!["network.http".to_string()];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert("network.http".to_string());

    policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect("capability allowed");
}

#[test]
fn sandbox_policy_allows_wasix_pinned_subprocess_capability_when_listed() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_python").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "python",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string()];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string());

    policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect("bounded wasix subprocess capability allowed");
}

#[test]
fn sandbox_policy_denies_unsupported_runtime_command_capability_even_when_allowed() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "sh",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string()];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());

    let error = policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect_err("unsupported runtime capability denied");

    assert_eq!(error.code, "command-capability-unsupported");
    assert!(
        error
            .detail
            .contains("WASIX sandbox runtime does not support")
    );
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sandbox_policy_allows_strict_pinned_child_exec_containment_when_allowlisted() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_wasix_strict").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "sh",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
        SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string(),
    ];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string());

    policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect("strict pinned WASIX child exec containment is supported by the runtime");
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sandbox_policy_requires_explicit_wasix_unpinned_child_exec_acknowledgement() {
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "sh",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string()];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());

    let error = policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect_err("WASIX child-process exec command manifest must declare the current gap");

    assert_eq!(error.code, "command-capability-missing");
    assert!(
        error
            .detail
            .contains(SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
    );
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sandbox_policy_denies_wasix_unpinned_child_exec_acknowledgement_when_not_allowlisted() {
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "sh",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
        SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string(),
    ];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());

    let error = policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect_err("WASIX child-process exec acknowledgement must be allowlisted by policy");

    assert_eq!(error.code, "command-capability-denied");
    assert!(
        error
            .detail
            .contains(SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
    );
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sandbox_policy_allows_wasix_child_process_capability_when_all_gaps_are_allowlisted() {
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "sh",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
        SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string(),
    ];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string());

    policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect("WASIX child-process capability is runtime-supported when policy allows it");
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sandbox_policy_allows_wasix_asyncified_fork_capability_when_allowlisted() {
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "fork-helper",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY.to_string()];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY.to_string());

    policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect("WASIX asyncified fork capability is supported when the WASIX runtime is enabled and policy allowlists it");
}

#[test]
#[cfg(not(feature = "wasix-runtime"))]
fn sandbox_policy_denies_wasix_asyncified_fork_capability_without_wasix_runtime() {
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "fork-helper",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY.to_string()];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY.to_string());

    let error = policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect_err("asyncified fork requires the WASIX runtime feature");

    assert_eq!(error.code, "command-capability-unsupported");
    assert!(
        error
            .detail
            .contains(SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY)
    );
}

#[test]
#[cfg(not(feature = "wasix-runtime"))]
fn sandbox_policy_denies_wasix_child_process_capability_without_runtime_feature() {
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "sh",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
        SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string(),
    ];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());

    let error = policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect_err("WASIX child process support must require the compiled runtime feature");

    assert_eq!(error.code, "command-capability-unsupported");
    assert!(
        error
            .detail
            .contains("Wasix sandbox runtime does not support")
    );
}

#[test]
fn sandbox_policy_denies_process_substrate_commands_without_support_even_without_declared_capability()
 {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");

    for command_name in [
        BASH_TOOLCHAIN_COMMAND,
        SH_TOOLCHAIN_COMMAND,
        PYTHON_TOOLCHAIN_COMMAND,
        PIP_TOOLCHAIN_COMMAND,
    ] {
        let command = SandboxToolchainCommand::new(
            &toolchain_id,
            command_name,
            b"wasm bytes",
            SandboxResourceLimits::default(),
        )
        .expect("command");

        let error = SandboxPolicy::default()
            .validate_command(&command, &SandboxWasiAbi::Wasix)
            .expect_err("process substrate command denied");

        assert_eq!(error.code, "command-capability-unsupported");
        assert!(error.detail.contains("child-process execution substrate"));
    }
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sandbox_policy_allows_wasix_sh_when_child_process_capability_is_allowlisted() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_bash_wasix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        SH_TOOLCHAIN_COMMAND,
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec![
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
        SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string(),
    ];
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string());

    policy
        .validate_command(&command, &SandboxWasiAbi::Wasix)
        .expect("WASIX sh should be allowed when child-process-exec and pinned child containment are allowlisted");
}

#[test]
fn sandbox_default_policy_uses_host_memory_and_unlimited_size_limits() {
    let policy = SandboxPolicy::default();

    assert!(policy.max_limits.memory_mb > 0);
    assert_eq!(
        policy.max_limits.wall_time_ms,
        SANDBOX_UNLIMITED_WALL_TIME_MS
    );
    assert_eq!(
        policy.max_limits.max_input_bytes,
        SANDBOX_UNLIMITED_SIZE_LIMIT
    );
    assert_eq!(
        policy.max_limits.max_output_bytes,
        SANDBOX_UNLIMITED_SIZE_LIMIT
    );
    assert_eq!(
        policy.max_limits.max_workspace_bytes,
        SANDBOX_UNLIMITED_SIZE_LIMIT
    );
    assert_eq!(
        policy.max_limits.max_written_bytes_per_command,
        SANDBOX_UNLIMITED_SIZE_LIMIT
    );
    assert_eq!(
        policy.max_limits.max_file_count,
        SANDBOX_UNLIMITED_SIZE_LIMIT
    );
}

#[test]
fn sandbox_policy_denies_resource_limits_above_budget() {
    let policy = SandboxPolicy {
        max_limits: SandboxResourceLimits {
            memory_mb: 64,
            ..SandboxResourceLimits::default()
        },
        ..SandboxPolicy::default()
    };
    let limits = SandboxResourceLimits {
        memory_mb: 128,
        ..SandboxResourceLimits::default()
    };

    let error = policy
        .validate_resource_limits(&limits)
        .expect_err("limit denied");

    assert_eq!(error.code, "resource-limit-denied");

    let policy = SandboxPolicy {
        max_limits: SandboxResourceLimits {
            max_input_bytes: 64,
            ..SandboxResourceLimits::default()
        },
        ..SandboxPolicy::default()
    };
    let limits = SandboxResourceLimits {
        max_input_bytes: 128,
        ..SandboxResourceLimits::default()
    };

    let error = policy
        .validate_resource_limits(&limits)
        .expect_err("input limit denied");

    assert_eq!(error.code, "resource-limit-denied");
    assert!(error.detail.contains("max-input-bytes"));
}

#[test]
fn sandbox_policy_requires_signed_allowed_toolchain() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let policy = SandboxPolicy {
        allowed_toolchain_ids: BTreeSet::from([toolchain_id]),
        ..SandboxPolicy::default()
    };

    let error = policy
        .validate_toolchain(&manifest)
        .expect_err("unsigned denied");
    assert_eq!(error.code, "toolchain-unsigned-denied");

    manifest.signature = Some("test-signature".to_string());
    policy
        .validate_toolchain(&manifest)
        .expect("signed allowlisted toolchain");
}

#[test]
fn sandbox_policy_toolchain_update_preflight_does_not_require_signature() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_update").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
        "unit test import",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest
        .insert_command(
            SandboxToolchainCommand::new(
                &toolchain_id,
                "cat",
                b"cat module",
                SandboxResourceLimits::default(),
            )
            .expect("command"),
        )
        .expect("insert command");
    let policy = SandboxPolicy::default();

    policy
        .validate_toolchain_update(&manifest)
        .expect("unsigned toolchain update preflight allowed");
    let error = policy
        .validate_toolchain(&manifest)
        .expect_err("unsigned invocation denied");
    assert_eq!(error.code, "toolchain-unsigned-denied");
}

#[test]
fn sandbox_policy_validates_toolchain_provenance_prefix() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
        "local-cli import",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.signature = Some("test-signature".to_string());
    let mut policy = SandboxPolicy {
        allowed_toolchain_provenance_prefixes: BTreeSet::from([format!("sinomo://toolchains/")]),
        ..SandboxPolicy::default()
    };

    let error = policy
        .validate_toolchain(&manifest)
        .expect_err("provenance denied");
    assert_eq!(error.code, "toolchain-provenance-denied");

    manifest.provenance = "sinomo://toolchains/busybox/1.36.1".to_string();
    policy
        .validate_toolchain(&manifest)
        .expect("matching provenance allowed");

    policy.allowed_toolchain_provenance_prefixes.clear();
    manifest.provenance = "local-cli import".to_string();
    policy
        .validate_toolchain(&manifest)
        .expect("empty provenance allowlist does not add a restriction");
}

#[test]
fn sandbox_policy_validates_toolchain_package_cache_paths() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_python").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "python-1.0",
        SandboxWasiAbi::Wasix,
        "sinomo://toolchains/python/1.0",
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.signature = Some("test-signature".to_string());
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "python",
        b"python wasm",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    let artifact =
        SandboxToolchainArtifact::new(&toolchain_id, "share/python/stdlib.zip", b"stdlib")
            .expect("artifact");
    manifest.insert_artifact(artifact).expect("insert artifact");

    SandboxPolicy::default()
        .validate_toolchain(&manifest)
        .expect("default toolchain cache path allowed");

    command.module_path = "/services/untrusted/toolchains/python/bin/python.wasm".to_string();
    manifest.insert_command(command).expect("insert command");
    let error = SandboxPolicy::default()
        .validate_toolchain(&manifest)
        .expect_err("module path denied");
    assert_eq!(error.code, "package-cache-path-denied");

    let mut policy = SandboxPolicy::default();
    policy
        .allowed_package_cache_prefixes
        .push("/services/untrusted/toolchains".to_string());
    policy
        .validate_toolchain(&manifest)
        .expect("custom package cache prefix allowed");
}

#[test]
fn sandbox_policy_validates_complete_toolchain_invocation() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_policy_invocation").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let instance =
        SandboxInstanceManifest::new(sandbox_id, toolchain_id.clone(), OffsetDateTime::now_utc())
            .expect("instance");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain.signature = Some("test-signature".to_string());
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "curl",
        b"wasm bytes",
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec!["network.http".to_string()];
    toolchain.insert_command(command).expect("insert command");
    let command = toolchain.command("curl").expect("command");
    let node_id = NodeId::from_legacy_named("node_invocation_policy").expect("node");
    let mut policy = SandboxPolicy {
        allowed_toolchain_ids: BTreeSet::from([toolchain_id]),
        ..SandboxPolicy::default()
    };

    let error = policy
        .validate_invocation(&instance, &toolchain, command, &node_id)
        .expect_err("capability denied");
    assert_eq!(error.code, "command-capability-denied");

    policy
        .allowed_command_capabilities
        .insert("network.http".to_string());
    policy
        .validate_invocation(&instance, &toolchain, command, &node_id)
        .expect("invocation allowed");
}

#[test]
fn sandbox_policy_validates_effective_invocation_limits_not_package_defaults() {
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_policy_effective_limits").expect("sandbox");
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_policy_effective_limits")
        .expect("toolchain");
    let mut instance =
        SandboxInstanceManifest::new(sandbox_id, toolchain_id.clone(), OffsetDateTime::now_utc())
            .expect("instance");
    instance.limits.wall_time_ms = 30_000;
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "python-1.0",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain.signature = Some("test-signature".to_string());
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "python",
        b"wasm bytes",
        SandboxResourceLimits {
            wall_time_ms: 900_000,
            ..SandboxResourceLimits::default()
        },
    )
    .expect("command");
    command.required_capabilities = vec![
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
        SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string(),
    ];
    toolchain.insert_command(command).expect("insert command");
    let command = toolchain.command("python").expect("command");
    let node_id = NodeId::from_legacy_named("node_effective_limits").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.max_limits.wall_time_ms = 60_000;
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string());

    policy
        .validate_invocation(&instance, &toolchain, command, &node_id)
        .expect("effective invocation wall time is within policy");
    assert_eq!(
        policy
            .validate_toolchain_update(&toolchain)
            .expect_err("toolchain update still rejects oversized defaults")
            .code,
        "resource-limit-denied"
    );
}

#[test]
fn sandbox_policy_config_round_trips_through_cluster_kv_snapshot() {
    let mut config = SandboxClusterPolicyConfig::default();
    config
        .policy
        .allowed_command_capabilities
        .insert("network.http".to_string());
    config.policy.max_limits.memory_mb = 256;
    config.policy.max_limits.max_input_bytes = 2048;
    let txn = sandbox_policy_config_cluster_kv_txn(&config).expect("txn");
    let mut store = InProcessKvStore::new();
    store.txn(txn).expect("commit");

    let loaded = sandbox_policy_config_from_cluster_kv_snapshot(&store.snapshot())
        .expect("load")
        .expect("config");

    assert_eq!(loaded, config);
    assert_eq!(
        sandbox_policy_config_from_cluster_kv_entries(&[]).expect("empty entries"),
        None
    );
}

#[test]
fn sandbox_policy_config_rejects_invalid_allowed_capability_on_write_and_read() {
    let mut config = SandboxClusterPolicyConfig::default();
    config
        .policy
        .allowed_command_capabilities
        .insert("network\0http".to_string());

    let write_error = sandbox_policy_config_cluster_kv_txn(&config)
        .expect_err("invalid capability should not be written");
    assert!(matches!(
        write_error,
        SandboxPolicyConfigError::Invalid { ref detail, .. }
            if detail.contains("allowed command capability")
                && detail.contains("embedded NUL byte")
    ));

    let entry = KvEntry {
        key: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
        value: sinomo_kv::encode_kv_value(&config).expect("encode invalid policy"),
        create_revision: sinomo_kv::KvRevision(1),
        modify_revision: sinomo_kv::KvRevision(1),
        version: sinomo_kv::KvVersion(1),
        lease_id: None,
    };
    let read_error = sandbox_policy_config_from_cluster_kv_entries(&[entry])
        .expect_err("invalid capability should not be loaded");
    assert!(matches!(
        read_error,
        SandboxPolicyConfigError::Invalid { ref detail, .. }
            if detail.contains("allowed command capability")
                && detail.contains("embedded NUL byte")
    ));
}

#[test]
fn sandbox_policy_config_rejects_invalid_prefixes_and_hosts() {
    let mut mount_config = SandboxClusterPolicyConfig::default();
    mount_config
        .policy
        .allowed_mount_prefixes
        .push("/services/../secret".to_string());
    let mount_error = sandbox_policy_config_cluster_kv_txn(&mount_config)
        .expect_err("invalid mount prefix should not be written");
    assert!(matches!(
        mount_error,
        SandboxPolicyConfigError::Invalid { ref detail, .. }
            if detail.contains("allowed mount prefix")
                && detail.contains("valid SIFS path prefix")
    ));

    let mut cache_config = SandboxClusterPolicyConfig::default();
    cache_config
        .policy
        .allowed_package_cache_prefixes
        .push("relative/cache".to_string());
    let cache_error = sandbox_policy_config_cluster_kv_txn(&cache_config)
        .expect_err("invalid cache prefix should not be written");
    assert!(matches!(
        cache_error,
        SandboxPolicyConfigError::Invalid { ref detail, .. }
            if detail.contains("allowed package cache prefix")
                && detail.contains("valid SIFS path prefix")
    ));

    let mut host_config = SandboxClusterPolicyConfig::default();
    host_config
        .policy
        .allowed_http_hosts
        .insert("packages.example:443".to_string());
    let host_error = sandbox_policy_config_cluster_kv_txn(&host_config)
        .expect_err("invalid host should not be written");
    assert!(matches!(
        host_error,
        SandboxPolicyConfigError::Invalid { ref detail, .. }
            if detail.contains("allowed HTTP host")
                && detail.contains("invalid")
    ));
}
