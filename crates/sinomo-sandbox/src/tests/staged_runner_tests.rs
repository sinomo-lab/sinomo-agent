use super::*;

#[test]
fn local_staged_runner_persists_command_outputs_and_records() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_staged_runner_success");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module(
            &store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_success").expect("sandbox"),
            wasi_stdout_module(),
            vec!["echo".to_string()],
            1_000,
        )
        .expect("run");

    assert_eq!(outcome.response.exit_status, 0);
    let fs = LocalSandboxFileSystem::new(layout);
    assert_eq!(
        fs.read(
            &sandbox_command_stdout_log_path(&outcome.response.command_id).expect("stdout"),
            None,
        )
        .expect("stdout"),
        b"hello\n"
    );
    let record = load_command_record(&fs, &outcome.response.command_id).expect("command record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Completed);
    assert_eq!(
        record.response.as_ref().expect("response").stdout_ref,
        outcome.response.stdout_ref
    );
    let (manifest, _) = store.load(&record.sandbox_id).expect("reload manifest");
    assert_eq!(manifest.command_history, vec![record.command_id]);
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_passes_stdin_and_persists_output() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_staged_runner_stdin");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module_with_stdin(
            &store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_stdin").expect("sandbox"),
            wasi_stdin_to_stdout_module(),
            vec!["cat".to_string()],
            Some(b"persist me\n".to_vec()),
            1_000,
        )
        .expect("run");

    assert_eq!(outcome.stdout, b"persist me\n");
    let fs = LocalSandboxFileSystem::new(layout);
    assert_eq!(
        fs.read(
            &sandbox_command_stdout_log_path(&outcome.response.command_id).expect("stdout"),
            None,
        )
        .expect("stdout"),
        b"persist me\n"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_applies_explicit_command_limits() {
    let (_manifest, _layout, runtime_root) = create_test_instance("sandbox_staged_runner_limits");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");

    let error = runner
        .run_module_with_stdin_and_limits(
            &store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_limits").expect("sandbox"),
            wasi_stdout_module(),
            vec!["limited".to_string()],
            None,
            1_000,
            Some(SandboxResourceLimits {
                max_output_bytes: 4,
                ..SandboxResourceLimits::default()
            }),
        )
        .expect_err("output limit");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::OutputLimitExceeded { .. })
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_validates_toolchain_command_policy() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_toolchain_policy");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_toolchain_policy").expect("node");

    let error = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_toolchain_policy")
                .expect("sandbox"),
            "echo",
            vec!["echo".to_string()],
            None,
            1_000,
            &node_id,
            &SandboxPolicy::default(),
        )
        .expect_err("unsigned denied");
    assert!(matches!(
        error,
        SandboxCommandExecutionError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "toolchain-unsigned-denied"
    ));

    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let mismatch = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_toolchain_policy")
                .expect("sandbox"),
            "echo",
            vec!["cat".to_string()],
            None,
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("argv0 mismatch");
    assert!(matches!(
        mismatch,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("matching argv[0]")
    ));
    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_toolchain_policy")
                .expect("sandbox"),
            "echo",
            vec!["echo".to_string()],
            None,
            1_000,
            &node_id,
            &policy,
        )
        .expect("run allowed");
    assert_eq!(outcome.stdout, b"hello\n");
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn toolchain_command_argv_prefix_preserves_command_argv0() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_argv_prefix").expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "pip",
        wasi_stdout_module().as_bytes(),
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.argv_prefix = vec!["-m".to_string(), "pip".to_string()];
    command
        .default_env
        .insert("PYTHONHOME".to_string(), "/".to_string());

    assert_eq!(
        command.effective_argv(vec!["pip".to_string(), "--version".to_string()]),
        vec![
            "pip".to_string(),
            "-m".to_string(),
            "pip".to_string(),
            "--version".to_string()
        ]
    );
    assert_eq!(command.default_env["PYTHONHOME"], "/");
}

#[test]
fn toolchain_manifest_rejects_invalid_argv_prefix_value() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_bad_argv_prefix").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "python",
        wasi_stdout_module().as_bytes(),
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.argv_prefix = vec!["-m".to_string(), "bad\0module".to_string()];

    let error = manifest
        .insert_command(command)
        .expect_err("invalid argv prefix");

    assert!(matches!(
        error,
        SandboxToolchainError::InvalidManifest { ref detail }
            if detail.contains("python")
                && detail.contains("argv_prefix[1] contains an embedded NUL byte")
    ));
}

#[test]
fn toolchain_manifest_rejects_invalid_required_capability() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_bad_capability").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "fetch",
        wasi_stdout_module().as_bytes(),
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command.required_capabilities = vec!["network\0http".to_string()];

    let error = manifest
        .insert_command(command)
        .expect_err("invalid required capability");

    assert!(matches!(
        error,
        SandboxToolchainError::InvalidManifest { ref detail }
            if detail.contains("fetch")
                && detail.contains("required capability")
                && detail.contains("embedded NUL byte")
    ));
}

#[test]
fn toolchain_manifest_rejects_invalid_default_env_name() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_bad_env").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "envdump",
        wasi_stdout_module().as_bytes(),
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command
        .default_env
        .insert("BAD-NAME".to_string(), "value".to_string());

    let error = manifest
        .insert_command(command)
        .expect_err("invalid default env name");

    assert!(matches!(
        error,
        SandboxToolchainError::InvalidManifest { ref detail }
            if detail.contains("envdump")
                && detail.contains("invalid default environment variable name `BAD-NAME`")
    ));
}

#[test]
fn toolchain_manifest_rejects_invalid_default_env_value() {
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_bad_env_value").expect("toolchain");
    let mut manifest = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    let mut command = SandboxToolchainCommand::new(
        &toolchain_id,
        "envdump",
        wasi_stdout_module().as_bytes(),
        SandboxResourceLimits::default(),
    )
    .expect("command");
    command
        .default_env
        .insert("PYTHONHOME".to_string(), "/\0bad".to_string());

    let error = manifest
        .insert_command(command)
        .expect_err("invalid default env value");

    assert!(matches!(
        error,
        SandboxToolchainError::InvalidManifest { ref detail }
            if detail.contains("envdump")
                && detail.contains("default environment variable `PYTHONHOME` contains an embedded NUL byte")
    ));
}

#[test]
fn local_staged_runner_executes_process_plan_with_stdout_pipe() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put cat");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let streamed_chunks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let streamed_chunks_for_sink = streamed_chunks.clone();
    let output_sink: std::sync::Arc<dyn SandboxOutputSink> =
        std::sync::Arc::new(move |chunk: SandboxOutputChunk| {
            streamed_chunks_for_sink
                .lock()
                .expect("streamed chunks")
                .push(chunk);
        });

    let outcome = runner
        .run_toolchain_process_plan_with_policy_and_output_sink(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan").expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
            Some(output_sink),
        )
        .expect("process plan");

    assert_eq!(outcome.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    assert_eq!(outcome.step_outcomes.len(), 2);
    let streamed_chunks = streamed_chunks.lock().expect("streamed chunks");
    assert_eq!(streamed_chunks.len(), 2);
    assert_eq!(
        streamed_chunks
            .iter()
            .map(|chunk| chunk.bytes.as_slice())
            .collect::<Vec<_>>(),
        vec![b"hello\n".as_slice(), b"hello\n".as_slice()]
    );
    let (manifest, _) = store
        .load(&SandboxId::from_legacy_named("sandbox_staged_runner_process_plan").expect("sandbox"))
        .expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    fs::remove_dir_all(runtime_root).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn local_staged_runner_executes_wasix_process_plan_with_concurrent_fd_pipe() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_wasix_process_plan");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "wasix-pipeline-1.0",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put cat");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_wasix_process_plan").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let outcome = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_wasix_process_plan")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect("wasix process plan");

    assert_eq!(outcome.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    assert_eq!(outcome.step_outcomes.len(), 2);
    assert_eq!(outcome.step_outcomes[0].stdout, b"hello\n");
    assert_eq!(outcome.step_outcomes[1].stdout, b"hello\n");
    let (manifest, _) = store
        .load(
            &SandboxId::from_legacy_named("sandbox_staged_runner_wasix_process_plan")
                .expect("sandbox"),
        )
        .expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_preflights_process_plan_policy_before_first_step() {
    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_late_policy");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "write",
            wasi_write_file_module("side.txt").as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put writer");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "python",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
            vec![SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY.to_string()],
        )
        .expect("put capability command");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_late_policy").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_late_policy")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "write".to_string(),
                        argv: vec!["write".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                    SandboxProcessPlanStep {
                        command_name: "python".to_string(),
                        argv: vec!["python".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("later process-plan capability denial should preflight");

    assert!(
        matches!(
            error,
            SandboxCommandExecutionError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
                if code == "command-capability-denied"
                    || code == "command-capability-unsupported"
        ),
        "unexpected error: {error:?}"
    );
    let sandbox_fs = LocalSandboxFileSystem::new(layout);
    assert!(matches!(
        sandbox_fs
            .read(
                &SandboxPath::new("/work/side.txt").expect("side effect"),
                None
            )
            .expect_err("first step must not run"),
        SandboxFsError::NotFound { .. }
    ));
    let (manifest, _) = store
        .load(
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_late_policy")
                .expect("sandbox"),
        )
        .expect("manifest");
    assert!(manifest.command_history.is_empty());
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_preflights_process_plan_explicit_stdin_before_first_step() {
    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_late_stdin");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "write",
            wasi_write_file_module("side.txt").as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put writer");
    let mut cat_limits = SandboxResourceLimits::default();
    cat_limits.max_input_bytes = 4;
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            cat_limits,
        )
        .expect("put cat");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_late_stdin").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_late_stdin")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "write".to_string(),
                        argv: vec!["write".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: Some(b"too much stdin".to_vec()),
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("later explicit stdin limit denial should preflight");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InputLimitExceeded { .. })
    ));
    let sandbox_fs = LocalSandboxFileSystem::new(layout);
    assert!(matches!(
        sandbox_fs
            .read(
                &SandboxPath::new("/work/side.txt").expect("side effect"),
                None
            )
            .expect_err("first step must not run"),
        SandboxFsError::NotFound { .. }
    ));
    let (manifest, _) = store
        .load(
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_late_stdin")
                .expect("sandbox"),
        )
        .expect("manifest");
    assert!(manifest.command_history.is_empty());
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_process_plan_piped_stdout_obeys_receiver_input_limit() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_pipe_input_limit");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let mut cat_limits = SandboxResourceLimits::default();
    cat_limits.max_input_bytes = 4;
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            cat_limits,
        )
        .expect("put cat");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_pipe_input_limit").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_pipe_input_limit")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("piped stdout should obey receiver input limit");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InputLimitExceeded { .. })
    ));
    let (manifest, _) = store
        .load(
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_pipe_input_limit")
                .expect("sandbox"),
        )
        .expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    let sandbox_fs = LocalSandboxFileSystem::new(store.layout(&manifest.sandbox_id));
    let receiver_record =
        load_command_record(&sandbox_fs, &manifest.command_history[1]).expect("receiver record");
    assert_eq!(receiver_record.argv, vec!["cat"]);
    assert_eq!(
        receiver_record.status,
        SandboxCommandRecordStatus::ResourceExhausted
    );
    assert!(
        receiver_record
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("input limit"),
        "receiver error should explain the input limit failure: {receiver_record:?}"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_process_plan_step_overrides_cwd_and_env() {
    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_step_overrides");
    fs::write(layout.tmp.join("cwd.txt"), b"tmp-file").expect("write cwd fixture");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "envdump",
            wasi_environ_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put envdump");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "readcwd",
            wasi_read_path_to_stdout_module(3, "cwd.txt", 8).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put readcwd");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_overrides").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let outcome = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_step_overrides")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "envdump".to_string(),
                        argv: vec!["envdump".to_string()],
                        stdin: None,
                        cwd: Some(SandboxPath::new("/tmp").expect("cwd")),
                        env: BTreeMap::from([("PLAN_VAR".to_string(), "step".to_string())]),
                        pipe_stdout_to_next: false,
                    },
                    SandboxProcessPlanStep {
                        command_name: "readcwd".to_string(),
                        argv: vec!["readcwd".to_string()],
                        stdin: None,
                        cwd: Some(SandboxPath::new("/tmp").expect("cwd")),
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect("process plan");

    assert_eq!(outcome.exit_status, 0);
    assert_eq!(outcome.stdout, b"tmp-file");
    assert_eq!(outcome.step_outcomes.len(), 2);
    let env_output = &outcome.step_outcomes[0].stdout;
    assert!(
        env_output
            .windows(b"PLAN_VAR=step\0".len())
            .any(|window| window == b"PLAN_VAR=step\0"),
        "env output should contain step override: {env_output:?}"
    );
    assert!(
        env_output
            .windows(b"PWD=/tmp\0".len())
            .any(|window| window == b"PWD=/tmp\0"),
        "env output should contain step cwd: {env_output:?}"
    );
    let (manifest, layout) = store
        .load(
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_step_overrides")
                .expect("sandbox"),
        )
        .expect("manifest");
    let sandbox_fs = LocalSandboxFileSystem::new(layout);
    let env_record =
        load_command_record(&sandbox_fs, &manifest.command_history[0]).expect("env record");
    assert_eq!(
        env_record.cwd,
        SandboxPath::new("/tmp").expect("record cwd")
    );
    assert_eq!(env_record.env["PLAN_VAR"], "step");
    assert_eq!(env_record.env["PWD"], "/tmp");
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_rejects_process_plan_trailing_pipe() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_trailing");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_trailing").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_trailing")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: true,
                }],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("trailing pipe");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { .. })
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_rejects_process_plan_ambiguous_piped_stdin() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_piped_stdin");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put cat");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_piped_stdin").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_piped_stdin")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: Some(b"explicit stdin\n".to_vec()),
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("ambiguous piped stdin");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 1")
                && detail.contains("previous step pipes stdout")
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_rejects_process_plan_invalid_env_name() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_invalid_env");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_invalid_env").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_invalid_env")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: None,
                    env: BTreeMap::from([("BAD-NAME".to_string(), "value".to_string())]),
                    pipe_stdout_to_next: false,
                }],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("invalid process-plan env name");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 0")
                && detail.contains("invalid environment variable name `BAD-NAME`")
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_rejects_process_plan_invalid_env_value() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_invalid_env_value");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_invalid_env_value").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_invalid_env_value")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: None,
                    env: BTreeMap::from([("PLAN_VAR".to_string(), "bad\0value".to_string())]),
                    pipe_stdout_to_next: false,
                }],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("invalid process-plan env value");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 0")
                && detail.contains("environment variable `PLAN_VAR` contains an embedded NUL byte")
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_preflights_process_plan_cwd_before_first_step() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_bad_cwd");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_bad_cwd")
        .expect("sandbox");
    let node_id = NodeId::from_legacy_named("node_process_plan_bad_cwd").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &sandbox_id,
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: Some(SandboxPath::new("/work/missing").expect("cwd")),
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("invalid process-plan cwd");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 1 cwd `/work/missing` is invalid")
    ));
    let (manifest, _) = store.load(&sandbox_id).expect("manifest");
    assert!(
        manifest.command_history.is_empty(),
        "process-plan cwd preflight should happen before the first command runs"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_rejects_process_plan_argv0_mismatch() {
    let (_manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_process_plan_argv0");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_process_plan_argv0").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = runner
        .run_toolchain_process_plan_with_policy(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_process_plan_argv0")
                .expect("sandbox"),
            &SandboxProcessPlan {
                steps: vec![SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["cat".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                }],
            },
            1_000,
            &node_id,
            &policy,
        )
        .expect_err("argv0 mismatch");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 0")
                && detail.contains("matching argv[0]")
    ));
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_verifies_toolchain_fidelity_cases() {
    let (_manifest, _layout, runtime_root) = create_test_instance("sandbox_staged_runner_fidelity");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let node_id = NodeId::from_legacy_named("node_toolchain_fidelity").expect("node");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    const CASES: &[SandboxBusyBoxFidelityCase] = &[SandboxBusyBoxFidelityCase {
        name: "echo-stdout",
        command: "echo",
        argv: &["echo"],
        stdin: None,
        expected_stdout: "hello\n",
        expected_exit_status: 0,
    }];

    let results = runner
        .verify_busybox_toolchain_fidelity(
            &store,
            &toolchain_store,
            &SandboxId::from_legacy_named("sandbox_staged_runner_fidelity").expect("sandbox"),
            &node_id,
            &policy,
            CASES,
        )
        .expect("fidelity");

    assert_eq!(results.len(), 1);
    assert!(results[0].passed);
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_rolls_back_written_byte_quota_violation() {
    let (mut manifest, layout, runtime_root) = create_test_instance("sandbox_staged_runner_quota");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    manifest.limits.max_written_bytes_per_command = 3;
    store.save_manifest(&manifest).expect("save limits");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");

    let error = runner
        .run_module(
            &store,
            &manifest.sandbox_id,
            wasi_write_file_module("out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect_err("quota");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Fs(SandboxFsError::QuotaExceeded { .. })
    ));
    let fs = LocalSandboxFileSystem::new(layout);
    assert!(matches!(
        fs.read(&SandboxPath::new("/work/out.txt").expect("out"), None)
            .expect_err("rolled back"),
        SandboxFsError::NotFound { .. }
    ));
    assert!(
        fs.list(&SandboxPath::new("/logs").expect("logs"))
            .expect("logs")
            .iter()
            .any(|entry| entry.path.as_str().ends_with(".record.sobj"))
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_runner_policy_denial_blocks_execution() {
    let (mut manifest, layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_policy_denied");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    manifest.limits.memory_mb = 1024;
    store.save_manifest(&manifest).expect("save manifest");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let policy = SandboxPolicy {
        max_limits: SandboxResourceLimits {
            memory_mb: 64,
            ..SandboxResourceLimits::default()
        },
        ..SandboxPolicy::default()
    };

    let error = runner
        .run_module_with_policy(
            &store,
            &manifest.sandbox_id,
            wasi_stdout_module(),
            vec!["echo".to_string()],
            1_000,
            &NodeId::from_legacy_named("node_local").expect("node"),
            &policy,
        )
        .expect_err("policy denied");

    assert!(matches!(
        error,
        SandboxCommandExecutionError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "resource-limit-denied"
    ));
    let fs = LocalSandboxFileSystem::new(layout);
    assert!(
        fs.list(&SandboxPath::new("/logs").expect("logs"))
            .expect("logs")
            .is_empty()
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn local_staged_toolchain_command_stages_support_artifacts_read_only() {
    let (manifest, layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_toolchain_artifacts");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let mut toolchain = SandboxToolchainManifest::new(
        manifest.toolchain_id.clone(),
        "artifact-test",
        SandboxWasiAbi::Preview1,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_read_path_from_any_preopen_to_stdout_module("pkg/data.txt", 7, 10, 12).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "readlib",
            wasi_read_path_from_any_preopen_to_stdout_module("site.py", 8, 10, 12).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put readlib command");
    toolchain_store
        .put_artifact(&mut toolchain, "share/pkg/data.txt", b"support")
        .expect("put artifact");
    toolchain_store
        .put_artifact(&mut toolchain, "Lib/site.py", b"stdlib!!")
        .expect("put uppercase artifact");
    toolchain_store
        .put_artifact(
            &mut toolchain,
            "Lib/site-packages/setuptools/script (dev).tmpl",
            b"template",
        )
        .expect("put real wheel artifact name");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &manifest.sandbox_id,
            "echo",
            vec!["echo".to_string()],
            None,
            1_000,
            &NodeId::from_legacy_named("node_local_artifacts").expect("node"),
            &policy,
        )
        .expect("run artifact command");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"support");
    let uppercase_outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &manifest.sandbox_id,
            "readlib",
            vec!["readlib".to_string()],
            None,
            1_000,
            &NodeId::from_legacy_named("node_local_artifacts").expect("node"),
            &policy,
        )
        .expect("run uppercase artifact command");
    assert_eq!(uppercase_outcome.response.exit_status, 0);
    assert_eq!(uppercase_outcome.stdout, b"stdlib!!");
    assert_eq!(
        read_staged_toolchain_cache_file(&layout, "share/pkg/data.txt"),
        b"support"
    );
    assert_eq!(
        read_staged_toolchain_cache_file(&layout, "Lib/site.py"),
        b"stdlib!!"
    );
    assert_eq!(
        read_staged_toolchain_cache_file(&layout, "Lib/site-packages/setuptools/script (dev).tmpl"),
        b"template"
    );
    let staged_command = read_staged_toolchain_cache_file(&layout, "bin/echo");
    assert_eq!(
        sandbox_module_hash(&staged_command),
        toolchain.command("echo").expect("command").content_hash
    );
    assert!(
        !outcome
            .response
            .changed_paths
            .iter()
            .any(|path| path.as_str().starts_with("/share")),
        "read-only toolchain artifact mounts must not be recorded as workspace changes"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn local_staged_wasix_toolchain_command_spawns_pinned_bin_child() {
    let (manifest, layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_wasix_spawn");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let mut toolchain = SandboxToolchainManifest::new(
        manifest.toolchain_id.clone(),
        "wasix-spawn-test",
        SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "sh",
            wasix_spawn_echo_module().as_bytes(),
            SandboxResourceLimits::default(),
            vec![
                SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
                SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string(),
            ],
        )
        .expect("put parent command");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put child command");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string());

    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &manifest.sandbox_id,
            "sh",
            vec!["sh".to_string()],
            None,
            1_000,
            &NodeId::from_legacy_named("node_local_wasix_spawn").expect("node"),
            &policy,
        )
        .expect("run WASIX spawn command");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    for command_name in ["sh", "echo"] {
        let staged_command =
            read_staged_toolchain_cache_file(&layout, &format!("bin/{command_name}"));
        assert_eq!(
            sandbox_module_hash(&staged_command),
            toolchain
                .command(command_name)
                .expect("command")
                .content_hash
        );
    }
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn local_staged_wasix_toolchain_command_searches_pinned_bin_for_child() {
    let (manifest, _layout, runtime_root) =
        create_test_instance("sandbox_staged_runner_wasix_path_spawn");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    let toolchain_store = LocalSandboxToolchainStore::new(runtime_root.join("toolchains"));
    let mut toolchain = SandboxToolchainManifest::new(
        manifest.toolchain_id.clone(),
        "wasix-path-spawn-test",
        SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "sh",
            wasix_spawn_echo_via_path_module().as_bytes(),
            SandboxResourceLimits::default(),
            vec![
                SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
                SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string(),
            ],
        )
        .expect("put parent command");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put child command");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string());

    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            &store,
            &toolchain_store,
            &manifest.sandbox_id,
            "sh",
            vec!["sh".to_string()],
            None,
            1_000,
            &NodeId::from_legacy_named("node_local_wasix_path_spawn").expect("node"),
            &policy,
        )
        .expect("run WASIX path spawn command");

    assert_eq!(outcome.response.exit_status, 0);
    assert_eq!(outcome.stdout, b"hello\n");
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sifs_staged_runner_runs_locally_and_syncs_back_to_sifs() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-runner-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-runner-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_staged_runner").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_staged_runner").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let _manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let chunks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let chunks_for_sink = chunks.clone();

    let outcome = runner
        .run_module_with_stdin_limits_and_output_sink(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasi_stdout_module(),
            vec!["echo".to_string()],
            None,
            1_000,
            None,
            Some(Arc::new(move |chunk: SandboxOutputChunk| {
                chunks_for_sink.lock().expect("chunks").push(chunk);
            })),
        )
        .expect("run setup");
    let command = outcome.command.expect("command");
    let chunks = chunks.lock().expect("chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].command_id, command.response.command_id);
    assert_eq!(chunks[0].bytes, b"hello\n");
    drop(chunks);

    let sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("updated manifest");
    let placement = manifest.last_placement.as_ref().expect("placement");
    assert_eq!(placement.reason, SandboxPlacementReason::AnyEligibleNode);
    assert_eq!(placement.local_owned_mount_nodes, Vec::<NodeId>::new());
    assert_eq!(
        manifest.command_history,
        vec![command.response.command_id.clone()]
    );

    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(
                &sandbox_command_stdout_log_path(&command.response.command_id).expect("stdout"),
                None,
            )
            .expect("sifs stdout"),
        b"hello\n"
    );
    let record = load_command_record(&sifs_fs, &command.response.command_id).expect("sifs record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Completed);
    assert_eq!(
        record.response.as_ref().expect("response").command_id,
        command.response.command_id
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_toolchain_command_stages_support_artifacts_read_only() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-artifacts-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-artifacts-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_staged_artifacts").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_staged_artifacts").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            toolchain_id.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "artifact-test",
        SandboxWasiAbi::Preview1,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_read_path_from_any_preopen_to_stdout_module("pkg/data.txt", 7, 3, 16).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    toolchain_store
        .put_artifact(&mut toolchain, "share/pkg/data.txt", b"support")
        .expect("put artifact");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            "echo",
            vec!["echo".to_string()],
            None,
            1_000,
            &policy,
        )
        .expect("run artifact command");
    let command = outcome.command.expect("command");

    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"support");
    assert_eq!(
        read_staged_toolchain_cache_file(&local_store.layout(&sandbox_id), "share/pkg/data.txt"),
        b"support"
    );
    let staged_command =
        read_staged_toolchain_cache_file(&local_store.layout(&sandbox_id), "bin/echo");
    assert_eq!(
        sandbox_module_hash(&staged_command),
        toolchain.command("echo").expect("command").content_hash
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_toolchain_command_spawns_pinned_bin_child() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-spawn-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-spawn-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_spawn").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_spawn").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_wasix_spawn").expect("toolchain");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            toolchain_id.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "wasix-spawn-test",
        SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "sh",
            wasix_spawn_echo_module().as_bytes(),
            SandboxResourceLimits::default(),
            vec![
                SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string(),
                SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string(),
            ],
        )
        .expect("put parent command");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put child command");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY.to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY.to_string());

    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            "sh",
            vec!["sh".to_string()],
            None,
            1_000,
            &policy,
        )
        .expect("run SIFS WASIX spawn command");
    let command = outcome.command.expect("command");

    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"hello\n");
    let layout = local_store.layout(&sandbox_id);
    for command_name in ["sh", "echo"] {
        let staged_command =
            read_staged_toolchain_cache_file(&layout, &format!("bin/{command_name}"));
        assert_eq!(
            sandbox_module_hash(&staged_command),
            toolchain
                .command(command_name)
                .expect("command")
                .content_hash
        );
    }
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(not(feature = "wasix-runtime"))]
fn sifs_staged_toolchain_command_preserves_wasix_target_abi() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-wasix-abi-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-wasix-abi-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_staged_wasix_abi").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_staged_wasix_abi").expect("sandbox");
    let toolchain_id = SandboxToolchainId::from_legacy_named("toolchain_wasix").expect("toolchain");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            toolchain_id.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "wasix-test",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let outcome = runner
        .run_toolchain_command_with_stdin_and_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            "echo",
            vec!["echo".to_string()],
            None,
            1_000,
            &policy,
        )
        .expect("staged command");
    let error = outcome
        .command
        .expect_err("wasix feature should be required");
    assert!(
        error.to_string().contains("wasix-runtime"),
        "SIFS toolchain execution must preserve target_abi=wasix instead of falling back to preview1: {error}"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_executes_process_plan_and_syncs_command_history() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_process_plan_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_process_plan_replica").expect("replica");
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
    owner_dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_process_plan").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put cat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let streamed_chunks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let streamed_chunks_for_sink = streamed_chunks.clone();
    let output_sink: std::sync::Arc<dyn SandboxOutputSink> =
        std::sync::Arc::new(move |chunk: SandboxOutputChunk| {
            streamed_chunks_for_sink
                .lock()
                .expect("streamed chunks")
                .push(chunk);
        });
    let outcome = runner
        .run_toolchain_process_plan_with_policy_and_output_sink(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &policy,
            Some(output_sink),
        )
        .expect("process plan setup");
    let process = outcome.process.expect("process plan");

    assert_eq!(process.exit_status, 0);
    assert_eq!(process.stdout, b"hello\n");
    assert_eq!(process.step_outcomes.len(), 2);
    let streamed_chunks = streamed_chunks.lock().expect("streamed chunks");
    assert_eq!(streamed_chunks.len(), 2);
    assert_eq!(
        streamed_chunks
            .iter()
            .map(|chunk| chunk.bytes.as_slice())
            .collect::<Vec<_>>(),
        vec![b"hello\n".as_slice(), b"hello\n".as_slice()]
    );
    let sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    let final_command_id = manifest.command_history[1].clone();
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(
                &sandbox_command_stdout_log_path(&final_command_id).expect("stdout"),
                None,
            )
            .expect("stdout"),
        b"hello\n"
    );

    replica_dfs
        .process_incoming()
        .expect("replica receives process plan output");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let resumed_manifest = replica_store.load(&sandbox_id).expect("load from replica");
    assert_eq!(resumed_manifest.command_history.len(), 2);
    let final_command_id = resumed_manifest.command_history[1].clone();
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &resumed_manifest);
    assert_eq!(
        replica_fs
            .read(
                &sandbox_command_stdout_log_path(&final_command_id).expect("stdout"),
                None,
            )
            .expect("replica stdout"),
        b"hello\n"
    );
    let record = load_command_record(&replica_fs, &final_command_id).expect("replica final record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Completed);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn sifs_staged_runner_executes_wasix_process_plan_with_concurrent_fd_pipe() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-process-plan-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-process-plan-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_process_plan").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_process_plan").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-wasix-1.0",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put cat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = runner
        .run_toolchain_process_plan_with_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &policy,
        )
        .expect("process plan setup");
    let process = outcome.process.expect("process plan");

    assert_eq!(process.exit_status, 0);
    assert_eq!(process.stdout, b"hello\n");
    assert_eq!(process.step_outcomes.len(), 2);
    assert_eq!(process.step_outcomes[0].stdout, b"hello\n");
    assert_eq!(process.step_outcomes[1].stdout, b"hello\n");
    let sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    let final_command_id = manifest.command_history[1].clone();
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(
                &sandbox_command_stdout_log_path(&final_command_id).expect("stdout"),
                None,
            )
            .expect("stdout"),
        b"hello\n"
    );
    let local_toolchain_layout = LocalSandboxToolchainStore::new(
        local_store
            .layout(&sandbox_id)
            .root
            .join("sifs-process-plan-toolchain"),
    )
    .layout(&manifest.toolchain_id);
    assert!(
        local_toolchain_layout.manifest.is_file(),
        "SIFS WASIX process-plan fast path should materialize the SIFS toolchain locally"
    );
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[cfg(feature = "wasix-runtime")]
#[test]
fn sifs_staged_runner_executes_wasix_process_plan_with_concurrent_fd_pipe_and_mount() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-process-plan-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-process-plan-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_process_plan_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared")
        .expect("mount source");
    dfs.posix_write_file(
        "/services/sandbox/shared/input.txt",
        b"mounted\n",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("mount input");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_process_plan_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: SandboxPath::new("/shared").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-wasix-mount-1.0",
        SandboxWasiAbi::Wasix,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "read",
            wasi_read_path_from_any_preopen_to_stdout_module("input.txt", 8, 3, 16).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put read");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put cat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = runner
        .run_toolchain_process_plan_with_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "read".to_string(),
                        argv: vec!["read".to_string()],
                        stdin: None,
                        cwd: Some(SandboxPath::new("/shared").expect("mount cwd")),
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &policy,
        )
        .expect("process plan setup");
    let process = outcome.process.expect("process plan");

    assert_eq!(process.exit_status, 0);
    assert_eq!(process.stdout, b"mounted\n");
    assert_eq!(process.step_outcomes.len(), 2);
    let sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_preflights_process_plan_policy_before_first_step() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-late-policy-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-late-policy-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_late_policy").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_late_policy").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "write",
            wasi_write_file_module("side.txt").as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put writer");
    toolchain_store
        .put_command_module_with_capabilities(
            &mut toolchain,
            "python",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
            vec![SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY.to_string()],
        )
        .expect("put capability command");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let error = match runner.run_toolchain_process_plan_with_policy(
        toolchain_store.into_inner(),
        &local_store,
        &sandbox_id,
        &SandboxProcessPlan {
            steps: vec![
                SandboxProcessPlanStep {
                    command_name: "write".to_string(),
                    argv: vec!["write".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
                SandboxProcessPlanStep {
                    command_name: "python".to_string(),
                    argv: vec!["python".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
            ],
        },
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("later process-plan capability denial should preflight"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            SandboxCommandExecutionError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
                if code == "command-capability-denied"
                    || code == "command-capability-unsupported"
        ),
        "unexpected error: {error:?}"
    );
    assert!(
        !local_store.layout(&sandbox_id).root.exists(),
        "SIFS process-plan policy preflight should happen before local staging starts"
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_preflights_process_plan_explicit_stdin_before_first_step() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-late-stdin-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-late-stdin-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_late_stdin").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_late_stdin").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "write",
            wasi_write_file_module("side.txt").as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put writer");
    let mut cat_limits = SandboxResourceLimits::default();
    cat_limits.max_input_bytes = 4;
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            cat_limits,
        )
        .expect("put cat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let error = match runner.run_toolchain_process_plan_with_policy(
        toolchain_store.into_inner(),
        &local_store,
        &sandbox_id,
        &SandboxProcessPlan {
            steps: vec![
                SandboxProcessPlanStep {
                    command_name: "write".to_string(),
                    argv: vec!["write".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
                SandboxProcessPlanStep {
                    command_name: "cat".to_string(),
                    argv: vec!["cat".to_string()],
                    stdin: Some(b"too much stdin".to_vec()),
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
            ],
        },
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("later explicit stdin limit denial should preflight"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InputLimitExceeded { .. })
    ));
    assert!(
        !local_store.layout(&sandbox_id).root.exists(),
        "SIFS process-plan stdin preflight should happen before local staging starts"
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_process_plan_piped_stdout_obeys_receiver_input_limit() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-input-limit-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-input-limit-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_input_limit").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id.clone()),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_input_limit").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");
    let mut cat_limits = SandboxResourceLimits::default();
    cat_limits.max_input_bytes = 4;
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "cat",
            wasi_stdin_to_stdout_module().as_bytes(),
            cat_limits,
        )
        .expect("put cat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = runner
        .run_toolchain_process_plan_with_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "echo".to_string(),
                        argv: vec!["echo".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: true,
                    },
                    SandboxProcessPlanStep {
                        command_name: "cat".to_string(),
                        argv: vec!["cat".to_string()],
                        stdin: None,
                        cwd: None,
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &policy,
        )
        .expect("process plan setup");
    let error = outcome
        .process
        .expect_err("receiver input limit should fail the process plan");
    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InputLimitExceeded { .. })
    ));
    let sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(manifest.command_history.len(), 2);
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let receiver_record =
        load_command_record(&sifs_fs, &manifest.command_history[1]).expect("receiver record");
    assert_eq!(receiver_record.argv, vec!["cat"]);
    assert_eq!(
        receiver_record.status,
        SandboxCommandRecordStatus::ResourceExhausted
    );
    assert!(
        receiver_record
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("input limit"),
        "receiver error should explain the input limit failure: {receiver_record:?}"
    );
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_process_plan_step_overrides_cwd_and_env() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-overrides-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-overrides-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_overrides").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_overrides").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs
        .write(
            &SandboxPath::new("/tmp/cwd.txt").expect("cwd fixture"),
            b"tmp-file",
        )
        .expect("write cwd fixture");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_fs.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "envdump",
            wasi_environ_to_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put envdump");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "readcwd",
            wasi_read_path_to_stdout_module(3, "cwd.txt", 8).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put readcwd");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = runner
        .run_toolchain_process_plan_with_policy(
            toolchain_store.into_inner(),
            &local_store,
            &sandbox_id,
            &SandboxProcessPlan {
                steps: vec![
                    SandboxProcessPlanStep {
                        command_name: "envdump".to_string(),
                        argv: vec!["envdump".to_string()],
                        stdin: None,
                        cwd: Some(SandboxPath::new("/tmp").expect("cwd")),
                        env: BTreeMap::from([("PLAN_VAR".to_string(), "sifs-step".to_string())]),
                        pipe_stdout_to_next: false,
                    },
                    SandboxProcessPlanStep {
                        command_name: "readcwd".to_string(),
                        argv: vec!["readcwd".to_string()],
                        stdin: None,
                        cwd: Some(SandboxPath::new("/tmp").expect("cwd")),
                        env: Default::default(),
                        pipe_stdout_to_next: false,
                    },
                ],
            },
            1_000,
            &policy,
        )
        .expect("process plan setup");
    let process = outcome.process.expect("process plan");

    assert_eq!(process.exit_status, 0);
    assert_eq!(process.stdout, b"tmp-file");
    assert_eq!(process.step_outcomes.len(), 2);
    let env_output = &process.step_outcomes[0].stdout;
    assert!(
        env_output
            .windows(b"PLAN_VAR=sifs-step\0".len())
            .any(|window| window == b"PLAN_VAR=sifs-step\0"),
        "env output should contain step override: {env_output:?}"
    );
    assert!(
        env_output
            .windows(b"PWD=/tmp\0".len())
            .any(|window| window == b"PWD=/tmp\0"),
        "env output should contain step cwd: {env_output:?}"
    );
    let sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(
        manifest.cwd,
        SandboxPath::new("/work").expect("manifest cwd unchanged")
    );
    assert!(manifest.env.is_empty());
    assert_eq!(manifest.command_history.len(), 2);
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let env_record =
        load_command_record(&sifs_fs, &manifest.command_history[0]).expect("env record");
    assert_eq!(
        env_record.cwd,
        SandboxPath::new("/tmp").expect("record cwd")
    );
    assert_eq!(env_record.env["PLAN_VAR"], "sifs-step");
    assert_eq!(env_record.env["PWD"], "/tmp");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_preflights_process_plan_cwd_before_first_step() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-bad-cwd-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-bad-cwd-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_bad_cwd").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_bad_cwd").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put echo");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let error = match runner.run_toolchain_process_plan_with_policy(
        toolchain_store.into_inner(),
        &local_store,
        &sandbox_id,
        &SandboxProcessPlan {
            steps: vec![
                SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
                SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: Some(SandboxPath::new("/work/missing").expect("cwd")),
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
            ],
        },
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("invalid process-plan cwd should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 1 cwd `/work/missing` is invalid")
    ));
    assert!(
        !local_store.layout(&sandbox_id).root.exists(),
        "SIFS process-plan cwd preflight should happen before local staging starts"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_rejects_process_plan_argv0_mismatch() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-argv0-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-argv0-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_argv0").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_argv0").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = match runner.run_toolchain_process_plan_with_policy(
        sifs_store.into_inner(),
        &local_store,
        &sandbox_id,
        &SandboxProcessPlan {
            steps: vec![SandboxProcessPlanStep {
                command_name: "echo".to_string(),
                argv: vec!["cat".to_string()],
                stdin: None,
                cwd: None,
                env: Default::default(),
                pipe_stdout_to_next: false,
            }],
        },
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("argv0 mismatch should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 0")
                && detail.contains("matching argv[0]")
    ));
    assert!(!local_store.layout(&sandbox_id).root.exists());
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_rejects_process_plan_ambiguous_piped_stdin() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-piped-stdin-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-process-plan-piped-stdin-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_process_plan_piped_stdin").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_process_plan_piped_stdin").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let mut policy = SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;

    let error = match runner.run_toolchain_process_plan_with_policy(
        sifs_store.into_inner(),
        &local_store,
        &sandbox_id,
        &SandboxProcessPlan {
            steps: vec![
                SandboxProcessPlanStep {
                    command_name: "echo".to_string(),
                    argv: vec!["echo".to_string()],
                    stdin: None,
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: true,
                },
                SandboxProcessPlanStep {
                    command_name: "cat".to_string(),
                    argv: vec!["cat".to_string()],
                    stdin: Some(b"explicit stdin\n".to_vec()),
                    cwd: None,
                    env: Default::default(),
                    pipe_stdout_to_next: false,
                },
            ],
        },
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("ambiguous piped stdin should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SandboxCommandExecutionError::Wasi(SandboxWasiError::InvalidRequest { ref detail })
            if detail.contains("process plan step 1")
                && detail.contains("previous step pipes stdout")
    ));
    assert!(!local_store.layout(&sandbox_id).root.exists());
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_command_progress_resumes_from_replica_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-staged-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_staged_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_staged_replica").expect("replica");
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
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_staged_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasi_write_file_module("progress.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run setup");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    assert!(
        command
            .response
            .changed_paths
            .iter()
            .any(|path| path == &SandboxPath::new("/work/progress.txt").expect("path")),
        "command changed paths should include the durable workspace write"
    );

    replica_dfs
        .process_incoming()
        .expect("replica receives command progress");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let resumed_manifest = replica_store.load(&sandbox_id).expect("load from replica");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &resumed_manifest);

    assert_eq!(
        resumed_manifest.command_history,
        vec![command.response.command_id.clone()]
    );
    assert_eq!(
        replica_fs
            .read(
                &SandboxPath::new("/work/progress.txt").expect("progress"),
                None,
            )
            .expect("replica read command output"),
        b"created"
    );
    let record =
        load_command_record(&replica_fs, &command.response.command_id).expect("replica record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Completed);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_stages_rw_mounts_and_syncs_back_to_sifs() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared")
        .expect("mount source");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: SandboxPath::new("/shared").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasi_write_file_module_on_fd(10, "out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run setup");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(
        outcome
            .dfs
            .posix_read_file("/services/sandbox/shared/out.txt")
            .expect("mounted output"),
        b"created"
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_replicates_rw_mount_writeback_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_mount_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_mount_replica").expect("replica");
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
    owner_dfs
        .posix_mkdir_all("/services/sandbox/shared")
        .expect("mount source");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_mount_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let mut manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: SandboxPath::new("/shared").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    owner_store.save_manifest(&manifest).expect("save manifest");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasi_write_file_module_on_fd(10, "out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run setup");
    assert_eq!(outcome.command.expect("command").response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives mount writeback");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    assert_eq!(
        replica_dfs
            .posix_read_file("/services/sandbox/shared/out.txt")
            .expect("replica mounted output"),
        b"created"
    );

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_keeps_read_only_mounts_read_only() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_ro_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared")
        .expect("mount source");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_ro_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: SandboxPath::new("/shared").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasi_write_file_module_on_fd(10, "out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run setup");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, SINOMO_WASI_ERRNO_NOTCAPABLE);
    assert!(matches!(
        outcome
            .dfs
            .posix_read_file("/services/sandbox/shared/out.txt"),
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
    ));
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_policy_denial_blocks_staging() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-policy-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-policy-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_policy").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_staged_policy").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/users/alice".to_string(),
        mount_path: SandboxPath::new("/users").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let error = match runner.run_module_with_policy(
        sifs_store.into_inner(),
        &local_store,
        &sandbox_id,
        wasi_stdout_module(),
        vec!["echo".to_string()],
        1_000,
        &SandboxPolicy::default(),
    ) {
        Ok(_) => panic!("policy should deny staging"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SandboxCommandExecutionError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "mount-source-denied"
    ));
    assert!(!local_store.layout(&sandbox_id).root.exists());
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_denies_remote_local_owned_mount_before_staging() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-local-owned-policy-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-local-owned-policy-local");
    let network = InProcessNetwork::new();
    let execution_node = NodeId::from_legacy_named("node_sifs_local_owned_exec").expect("node");
    let owner_node = NodeId::from_legacy_named("node_sifs_local_owned_owner").expect("owner");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(execution_node.clone()),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_local_owned_policy").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{owner_node}/files"),
        mount_path: SandboxPath::new("/host").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        ..SandboxPolicy::default()
    };

    let error = match runner.run_module_with_policy(
        sifs_store.into_inner(),
        &local_store,
        &sandbox_id,
        wasi_stdout_module(),
        vec!["echo".to_string()],
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("remote local-owned mount should be denied before staging"),
        Err(SandboxCommandExecutionError::PolicyDenied(error)) => error,
        Err(error) => panic!("expected policy denial, got {error:?}"),
    };

    assert_eq!(error.code, "mount-local-owned-remote-denied");
    assert!(
        !local_store.layout(&sandbox_id).root.exists(),
        "denied policy must not create a local staging workspace"
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_validates_sifs_toolchain_command_policy() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-toolchain-policy-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-toolchain-policy-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_toolchain_policy").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_toolchain_policy").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    sifs_store
        .create(
            sandbox_id.clone(),
            toolchain_id.clone(),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut toolchain_store = SifsSandboxToolchainStore::new(sifs_store.into_inner());
    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id,
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "test build",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasi_stdout_module().as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let error = match runner.run_toolchain_command_with_stdin_and_policy(
        toolchain_store.into_inner(),
        &local_store,
        &sandbox_id,
        "echo",
        vec!["echo".to_string()],
        None,
        1_000,
        &SandboxPolicy::default(),
    ) {
        Ok(_) => panic!("policy should deny unsigned SIFS toolchain"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        SandboxCommandExecutionError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "toolchain-unsigned-denied"
    ));
    assert!(!local_store.layout(&sandbox_id).root.exists());
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}
