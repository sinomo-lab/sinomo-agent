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
    let mut fs = LocalSandboxFileSystem::new(layout);
    assert_eq!(
        fs.read(
            &sandbox_command_stdout_log_path(&outcome.response.command_id).expect("stdout"),
            None,
        )
        .expect("stdout"),
        b"hello\n"
    );
    let record =
        load_command_record(&mut fs, &outcome.response.command_id).expect("command record");
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
        SandboxWasiAbi::Wasix,
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
        SandboxWasiAbi::Wasix,
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
        SandboxWasiAbi::Wasix,
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
        SandboxWasiAbi::Wasix,
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
        SandboxWasiAbi::Wasix,
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
            vec![SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string()],
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
        SandboxWasiAbi::Wasix,
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
    let mut sandbox_fs = LocalSandboxFileSystem::new(store.layout(&manifest.sandbox_id));
    let receiver_record = load_command_record(&mut sandbox_fs, &manifest.command_history[1])
        .expect("receiver record");
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
        SandboxWasiAbi::Wasix,
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
            wasix_read_absolute_file_to_stdout_module("/tmp/cwd.txt", 8).as_bytes(),
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
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let env_record =
        load_command_record(&mut sandbox_fs, &manifest.command_history[0]).expect("env record");
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
fn local_staged_runner_preserves_write_before_quota_failure() {
    let (mut manifest, layout, runtime_root) = create_test_instance("sandbox_staged_runner_quota");
    let store = LocalSandboxInstanceStore::new(&runtime_root);
    manifest.limits.max_written_bytes_per_command = 3;
    store.save_manifest(&manifest).expect("save limits");
    let runner = LocalStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module(
            &store,
            &manifest.sandbox_id,
            wasi_write_file_module("out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run");

    assert_ne!(outcome.response.exit_status, 0);
    let fs = LocalSandboxFileSystem::new(layout);
    assert_eq!(
        fs.read(&SandboxPath::new("/work/out.txt").expect("out"), None)
            .expect("completed write remains visible"),
        b"created"
    );
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
        SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasix_read_absolute_file_to_stdout_module("/share/pkg/data.txt", 7).as_bytes(),
            SandboxResourceLimits::default(),
        )
        .expect("put command");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "readlib",
            wasix_read_absolute_file_to_stdout_module("/Lib/site.py", 8).as_bytes(),
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
fn sifs_staged_runner_executes_directly_on_sifs_and_persists_records() {
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

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("updated manifest");
    let placement = manifest.last_placement.as_ref().expect("placement");
    assert_eq!(placement.reason, SandboxPlacementReason::AnyEligibleNode);
    assert_eq!(placement.local_owned_mount_nodes, Vec::<NodeId>::new());
    assert_eq!(
        manifest.command_history,
        vec![command.response.command_id.clone()]
    );

    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(
                &sandbox_command_stdout_log_path(&command.response.command_id).expect("stdout"),
                None,
            )
            .expect("sifs stdout"),
        b"hello\n"
    );
    let record =
        load_command_record(&mut sifs_fs, &command.response.command_id).expect("sifs record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Completed);
    assert_eq!(
        record.response.as_ref().expect("response").command_id,
        command.response.command_id
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_link_preserves_symlink_object() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    sifs_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("symlink"),
        )
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_module("/work/link.txt", "/work/link-alias.txt"),
            vec!["link".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let alias = SandboxPath::new("/work/link-alias.txt").expect("alias");
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    let alias_metadata = sifs_fs.stat(&alias).expect("alias stat");
    assert!(link_metadata.is_symlink);
    assert!(alias_metadata.is_symlink);
    assert_eq!(link_metadata.inode, alias_metadata.inode);
    assert_eq!(alias_metadata.link_count, 2);
    assert_eq!(
        sifs_fs.readlink(&alias).expect("alias readlink"),
        PathBuf::from("target.txt")
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_link_follow_symlink_links_target_object() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-follow-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-follow-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_follow").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_follow").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let alias = SandboxPath::new("/work/target-alias.txt").expect("alias");
    sifs_fs.write(&target, b"target").expect("write target");
    sifs_fs
        .symlink(Path::new("target.txt"), &link)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_with_flags_module(
                "/work/link.txt",
                "/work/target-alias.txt",
                1,
            ),
            vec!["link-follow".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link follow module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target).expect("target stat");
    let alias_metadata = sifs_fs.stat(&alias).expect("alias stat");
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    assert!(!alias_metadata.is_symlink);
    assert_eq!(target_metadata.inode, alias_metadata.inode);
    assert_eq!(target_metadata.link_count, 2);
    assert_eq!(alias_metadata.link_count, 2);
    assert!(link_metadata.is_symlink);
    assert_eq!(link_metadata.link_count, 1);
    assert_eq!(sifs_fs.read(&alias, None).expect("alias read"), b"target");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_filestat_follow_symlink_reports_target_inode() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-stat-follow-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-stat-follow-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_stat_follow").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_stat_follow").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    sifs_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("symlink"),
        )
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_filestat_follow_symlink_matches_target_module(
                "/work/link.txt",
                "/work/target.txt",
            ),
            vec!["stat-follow".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run stat follow module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_link_across_mount_boundary_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-cross-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-cross-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_cross_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let shared_dir = "/services/shared/wasix-link-cross-mount";
    let shared_file = "/services/shared/wasix-link-cross-mount/source.txt";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    dfs.posix_write_file(
        shared_file,
        b"source",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("shared source");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_cross_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/shared").expect("shared mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_module("/shared/source.txt", "/work/alias.txt"),
            vec!["link".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&SandboxPath::new("/work/alias.txt").expect("alias")),
        Err(SandboxFsError::NotFound { .. })
    ));
    let dfs = sifs_fs.into_inner();
    assert_eq!(
        dfs.posix_stat(shared_file).expect("source stat").link_count,
        1
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readlink_and_lstat_work_on_hardlinked_symlink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readlink-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readlink-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readlink_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readlink_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    sifs_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("symlink"),
        )
        .expect("symlink");
    sifs_fs
        .hard_link(
            &SandboxPath::new("/work/link.txt").expect("link"),
            &SandboxPath::new("/work/link-alias.txt").expect("alias"),
        )
        .expect("hardlink symlink");
    let alias = SandboxPath::new("/work/link-alias.txt").expect("alias");
    sifs_fs
        .set_symlink_times(&alias, Some(12_345), Some(67_890))
        .expect("old symlink times");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let readlink_outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readlink_absolute_path_to_stdout_module("/work/link-alias.txt"),
            vec!["readlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readlink module");
    let command = readlink_outcome.command.expect("readlink command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"target.txt");

    let lstat_outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            readlink_outcome.dfs,
            &local_store,
            &sandbox_id,
            wasix_lstat_filetype_equals_module("/work/link-alias.txt", 7),
            vec!["lstat".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run lstat module");
    let command = lstat_outcome.command.expect("lstat command");
    assert_eq!(command.response.exit_status, 0);
    let mut sifs_store = SifsSandboxInstanceStore::new(lstat_outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let metadata = sifs_fs.stat_unchecked(&alias).expect("alias stat");
    assert!(metadata.accessed_at_unix_nanos > 12_345);
    assert_eq!(metadata.modified_at_unix_nanos, 67_890);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_filestat_reports_sifs_hardlink_count() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-filestat-nlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-filestat-nlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_filestat_nlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_filestat_nlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"content",
        )
        .expect("write original");
    sifs_fs
        .hard_link(
            &SandboxPath::new("/work/original.txt").expect("original"),
            &SandboxPath::new("/work/alias.txt").expect("alias"),
        )
        .expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_filestat_same_inode_and_nlink_module("/work/original.txt", "/work/alias.txt", 2),
            vec!["stat".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run stat module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_reports_sifs_hardlink_identity() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-filestat-nlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-filestat-nlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_filestat_nlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_filestat_nlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"content",
        )
        .expect("write original");
    sifs_fs
        .hard_link(
            &SandboxPath::new("/work/original.txt").expect("original"),
            &SandboxPath::new("/work/alias.txt").expect("alias"),
        )
        .expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_fd_filestat_same_inode_and_nlink_module(
                "/work/original.txt",
                "/work/alias.txt",
                2,
            ),
            vec!["fd-stat".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd stat module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_reports_sifs_ctime() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-filestat-ctime-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-filestat-ctime-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_filestat_ctime").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_filestat_ctime").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/ctime.txt").expect("path");
    sifs_fs.write(&path, b"metadata").expect("write file");
    let before = sifs_fs.stat(&path).expect("before stat");
    sifs_fs.set_mode(&path, 0o600).expect("chmod changes ctime");
    let after = sifs_fs.stat(&path).expect("after stat");
    assert_eq!(
        after.created_at_unix_nanos, before.created_at_unix_nanos,
        "chmod must not change birth time"
    );
    assert!(
        after.changed_at_unix_nanos >= before.changed_at_unix_nanos,
        "chmod must advance ctime"
    );

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_fd_filestat_ctime_equals_module("/work/ctime.txt", after.changed_at_unix_nanos),
            vec!["fd-stat-ctime".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd stat ctime module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_reports_link_added_after_open() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-link-added-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-link-added-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_link_added").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_link_added").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"content",
        )
        .expect("write original");
    let work = SandboxPath::new("/work").expect("work");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    sifs_fs
        .set_times(&work, Some(12_345), Some(67_890))
        .expect("old work times");
    sifs_fs
        .set_times(&original, Some(23_456), Some(78_901))
        .expect("old original times");
    let work_before = sifs_fs.stat(&work).expect("work before link");
    let original_before = sifs_fs.stat(&original).expect("original before link");
    std::thread::sleep(std::time::Duration::from_millis(1));

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_link_then_fd_filestat_nlink_module(
                "/work/original.txt",
                "/work/alias.txt",
                2,
            ),
            vec!["fd-stat-link".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd stat link module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    let work_after = sifs_fs.stat(&work).expect("work after link");
    let original_after = sifs_fs.stat(&original).expect("original after link");
    assert_eq!(
        original_after.modified_at_unix_nanos, original_before.modified_at_unix_nanos,
        "adding a hardlink must not change file content mtime"
    );
    assert!(
        original_after.changed_at_unix_nanos > original_before.changed_at_unix_nanos,
        "adding a hardlink must advance the target inode ctime"
    );
    assert_eq!(original_after.link_count, 2);
    assert_eq!(sifs_fs.stat(&alias).expect("alias stat").link_count, 2);
    assert!(
        work_after.modified_at_unix_nanos > work_before.modified_at_unix_nanos,
        "adding a directory entry must advance parent directory mtime"
    );
    assert!(
        work_after.changed_at_unix_nanos > work_before.changed_at_unix_nanos,
        "adding a directory entry must advance parent directory ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_relative_hardlink_between_directory_fds_updates_sifs() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-hardlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-hardlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_relative_hardlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_relative_hardlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source_dir = SandboxPath::new("/work/src").expect("source dir");
    let target_dir = SandboxPath::new("/work/dst").expect("target dir");
    let source = SandboxPath::new("/work/src/file.txt").expect("source");
    let alias = SandboxPath::new("/work/dst/alias.txt").expect("alias");
    sifs_fs.create_dir(&source_dir).expect("create source dir");
    sifs_fs.create_dir(&target_dir).expect("create target dir");
    sifs_fs.write(&source, b"shared").expect("write source");
    sifs_fs
        .set_times(&source_dir, Some(12_345), Some(67_890))
        .expect("old source dir times");
    sifs_fs
        .set_times(&target_dir, Some(22_345), Some(77_890))
        .expect("old target dir times");
    sifs_fs
        .set_times(&source, Some(32_345), Some(87_890))
        .expect("old source times");
    let source_dir_before = sifs_fs.stat(&source_dir).expect("source dir before link");
    let target_dir_before = sifs_fs.stat(&target_dir).expect("target dir before link");
    let source_before = sifs_fs.stat(&source).expect("source before link");
    std::thread::sleep(std::time::Duration::from_millis(1));

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_relative_between_directories_module(
                "/work/src",
                "file.txt",
                "/work/dst",
                "alias.txt",
            ),
            vec!["relative-link".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run relative link module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let source_after = sifs_fs.stat(&source).expect("source after link");
    let alias_after = sifs_fs.stat(&alias).expect("alias after link");
    assert_eq!(sifs_fs.read(&alias, None).expect("alias read"), b"shared");
    assert_eq!(source_after.inode, alias_after.inode);
    assert_eq!(source_after.link_count, 2);
    assert_eq!(alias_after.link_count, 2);
    assert_eq!(
        source_after.modified_at_unix_nanos, source_before.modified_at_unix_nanos,
        "relative hardlink must not change source file content mtime"
    );
    assert!(
        source_after.changed_at_unix_nanos > source_before.changed_at_unix_nanos,
        "relative hardlink must advance source inode ctime"
    );
    let source_dir_after = sifs_fs.stat(&source_dir).expect("source dir after link");
    assert_eq!(
        source_dir_after.modified_at_unix_nanos, source_dir_before.modified_at_unix_nanos,
        "hardlink creation must not mutate the source directory mtime"
    );
    assert_eq!(
        source_dir_after.changed_at_unix_nanos, source_dir_before.changed_at_unix_nanos,
        "hardlink creation must not mutate the source directory ctime"
    );
    let target_dir_after = sifs_fs.stat(&target_dir).expect("target dir after link");
    assert!(
        target_dir_after.modified_at_unix_nanos > target_dir_before.modified_at_unix_nanos,
        "relative hardlink must advance target directory mtime"
    );
    assert!(
        target_dir_after.changed_at_unix_nanos > target_dir_before.changed_at_unix_nanos,
        "relative hardlink must advance target directory ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_refreshes_size_after_alias_write() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-size-refresh-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-size-refresh-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_size_refresh").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_size_refresh").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs.write(&original, b"old").expect("write original");
    sifs_fs.hard_link(&original, &alias).expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_then_write_alias_then_fd_filestat_size_module(
                "/work/original.txt",
                "/work/alias.txt",
                b"expanded",
                8,
            ),
            vec!["fd-stat-size-refresh".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd stat size refresh module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&original, None).expect("original"),
        b"expanded"
    );
    assert_eq!(sifs_fs.read(&alias, None).expect("alias"), b"expanded");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_write_through_hardlink_updates_shared_sifs_object() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-write-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-write-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_write").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_write").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"initial",
        )
        .expect("write original");
    sifs_fs
        .hard_link(
            &SandboxPath::new("/work/original.txt").expect("original"),
            &SandboxPath::new("/work/alias.txt").expect("alias"),
        )
        .expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/alias.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    assert_eq!(sifs_fs.read(&original, None).expect("original"), b"created");
    assert_eq!(sifs_fs.read(&alias, None).expect("alias"), b"created");
    assert_eq!(
        sifs_fs.stat(&original).expect("original stat").link_count,
        2
    );
    assert_eq!(
        sifs_fs.stat(&original).expect("original inode").inode,
        sifs_fs.stat(&alias).expect("alias inode").inode
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_resource_failure_preserves_live_hardlink_write() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-resource-hardlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-resource-hardlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_resource_hardlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_resource_hardlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.limits.max_written_bytes_per_command = 3;
    sifs_store.save_manifest(&manifest).expect("save limits");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs
        .write(&original, b"initial")
        .expect("write original");
    sifs_fs.hard_link(&original, &alias).expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/alias.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect_err("resource limit should fail");
    assert!(
        command.to_string().contains("quota exceeded")
            || command.to_string().contains("resource")
            || command.to_string().contains("limit"),
        "unexpected resource failure: {command}"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        manifest.command_history.len(),
        1,
        "resource-exhausted SIFS runs should remain discoverable through command history"
    );
    let record =
        load_command_record(&mut sifs_fs, &manifest.command_history[0]).expect("resource record");
    assert_eq!(record.status, SandboxCommandRecordStatus::ResourceExhausted);
    assert_eq!(sifs_fs.read(&original, None).expect("original"), b"created");
    assert_eq!(sifs_fs.read(&alias, None).expect("alias"), b"created");
    let original_stat = sifs_fs.stat(&original).expect("original stat");
    let alias_stat = sifs_fs.stat(&alias).expect("alias stat");
    assert_eq!(original_stat.inode, alias_stat.inode);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_resource_failure_preserves_created_directories() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-resource-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-resource-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_resource_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_resource_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.limits.max_written_bytes_per_command = 3;
    sifs_store.save_manifest(&manifest).expect("save limits");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_create_directory_then_write_absolute_file_module(
                "/work/generated",
                "/work/generated/out.txt",
            ),
            vec!["mkdir-write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run mkdir-write module");
    let command = outcome.command.expect_err("resource limit should fail");
    assert!(
        command.to_string().contains("quota exceeded")
            || command.to_string().contains("resource")
            || command.to_string().contains("limit"),
        "unexpected resource failure: {command}"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/generated").expect("generated"))
            .expect("generated directory remains visible")
            .is_dir
    );
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/work/generated/out.txt").expect("generated file"),
                None,
            )
            .expect("completed write remains visible"),
        b"created"
    );
    assert_eq!(manifest.command_history.len(), 1);
    let record =
        load_command_record(&mut sifs_fs, &manifest.command_history[0]).expect("resource record");
    assert_eq!(record.status, SandboxCommandRecordStatus::ResourceExhausted);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_resource_failure_preserves_replaced_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-resource-type-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-resource-type-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_resource_type").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_resource_type").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.limits.max_written_bytes_per_command = 3;
    sifs_store.save_manifest(&manifest).expect("save limits");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let replaced_path = SandboxPath::new("/work/replaced").expect("replaced");
    let child_path = SandboxPath::new("/work/replaced/out.txt").expect("child");
    sifs_fs
        .write(&replaced_path, b"initial")
        .expect("write replaced");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_replace_file_with_directory_then_write_module(
                replaced_path.as_str(),
                child_path.as_str(),
            ),
            vec!["replace-file-with-dir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run replace module");
    let command = outcome.command.expect_err("resource limit should fail");
    assert!(
        command.to_string().contains("quota exceeded")
            || command.to_string().contains("resource")
            || command.to_string().contains("limit"),
        "unexpected resource failure: {command}"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let replaced_metadata = sifs_fs.stat(&replaced_path).expect("replaced path remains");
    assert!(replaced_metadata.is_dir);
    assert_eq!(
        sifs_fs.read(&child_path, None).expect("replacement child"),
        b"created"
    );
    assert_eq!(manifest.command_history.len(), 1);
    let record =
        load_command_record(&mut sifs_fs, &manifest.command_history[0]).expect("resource record");
    assert_eq!(record.status, SandboxCommandRecordStatus::ResourceExhausted);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_startup_failure_is_recorded_in_command_history() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-startup-failure-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-startup-failure-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_startup_failure").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_startup_failure").expect("sandbox");
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
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            b"not a wasm module",
            vec!["broken".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("startup failure should still return SIFS state");
    let error = outcome.command.expect_err("invalid module must fail");
    assert!(
        error.to_string().contains("WASM") || error.to_string().contains("module"),
        "unexpected startup error: {error}"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(manifest.command_history.len(), 1);
    let record = load_command_record(&mut sifs_fs, &manifest.command_history[0]).expect("record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Failed);
    assert_eq!(record.argv, vec!["broken"]);
    assert!(record.response.is_none());
    let error_text = error.to_string();
    assert_eq!(
        record.error.as_deref(),
        Some(error_text.as_str()),
        "persisted command record should carry the same startup error returned to the caller"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_hardlink_write_replicates_shared_object_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_hardlink_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_hardlink_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"initial",
        )
        .expect("write original");
    owner_fs
        .hard_link(
            &SandboxPath::new("/work/original.txt").expect("original"),
            &SandboxPath::new("/work/alias.txt").expect("alias"),
        )
        .expect("hardlink");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/alias.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives hardlink mutation");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");

    assert_eq!(
        replica_fs.read(&original, None).expect("replica original"),
        b"created"
    );
    assert_eq!(
        replica_fs.read(&alias, None).expect("replica alias"),
        b"created"
    );
    assert_eq!(
        replica_fs.stat(&original).expect("original stat").inode,
        replica_fs.stat(&alias).expect("alias stat").inode
    );
    assert_eq!(replica_fs.stat(&alias).expect("alias nlink").link_count, 2);

    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_append_through_hardlink_preserves_existing_shared_content() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-append-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-append-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_append").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_append").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"base",
        )
        .expect("write original");
    sifs_fs
        .hard_link(
            &SandboxPath::new("/work/original.txt").expect("original"),
            &SandboxPath::new("/work/alias.txt").expect("alias"),
        )
        .expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_append_absolute_file_module("/work/alias.txt"),
            vec!["append".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run append module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    assert_eq!(
        sifs_fs.read(&original, None).expect("original"),
        b"base-appended"
    );
    assert_eq!(sifs_fs.read(&alias, None).expect("alias"), b"base-appended");
    assert_eq!(sifs_fs.stat(&alias).expect("alias nlink").link_count, 2);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_metadata_changes_through_hardlink_update_shared_object() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-metadata-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-metadata-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_metadata").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_metadata").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs
        .write(&original, b"content")
        .expect("write original");
    sifs_fs
        .hard_link(&original, &alias)
        .expect("hardlink alias");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_metadata_module(
                "/work/alias.txt",
                0o750,
                1001,
                1002,
                12_345,
                67_890,
            ),
            vec!["metadata".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run metadata module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let original_stat = sifs_fs.stat(&original).expect("original stat");
    let alias_stat = sifs_fs.stat(&alias).expect("alias stat");
    assert_eq!(original_stat.inode, alias_stat.inode);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(original_stat.mode, 0o750);
    assert_eq!(alias_stat.mode, 0o750);
    assert_eq!(original_stat.uid, 1001);
    assert_eq!(original_stat.gid, 1002);
    assert_eq!(alias_stat.uid, 1001);
    assert_eq!(alias_stat.gid, 1002);
    assert_eq!(original_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(original_stat.modified_at_unix_nanos, 67_890);
    assert_eq!(alias_stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(alias_stat.modified_at_unix_nanos, 67_890);
    assert!(original_stat.changed_at_unix_nanos > 67_890);
    assert_eq!(
        original_stat.changed_at_unix_nanos,
        alias_stat.changed_at_unix_nanos
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_metadata_changes_replicate_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-metadata-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-metadata-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_metadata_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_metadata_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_metadata_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    owner_fs.write(&file, b"content").expect("write file");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_metadata_module(
                "/work/file.txt",
                0o640,
                1001,
                1002,
                12_345,
                67_890,
            ),
            vec!["metadata".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run metadata module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    drop(outcome.dfs);

    replica_dfs
        .process_incoming()
        .expect("replica receives metadata");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let stat = replica_fs.stat(&file).expect("replica stat");
    assert_eq!(stat.mode, 0o640);
    assert_eq!(stat.uid, 1001);
    assert_eq!(stat.gid, 1002);
    assert_eq!(stat.accessed_at_unix_nanos, 12_345);
    assert_eq!(stat.modified_at_unix_nanos, 67_890);
    assert_eq!(
        replica_fs.read(&file, None).expect("replica read"),
        b"content"
    );

    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_chmod_read_only_file_denies_write() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-read-only-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-read-only-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_chmod_read_only").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_chmod_read_only").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/read-only.txt").expect("file");
    sifs_fs.write(&file, b"original").expect("write file");
    sifs_fs.set_mode(&file, 0o444).expect("chmod read-only");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/read-only.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_ne!(
        command.response.exit_status, 0,
        "write to chmod 0444 file must fail"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("file content"),
        b"original"
    );
    assert_eq!(sifs_fs.stat(&file).expect("stat").mode, 0o444);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_write_only_file_allows_write() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-write-only-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-write-only-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_write_only").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_write_only").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/write-only.txt").expect("file");
    sifs_fs.write(&file, b"original").expect("write file");
    sifs_fs.set_mode(&file, 0o222).expect("chmod write-only");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/write-only.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_eq!(
        command.response.exit_status, 0,
        "write-only files should permit write/truncate opens"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read_unchecked(&file, None)
            .expect("file content after write"),
        b"created"
    );
    assert_eq!(sifs_fs.stat_unchecked(&file).expect("stat").mode, 0o222);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_write_only_file_allows_append() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-write-only-append-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-write-only-append-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_write_only_append").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_write_only_append").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/write-only-append.txt").expect("file");
    sifs_fs.write(&file, b"original").expect("write file");
    sifs_fs.set_mode(&file, 0o222).expect("chmod write-only");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_append_absolute_file_module("/work/write-only-append.txt"),
            vec!["append".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run append module");
    let command = outcome.command.expect("command");
    assert_eq!(
        command.response.exit_status, 0,
        "write-only files should permit append opens"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read_unchecked(&file, None)
            .expect("file content after append"),
        b"original-appended"
    );
    assert_eq!(sifs_fs.stat_unchecked(&file).expect("stat").mode, 0o222);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_seek_end_observes_second_descriptor_append() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-seek-end-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-seek-end-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_seek_end").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_seek_end").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/seek-end.txt").expect("file");
    sifs_fs.write(&file, b"old").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_seek_end_after_second_fd_append_module("/work/seek-end.txt"),
            vec!["seek-end".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run seek end module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("file content"),
        b"oldBC",
        "SEEK_END must use the current SIFS object length, not the descriptor's stale cached length"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_append_tell_uses_current_backing_size() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-append-tell-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-append-tell-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_append_tell").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_append_tell").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/append-tell.txt").expect("path");
    sifs_fs.write(&path, b"abc").expect("write file");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_append_tell_after_second_fd_pwrite_module("/work/append-tell.txt", 9, 11),
            vec!["append-tell".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run append tell module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&path, None).expect("read append tell result"),
        b"abc\0\0\0\0\0\0ZA",
        "append writes must use the current SIFS EOF even after another descriptor extends the file"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_descriptor_keeps_write_right_after_chmod() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-chmod-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-chmod-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_chmod").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_chmod").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/open-chmod.txt").expect("file");
    sifs_fs.write(&file, b"old").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_append_then_chmod_then_write_module("/work/open-chmod.txt"),
            vec!["open-chmod".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open chmod module");
    let command = outcome.command.expect("command");
    assert_eq!(
        command.response.exit_status, 0,
        "chmod after open must not revoke the already-open descriptor's write right"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.read_unchecked(&file, None).expect("file"), b"oldX");
    assert_eq!(sifs_fs.stat_unchecked(&file).expect("stat").mode, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_descriptor_keeps_read_right_after_chmod() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-read-chmod-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-read-chmod-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_read_chmod").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_read_chmod").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/open-read-chmod.txt").expect("file");
    sifs_fs.write(&file, b"readable").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_read_then_chmod_then_read_module("/work/open-read-chmod.txt"),
            vec!["open-read-chmod".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open read chmod module");
    let command = outcome.command.expect("command");
    assert_eq!(
        command.response.exit_status, 0,
        "chmod after open must not revoke the already-open descriptor's read right"
    );
    assert_eq!(command.stdout, b"readable");
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.stat_unchecked(&file).expect("stat").mode, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_chmod_read_only_directory_denies_create() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_chmod_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_chmod_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/locked").expect("dir");
    let child = SandboxPath::new("/work/locked/new.txt").expect("child");
    sifs_fs.create_dir(&dir).expect("create dir");
    sifs_fs.set_mode(&dir, 0o555).expect("chmod read-only dir");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/locked/new.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_ne!(
        command.response.exit_status, 0,
        "creating a file in chmod 0555 directory must fail"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&child).expect_err("child must not exist"),
        SandboxFsError::NotFound { .. }
    ));
    assert_eq!(sifs_fs.stat(&dir).expect("dir stat").mode, 0o555);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_directory_without_execute_denies_child_open() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-noexec-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-noexec-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_noexec_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_noexec_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/private").expect("dir");
    let child = SandboxPath::new("/work/private/child.txt").expect("child");
    sifs_fs.create_dir(&dir).expect("create dir");
    sifs_fs.write(&child, b"secret").expect("write child");
    sifs_fs
        .set_mode(&dir, 0o666)
        .expect("remove directory execute bit");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_read_absolute_file_to_stdout_module("/work/private/child.txt", 6),
            vec!["read".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run read module");
    let command = outcome.command.expect("command");
    assert_ne!(
        command.response.exit_status, 0,
        "opening a file through a directory without execute permission must fail"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.stat(&dir).expect("dir stat").mode, 0o666);
    assert_eq!(
        sifs_fs.stat_unchecked(&child).expect("child stat").mode,
        0o644,
        "denied open must not change child metadata"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_read_updates_sifs_atime() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-read-atime-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-read-atime-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_read_atime").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_read_atime").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/read-atime.txt").expect("path");
    sifs_fs.write(&path, b"readable").expect("write file");
    sifs_fs
        .set_times(&path, Some(12_345), Some(67_890))
        .expect("set old times");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_read_absolute_file_to_stdout_module("/work/read-atime.txt", 8),
            vec!["read".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run read module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"readable");

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let metadata = sifs_fs.stat_unchecked(&path).expect("stat");
    assert!(metadata.accessed_at_unix_nanos > 12_345);
    assert_eq!(metadata.modified_at_unix_nanos, 67_890);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_set_times_without_flags_is_noop() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-path-times-noop-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-path-times-noop-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_path_times_noop").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_path_times_noop").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/times-noop.txt").expect("path");
    sifs_fs.write(&path, b"content").expect("write file");
    sifs_fs
        .set_times(&path, Some(12_345), Some(67_890))
        .expect("set baseline times");
    let before = sifs_fs.stat_unchecked(&path).expect("before stat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_times_with_flags_module(
                "/work/times-noop.txt",
                111_111,
                222_222,
                0,
            ),
            vec!["path-times-noop".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run path timestamp noop module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let after = sifs_fs.stat_unchecked(&path).expect("after stat");
    assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(
        after.changed_at_unix_nanos, before.changed_at_unix_nanos,
        "path timestamp call with no flags must not advance ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_set_times_without_flags_is_noop() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-times-noop-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-times-noop-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_times_noop").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_times_noop").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/fd-times-noop.txt").expect("path");
    sifs_fs.write(&path, b"content").expect("write file");
    sifs_fs
        .set_times(&path, Some(12_345), Some(67_890))
        .expect("set baseline times");
    let before = sifs_fs.stat_unchecked(&path).expect("before stat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_fd_times_with_flags_module("/work/fd-times-noop.txt", 111_111, 222_222, 0),
            vec!["fd-times-noop".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd timestamp noop module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let after = sifs_fs.stat_unchecked(&path).expect("after stat");
    assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(
        after.changed_at_unix_nanos, before.changed_at_unix_nanos,
        "fd timestamp call with no flags must not advance ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_create_updates_parent_directory_mtime() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-parent-mtime-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-parent-mtime-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_parent_mtime").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_parent_mtime").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let work = SandboxPath::new("/work").expect("work");
    sifs_fs
        .set_times(&work, Some(12_345), Some(67_890))
        .expect("set old times");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/new-file.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let metadata = sifs_fs.stat_unchecked(&work).expect("stat");
    assert!(metadata.modified_at_unix_nanos > 67_890);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readdir_updates_sifs_atime() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-atime-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-atime-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readdir_atime").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readdir_atime").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/read-dir").expect("dir");
    let child = SandboxPath::new("/work/read-dir/child.txt").expect("child");
    sifs_fs.create_dir(&dir).expect("create dir");
    sifs_fs.write(&child, b"visible").expect("write child");
    sifs_fs
        .set_times(&dir, Some(12_345), Some(67_890))
        .expect("set old times");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_module("/work/read-dir"),
            vec!["readdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let metadata = sifs_fs.stat_unchecked(&dir).expect("stat");
    assert!(metadata.accessed_at_unix_nanos > 12_345);
    assert_eq!(metadata.modified_at_unix_nanos, 67_890);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readdir_reports_sifs_entry_inodes() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-inodes-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-inodes-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readdir_inodes").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readdir_inodes").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs
        .write(&original, b"same object")
        .expect("write original");
    sifs_fs.hard_link(&original, &alias).expect("hardlink");
    let expected_inode = sifs_fs.stat(&original).expect("original stat").inode;

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_to_stdout_module("/work"),
            vec!["readdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    let entries = parse_wasi_dirents(&command.stdout);
    assert_eq!(entries.get("original.txt"), Some(&expected_inode));
    assert_eq!(entries.get("alias.txt"), Some(&expected_inode));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readdir_does_not_report_unlinked_cached_child() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-unlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-unlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readdir_unlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readdir_unlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let stale = SandboxPath::new("/work/stale.txt").expect("stale");
    let kept = SandboxPath::new("/work/kept.txt").expect("kept");
    sifs_fs.write(&stale, b"stale").expect("write stale");
    sifs_fs.write(&kept, b"kept").expect("write kept");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_child_unlink_then_readdir_to_stdout_module("/work", "/work/stale.txt"),
            vec!["readdir-unlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir unlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    let entries = parse_wasi_dirents(&command.stdout);
    assert!(
        !entries.contains_key("stale.txt"),
        "readdir must not report a child removed from SIFS after it was cached"
    );
    assert!(entries.contains_key("kept.txt"));

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&stale),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(sifs_fs.read(&kept, None).expect("kept"), b"kept");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_unlinks_file_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-unlink-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-unlink-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_unlink_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_unlink_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let mirror = SandboxPath::new("/opt/mirror/original.txt").expect("mirror");
    sifs_fs
        .write(&original, b"mounted")
        .expect("write original");
    assert_eq!(
        sifs_fs.read(&mirror, None).expect("mirror read"),
        b"mounted"
    );

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_unlink_absolute_path_module("/opt/mirror/original.txt"),
            vec!["unlink-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run unlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&original),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        sifs_fs.stat(&mirror),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_renames_file_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/source.txt").expect("original");
    let renamed = SandboxPath::new("/work/renamed.txt").expect("renamed");
    let mirror_source = SandboxPath::new("/opt/mirror/source.txt").expect("mirror source");
    let mirror_renamed = SandboxPath::new("/opt/mirror/renamed.txt").expect("mirror renamed");
    sifs_fs
        .write(&original, b"mounted")
        .expect("write original");
    assert_eq!(
        sifs_fs.read(&mirror_source, None).expect("mirror read"),
        b"mounted"
    );

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/opt/mirror/source.txt", "/opt/mirror/renamed.txt"),
            vec!["rename-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&original),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        sifs_fs.stat(&mirror_source),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(sifs_fs.read(&renamed, None).expect("renamed"), b"mounted");
    assert_eq!(
        sifs_fs.read(&mirror_renamed, None).expect("mirror renamed"),
        b"mounted"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_hardlinks_file_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/source.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    let mirror_source = SandboxPath::new("/opt/mirror/source.txt").expect("mirror source");
    let mirror_alias = SandboxPath::new("/opt/mirror/alias.txt").expect("mirror alias");
    sifs_fs
        .write(&original, b"mounted")
        .expect("write original");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_module("/opt/mirror/source.txt", "/opt/mirror/alias.txt"),
            vec!["link-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let original_stat = sifs_fs.stat(&original).expect("original stat");
    let alias_stat = sifs_fs.stat(&alias).expect("alias stat");
    let mirror_source_stat = sifs_fs.stat(&mirror_source).expect("mirror source stat");
    let mirror_alias_stat = sifs_fs.stat(&mirror_alias).expect("mirror alias stat");
    assert_eq!(original_stat.inode, alias_stat.inode);
    assert_eq!(original_stat.inode, mirror_source_stat.inode);
    assert_eq!(original_stat.inode, mirror_alias_stat.inode);
    assert_eq!(original_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);
    assert_eq!(sifs_fs.read(&alias, None).expect("alias"), b"mounted");
    assert_eq!(
        sifs_fs.read(&mirror_alias, None).expect("mirror alias"),
        b"mounted"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_creates_directory_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_mkdir_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_mkdir_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_create_directory_absolute_path_module("/opt/mirror/created-dir"),
            vec!["mkdir-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run mkdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/created-dir").expect("work dir"))
            .expect("work dir stat")
            .is_dir
    );
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/opt/mirror/created-dir").expect("mirror dir"))
            .expect("mirror dir stat")
            .is_dir
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_removes_directory_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rmdir_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rmdir_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/empty-dir").expect("dir");
    let mirror = SandboxPath::new("/opt/mirror/empty-dir").expect("mirror");
    sifs_fs.create_dir(&dir).expect("create dir");
    assert!(sifs_fs.stat(&mirror).expect("mirror stat").is_dir);

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_remove_directory_absolute_path_module("/opt/mirror/empty-dir"),
            vec!["rmdir-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rmdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&dir),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert!(matches!(
        sifs_fs.stat(&mirror),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_updates_metadata_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-metadata-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-metadata-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_metadata_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_metadata_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/metadata.txt").expect("original");
    let mirror = SandboxPath::new("/opt/mirror/metadata.txt").expect("mirror");
    sifs_fs.write(&original, b"metadata").expect("write file");
    assert_eq!(
        sifs_fs.read(&mirror, None).expect("mirror read"),
        b"metadata"
    );

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_metadata_module(
                "/opt/mirror/metadata.txt",
                0o640,
                1001,
                1002,
                123_456,
                789_012,
            ),
            vec!["metadata-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run metadata module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let original_stat = sifs_fs.stat(&original).expect("original stat");
    let mirror_stat = sifs_fs.stat(&mirror).expect("mirror stat");
    assert_eq!(original_stat.mode & 0o777, 0o640);
    assert_eq!(mirror_stat.mode & 0o777, 0o640);
    assert_eq!(original_stat.uid, 1001);
    assert_eq!(original_stat.gid, 1002);
    assert_eq!(mirror_stat.uid, 1001);
    assert_eq!(mirror_stat.gid, 1002);
    assert_eq!(original_stat.accessed_at_unix_nanos, 123_456);
    assert_eq!(original_stat.modified_at_unix_nanos, 789_012);
    assert_eq!(mirror_stat.accessed_at_unix_nanos, 123_456);
    assert_eq!(mirror_stat.modified_at_unix_nanos, 789_012);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_symlink_and_readlink_through_nested_manifest_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-nested-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-nested-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_symlink_nested_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_symlink_nested_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
        mount_path: SandboxPath::new("/opt/mirror").expect("mount path"),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let mirror_link = SandboxPath::new("/opt/mirror/link.txt").expect("mirror link");
    sifs_fs.write(&target, b"target").expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let symlink_outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_symlink_absolute_path_module("target.txt", "/opt/mirror/link.txt"),
            vec!["symlink-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run symlink module");
    let command = symlink_outcome.command.expect("symlink command");
    assert_eq!(command.response.exit_status, 0);

    let readlink_outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            symlink_outcome.dfs,
            &local_store,
            &sandbox_id,
            wasix_readlink_absolute_path_to_stdout_module("/opt/mirror/link.txt"),
            vec!["readlink-mount".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readlink module");
    let command = readlink_outcome.command.expect("readlink command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"target.txt");

    let mut sifs_store = SifsSandboxInstanceStore::new(readlink_outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    let mirror_metadata = sifs_fs.stat(&mirror_link).expect("mirror link stat");
    assert!(link_metadata.is_symlink);
    assert!(mirror_metadata.is_symlink);
    assert_eq!(link_metadata.inode, mirror_metadata.inode);
    assert_eq!(
        sifs_fs.readlink(&link).expect("link readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        sifs_fs.readlink(&mirror_link).expect("mirror readlink"),
        PathBuf::from("target.txt")
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_refreshes_open_directory_after_mkdir() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-dir-fd-stat-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-dir-fd-stat-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_dir_fd_stat").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_dir_fd_stat").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/stat-dir").expect("dir");
    let child = SandboxPath::new("/work/stat-dir/child").expect("child");
    sifs_fs.create_dir(&dir).expect("create dir");
    sifs_fs
        .set_times(&dir, Some(12_345), Some(67_890))
        .expect("old dir times");
    let before = sifs_fs.stat(&dir).expect("dir before");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_dir_mkdir_then_fd_filestat_mtime_module("/work/stat-dir", "child"),
            vec!["dir-fd-stat".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run directory fd stat module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(sifs_fs.stat(&child).expect("child stat").is_dir);
    let after = sifs_fs.stat(&dir).expect("dir after");
    assert!(
        after.modified_at_unix_nanos > before.modified_at_unix_nanos,
        "creating a child must advance directory mtime in SIFS"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readdir_sees_nested_mount_under_default_system_parent() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-nested-opt-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-nested-opt-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_nested_opt").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/toolchains/pkg")
        .expect("mount source");
    dfs.posix_write_file(
        "/services/sandbox/toolchains/pkg/tool.txt",
        b"tool",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("tool file");

    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_nested_opt").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/toolchains/pkg".to_string(),
        mount_path: SandboxPath::new("/opt/pkg").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_to_stdout_module("/opt"),
            vec!["readdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    let entries = parse_wasi_dirents(&command.stdout);
    assert!(
        entries.contains_key("pkg"),
        "nested /opt/pkg mount should be visible when listing /opt; entries: {entries:?}"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_root_readdir_merges_sifs_root_and_system_mounts() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-root-readdir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-root-readdir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_root_readdir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/toolchains/pkg")
        .expect("mount source");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_root_readdir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/toolchains/pkg".to_string(),
        mount_path: SandboxPath::new("/opt/pkg").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_to_stdout_module("/"),
            vec!["readdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run root readdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    let entries = parse_wasi_dirents(&command.stdout);
    assert!(
        entries.contains_key("work"),
        "root listing should include SIFS workspace entries: {entries:?}"
    );
    assert!(
        entries.contains_key("opt"),
        "root listing should include generated system mount entries: {entries:?}"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_nested_system_mount_keeps_read_only_mode() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-nested-opt-ro-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-nested-opt-ro-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_nested_opt_ro").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/toolchains/pkg")
        .expect("mount source");
    dfs.posix_write_file(
        "/services/sandbox/toolchains/pkg/tool.txt",
        b"tool",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("tool file");

    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_nested_opt_ro").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/toolchains/pkg".to_string(),
        mount_path: SandboxPath::new("/opt/pkg").expect("mount path"),
        mode: SandboxMountMode::ReadOnly,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/opt/pkg/tool.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run write module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);
    assert_eq!(
        outcome
            .dfs
            .posix_read_file("/services/sandbox/toolchains/pkg/tool.txt")
            .expect("tool file"),
        b"tool",
        "read-only nested SIFS mount must not be modified through its generated system parent"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readdir_reports_sifs_dot_and_parent_inodes() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-dot-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-dot-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readdir_dot").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readdir_dot").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let work = SandboxPath::new("/work").expect("work");
    let dir = SandboxPath::new("/work/listed").expect("dir");
    sifs_fs.create_dir(&dir).expect("create dir");
    let expected_dir_inode = sifs_fs.stat(&dir).expect("dir stat").inode;
    let expected_parent_inode = sifs_fs.stat(&work).expect("work stat").inode;

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_to_stdout_module("/work/listed"),
            vec!["readdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    let entries = parse_wasi_dirents(&command.stdout);
    assert_eq!(entries.get("."), Some(&expected_dir_inode));
    assert_eq!(entries.get(".."), Some(&expected_parent_inode));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_directory_without_read_denies_readdir() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-noread-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-noread-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_noread_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_noread_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/search-only").expect("dir");
    let child = SandboxPath::new("/work/search-only/child.txt").expect("child");
    sifs_fs.create_dir(&dir).expect("create dir");
    sifs_fs.write(&child, b"visible").expect("write child");
    sifs_fs
        .set_mode(&dir, 0o111)
        .expect("remove directory read bit");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_module("/work/search-only"),
            vec!["readdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir module");
    let command = outcome.command.expect("command");
    assert_ne!(
        command.response.exit_status, 0,
        "readdir on a directory without read permission must fail"
    );

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.stat(&dir).expect("dir stat").mode, 0o111);
    assert_eq!(
        sifs_fs.stat(&child).expect("child stat").mode,
        0o644,
        "search-only directory should still permit child lookup"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_unlink_canonical_hardlink_promotes_remaining_alias() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-unlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-unlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_unlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_unlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    let work = SandboxPath::new("/work").expect("work");
    sifs_fs
        .write(&original, b"canonical-content")
        .expect("write original");
    let original_inode = sifs_fs.stat(&original).expect("original stat").inode;
    sifs_fs
        .hard_link(&original, &alias)
        .expect("hardlink alias");
    sifs_fs
        .set_times(&work, Some(12_345), Some(67_890))
        .expect("old work times");
    sifs_fs
        .set_times(&original, Some(23_456), Some(78_901))
        .expect("old original times");
    let work_before = sifs_fs.stat(&work).expect("work before unlink");
    let object_before = sifs_fs.stat(&alias).expect("alias before unlink");
    std::thread::sleep(std::time::Duration::from_millis(1));

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_unlink_absolute_path_module("/work/original.txt"),
            vec!["unlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run unlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&original),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    assert_eq!(
        sifs_fs.read(&alias, None).expect("read promoted alias"),
        b"canonical-content"
    );
    let alias_stat = sifs_fs.stat(&alias).expect("alias stat");
    assert_eq!(alias_stat.inode, original_inode);
    assert_eq!(alias_stat.link_count, 1);
    assert_eq!(
        alias_stat.modified_at_unix_nanos, object_before.modified_at_unix_nanos,
        "removing a hardlink must not change file content mtime"
    );
    assert!(
        alias_stat.changed_at_unix_nanos > object_before.changed_at_unix_nanos,
        "removing a hardlink must advance the surviving inode ctime"
    );
    let work_after = sifs_fs.stat(&work).expect("work after unlink");
    assert!(
        work_after.modified_at_unix_nanos > work_before.modified_at_unix_nanos,
        "removing a directory entry must advance parent directory mtime"
    );
    assert!(
        work_after.changed_at_unix_nanos > work_before.changed_at_unix_nanos,
        "removing a directory entry must advance parent directory ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_hardlink_alias_over_canonical_preserves_target() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-rename-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-rename-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_rename").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_rename").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let canonical = SandboxPath::new("/work/file.txt").expect("canonical");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs
        .write(&canonical, b"canonical-content")
        .expect("write canonical");
    let canonical_inode = sifs_fs.stat(&canonical).expect("canonical stat").inode;
    sifs_fs
        .hard_link(&canonical, &alias)
        .expect("hardlink alias");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/alias.txt", "/work/file.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let alias_stat = sifs_fs.stat(&alias).expect("alias remains");
    assert_eq!(
        sifs_fs.read(&canonical, None).expect("canonical remains"),
        b"canonical-content"
    );
    assert_eq!(
        sifs_fs.read(&alias, None).expect("alias remains"),
        b"canonical-content"
    );
    let canonical_stat = sifs_fs.stat(&canonical).expect("canonical stat");
    assert_eq!(canonical_stat.inode, canonical_inode);
    assert_eq!(alias_stat.inode, canonical_inode);
    assert_eq!(canonical_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_hardlink_over_other_canonical_preserves_both_targets() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-rename-other-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-rename-other-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_rename_other").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_rename_other").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source = SandboxPath::new("/work/source.txt").expect("source");
    let source_alias = SandboxPath::new("/work/source-alias.txt").expect("source alias");
    let dest = SandboxPath::new("/work/dest.txt").expect("dest");
    let dest_alias = SandboxPath::new("/work/dest-alias.txt").expect("dest alias");
    sifs_fs.write(&source, b"source").expect("write source");
    sifs_fs
        .hard_link(&source, &source_alias)
        .expect("source hardlink");
    let source_inode = sifs_fs.stat(&source).expect("source stat").inode;
    sifs_fs.write(&dest, b"dest").expect("write dest");
    sifs_fs
        .hard_link(&dest, &dest_alias)
        .expect("dest hardlink");
    let dest_inode = sifs_fs.stat(&dest).expect("dest stat").inode;

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/source-alias.txt", "/work/dest.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&source_alias),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    let source_stat = sifs_fs.stat(&source).expect("source remains");
    let renamed_stat = sifs_fs.stat(&dest).expect("renamed source alias");
    let dest_alias_stat = sifs_fs.stat(&dest_alias).expect("dest alias remains");
    assert_eq!(source_stat.inode, source_inode);
    assert_eq!(renamed_stat.inode, source_inode);
    assert_eq!(source_stat.link_count, 2);
    assert_eq!(renamed_stat.link_count, 2);
    assert_eq!(dest_alias_stat.inode, dest_inode);
    assert_eq!(dest_alias_stat.link_count, 1);
    assert_eq!(sifs_fs.read(&source, None).expect("read source"), b"source");
    assert_eq!(sifs_fs.read(&dest, None).expect("read renamed"), b"source");
    assert_eq!(
        sifs_fs.read(&dest_alias, None).expect("read dest alias"),
        b"dest"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_over_existing_file_survives_sifs_reload() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-over-file-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-over-file-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_over_file").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_over_file").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source = SandboxPath::new("/work/source.txt").expect("source");
    let dest = SandboxPath::new("/work/dest.txt").expect("dest");
    sifs_fs.write(&source, b"source").expect("write source");
    let source_inode = sifs_fs.stat(&source).expect("source stat").inode;
    sifs_fs.write(&dest, b"dest").expect("write dest");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/source.txt", "/work/dest.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&source),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    let dest_stat = sifs_fs.stat(&dest).expect("dest survives reload");
    assert_eq!(dest_stat.inode, source_inode);
    assert_eq!(dest_stat.link_count, 1);
    assert_eq!(sifs_fs.read(&dest, None).expect("read dest"), b"source");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_descriptor_write_follows_rename() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-rename-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-rename-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_rename").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_rename").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let old_path = SandboxPath::new("/work/old.txt").expect("old");
    let new_path = SandboxPath::new("/work/new.txt").expect("new");
    sifs_fs.write(&old_path, b"old").expect("write file");
    let old_inode = sifs_fs.stat(&old_path).expect("old stat").inode;

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_rename_then_write_module("/work/old.txt", "/work/new.txt"),
            vec!["open-rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&old_path),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    assert_eq!(
        sifs_fs.read(&new_path, None).expect("new file"),
        b"old-renamed"
    );
    assert_eq!(sifs_fs.stat(&new_path).expect("new stat").inode, old_inode);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_replaced_destination_reads_original_file() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-replace-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-replace-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_replace").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_replace").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source_path = SandboxPath::new("/work/source.txt").expect("source");
    let target_path = SandboxPath::new("/work/target.txt").expect("target");
    sifs_fs
        .write(&source_path, b"source")
        .expect("write source");
    sifs_fs
        .write(&target_path, b"target")
        .expect("write target");
    let target_inode = sifs_fs.stat(&target_path).expect("target stat").inode;

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_target_rename_source_over_then_read_module(
                "/work/source.txt",
                "/work/target.txt",
                32,
            ),
            vec!["open-replace".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open replace module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"target");

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&source_path),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    assert_eq!(
        sifs_fs.read(&target_path, None).expect("replacement"),
        b"source"
    );
    assert_ne!(
        sifs_fs.stat(&target_path).expect("replacement stat").inode,
        target_inode
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_set_times_after_replacement_does_not_touch_new_path() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-times-replace-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-times-replace-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_times_replace").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_times_replace").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source_path = SandboxPath::new("/work/source.txt").expect("source");
    let target_path = SandboxPath::new("/work/target.txt").expect("target");
    sifs_fs
        .write(&source_path, b"source")
        .expect("write source");
    sifs_fs
        .write(&target_path, b"target")
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_target_rename_source_over_then_set_fd_times_module(
                "/work/source.txt",
                "/work/target.txt",
                12_345,
                67_890,
            ),
            vec!["fd-times-replace".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd set times replace module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target_path).expect("target stat");
    assert_eq!(sifs_fs.read(&target_path, None).expect("target"), b"source");
    assert_ne!(
        target_metadata.modified_at_unix_nanos, 67_890,
        "fd set_times for the replaced destination must not mutate the replacement path"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_touch_after_replacement_does_not_touch_new_path() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-touch-replace-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-touch-replace-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_touch_replace").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_touch_replace").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source_path = SandboxPath::new("/work/source.txt").expect("source");
    let target_path = SandboxPath::new("/work/target.txt").expect("target");
    sifs_fs
        .write(&source_path, b"source")
        .expect("write source");
    sifs_fs
        .set_times(&source_path, Some(11_111), Some(22_222))
        .expect("set source times");
    sifs_fs
        .write(&target_path, b"target")
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_target_rename_source_over_then_fd_touch_module(
                "/work/source.txt",
                "/work/target.txt",
            ),
            vec!["fd-touch-replace".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd touch replace module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target_path).expect("target stat");
    assert_eq!(sifs_fs.read(&target_path, None).expect("target"), b"source");
    assert_eq!(
        target_metadata.modified_at_unix_nanos, 22_222,
        "fd_touch for the replaced destination must not mutate the replacement path"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_reports_zero_links_after_replacement() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-stat-replace-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-stat-replace-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_stat_replace").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_stat_replace").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/source.txt").expect("source"),
            b"source",
        )
        .expect("write source");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_target_rename_source_over_then_fd_filestat_nlink_module(
                "/work/source.txt",
                "/work/target.txt",
            ),
            vec!["fd-stat-replace".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd stat replace module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_chmod_chown_after_replacement_do_not_touch_new_path() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-meta-replace-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-meta-replace-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_meta_replace").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_meta_replace").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let source_path = SandboxPath::new("/work/source.txt").expect("source");
    let target_path = SandboxPath::new("/work/target.txt").expect("target");
    sifs_fs
        .write(&source_path, b"source")
        .expect("write source");
    sifs_fs
        .write(&target_path, b"target")
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_target_rename_source_over_then_fd_metadata_module(
                "/work/source.txt",
                "/work/target.txt",
                0o600,
                1234,
                5678,
            ),
            vec!["fd-meta-replace".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run fd metadata replace module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target_path).expect("target stat");
    assert_ne!(target_metadata.mode, 0o600);
    assert_ne!((target_metadata.uid, target_metadata.gid), (1234, 5678));
    assert_eq!(sifs_fs.read(&target_path, None).expect("target"), b"source");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_hardlink_unlink_promotion_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-unlink-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-unlink-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_unlink_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_unlink_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_unlink_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    owner_fs
        .write(&original, b"canonical-content")
        .expect("write original");
    let original_inode = owner_fs.stat(&original).expect("original stat").inode;
    owner_fs
        .hard_link(&original, &alias)
        .expect("hardlink alias");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_unlink_absolute_path_module("/work/original.txt"),
            vec!["unlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run unlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives unlink promotion");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);

    assert!(matches!(
        replica_fs.stat(&original),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    assert_eq!(
        replica_fs
            .read(&alias, None)
            .expect("replica promoted alias"),
        b"canonical-content"
    );
    let alias_stat = replica_fs.stat(&alias).expect("replica alias stat");
    assert_eq!(alias_stat.inode, original_inode);
    assert_eq!(alias_stat.link_count, 1);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_hardlink_rename_replacement_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-rename-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-rename-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_rename_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_rename_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    let canonical = SandboxPath::new("/work/file.txt").expect("canonical");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    owner_fs
        .write(&canonical, b"canonical-content")
        .expect("write canonical");
    let canonical_inode = owner_fs.stat(&canonical).expect("canonical stat").inode;
    owner_fs
        .hard_link(&canonical, &alias)
        .expect("hardlink alias");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/alias.txt", "/work/file.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives rename replacement");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);

    let alias_stat = replica_fs.stat(&alias).expect("replica alias remains");
    assert_eq!(
        replica_fs
            .read(&canonical, None)
            .expect("replica canonical"),
        b"canonical-content"
    );
    assert_eq!(
        replica_fs.read(&alias, None).expect("replica alias"),
        b"canonical-content"
    );
    let canonical_stat = replica_fs.stat(&canonical).expect("replica canonical stat");
    assert_eq!(canonical_stat.inode, canonical_inode);
    assert_eq!(alias_stat.inode, canonical_inode);
    assert_eq!(canonical_stat.link_count, 2);
    assert_eq!(alias_stat.link_count, 2);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_truncate_through_hardlink_updates_shared_sifs_object() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-truncate-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-hardlink-truncate-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_hardlink_truncate").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_hardlink_truncate").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/original.txt").expect("original"),
            b"abcdef",
        )
        .expect("write original");
    sifs_fs
        .hard_link(
            &SandboxPath::new("/work/original.txt").expect("original"),
            &SandboxPath::new("/work/alias.txt").expect("alias"),
        )
        .expect("hardlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_truncate_absolute_file_module("/work/alias.txt", 3),
            vec!["truncate".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run truncate module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let original = SandboxPath::new("/work/original.txt").expect("original");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    assert_eq!(sifs_fs.read(&original, None).expect("original"), b"abc");
    assert_eq!(sifs_fs.read(&alias, None).expect("alias"), b"abc");
    assert_eq!(sifs_fs.stat(&alias).expect("alias stat").link_count, 2);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_file_survives_unlink_without_resurrection() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-unlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-unlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_unlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_unlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/unlinked.txt").expect("file"),
            b"still-readable",
        )
        .expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_unlink_then_read_to_stdout_module("/work/unlinked.txt", 32),
            vec!["open-unlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open-unlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"still-readable");

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.read(&SandboxPath::new("/work/unlinked.txt").expect("file"), None),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_filestat_reports_zero_links_after_unlink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-stat-unlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-fd-stat-unlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_fd_stat_unlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_fd_stat_unlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/unlinked.txt").expect("file"),
            b"still-readable",
        )
        .expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_unlink_then_fd_filestat_nlink_module("/work/unlinked.txt"),
            vec!["open-unlink-fstat".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open-unlink-fstat module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_file_survives_unlink_and_recreate() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-recreate-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-recreate-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_recreate").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_recreate").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/recreated.txt").expect("file");
    sifs_fs.write(&file, b"old").expect("write file");
    let old_inode = sifs_fs.stat(&file).expect("old stat").inode;

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_unlink_recreate_then_read_to_stdout_module("/work/recreated.txt", 32),
            vec!["open-recreate".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open-recreate module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"old");

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.read(&file, None).expect("new file"), b"new");
    assert_ne!(sifs_fs.stat(&file).expect("new stat").inode, old_inode);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_unlink_removes_symlink_from_sifs() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-unlink-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-unlink-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_unlink_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_unlink_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    sifs_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("symlink"),
        )
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_unlink_absolute_path_module("/work/link.txt"),
            vec!["unlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run unlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    assert!(sifs_fs.stat(&link).is_err());
    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/target.txt").expect("target"), None,)
            .expect("target"),
        b"target"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_symlink_creates_sifs_symlink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-create-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-create-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_create_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_create_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_symlink_absolute_path_module("target.txt", "/work/link.txt"),
            vec!["symlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run symlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        sifs_fs.readlink(&link).expect("link readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        sifs_fs.read(&link, None).expect("read through link"),
        b"target"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_exclusive_create_rejects_existing_symlink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-excl-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-excl-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_excl_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_excl_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let link = SandboxPath::new("/work/link.txt").expect("link");
    sifs_fs.write(&target, b"target").expect("write target");
    sifs_fs
        .symlink(Path::new("target.txt"), &link)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_exclusive_create_must_fail_module("/work/link.txt"),
            vec!["open-excl-symlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run exclusive create module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(sifs_fs.read(&target, None).expect("target"), b"target");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_exclusive_create_rejects_dangling_symlink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-excl-dangling-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-excl-dangling-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_excl_dangling_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_excl_dangling_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let missing = SandboxPath::new("/work/missing.txt").expect("missing");
    let link = SandboxPath::new("/work/link.txt").expect("link");
    sifs_fs
        .symlink(Path::new("missing.txt"), &link)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_exclusive_create_must_fail_module("/work/link.txt"),
            vec!["open-excl-dangling-symlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run exclusive create module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        sifs_fs.readlink(&link).expect("link target"),
        PathBuf::from("missing.txt")
    );
    assert!(matches!(
        sifs_fs.stat(&missing),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_link_operations_do_not_create_missing_parents() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-missing-parent-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-missing-parent-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_missing_parent").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_missing_parent").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let symlink_outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_symlink_absolute_path_module("target.txt", "/work/missing/link.txt"),
            vec!["symlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run symlink module");
    let symlink_command = symlink_outcome.command.expect("symlink command");
    assert_ne!(symlink_command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(symlink_outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&SandboxPath::new("/work/missing").expect("missing parent")),
        Err(SandboxFsError::NotFound { .. })
    ));

    let link_outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_module("/work/target.txt", "/work/missing/alias.txt"),
            vec!["link".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link module");
    let link_command = link_outcome.command.expect("link command");
    assert_ne!(link_command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(link_outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&SandboxPath::new("/work/missing").expect("missing parent")),
        Err(SandboxFsError::NotFound { .. })
    ));
    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/target.txt").expect("target"), None,)
            .expect("target remains"),
        b"target"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_link_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_module("/work/file.txt/", "/work/alias.txt"),
            vec!["link-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("original remains"),
        b"content"
    );
    assert!(matches!(
        sifs_fs.stat(&alias),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_link_target_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-target-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-link-target-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_link_target_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_link_target_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    let alias = SandboxPath::new("/work/alias.txt").expect("alias");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_link_absolute_paths_module("/work/file.txt", "/work/alias.txt/"),
            vec!["link-target-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run link module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("original remains"),
        b"content"
    );
    assert!(matches!(
        sifs_fs.stat(&alias),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_symlink_with_trailing_destination_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_symlink_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_symlink_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let link = SandboxPath::new("/work/link.txt").expect("link");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_symlink_absolute_path_module("target.txt", "/work/link.txt/"),
            vec!["symlink-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run symlink module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&link),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_unlink_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-unlink-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-unlink-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_unlink_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_unlink_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_unlink_absolute_path_module("/work/file.txt/"),
            vec!["unlink-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run unlink module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("original remains"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readlink_symlink_with_trailing_slash_fails() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readlink-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readlink-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readlink_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readlink_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readlink_absolute_path_to_stdout_module("/work/link-dir/"),
            vec!["readlink-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readlink module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.readlink(&link_dir).expect("link remains"),
        PathBuf::from("target-dir")
    );
    assert!(sifs_fs.stat(&target_dir).expect("target remains").is_dir);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_lstat_file_with_trailing_slash_fails() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-lstat-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-lstat-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_lstat_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_lstat_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_lstat_filetype_equals_module("/work/file.txt/", 4),
            vec!["lstat-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run lstat module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("original remains"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_lstat_symlink_to_directory_with_trailing_slash_reports_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-lstat-link-dir-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-lstat-link-dir-trailing-local");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_wasix_lstat_link_dir_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_lstat_link_dir_trailing")
        .expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_lstat_filetype_equals_module("/work/link-dir/", 3),
            vec!["lstat-link-dir-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run lstat module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_set_times_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-set-times-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-set-times-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_set_times_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_set_times_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");
    let before = sifs_fs.stat(&file).expect("initial stat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_times_module("/work/file.txt/", 11, 22),
            vec!["set-times-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run set-times module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let after = sifs_fs.stat(&file).expect("file remains");
    assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(
        sifs_fs.read(&file, None).expect("content remains"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_chmod_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_chmod_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_chmod_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");
    let before = sifs_fs.stat(&file).expect("initial stat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_chmod_absolute_path_module("/work/file.txt/", 0),
            vec!["chmod-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run chmod module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let after = sifs_fs.stat(&file).expect("file remains");
    assert_eq!(after.mode, before.mode);
    assert_eq!(
        sifs_fs.read(&file, None).expect("content remains"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_chmod_symlink_to_directory_with_trailing_slash_updates_target() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-link-dir-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chmod-link-dir-trailing-local");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_wasix_chmod_link_dir_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_chmod_link_dir_trailing")
        .expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .set_mode(&target_dir, 0o755)
        .expect("set target mode");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");
    let link_before = sifs_fs.stat(&link_dir).expect("link before");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_chmod_absolute_path_module("/work/link-dir/", 0o700),
            vec!["chmod-link-dir-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run chmod module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.stat(&target_dir).expect("target stat").mode, 0o700);
    let link_after = sifs_fs.stat(&link_dir).expect("link after");
    assert!(link_after.is_symlink);
    assert_eq!(link_after.mode, link_before.mode);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_chown_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chown-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chown-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_chown_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_chown_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");
    let before = sifs_fs.stat(&file).expect("initial stat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_chown_absolute_path_module("/work/file.txt/", 123, 456),
            vec!["chown-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run chown module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let after = sifs_fs.stat(&file).expect("file remains");
    assert_eq!(after.uid, before.uid);
    assert_eq!(after.gid, before.gid);
    assert_eq!(
        sifs_fs.read(&file, None).expect("content remains"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_chown_symlink_to_directory_with_trailing_slash_updates_target() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chown-link-dir-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-chown-link-dir-trailing-local");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_wasix_chown_link_dir_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_chown_link_dir_trailing")
        .expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .set_owner(&target_dir, 100, 200)
        .expect("set target owner");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");
    let link_before = sifs_fs.stat(&link_dir).expect("link before");
    assert!(link_before.is_symlink);

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_chown_absolute_path_module("/work/link-dir/", 500, 600),
            vec!["chown-link-dir-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run chown module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target_dir).expect("target stat");
    assert_eq!((target_metadata.uid, target_metadata.gid), (500, 600));
    let link_metadata = sifs_fs.stat(&link_dir).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        (link_metadata.uid, link_metadata.gid),
        (link_before.uid, link_before.gid)
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_touch_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-touch-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-touch-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_touch_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_touch_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");
    let before = sifs_fs.stat(&file).expect("initial stat");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_touch_absolute_path_module("/work/file.txt/"),
            vec!["touch-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run touch module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let after = sifs_fs.stat(&file).expect("file remains");
    assert_eq!(after.accessed_at_unix_nanos, before.accessed_at_unix_nanos);
    assert_eq!(after.modified_at_unix_nanos, before.modified_at_unix_nanos);
    assert_eq!(
        sifs_fs.read(&file, None).expect("content remains"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_touch_symlink_to_directory_with_trailing_slash_updates_target() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-touch-link-dir-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-touch-link-dir-trailing-local");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_wasix_touch_link_dir_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_touch_link_dir_trailing")
        .expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .set_times(&target_dir, Some(11_111), Some(22_222))
        .expect("set target times");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");
    sifs_fs
        .set_symlink_times(&link_dir, Some(33_333), Some(44_444))
        .expect("set link times");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_touch_absolute_path_module("/work/link-dir/"),
            vec!["touch-link-dir-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run touch module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target_dir).expect("target stat");
    assert!(target_metadata.accessed_at_unix_nanos > 11_111);
    assert!(target_metadata.modified_at_unix_nanos > 22_222);
    let link_metadata = sifs_fs.stat(&link_dir).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(link_metadata.accessed_at_unix_nanos, 33_333);
    assert_eq!(link_metadata.modified_at_unix_nanos, 44_444);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_mkdir_file_with_trailing_slash_reports_not_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-file-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-file-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_mkdir_file_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_mkdir_file_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_create_directory_absolute_path_module("/work/file.txt/"),
            vec!["mkdir-file-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run mkdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 54);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.read(&file, None).expect("file remains"), b"content");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_open_file_with_trailing_slash_reports_not_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-file-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-open-file-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_open_file_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_open_file_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_absolute_path_expect_errno_module("/work/file.txt/", 0, 2, 54),
            vec!["open-file-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run open module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_create_file_with_trailing_slash_reports_is_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-create-file-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-create-file-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_create_file_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_create_file_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_open_absolute_path_expect_errno_module("/work/file.txt/", 9, 64, 31),
            vec!["create-file-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run create module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(sifs_fs.read(&file, None).expect("file remains"), b"content");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_mkdir_symlink_to_directory_with_trailing_slash_reports_exists() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-symlink-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-symlink-trailing-local");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_wasix_mkdir_symlink_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_mkdir_symlink_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_create_directory_absolute_path_module("/work/link-dir/"),
            vec!["mkdir-symlink-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run mkdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 20);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(sifs_fs.stat(&target_dir).expect("target remains").is_dir);
    assert_eq!(
        sifs_fs.readlink(&link_dir).expect("link remains"),
        PathBuf::from("target-dir")
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_set_times_symlink_to_directory_with_trailing_slash_updates_target() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-times-link-dir-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-times-link-dir-trailing-local");
    let network = InProcessNetwork::new();
    let node_id =
        NodeId::from_legacy_named("node_sifs_wasix_times_link_dir_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_times_link_dir_trailing")
        .expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&target_dir).expect("target dir");
    sifs_fs
        .set_times(&target_dir, Some(11_111), Some(22_222))
        .expect("set target times");
    sifs_fs
        .symlink(Path::new("target-dir"), &link_dir)
        .expect("symlink");
    sifs_fs
        .set_symlink_times(&link_dir, Some(33_333), Some(44_444))
        .expect("set link times");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_times_module("/work/link-dir/", 55_555, 66_666),
            vec!["utimens-link-dir-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run set-times module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_metadata = sifs_fs.stat(&target_dir).expect("target stat");
    assert_eq!(target_metadata.accessed_at_unix_nanos, 55_555);
    assert_eq!(target_metadata.modified_at_unix_nanos, 66_666);
    let link_metadata = sifs_fs.stat(&link_dir).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(link_metadata.accessed_at_unix_nanos, 33_333);
    assert_eq!(link_metadata.modified_at_unix_nanos, 44_444);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_set_times_without_follow_updates_symlink_metadata() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-times-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-times-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_symlink_times").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_symlink_times").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let target = SandboxPath::new("/work/target.txt").expect("target");
    let link = SandboxPath::new("/work/link.txt").expect("link");
    sifs_fs.write(&target, b"target").expect("write target");
    sifs_fs
        .set_times(&target, Some(11_111), Some(22_222))
        .expect("set target times");
    sifs_fs
        .symlink(Path::new("target.txt"), &link)
        .expect("create link");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_times_module("/work/link.txt", 33_333, 44_444),
            vec!["lutimens".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run symlink timestamp module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(link_metadata.accessed_at_unix_nanos, 33_333);
    assert_eq!(link_metadata.modified_at_unix_nanos, 44_444);
    let target_metadata = sifs_fs.stat(&target).expect("target stat");
    assert_eq!(target_metadata.accessed_at_unix_nanos, 11_111);
    assert_eq!(target_metadata.modified_at_unix_nanos, 22_222);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_symlink_relative_to_directory_fd_creates_sifs_symlink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_relative_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_relative_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_symlink_relative_to_directory_module("/work", "target.txt", "link.txt"),
            vec!["symlinkat".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run relative symlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let link_metadata = sifs_fs.stat(&link).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        sifs_fs.readlink(&link).expect("link readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        sifs_fs.read(&link, None).expect("read through link"),
        b"target"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_relative_file_namespace_ops_use_directory_fd() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-file-ops-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-file-ops-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_relative_file_ops").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_relative_file_ops").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/dir").expect("dir");
    let created = SandboxPath::new("/work/dir/created.txt").expect("created");
    let renamed = SandboxPath::new("/work/dir/renamed.txt").expect("renamed");
    sifs_fs.create_dir(&dir).expect("create dir");
    sifs_fs
        .set_times(&dir, Some(12_345), Some(67_890))
        .expect("old dir times");
    let dir_before = sifs_fs.stat(&dir).expect("dir before ops");
    std::thread::sleep(std::time::Duration::from_millis(1));

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_relative_create_rename_unlink_file_module(
                "/work/dir",
                "created.txt",
                "renamed.txt",
            ),
            vec!["relative-file-ops".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run relative file ops module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&created),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    assert!(matches!(
        sifs_fs.stat(&renamed),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    let dir_after = sifs_fs.stat(&dir).expect("dir after ops");
    assert!(
        dir_after.modified_at_unix_nanos > dir_before.modified_at_unix_nanos,
        "relative namespace mutations must advance directory mtime"
    );
    assert!(
        dir_after.changed_at_unix_nanos > dir_before.changed_at_unix_nanos,
        "relative namespace mutations must advance directory ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_relative_directory_namespace_ops_use_directory_fd() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-dir-ops-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-relative-dir-ops-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_relative_dir_ops").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_relative_dir_ops").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let base = SandboxPath::new("/work/base").expect("base");
    let child = SandboxPath::new("/work/base/child").expect("child");
    sifs_fs.create_dir(&base).expect("create base dir");
    sifs_fs
        .set_times(&base, Some(12_345), Some(67_890))
        .expect("old base times");
    let base_before = sifs_fs.stat(&base).expect("base before ops");
    std::thread::sleep(std::time::Duration::from_millis(1));

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_relative_create_remove_directory_module("/work/base", "child"),
            vec!["relative-dir-ops".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run relative dir ops module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(matches!(
        sifs_fs.stat(&child),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    let base_after = sifs_fs.stat(&base).expect("base after ops");
    assert!(
        base_after.modified_at_unix_nanos > base_before.modified_at_unix_nanos,
        "relative directory namespace mutations must advance parent mtime"
    );
    assert!(
        base_after.changed_at_unix_nanos > base_before.changed_at_unix_nanos,
        "relative directory namespace mutations must advance parent ctime"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_mkdir_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-mkdir-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_mkdir_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_mkdir_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_mkdir_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_create_directory_absolute_path_module("/work/generated"),
            vec!["mkdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run mkdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives mkdir");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let generated = SandboxPath::new("/work/generated").expect("generated");
    let metadata = replica_fs.stat(&generated).expect("replica mkdir stat");
    assert!(metadata.is_dir);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_symlink_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-symlink-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_symlink_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_symlink_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_symlink_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_symlink_absolute_path_module("target.txt", "/work/link.txt"),
            vec!["symlink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run symlink module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives symlink");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let link = SandboxPath::new("/work/link.txt").expect("link");
    let link_metadata = replica_fs.stat(&link).expect("replica link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        replica_fs.readlink(&link).expect("replica readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(replica_fs.read(&link, None).expect("read link"), b"target");

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_metadata_updates_replicate_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-metadata-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-metadata-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_metadata_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_metadata_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_metadata_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .write(
            &SandboxPath::new("/work/metadata.txt").expect("metadata"),
            b"metadata",
        )
        .expect("write metadata file");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_set_absolute_path_metadata_module(
                "/work/metadata.txt",
                0o640,
                1001,
                1002,
                123_456,
                789_012,
            ),
            vec!["metadata".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run metadata module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives metadata update");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let metadata_path = SandboxPath::new("/work/metadata.txt").expect("metadata");
    assert_eq!(
        replica_fs
            .read(&metadata_path, None)
            .expect("replica read metadata file"),
        b"metadata"
    );
    let metadata = replica_fs.stat(&metadata_path).expect("replica stat");
    assert_eq!(metadata.mode & 0o777, 0o640);
    assert_eq!(metadata.uid, 1001);
    assert_eq!(metadata.gid, 1002);
    assert_eq!(metadata.accessed_at_unix_nanos, 123_456);
    assert_eq!(metadata.modified_at_unix_nanos, 789_012);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_truncate_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-truncate-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-truncate-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_truncate_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_truncate_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_truncate_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .write(
            &SandboxPath::new("/work/truncate.txt").expect("truncate"),
            b"truncate-me",
        )
        .expect("write truncate file");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_truncate_absolute_file_module("/work/truncate.txt", 4),
            vec!["truncate".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run truncate module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives truncate");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let path = SandboxPath::new("/work/truncate.txt").expect("truncate");
    assert_eq!(
        replica_fs.read(&path, None).expect("replica read truncate"),
        b"trun"
    );
    assert_eq!(replica_fs.stat(&path).expect("replica stat").len, 4);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_sparse_pwrite_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-pwrite-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-pwrite-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_pwrite_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_pwrite_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_pwrite_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .write(
            &SandboxPath::new("/work/sparse.txt").expect("sparse"),
            b"base",
        )
        .expect("write sparse file");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_pwrite_absolute_file_module("/work/sparse.txt", 7, b"Z"),
            vec!["pwrite".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run pwrite module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives sparse pwrite");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    let path = SandboxPath::new("/work/sparse.txt").expect("sparse");
    assert_eq!(
        replica_fs.read(&path, None).expect("replica read sparse"),
        b"base\0\0\0Z"
    );
    assert_eq!(replica_fs.stat(&path).expect("replica stat").len, 8);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_ftruncate_preserves_fd_offset_for_sparse_write() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-ftruncate-offset-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-ftruncate-offset-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_ftruncate_offset").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_ftruncate_offset").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/ftruncate-offset.txt").expect("path");
    sifs_fs.write(&path, b"abcdef").expect("write file");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_seek_truncate_then_write_absolute_file_module(
                "/work/ftruncate-offset.txt",
                8,
                0,
                b"Z",
            ),
            vec!["ftruncate-offset".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run ftruncate offset module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&path, None).expect("read ftruncate result"),
        b"\0\0\0\0\0\0\0\0Z",
        "ftruncate must not move the open file descriptor offset"
    );
    assert_eq!(sifs_fs.stat(&path).expect("stat ftruncate result").len, 9);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_allocate_inside_file_does_not_shrink() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-allocate-no-shrink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-allocate-no-shrink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_allocate_no_shrink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_allocate_no_shrink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/allocate.txt").expect("path");
    sifs_fs.write(&path, b"abcdef").expect("write file");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_allocate_absolute_file_module("/work/allocate.txt", 1, 2),
            vec!["allocate-no-shrink".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run allocate module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&path, None).expect("read allocate result"),
        b"abcdef",
        "fd_allocate inside current EOF must not truncate existing content"
    );
    assert_eq!(sifs_fs.stat(&path).expect("stat allocate result").len, 6);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_allocate_beyond_eof_extends_with_zeroes() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-allocate-extend-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-allocate-extend-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_allocate_extend").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_allocate_extend").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/allocate-extend.txt").expect("path");
    sifs_fs.write(&path, b"abc").expect("write file");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_allocate_absolute_file_module("/work/allocate-extend.txt", 6, 2),
            vec!["allocate-extend".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run allocate extend module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&path, None).expect("read allocate result"),
        b"abc\0\0\0\0\0",
        "fd_allocate beyond EOF should extend with zeroes"
    );
    assert_eq!(sifs_fs.stat(&path).expect("stat allocate result").len, 8);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_fd_allocate_refreshes_stale_handle_before_no_shrink_check() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-allocate-stale-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-allocate-stale-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_allocate_stale").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_allocate_stale").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let path = SandboxPath::new("/work/allocate-stale.txt").expect("path");
    sifs_fs.write(&path, b"abc").expect("write file");
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_pwrite_then_allocate_first_fd_absolute_file_module(
                "/work/allocate-stale.txt",
                9,
                b"Z",
                5,
                1,
            ),
            vec!["allocate-stale".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run allocate stale module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&path, None).expect("read allocate result"),
        b"abc\0\0\0\0\0\0Z",
        "fd_allocate must compare against the current backing SIFS size, not a stale descriptor cache"
    );
    assert_eq!(sifs_fs.stat(&path).expect("stat allocate result").len, 10);

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_directory_rename_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-dir-rename-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-dir-rename-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_dir_rename_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_dir_rename_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_dir_rename_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .create_dir(&SandboxPath::new("/work/src").expect("src"))
        .expect("src dir");
    owner_fs
        .write(
            &SandboxPath::new("/work/src/nested.txt").expect("nested"),
            b"nested",
        )
        .expect("write nested");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/src", "/work/dst"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run directory rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives directory rename");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    assert!(matches!(
        replica_fs.stat(&SandboxPath::new("/work/src").expect("src")),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));
    let dst = SandboxPath::new("/work/dst").expect("dst");
    assert!(replica_fs.stat(&dst).expect("dst stat").is_dir);
    assert_eq!(
        replica_fs
            .read(
                &SandboxPath::new("/work/dst/nested.txt").expect("nested"),
                None,
            )
            .expect("read nested"),
        b"nested"
    );

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rmdir_replicates_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_wasix_rmdir_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_wasix_rmdir_replica").expect("replica");
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
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rmdir_replica").expect("sandbox");
    let mut owner_store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = owner_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs.ensure_workspace().expect("workspace");
    owner_fs
        .create_dir(&SandboxPath::new("/work/empty").expect("empty"))
        .expect("empty dir");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_remove_directory_absolute_path_module("/work/empty"),
            vec!["rmdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rmdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives rmdir");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store.load(&sandbox_id).expect("replica manifest");
    let replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    assert!(matches!(
        replica_fs.stat(&SandboxPath::new("/work/empty").expect("empty")),
        Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
    ));

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rmdir_rejects_non_empty_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-not-empty-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-not-empty-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rmdir_not_empty").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rmdir_not_empty").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .create_dir(&SandboxPath::new("/work/non-empty").expect("dir"))
        .expect("dir");
    sifs_fs
        .write(
            &SandboxPath::new("/work/non-empty/file.txt").expect("file"),
            b"content",
        )
        .expect("file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_remove_directory_absolute_path_module("/work/non-empty"),
            vec!["rmdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rmdir module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/non-empty").expect("dir"))
            .expect("dir stat")
            .is_dir
    );
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/work/non-empty/file.txt").expect("file"),
                None,
            )
            .expect("file content"),
        b"content"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_directory_over_non_empty_directory_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-non-empty-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-non-empty-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_non_empty_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_non_empty_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .create_dir(&SandboxPath::new("/work/source").expect("source"))
        .expect("source dir");
    sifs_fs
        .write(
            &SandboxPath::new("/work/source/source.txt").expect("source file"),
            b"source",
        )
        .expect("source file");
    sifs_fs
        .create_dir(&SandboxPath::new("/work/target").expect("target"))
        .expect("target dir");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target/target.txt").expect("target file"),
            b"target",
        )
        .expect("target file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/source", "/work/target"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/source").expect("source"))
            .expect("source stat")
            .is_dir
    );
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/work/source/source.txt").expect("source file"),
                None,
            )
            .expect("source content"),
        b"source"
    );
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/target").expect("target"))
            .expect("target stat")
            .is_dir
    );
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/work/target/target.txt").expect("target file"),
                None,
            )
            .expect("target content"),
        b"target"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_across_mount_boundary_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-cross-mount-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-cross-mount-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_cross_mount").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let shared_dir = "/services/shared/wasix-rename-cross-mount";
    dfs.posix_mkdir_all(shared_dir).expect("shared dir");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_cross_mount").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let mut manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    manifest.mounts.push(SandboxMount {
        mount_path: SandboxPath::new("/shared").expect("shared mount"),
        source_sifs_path: shared_dir.to_string(),
        mode: SandboxMountMode::ReadWrite,
    });
    sifs_store.save_manifest(&manifest).expect("save manifest");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/source.txt").expect("source"),
            b"source",
        )
        .expect("source");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/source.txt", "/shared/source.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/source.txt").expect("source"), None)
            .expect("source remains"),
        b"source"
    );
    assert!(matches!(
        sifs_fs.stat(&SandboxPath::new("/shared/source.txt").expect("shared")),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_file_over_directory_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-file-over-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-file-over-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_file_over_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_file_over_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(&SandboxPath::new("/work/file.txt").expect("file"), b"file")
        .expect("file");
    sifs_fs
        .create_dir(&SandboxPath::new("/work/dir").expect("dir"))
        .expect("dir");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/file.txt", "/work/dir"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/file.txt").expect("file"), None)
            .expect("file content"),
        b"file"
    );
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/dir").expect("dir"))
            .expect("dir stat")
            .is_dir
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_directory_over_file_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-dir-over-file-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-dir-over-file-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_dir_over_file").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_dir_over_file").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .create_dir(&SandboxPath::new("/work/dir").expect("dir"))
        .expect("dir");
    sifs_fs
        .write(
            &SandboxPath::new("/work/dir/child.txt").expect("child"),
            b"child",
        )
        .expect("child");
    sifs_fs
        .write(&SandboxPath::new("/work/file.txt").expect("file"), b"file")
        .expect("file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/dir", "/work/file.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert!(
        sifs_fs
            .stat(&SandboxPath::new("/work/dir").expect("dir"))
            .expect("dir stat")
            .is_dir
    );
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/work/dir/child.txt").expect("child"),
                None
            )
            .expect("child content"),
        b"child"
    );
    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/file.txt").expect("file"), None)
            .expect("file content"),
        b"file"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_path_rename_preserves_symlink_object() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .write(
            &SandboxPath::new("/work/target.txt").expect("target"),
            b"target",
        )
        .expect("write target");
    sifs_fs
        .symlink(
            Path::new("target.txt"),
            &SandboxPath::new("/work/link.txt").expect("symlink"),
        )
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/link.txt", "/work/renamed-link.txt"),
            vec!["rename".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let old_link = SandboxPath::new("/work/link.txt").expect("old link");
    let renamed_link = SandboxPath::new("/work/renamed-link.txt").expect("renamed link");
    assert!(sifs_fs.stat(&old_link).is_err());
    let renamed_metadata = sifs_fs.stat(&renamed_link).expect("renamed stat");
    assert!(renamed_metadata.is_symlink);
    assert_eq!(
        sifs_fs.readlink(&renamed_link).expect("renamed readlink"),
        PathBuf::from("target.txt")
    );
    assert_eq!(
        sifs_fs
            .read(&SandboxPath::new("/work/target.txt").expect("target"), None,)
            .expect("target"),
        b"target"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rename_file_with_trailing_slash_is_atomic_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-trailing-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rename-trailing-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rename_trailing").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rename_trailing").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let file = SandboxPath::new("/work/file.txt").expect("file");
    let renamed = SandboxPath::new("/work/renamed.txt").expect("renamed");
    sifs_fs.write(&file, b"content").expect("write file");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_rename_absolute_paths_module("/work/file.txt/", "/work/renamed.txt"),
            vec!["rename-trailing".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rename module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs.read(&file, None).expect("original remains"),
        b"content"
    );
    assert!(matches!(
        sifs_fs.stat(&renamed),
        Err(SandboxFsError::NotFound { .. })
    ));

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_rmdir_rejects_symlink_to_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-symlink-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-rmdir-symlink-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_rmdir_symlink").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_rmdir_symlink").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    sifs_fs
        .create_dir(&SandboxPath::new("/work/target-dir").expect("target dir"))
        .expect("target dir");
    sifs_fs
        .symlink(
            Path::new("target-dir"),
            &SandboxPath::new("/work/link-dir").expect("symlink"),
        )
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_remove_directory_absolute_path_module("/work/link-dir"),
            vec!["rmdir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run rmdir module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let target_dir = SandboxPath::new("/work/target-dir").expect("target dir");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    assert!(sifs_fs.stat(&target_dir).expect("target stat").is_dir);
    let link_metadata = sifs_fs.stat(&link_dir).expect("link stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(
        sifs_fs.readlink(&link_dir).expect("link readlink"),
        PathBuf::from("target-dir")
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_readdir_follows_symlink_to_directory() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-symlink-dir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-readdir-symlink-dir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_readdir_symlink_dir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_readdir_symlink_dir").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");
    let dir = SandboxPath::new("/work/dir").expect("dir");
    let child = SandboxPath::new("/work/dir/child.txt").expect("child");
    let link_dir = SandboxPath::new("/work/link-dir").expect("link dir");
    sifs_fs.create_dir(&dir).expect("dir");
    sifs_fs.write(&child, b"child").expect("child");
    sifs_fs
        .symlink(Path::new("dir"), &link_dir)
        .expect("symlink");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_readdir_absolute_path_module("/work/link-dir"),
            vec!["readdir-link-dir".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run readdir module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let link_metadata = sifs_fs.stat(&link_dir).expect("link dir stat");
    assert!(link_metadata.is_symlink);
    assert_eq!(sifs_fs.read(&child, None).expect("child"), b"child");

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_var_is_sifs_backed_workspace() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-var-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-var-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_var").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_var").expect("sandbox");
    let mut sifs_store = SifsSandboxInstanceStore::new(dfs);
    let manifest = sifs_store
        .create(
            sandbox_id.clone(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    sifs_fs.ensure_workspace().expect("workspace");

    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_limits_output_sink_and_abi(
            sifs_fs.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/var/package-state.txt"),
            vec!["write-var".to_string()],
            None,
            1_000,
            None,
            None,
            SandboxWasiAbi::Wasix,
        )
        .expect("run var write module");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/var/package-state.txt").expect("var file"),
                None,
            )
            .expect("read var file"),
        b"created"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(not(feature = "wasix-runtime"))]
fn sifs_staged_toolchain_command_requires_wasix_runtime_feature() {
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
        SandboxWasiAbi::Wasix,
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

    let error = match runner.run_toolchain_command_with_stdin_and_policy(
        toolchain_store.into_inner(),
        &local_store,
        &sandbox_id,
        "echo",
        vec!["echo".to_string()],
        None,
        1_000,
        &policy,
    ) {
        Ok(_) => panic!("SIFS WASIX toolchain execution must not fall back to host staging"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("requires WASIX"),
        "unexpected error: {error}"
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(not(feature = "wasix-runtime"))]
fn sifs_staged_direct_module_requires_wasix_runtime_feature() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-direct-wasix-required-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-direct-wasix-required-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_direct_wasix_required").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_direct_wasix_required").expect("sandbox");
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

    let error = match runner.run_module_with_stdin_limits_output_sink_and_abi(
        sifs_store.into_inner(),
        &local_store,
        &sandbox_id,
        wasi_stdout_module(),
        vec!["wasix".to_string()],
        None,
        1_000,
        None,
        None,
        SandboxWasiAbi::Wasix,
    ) {
        Ok(_) => panic!("SIFS direct WASIX execution must not fall back to host staging"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("requires WASIX"),
        "unexpected error: {error}"
    );
    assert!(
        !local_store.layout(&sandbox_id).root.exists(),
        "WASIX execution without the runtime feature must not create a local staged instance"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_toolchain_reads_support_artifact_from_virtual_preopen() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-artifacts-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-artifacts-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_artifacts").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_wasix_artifacts").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_wasix_artifacts").expect("toolchain");
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
        "wasix-artifact-test",
        SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "echo",
            wasix_read_absolute_file_to_stdout_module("/share/pkg/data.txt", 7).as_bytes(),
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
        .expect("run SIFS WASIX artifact command");
    let command = outcome.command.expect("command");

    assert_eq!(command.response.exit_status, 0);
    assert_eq!(command.stdout, b"support");
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_wasix_toolchain_does_not_create_local_staging_instance() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-wasix-no-staging-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-wasix-no-staging-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_wasix_no_staging").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_sifs_wasix_no_staging").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_wasix_no_staging").expect("toolchain");
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
        "wasix-no-staging-test",
        SandboxWasiAbi::Wasix,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain_store
        .put_command_module(
            &mut toolchain,
            "write",
            wasix_write_absolute_file_module("/work/no-staging.txt").as_bytes(),
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
            "write",
            vec!["write".to_string()],
            None,
            1_000,
            &policy,
        )
        .expect("run SIFS WASIX command");
    let command = outcome.command.expect("command");
    assert_eq!(command.response.exit_status, 0);

    let local_layout = local_store.layout(&sandbox_id);
    assert!(
        !local_layout.root.exists(),
        "SIFS WASIX execution must use VFS-backed preopens, not a local staged instance at {}",
        local_layout.root.display()
    );
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    let sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    assert_eq!(
        sifs_fs
            .read(
                &SandboxPath::new("/work/no-staging.txt").expect("output path"),
                None,
            )
            .expect("read output"),
        b"created"
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
    assert!(
        !layout.toolchain_runtime_cache_root().exists(),
        "SIFS WASIX toolchain commands should be mounted from virtual preopens instead of a host cache"
    );
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
        "SIFS toolchain execution must preserve target_abi=wasix and require the WASIX runtime: {error}"
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
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(runtime_root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    owner_dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
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
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let resumed_manifest = replica_store.load(&sandbox_id).expect("load from replica");
    assert_eq!(resumed_manifest.command_history.len(), 2);
    let final_command_id = resumed_manifest.command_history[1].clone();
    let mut replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &resumed_manifest);
    assert_eq!(
        replica_fs
            .read(
                &sandbox_command_stdout_log_path(&final_command_id).expect("stdout"),
                None,
            )
            .expect("replica stdout"),
        b"hello\n"
    );
    let record =
        load_command_record(&mut replica_fs, &final_command_id).expect("replica final record");
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
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
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
    assert!(
        !local_store
            .layout(&sandbox_id)
            .root
            .join("sifs-process-plan-toolchain")
            .exists(),
        "SIFS WASIX process-plan fast path should run through SIFS virtual preopens instead of materializing a local sandbox toolchain"
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
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
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
        SandboxWasiAbi::Wasix,
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
            vec![SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY.to_string()],
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
        SandboxWasiAbi::Wasix,
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
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(
        manifest.command_history.len(),
        0,
        "WASIX SIFS process plans reject oversized piped input before committing per-step command records"
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
        SandboxWasiAbi::Wasix,
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
            wasix_read_absolute_file_to_stdout_module("/tmp/cwd.txt", 8).as_bytes(),
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
    let mut sifs_store = SifsSandboxInstanceStore::new(outcome.dfs);
    let manifest = sifs_store.load(&sandbox_id).expect("manifest");
    assert_eq!(
        manifest.cwd,
        SandboxPath::new("/work").expect("manifest cwd unchanged")
    );
    assert!(manifest.env.is_empty());
    assert_eq!(manifest.command_history.len(), 2);
    let mut sifs_fs = SifsSandboxFileSystem::new(sifs_store.into_inner(), &manifest);
    let env_record =
        load_command_record(&mut sifs_fs, &manifest.command_history[0]).expect("env record");
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
    let manifest = owner_store.load(&sandbox_id).expect("manifest");
    let mut owner_fs = SifsSandboxFileSystem::new(owner_store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    owner_fs
        .create_dir_all(&SandboxPath::new("/work/target").expect("target dir"))
        .expect("target dir");
    let owner_store = SifsSandboxInstanceStore::new(owner_fs.into_inner())
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new()
        .expect("runner")
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);

    let outcome = runner
        .run_module(
            owner_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/work/target/progress.txt"),
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
            .any(|path| path == &SandboxPath::new("/work/target/progress.txt").expect("path")),
        "command changed paths should include the durable workspace write"
    );

    replica_dfs
        .process_incoming()
        .expect("replica receives command progress");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let resumed_manifest = replica_store.load(&sandbox_id).expect("load from replica");
    let mut replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &resumed_manifest);

    assert_eq!(
        resumed_manifest.command_history,
        vec![command.response.command_id.clone()]
    );
    assert_eq!(
        replica_fs
            .read(
                &SandboxPath::new("/work/target/progress.txt").expect("progress"),
                None,
            )
            .expect("replica read command output"),
        b"created"
    );
    let record =
        load_command_record(&mut replica_fs, &command.response.command_id).expect("replica record");
    assert_eq!(record.status, SandboxCommandRecordStatus::Completed);

    drop(outcome.dfs);
    fs::remove_dir_all(runtime_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_writes_live_read_write_sifs_mounts() {
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
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    dfs.posix_write_file(
        "/services/sandbox/shared/target/.keep",
        b"",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("mount target marker");
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
            wasix_write_absolute_file_module("/shared/target/out.txt"),
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
            .any(|path| path == &SandboxPath::new("/shared/target/out.txt").expect("path")),
        "command changed paths should include writes through read-write SIFS mounts"
    );
    assert_eq!(
        outcome
            .dfs
            .posix_read_file("/services/sandbox/shared/target/out.txt")
            .expect("mounted output"),
        b"created"
    );
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_preserves_rw_mount_write_on_resource_failure() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-quota-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-quota-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_mount_quota").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared")
        .expect("mount source");
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_mount_quota").expect("sandbox");
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
    let mut limits = SandboxResourceLimits::default();
    limits.max_written_bytes_per_command = 3;
    let local_store = LocalSandboxInstanceStore::new(&local_root);
    let runner = SifsStagedSandboxCommandRunner::new().expect("runner");
    let outcome = runner
        .run_module_with_stdin_and_limits(
            sifs_store.into_inner(),
            &local_store,
            &sandbox_id,
            wasix_write_absolute_file_module("/shared/target/out.txt"),
            vec!["write".to_string()],
            None,
            1_000,
            Some(limits),
        )
        .expect("run setup");
    assert!(matches!(
        outcome.command,
        Err(SandboxCommandExecutionError::Fs(
            SandboxFsError::QuotaExceeded { .. }
        ))
    ));
    assert_eq!(
        outcome
            .dfs
            .posix_read_file("/services/sandbox/shared/target/out.txt")
            .expect("completed mount write remains visible"),
        b"created"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_replicates_live_rw_mount_write_to_candidate_node() {
    let runtime_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-replica");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-mount-replica-local");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_mount_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_mount_replica").expect("replica");
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
    owner_dfs
        .posix_mkdir_all("/services/sandbox/shared")
        .expect("mount source");
    owner_dfs
        .posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    owner_dfs
        .posix_write_file(
            "/services/sandbox/shared/target/.keep",
            b"",
            DfsWriteOptions::replicated_eventual(vec![owner_dfs.node_id().clone()]),
        )
        .expect("mount target marker");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
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
            wasix_write_absolute_file_module("/shared/target/out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run setup");
    assert_eq!(outcome.command.expect("command").response.exit_status, 0);

    replica_dfs
        .process_incoming()
        .expect("replica receives live mount write");
    let replica_backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    assert_eq!(
        replica_dfs
            .posix_read_file("/services/sandbox/shared/target/out.txt")
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
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    dfs.posix_write_file(
        "/services/sandbox/shared/target/.keep",
        b"",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("mount target marker");
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
            wasix_write_absolute_file_module("/shared/target/out.txt"),
            vec!["write".to_string()],
            1_000,
        )
        .expect("run setup");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);
    assert!(matches!(
        outcome
            .dfs
            .posix_read_file("/services/sandbox/shared/target/out.txt"),
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
    ));
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
fn sifs_staged_runner_denies_mkdir_in_read_only_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-mkdir-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-mkdir-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_ro_mkdir").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_ro_mkdir").expect("sandbox");
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
            wasix_create_directory_absolute_path_module("/shared/target/new-dir"),
            vec!["mkdir".to_string()],
            1_000,
        )
        .expect("run setup");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);
    assert!(matches!(
        outcome
            .dfs
            .posix_stat("/services/sandbox/shared/target/new-dir"),
        Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
    ));
    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_runner_denies_timestamp_update_in_read_only_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-times-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-times-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_ro_times").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    dfs.posix_write_file(
        "/services/sandbox/shared/target/file.txt",
        b"immutable metadata",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("mount target file");
    let before = dfs
        .posix_stat("/services/sandbox/shared/target/file.txt")
        .expect("before stat");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_ro_times").expect("sandbox");
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
            wasix_set_absolute_path_times_module("/shared/target/file.txt", 12_345, 67_890),
            vec!["utimens".to_string()],
            1_000,
        )
        .expect("run timestamp module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);
    let after = outcome
        .dfs
        .posix_stat("/services/sandbox/shared/target/file.txt")
        .expect("after stat");
    assert_eq!(
        after.accessed_at_unix_nanos, before.accessed_at_unix_nanos,
        "read-only mount must not allow atime mutation"
    );
    assert_eq!(
        after.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "read-only mount must not allow mtime mutation"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_runner_denies_fd_timestamp_update_in_read_only_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-fd-times-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-fd-times-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_ro_fd_times").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    dfs.posix_write_file(
        "/services/sandbox/shared/target/file.txt",
        b"immutable metadata",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("mount target file");
    let before = dfs
        .posix_stat("/services/sandbox/shared/target/file.txt")
        .expect("before stat");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_ro_fd_times").expect("sandbox");
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
            wasix_set_fd_times_module("/shared/target/file.txt", 12_345, 67_890),
            vec!["futimens".to_string()],
            1_000,
        )
        .expect("run fd timestamp module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);
    let after = outcome
        .dfs
        .posix_stat("/services/sandbox/shared/target/file.txt")
        .expect("after stat");
    assert_eq!(
        after.accessed_at_unix_nanos, before.accessed_at_unix_nanos,
        "read-only mount must not allow fd atime mutation"
    );
    assert_eq!(
        after.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "read-only mount must not allow fd mtime mutation"
    );

    fs::remove_dir_all(sifs_root).ok();
    fs::remove_dir_all(local_root).ok();
}

#[test]
#[cfg(feature = "wasix-runtime")]
fn sifs_staged_runner_denies_touch_in_read_only_mount() {
    let sifs_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-touch-sifs");
    let local_root = unique_test_dir("sinomo-sandbox-sifs-staged-ro-touch-local");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_ro_touch").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(sifs_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    dfs.posix_mkdir_all("/services/sandbox/shared/target")
        .expect("mount target");
    dfs.posix_write_file(
        "/services/sandbox/shared/target/file.txt",
        b"immutable metadata",
        DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
    )
    .expect("mount target file");
    let before = dfs
        .posix_stat("/services/sandbox/shared/target/file.txt")
        .expect("before stat");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_sifs_ro_touch").expect("sandbox");
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
            wasix_touch_absolute_path_module("/shared/target/file.txt"),
            vec!["touch".to_string()],
            1_000,
        )
        .expect("run touch module");
    let command = outcome.command.expect("command");
    assert_ne!(command.response.exit_status, 0);
    let after = outcome
        .dfs
        .posix_stat("/services/sandbox/shared/target/file.txt")
        .expect("after stat");
    assert_eq!(
        after.accessed_at_unix_nanos, before.accessed_at_unix_nanos,
        "read-only mount must not allow touch atime mutation"
    );
    assert_eq!(
        after.modified_at_unix_nanos, before.modified_at_unix_nanos,
        "read-only mount must not allow touch mtime mutation"
    );

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

#[cfg(feature = "wasix-runtime")]
fn parse_wasi_dirents(bytes: &[u8]) -> std::collections::BTreeMap<String, u64> {
    let mut entries = std::collections::BTreeMap::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        assert!(
            offset + 24 <= bytes.len(),
            "truncated WASI dirent header at byte {offset}"
        );
        let inode = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let name_len =
            u32::from_le_bytes(bytes[offset + 16..offset + 20].try_into().unwrap()) as usize;
        let name_offset = offset + 24;
        let next_offset = name_offset + name_len;
        assert!(
            next_offset <= bytes.len(),
            "truncated WASI dirent name at byte {offset}"
        );
        let name = std::str::from_utf8(&bytes[name_offset..next_offset])
            .expect("WASI dirent name should be utf8 in test")
            .to_string();
        entries.insert(name, inode);
        offset = next_offset;
    }
    entries
}
