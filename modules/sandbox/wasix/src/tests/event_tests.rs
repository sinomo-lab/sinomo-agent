use super::*;

#[test]
fn sandbox_command_record_round_trips_through_sobj() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_record").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_record").expect("command");
    let now = OffsetDateTime::now_utc();
    let record = SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::Completed,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec!["cat".to_string(), "/work/main.rs".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::from([("LC_ALL".to_string(), "C".to_string())]),
        started_at: now,
        completed_at: Some(now),
        response: Some(SandboxCommandResponse {
            command_id: command_id.clone(),
            exit_status: 0,
            stdout_ref: sandbox_command_stdout_path(&sandbox_id, &command_id),
            stderr_ref: sandbox_command_stderr_path(&sandbox_id, &command_id),
            changed_paths: vec![SandboxPath::new("/work/main.rs").expect("changed")],
            resource_usage: SandboxResourceUsage {
                written_bytes: 12,
                ..SandboxResourceUsage::default()
            },
        }),
        error: None,
    };

    assert_eq!(
        sandbox_command_record_path(&sandbox_id, &command_id),
        format!("/services/sandbox/instances/{sandbox_id}/logs/{command_id}.record.sobj")
    );
    let bytes = encode_command_record(&record).expect("encode");
    assert_eq!(decode_command_record(&bytes).expect("decode"), record);
}

#[test]
fn sandbox_output_chunks_are_bounded_and_offset() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_chunked_output").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_chunked_output").expect("command");
    let bytes = vec![b'x'; SANDBOX_OUTPUT_CHUNK_BYTES * 2 + 7];
    let chunks = sandbox_output_chunks(
        &sandbox_id,
        &command_id,
        SandboxOutputStream::Stdout,
        &bytes,
    );
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].offset, 0);
    assert_eq!(chunks[0].bytes.len(), SANDBOX_OUTPUT_CHUNK_BYTES);
    assert_eq!(chunks[1].offset, SANDBOX_OUTPUT_CHUNK_BYTES as u64);
    assert_eq!(chunks[1].bytes.len(), SANDBOX_OUTPUT_CHUNK_BYTES);
    assert_eq!(chunks[2].offset, (SANDBOX_OUTPUT_CHUNK_BYTES * 2) as u64);
    assert_eq!(chunks[2].bytes.len(), 7);
}

#[test]
fn sandbox_command_started_event_reports_invocation_shape() {
    let node_id = NodeId::from_legacy_named("node_command_started").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_command_started").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let manifest =
        SandboxInstanceManifest::new(sandbox_id.clone(), toolchain_id, OffsetDateTime::now_utc())
            .expect("manifest");
    let command_id = SandboxCommandId::from_legacy_named("command_started_event").expect("command");
    let cwd = SandboxPath::new("/work").expect("cwd");
    let argv = vec!["grep".to_string(), "needle".to_string()];
    let started_at = OffsetDateTime::now_utc();

    let event = sandbox_command_started_event(
        node_id.clone(),
        &manifest,
        &command_id,
        &argv,
        &cwd,
        started_at,
    );

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.kind.as_str(), "sandbox.command.started");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["argv"][0], "grep");
    assert_eq!(event.payload["cwd"], "/work");
}

#[test]
fn sandbox_command_output_event_preserves_raw_bytes() {
    let node_id = NodeId::from_legacy_named("node_command_output").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_command_output").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_output_event").expect("command");
    let chunk = SandboxOutputChunk {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        stream: SandboxOutputStream::Stdout,
        offset: 4,
        bytes: vec![0, b'o', b'k', 255],
    };

    let event = sandbox_command_output_event(node_id.clone(), &chunk);

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.kind.as_str(), "sandbox.command.output");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["stream"], "stdout");
    assert_eq!(event.payload["offset"], 4);
    assert_eq!(event.payload["byte_len"], 4);
    assert_eq!(event.payload["bytes"][0], 0);
    assert_eq!(event.payload["bytes"][3], 255);
}

#[test]
fn sandbox_command_record_event_reports_completed_command() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_record_event").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_record_event").expect("command");
    let node_id = NodeId::from_legacy_named("node_sandbox").expect("node");
    let now = OffsetDateTime::now_utc();
    let record = SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::Completed,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec!["cat".to_string(), "/work/main.rs".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::from([("LC_ALL".to_string(), "C".to_string())]),
        started_at: now,
        completed_at: Some(now),
        response: Some(SandboxCommandResponse {
            command_id: command_id.clone(),
            exit_status: 0,
            stdout_ref: sandbox_command_stdout_path(&sandbox_id, &command_id),
            stderr_ref: sandbox_command_stderr_path(&sandbox_id, &command_id),
            changed_paths: vec![SandboxPath::new("/work/main.rs").expect("changed")],
            resource_usage: SandboxResourceUsage {
                written_bytes: 12,
                wall_time_ms: 34,
                ..SandboxResourceUsage::default()
            },
        }),
        error: None,
    };

    let event = sandbox_command_record_event(node_id.clone(), &record);

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.runtime");
    assert_eq!(event.kind.as_str(), "sandbox.command.completed");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["status"], "completed");
    assert_eq!(event.payload["env_keys"][0], "LC_ALL");
    assert_eq!(
        event.payload["stdout_ref"],
        record.response.as_ref().unwrap().stdout_ref
    );
    assert_eq!(event.payload["changed_paths"][0], "/work/main.rs");
    assert_eq!(event.payload["resource_usage"]["written_bytes"], 12);
    assert_eq!(event.payload["resource_usage"]["wall_time_ms"], 34);
}

#[test]
fn sandbox_command_record_event_reports_resource_exhaustion() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_record_exhausted").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_record_exhausted").expect("command");
    let record = SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::ResourceExhausted,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec![
            "sh".to_string(),
            "-c".to_string(),
            "yes > /work/out".to_string(),
        ],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::new(),
        started_at: OffsetDateTime::now_utc(),
        completed_at: None,
        response: None,
        error: Some("sandbox quota exceeded".to_string()),
    };

    let event = sandbox_command_record_event(
        NodeId::from_legacy_named("node_sandbox").expect("node"),
        &record,
    );

    assert_eq!(event.kind.as_str(), "sandbox.resource.exhausted");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["status"], "resource-exhausted");
    assert_eq!(event.payload["error"], "sandbox quota exceeded");
    assert!(event.payload["resource_usage"].is_null());
}

#[test]
fn sandbox_command_record_event_reports_failed_command() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_record_failed").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_record_failed").expect("command");
    let now = OffsetDateTime::now_utc();
    let record = SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::Failed,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec!["false".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::new(),
        started_at: now,
        completed_at: Some(now),
        response: Some(SandboxCommandResponse {
            command_id: command_id.clone(),
            exit_status: 1,
            stdout_ref: sandbox_command_stdout_path(&sandbox_id, &command_id),
            stderr_ref: sandbox_command_stderr_path(&sandbox_id, &command_id),
            changed_paths: Vec::new(),
            resource_usage: SandboxResourceUsage {
                wall_time_ms: 7,
                ..SandboxResourceUsage::default()
            },
        }),
        error: Some("command exited with status 1".to_string()),
    };

    let event = sandbox_command_record_event(
        NodeId::from_legacy_named("node_sandbox").expect("node"),
        &record,
    );

    assert_eq!(event.kind.as_str(), "sandbox.command.failed");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["status"], "failed");
    assert_eq!(event.payload["exit_status"], 1);
    assert_eq!(event.payload["error"], "command exited with status 1");
    assert_eq!(event.payload["resource_usage"]["wall_time_ms"], 7);
}

#[test]
fn sandbox_command_record_event_reports_cancellation() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_record_cancelled").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_record_cancelled").expect("command");
    let record = SandboxCommandRecord {
        sandbox_id: sandbox_id.clone(),
        command_id: command_id.clone(),
        status: SandboxCommandRecordStatus::Cancelled,
        workspace_root: sandbox_instance_root(&sandbox_id),
        toolchain_id: SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("toolchain"),
        argv: vec!["sleep".to_string(), "10".to_string()],
        cwd: SandboxPath::new("/work").expect("cwd"),
        env: BTreeMap::new(),
        started_at: OffsetDateTime::now_utc(),
        completed_at: Some(OffsetDateTime::now_utc()),
        response: None,
        error: Some("sandbox WASI command was cancelled".to_string()),
    };

    let event = sandbox_command_record_event(
        NodeId::from_legacy_named("node_sandbox").expect("node"),
        &record,
    );

    assert_eq!(event.kind.as_str(), "sandbox.command.cancelled");
    assert_eq!(event.payload["status"], "cancelled");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["command_id"], command_id.to_string());
}

#[test]
fn sandbox_mount_events_report_added_and_denied_shapes() {
    let node_id = NodeId::from_legacy_named("node_mount_events").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_mount_events").expect("sandbox");
    let mut manifest = SandboxInstanceManifest::new(
        sandbox_id.clone(),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let mount = SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: SandboxPath::new("/work/shared").expect("mount"),
        mode: SandboxMountMode::ReadWrite,
    };
    manifest.mounts.push(mount.clone());

    let added = sandbox_mount_added_event(node_id.clone(), &manifest, &mount);

    assert_eq!(added.origin_node_id, node_id);
    assert_eq!(added.kind.as_str(), "sandbox.mount.added");
    assert_eq!(added.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        added.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(
        added.payload["source_sifs_path"],
        "/services/sandbox/shared"
    );
    assert_eq!(added.payload["mount_path"], "/work/shared");
    assert_eq!(added.payload["mode"], "read-write");

    let denial = SandboxPolicyDenial::new("mount-source-denied", "outside allowed prefix");
    let denied = sandbox_mount_denied_event(
        NodeId::from_legacy_named("node_mount_denied").expect("node"),
        Some(&sandbox_id),
        "sandbox-create",
        &denial,
    );

    assert_eq!(denied.kind.as_str(), "sandbox.mount.denied");
    assert_eq!(denied.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        denied.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(denied.payload["phase"], "sandbox-create");
    assert_eq!(denied.payload["code"], "mount-source-denied");
}

#[test]
fn sandbox_toolchain_events_report_pin_and_update_shapes() {
    let node_id = NodeId::from_legacy_named("node_toolchain_events").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_toolchain_events").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let manifest = SandboxInstanceManifest::new(
        sandbox_id.clone(),
        toolchain_id.clone(),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");

    let pinned = sandbox_toolchain_pinned_event(node_id.clone(), &manifest);

    assert_eq!(pinned.origin_node_id, node_id);
    assert_eq!(pinned.kind.as_str(), "sandbox.toolchain.pinned");
    assert_eq!(pinned.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(pinned.payload["toolchain_id"], toolchain_id.to_string());

    let mut toolchain = SandboxToolchainManifest::new(
        toolchain_id.clone(),
        "busybox-1.0",
        SandboxWasiAbi::Preview1,
        "unit test",
        OffsetDateTime::now_utc(),
    )
    .expect("toolchain");
    toolchain
        .insert_command(
            SandboxToolchainCommand::new(
                &toolchain_id,
                "cat",
                b"wasm",
                SandboxResourceLimits::default(),
            )
            .expect("command"),
        )
        .expect("insert command");

    let updated = sandbox_toolchain_updated_event(
        NodeId::from_legacy_named("node_toolchain_updated").expect("node"),
        &toolchain,
        "hash",
    );

    assert_eq!(updated.kind.as_str(), "sandbox.toolchain.updated");
    assert_eq!(updated.payload["toolchain_id"], toolchain_id.to_string());
    assert_eq!(updated.payload["version"], "busybox-1.0");
    assert_eq!(updated.payload["target_abi"], "preview1");
    assert_eq!(updated.payload["command_count"], 1);
    assert_eq!(updated.payload["module_hash"], "hash");
    assert_eq!(updated.payload["signed"], false);
}

#[test]
fn wasi_resource_errors_map_to_resource_exhausted_records() {
    assert_eq!(
        sandbox_command_record_status_for_wasi_error(&SandboxWasiError::OutputLimitExceeded {
            limit: 4,
        }),
        SandboxCommandRecordStatus::ResourceExhausted
    );
    assert_eq!(
        sandbox_command_record_status_for_wasi_error(&SandboxWasiError::TimedOut {
            timeout_ms: 10,
        }),
        SandboxCommandRecordStatus::ResourceExhausted
    );
    assert_eq!(
        sandbox_command_record_status_for_wasi_error(&SandboxWasiError::Cancelled),
        SandboxCommandRecordStatus::Cancelled
    );
    assert_eq!(
        sandbox_command_record_status_for_wasi_error(&SandboxWasiError::Runtime {
            detail: "trap".to_string(),
        }),
        SandboxCommandRecordStatus::Failed
    );
}

#[test]
fn sandbox_instance_lifecycle_events_report_manifest_shape() {
    let node_id = NodeId::from_legacy_named("node_sandbox_lifecycle").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_lifecycle_event").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let now = OffsetDateTime::now_utc();
    let mut manifest = SandboxInstanceManifest::new(sandbox_id.clone(), toolchain_id.clone(), now)
        .expect("manifest");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: "/services/sandbox/shared".to_string(),
        mount_path: SandboxPath::new("/work/shared").expect("mount"),
        mode: SandboxMountMode::ReadWrite,
    });
    let created = sandbox_instance_created_event(node_id.clone(), &manifest);

    assert_eq!(created.origin_node_id, node_id);
    assert_eq!(created.source.as_str(), "sandbox.instance");
    assert_eq!(created.kind.as_str(), "sandbox.instance.created");
    assert_eq!(created.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(created.payload["workspace_root"], manifest.workspace_root);
    assert_eq!(created.payload["toolchain_id"], toolchain_id.to_string());
    assert_eq!(created.payload["cwd"], "/work");
    assert_eq!(
        created.payload["mounts"][0]["source_sifs_path"],
        "/services/sandbox/shared"
    );
    assert_eq!(created.payload["mounts"][0]["mount_path"], "/work/shared");
    assert_eq!(created.payload["mounts"][0]["mode"], "read-write");
    assert_eq!(created.payload["temp_policy"], "clear-on-session-start");

    let updated = sandbox_instance_updated_event(
        node_id.clone(),
        &manifest,
        true,
        &["PATH".to_string()],
        &["OLD_VAR".to_string()],
    );
    assert_eq!(updated.kind.as_str(), "sandbox.instance.updated");
    assert_eq!(updated.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(updated.payload["cwd"], "/work");
    assert_eq!(updated.payload["cwd_changed"], true);
    assert_eq!(updated.payload["env_set_keys"][0], "PATH");
    assert_eq!(updated.payload["env_unset_keys"][0], "OLD_VAR");

    let checkpoint = SandboxCheckpoint {
        checkpoint_id: SandboxCheckpointId::from_legacy_named("checkpoint_lifecycle")
            .expect("checkpoint"),
        sandbox_id: sandbox_id.clone(),
        created_at: now,
        manifest_ref: format!(
            "{}/checkpoints/checkpoint_lifecycle/manifest.sobj",
            manifest.workspace_root
        ),
    };
    let checkpointed = sandbox_instance_checkpointed_event(node_id.clone(), &manifest, &checkpoint);
    assert_eq!(checkpointed.kind.as_str(), "sandbox.instance.checkpointed");
    assert_eq!(
        checkpointed.payload["checkpoint_id"],
        checkpoint.checkpoint_id.to_string()
    );
    assert_eq!(
        checkpointed.payload["checkpoint_manifest_ref"],
        checkpoint.manifest_ref
    );

    let resumed =
        sandbox_instance_resumed_event(node_id.clone(), &manifest, &checkpoint.checkpoint_id);
    assert_eq!(resumed.kind.as_str(), "sandbox.instance.resumed");
    assert_eq!(
        resumed.payload["checkpoint_id"],
        checkpoint.checkpoint_id.to_string()
    );

    let destroyed =
        sandbox_instance_destroyed_event(node_id, &sandbox_id, manifest.workspace_root.clone());
    assert_eq!(destroyed.kind.as_str(), "sandbox.instance.destroyed");
    assert_eq!(destroyed.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(destroyed.payload["workspace_root"], manifest.workspace_root);
}

#[test]
fn sandbox_scheduling_failed_event_reports_inspectable_reason() {
    let node_id = NodeId::from_legacy_named("node_schedule_failed_event").expect("node");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_schedule_failed_event").expect("sandbox");
    let error =
        SandboxSchedulingError::new("unavailable-offline", "local-owned mount owner is offline");
    let event = sandbox_scheduling_failed_event(node_id.clone(), &sandbox_id, &error);

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.scheduler");
    assert_eq!(event.kind.as_str(), "sandbox.scheduler.failed");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(event.payload["code"], "unavailable-offline");
    assert_eq!(
        event.payload["detail"],
        "local-owned mount owner is offline"
    );
}

#[test]
fn sandbox_file_written_event_reports_path_and_size() {
    let node_id = NodeId::from_legacy_named("node_file_written_event").expect("node");
    let sandbox_id = SandboxId::from_legacy_named("sandbox_file_written_event").expect("sandbox");
    let path = SandboxPath::new("/work/main.rs").expect("path");
    let event = sandbox_file_written_event(node_id.clone(), &sandbox_id, &path, 13);

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.file");
    assert_eq!(event.kind.as_str(), "sandbox.file.written");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        event.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(event.payload["phase"], "sandbox-write");
    assert_eq!(event.payload["path"], "/work/main.rs");
    assert_eq!(event.payload["bytes_written"], 13);
    assert!(event.payload["command_id"].is_null());
    assert!(event.payload["request_id"].is_null());
}

#[test]
fn sandbox_file_written_event_can_report_fetch_context() {
    let node_id = NodeId::from_legacy_named("node_file_written_fetch_event").expect("node");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_file_written_fetch_event").expect("sandbox");
    let path = SandboxPath::new("/work/archive.tar.gz").expect("path");
    let command_id =
        SandboxCommandId::from_legacy_named("command_file_written_fetch").expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_file_written_fetch").expect("request");
    let event = sandbox_file_written_event_with_context(
        node_id.clone(),
        &sandbox_id,
        &path,
        13,
        "sandbox-fetch",
        Some(&command_id),
        Some(&request_id),
    );

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.file");
    assert_eq!(event.kind.as_str(), "sandbox.file.written");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        event.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(event.payload["phase"], "sandbox-fetch");
    assert_eq!(event.payload["path"], "/work/archive.tar.gz");
    assert_eq!(event.payload["bytes_written"], 13);
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["request_id"], request_id.to_string());
}

#[test]
fn sandbox_resource_exhausted_event_reports_quota_shape() {
    let node_id = NodeId::from_legacy_named("node_resource_exhausted_event").expect("node");
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_resource_exhausted_event").expect("sandbox");
    let event = sandbox_resource_exhausted_event(
        node_id.clone(),
        &sandbox_id,
        "sandbox-write",
        "workspace-bytes",
        21,
        8,
    );

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.resource");
    assert_eq!(event.kind.as_str(), "sandbox.resource.exhausted");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        event.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(event.payload["phase"], "sandbox-write");
    assert_eq!(event.payload["kind"], "workspace-bytes");
    assert_eq!(event.payload["actual"], 21);
    assert_eq!(event.payload["limit"], 8);
}
