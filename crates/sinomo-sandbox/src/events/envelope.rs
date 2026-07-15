use crate::internal::*;

fn sandbox_command_event_id(
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
    phase: &str,
    detail: &str,
) -> EventId {
    let identity = format!("{sandbox_id}\0{command_id}\0{phase}\0{detail}");
    EventId::derived("sinomo.sandbox.command-event.v1", identity.as_bytes())
}

pub fn sandbox_command_record_event(
    node_id: NodeId,
    record: &SandboxCommandRecord,
) -> EventEnvelope {
    let kind = match record.status {
        SandboxCommandRecordStatus::Completed => "sandbox.command.completed",
        SandboxCommandRecordStatus::Failed => "sandbox.command.failed",
        SandboxCommandRecordStatus::Cancelled => "sandbox.command.cancelled",
        SandboxCommandRecordStatus::ResourceExhausted => "sandbox.resource.exhausted",
    };
    let (exit_status, stdout_ref, stderr_ref, changed_paths, resource_usage) =
        if let Some(response) = &record.response {
            (
                Some(response.exit_status),
                Some(response.stdout_ref.clone()),
                Some(response.stderr_ref.clone()),
                response
                    .changed_paths
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                Some(response.resource_usage.clone()),
            )
        } else {
            (None, None, None, Vec::new(), None)
        };

    EventEnvelope::new(
        sandbox_command_event_id(&record.sandbox_id, &record.command_id, "terminal", ""),
        record.completed_at.unwrap_or_else(OffsetDateTime::now_utc),
        node_id,
        Source::new("sandbox.runtime").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(match record.status {
            SandboxCommandRecordStatus::Completed => 0.66,
            SandboxCommandRecordStatus::Cancelled => 0.72,
            SandboxCommandRecordStatus::Failed | SandboxCommandRecordStatus::ResourceExhausted => {
                0.88
            }
        })
        .expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": record.sandbox_id.to_string(),
        "command_id": record.command_id.to_string(),
        "status": record.status.clone(),
        "workspace_root": record.workspace_root.clone(),
        "toolchain_id": record.toolchain_id.to_string(),
        "argv": record.argv.clone(),
        "cwd": record.cwd.to_string(),
        "env_keys": record.env.keys().cloned().collect::<Vec<_>>(),
        "started_at": record.started_at,
        "completed_at": record.completed_at,
        "exit_status": exit_status,
        "stdout_ref": stdout_ref,
        "stderr_ref": stderr_ref,
        "changed_paths": changed_paths,
        "resource_usage": resource_usage,
        "error": record.error.clone(),
    }))
}

pub fn sandbox_command_started_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    command_id: &SandboxCommandId,
    argv: &[String],
    cwd: &SandboxPath,
    started_at: OffsetDateTime,
) -> EventEnvelope {
    EventEnvelope::new(
        sandbox_command_event_id(&manifest.sandbox_id, command_id, "started", ""),
        started_at,
        node_id,
        Source::new("sandbox.runtime").expect("valid source"),
        EventKind::new("sandbox.command.started").expect("valid event kind"),
        Salience::new(0.48).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": manifest.sandbox_id.to_string(),
        "command_id": command_id.to_string(),
        "workspace_root": manifest.workspace_root,
        "toolchain_id": manifest.toolchain_id.to_string(),
        "argv": argv,
        "cwd": cwd.to_string(),
        "started_at": started_at,
        "mounts": sandbox_mount_payload(&manifest.mounts),
        "last_placement": manifest.last_placement,
    }))
}

pub fn sandbox_command_started_event_from_record(
    node_id: NodeId,
    record: &SandboxCommandRecord,
) -> EventEnvelope {
    EventEnvelope::new(
        sandbox_command_event_id(&record.sandbox_id, &record.command_id, "started", ""),
        record.started_at,
        node_id,
        Source::new("sandbox.runtime").expect("valid source"),
        EventKind::new("sandbox.command.started").expect("valid event kind"),
        Salience::new(0.48).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": record.sandbox_id.to_string(),
        "command_id": record.command_id.to_string(),
        "workspace_root": record.workspace_root.clone(),
        "toolchain_id": record.toolchain_id.to_string(),
        "argv": record.argv.clone(),
        "cwd": record.cwd.to_string(),
        "env_keys": record.env.keys().cloned().collect::<Vec<_>>(),
        "started_at": record.started_at,
        "mounts": [],
        "last_placement": serde_json::Value::Null,
    }))
}

pub fn sandbox_command_output_event(node_id: NodeId, chunk: &SandboxOutputChunk) -> EventEnvelope {
    let stream = match chunk.stream {
        SandboxOutputStream::Stdout => "stdout",
        SandboxOutputStream::Stderr => "stderr",
    };
    EventEnvelope::new(
        sandbox_command_event_id(
            &chunk.sandbox_id,
            &chunk.command_id,
            "output",
            &format!("{stream}:{}", chunk.offset),
        ),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.runtime").expect("valid source"),
        EventKind::new("sandbox.command.output").expect("valid event kind"),
        Salience::new(0.34).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Interactive,
            PrivacyClass::Raw,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": chunk.sandbox_id.to_string(),
        "command_id": chunk.command_id.to_string(),
        "stream": chunk.stream,
        "offset": chunk.offset,
        "bytes": chunk.bytes,
        "byte_len": chunk.bytes.len(),
    }))
}

fn sandbox_mount_payload(mounts: &[SandboxMount]) -> Vec<serde_json::Value> {
    mounts
        .iter()
        .map(|mount| {
            serde_json::json!({
                "source_sifs_path": mount.source_sifs_path,
                "mount_path": mount.mount_path.to_string(),
                "mode": mount.mode,
            })
        })
        .collect()
}

fn sandbox_instance_lifecycle_event(
    node_id: NodeId,
    kind: &'static str,
    manifest: &SandboxInstanceManifest,
    extra_payload: serde_json::Value,
) -> EventEnvelope {
    let mut payload = serde_json::json!({
        "sandbox_id": manifest.sandbox_id.to_string(),
        "workspace_root": manifest.workspace_root,
        "toolchain_id": manifest.toolchain_id.to_string(),
        "cwd": manifest.cwd.to_string(),
        "mounts": sandbox_mount_payload(&manifest.mounts),
        "last_placement": manifest.last_placement,
        "temp_policy": manifest.temp_policy,
        "temp_epoch": manifest.temp_epoch,
        "limits": manifest.limits,
        "created_at": manifest.created_at,
        "updated_at": manifest.updated_at,
    });
    if let (Some(payload), Some(extra)) = (payload.as_object_mut(), extra_payload.as_object()) {
        payload.extend(extra.clone());
    }
    let event_id = sandbox_authoritative_mutation_event_id(manifest, kind, "")
        .unwrap_or_else(EventId::generate);
    EventEnvelope::new(
        event_id,
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.instance").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.7).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(payload)
}

fn sandbox_authoritative_mutation_event_id(
    manifest: &SandboxInstanceManifest,
    kind: &str,
    discriminator: &str,
) -> Option<EventId> {
    let pending = manifest.pending_reconciliation.as_ref()?;
    let operation = serde_json::to_string(&pending.operation).ok()?;
    Some(EventId::derived(
        "sinomo.sandbox.authoritative-mutation-event.v2",
        format!(
            "{}:{}:{}:{operation}:{kind}:{discriminator}",
            manifest.sandbox_id,
            manifest.effective_incarnation_id(),
            pending.transaction_id
        ),
    ))
}

pub fn sandbox_instance_created_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
) -> EventEnvelope {
    sandbox_instance_lifecycle_event(
        node_id,
        "sandbox.instance.created",
        manifest,
        serde_json::json!({}),
    )
}

pub fn sandbox_instance_updated_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    cwd_changed: bool,
    env_set_keys: &[String],
    env_unset_keys: &[String],
    mounts_replaced: bool,
    mounts_changed: bool,
    mounts_added: &[SandboxMount],
    mounts_removed: &[SandboxMount],
) -> EventEnvelope {
    sandbox_instance_lifecycle_event(
        node_id,
        "sandbox.instance.updated",
        manifest,
        serde_json::json!({
            "cwd_changed": cwd_changed,
            "env_set_keys": env_set_keys,
            "env_unset_keys": env_unset_keys,
            "mounts_replaced": mounts_replaced,
            "mounts_changed": mounts_changed,
            "mounts_added": sandbox_mount_payload(mounts_added),
            "mounts_removed": sandbox_mount_payload(mounts_removed),
        }),
    )
}

pub fn sandbox_instance_resumed_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    checkpoint_id: &SandboxCheckpointId,
) -> EventEnvelope {
    sandbox_instance_lifecycle_event(
        node_id,
        "sandbox.instance.resumed",
        manifest,
        serde_json::json!({
            "checkpoint_id": checkpoint_id.to_string(),
        }),
    )
}

pub fn sandbox_instance_rebooted_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    cleared_tmp: bool,
) -> EventEnvelope {
    sandbox_instance_lifecycle_event(
        node_id,
        "sandbox.instance.rebooted",
        manifest,
        serde_json::json!({
            "cleared_tmp": cleared_tmp,
        }),
    )
}

pub fn sandbox_instance_migrated_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
) -> EventEnvelope {
    sandbox_instance_lifecycle_event(
        node_id,
        "sandbox.instance.migrated",
        manifest,
        serde_json::json!({ "layout": "stable-symlink-generation" }),
    )
}

pub fn sandbox_instance_checkpointed_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    checkpoint: &SandboxCheckpoint,
) -> EventEnvelope {
    sandbox_instance_lifecycle_event(
        node_id,
        "sandbox.instance.checkpointed",
        manifest,
        serde_json::json!({
            "checkpoint_id": checkpoint.checkpoint_id.to_string(),
            "checkpoint_manifest_ref": checkpoint.manifest_ref,
            "checkpoint_created_at": checkpoint.created_at,
        }),
    )
}

pub fn sandbox_instance_destroyed_event(
    node_id: NodeId,
    sandbox_id: &SandboxId,
    workspace_root: impl Into<String>,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.instance").expect("valid source"),
        EventKind::new("sandbox.instance.destroyed").expect("valid event kind"),
        Salience::new(0.72).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": sandbox_id.to_string(),
        "workspace_root": workspace_root.into(),
    }))
}

pub fn sandbox_instance_destroyed_transaction_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
) -> EventEnvelope {
    EventEnvelope::new(
        sandbox_authoritative_mutation_event_id(manifest, "sandbox.instance.destroyed", "")
            .unwrap_or_else(EventId::generate),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.instance").expect("valid source"),
        EventKind::new("sandbox.instance.destroyed").expect("valid event kind"),
        Salience::new(0.72).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": manifest.sandbox_id.to_string(),
        "workspace_root": manifest.workspace_root,
        "incarnation_id": manifest.effective_incarnation_id().to_string(),
    }))
}

pub fn sandbox_mount_added_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    mount: &SandboxMount,
) -> EventEnvelope {
    sandbox_mount_change_event(node_id, manifest, mount, "sandbox.mount.added")
}

pub fn sandbox_mount_removed_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    mount: &SandboxMount,
) -> EventEnvelope {
    sandbox_mount_change_event(node_id, manifest, mount, "sandbox.mount.removed")
}

fn sandbox_mount_change_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
    mount: &SandboxMount,
    kind: &'static str,
) -> EventEnvelope {
    EventEnvelope::new(
        sandbox_authoritative_mutation_event_id(manifest, kind, mount.mount_path.as_str())
            .unwrap_or_else(EventId::generate),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.mount").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.58).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": manifest.sandbox_id.to_string(),
        "workspace_root": manifest.workspace_root,
        "source_sifs_path": mount.source_sifs_path,
        "mount_path": mount.mount_path.to_string(),
        "mode": mount.mode,
    }))
}

pub fn sandbox_mount_denied_event(
    node_id: NodeId,
    sandbox_id: Option<&SandboxId>,
    phase: impl Into<String>,
    denial: &SandboxPolicyDenial,
) -> EventEnvelope {
    let workspace_root = sandbox_id.map(sandbox_instance_root);
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.mount").expect("valid source"),
        EventKind::new("sandbox.mount.denied").expect("valid event kind"),
        Salience::new(0.86).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": sandbox_id.map(ToString::to_string),
        "workspace_root": workspace_root,
        "phase": phase.into(),
        "code": denial.code,
        "detail": denial.detail,
    }))
}

pub fn sandbox_toolchain_pinned_event(
    node_id: NodeId,
    manifest: &SandboxInstanceManifest,
) -> EventEnvelope {
    EventEnvelope::new(
        sandbox_authoritative_mutation_event_id(
            manifest,
            "sandbox.toolchain.pinned",
            manifest.toolchain_id.as_str(),
        )
        .unwrap_or_else(EventId::generate),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.toolchain").expect("valid source"),
        EventKind::new("sandbox.toolchain.pinned").expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": manifest.sandbox_id.to_string(),
        "workspace_root": manifest.workspace_root,
        "toolchain_id": manifest.toolchain_id.to_string(),
        "created_at": manifest.created_at,
        "updated_at": manifest.updated_at,
    }))
}

pub fn sandbox_toolchain_updated_event(
    node_id: NodeId,
    manifest: &SandboxToolchainManifest,
    module_hash: impl Into<String>,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.toolchain").expect("valid source"),
        EventKind::new("sandbox.toolchain.updated").expect("valid event kind"),
        Salience::new(0.68).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "toolchain_id": manifest.toolchain_id.to_string(),
        "version": manifest.version,
        "target_abi": manifest.target_abi,
        "command_count": manifest.commands.len(),
        "provenance": manifest.provenance,
        "signed": manifest.signature.is_some(),
        "module_hash": module_hash.into(),
        "created_at": manifest.created_at,
    }))
}

pub fn sandbox_policy_denied_event(
    node_id: NodeId,
    sandbox_id: Option<&SandboxId>,
    phase: impl Into<String>,
    denial: &SandboxPolicyDenial,
) -> EventEnvelope {
    let phase = phase.into();
    let workspace_root = sandbox_id.map(sandbox_instance_root);
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.policy").expect("valid source"),
        EventKind::new("sandbox.policy.denied").expect("valid event kind"),
        Salience::new(0.9).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": sandbox_id.map(ToString::to_string),
        "workspace_root": workspace_root,
        "phase": phase,
        "code": denial.code,
        "detail": denial.detail,
    }))
}

pub fn sandbox_scheduling_failed_event(
    node_id: NodeId,
    sandbox_id: &SandboxId,
    error: &SandboxSchedulingError,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.scheduler").expect("valid source"),
        EventKind::new("sandbox.scheduler.failed").expect("valid event kind"),
        Salience::new(0.86).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": sandbox_id.to_string(),
        "code": error.code,
        "detail": error.detail,
    }))
}

pub fn sandbox_file_written_event(
    node_id: NodeId,
    sandbox_id: &SandboxId,
    path: &SandboxPath,
    bytes_written: u64,
) -> EventEnvelope {
    sandbox_file_written_event_with_context(
        node_id,
        sandbox_id,
        path,
        bytes_written,
        "sandbox-write",
        None,
        None,
    )
}

pub fn sandbox_file_written_event_with_context(
    node_id: NodeId,
    sandbox_id: &SandboxId,
    path: &SandboxPath,
    bytes_written: u64,
    phase: impl Into<String>,
    command_id: Option<&SandboxCommandId>,
    request_id: Option<&SandboxNetworkRequestId>,
) -> EventEnvelope {
    let phase = phase.into();
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.file").expect("valid source"),
        EventKind::new("sandbox.file.written").expect("valid event kind"),
        Salience::new(0.44).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": sandbox_id.to_string(),
        "workspace_root": sandbox_instance_root(sandbox_id),
        "phase": phase,
        "path": path.to_string(),
        "bytes_written": bytes_written,
        "command_id": command_id.map(ToString::to_string),
        "request_id": request_id.map(ToString::to_string),
    }))
}

pub fn sandbox_resource_exhausted_event(
    node_id: NodeId,
    sandbox_id: &SandboxId,
    phase: impl Into<String>,
    kind: impl Into<String>,
    actual: u64,
    limit: u64,
) -> EventEnvelope {
    let phase = phase.into();
    let kind = kind.into();
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.resource").expect("valid source"),
        EventKind::new("sandbox.resource.exhausted").expect("valid event kind"),
        Salience::new(0.82).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": sandbox_id.to_string(),
        "workspace_root": sandbox_instance_root(sandbox_id),
        "phase": phase,
        "kind": kind,
        "actual": actual,
        "limit": limit,
    }))
}

pub fn sandbox_http_exchange_event(
    node_id: NodeId,
    record: &SandboxHttpExchangeRecord,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node_id,
        Source::new("sandbox.network").expect("valid source"),
        EventKind::new("sandbox.network.http").expect("valid event kind"),
        Salience::new(if record.status.is_some() { 0.54 } else { 0.76 }).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "sandbox_id": record.sandbox_id.to_string(),
        "workspace_root": sandbox_instance_root(&record.sandbox_id),
        "command_id": record.command_id.to_string(),
        "request_id": record.request_id.to_string(),
        "method": record.method.clone(),
        "url": record.url.clone(),
        "host": record.host.clone(),
        "status": record.status,
        "request_bytes": record.request_bytes,
        "response_bytes": record.response_bytes,
        "redacted_headers": record.redacted_headers.clone(),
    }))
}

#[cfg(test)]
mod command_event_id_tests {
    use super::*;

    fn command_record() -> SandboxCommandRecord {
        SandboxCommandRecord {
            sandbox_id: SandboxId::from_legacy_named("event_id_sandbox").expect("sandbox"),
            command_id: SandboxCommandId::from_legacy_named("event_id_command").expect("command"),
            status: SandboxCommandRecordStatus::Cancelled,
            workspace_root: "/services/sandbox/instances/event".to_string(),
            toolchain_id: SandboxToolchainId::from_legacy_named("event_id_toolchain")
                .expect("toolchain"),
            argv: vec!["sh".to_string()],
            cwd: SandboxPath::new("/work").expect("cwd"),
            env: BTreeMap::new(),
            started_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: Some(OffsetDateTime::UNIX_EPOCH),
            response: None,
            error: Some("cancelled".to_string()),
        }
    }

    #[test]
    fn command_event_ids_are_stable_across_reconciliation_replay() {
        let record = command_record();
        let node = NodeId::from_legacy_named("event_id_node").expect("node");
        let first_started = sandbox_command_started_event_from_record(node.clone(), &record);
        let second_started = sandbox_command_started_event_from_record(node.clone(), &record);
        assert_eq!(first_started.event_id, second_started.event_id);

        let first_terminal = sandbox_command_record_event(node.clone(), &record);
        let second_terminal = sandbox_command_record_event(node.clone(), &record);
        assert_eq!(first_terminal.event_id, second_terminal.event_id);

        let chunk = SandboxOutputChunk {
            sandbox_id: record.sandbox_id.clone(),
            command_id: record.command_id.clone(),
            stream: SandboxOutputStream::Stdout,
            offset: 0,
            bytes: b"output".to_vec(),
        };
        let first_output = sandbox_command_output_event(node.clone(), &chunk);
        let second_output = sandbox_command_output_event(node, &chunk);
        assert_eq!(first_output.event_id, second_output.event_id);
        assert_ne!(first_started.event_id, first_terminal.event_id);
        assert_ne!(first_started.event_id, first_output.event_id);
    }

    #[test]
    fn sandbox_update_event_records_full_mount_reconciliation() {
        let mut manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("event_mount_update_sandbox").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("event_mount_update_toolchain")
                .expect("toolchain"),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("manifest");
        let removed = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/old".to_string(),
            mount_path: SandboxPath::new("/work").expect("mount path"),
            mode: SandboxMountMode::ReadOnly,
        };
        let added = SandboxMount {
            source_sifs_path: "/services/sandbox-shared/new".to_string(),
            mount_path: SandboxPath::new("/work").expect("mount path"),
            mode: SandboxMountMode::ReadWrite,
        };
        manifest.mounts = vec![added.clone()];
        let node = NodeId::from_legacy_named("event_mount_update_node").expect("node");

        let updated = sandbox_instance_updated_event(
            node.clone(),
            &manifest,
            false,
            &[],
            &[],
            true,
            true,
            std::slice::from_ref(&added),
            std::slice::from_ref(&removed),
        );
        assert_eq!(updated.payload["mounts_replaced"], true);
        assert_eq!(updated.payload["mounts_changed"], true);
        assert_eq!(
            updated.payload["mounts_added"][0]["source_sifs_path"],
            added.source_sifs_path
        );
        assert_eq!(
            updated.payload["mounts_removed"][0]["source_sifs_path"],
            removed.source_sifs_path
        );

        let removed_event = sandbox_mount_removed_event(node, &manifest, &removed);
        assert_eq!(removed_event.kind.as_str(), "sandbox.mount.removed");
        assert_eq!(
            removed_event.payload["source_sifs_path"],
            removed.source_sifs_path
        );
    }
}
