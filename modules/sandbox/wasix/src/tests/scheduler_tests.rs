use super::*;

#[test]
fn sandbox_scheduler_selects_any_online_node_without_local_owned_mounts() {
    let manifest = SandboxInstanceManifest::new(
        SandboxId::from_legacy_named("sandbox_schedule_any").expect("sandbox"),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let node_a = NodeId::from_legacy_named("node_schedule_a").expect("node");
    let node_b = NodeId::from_legacy_named("node_schedule_b").expect("node");
    let placement = select_sandbox_execution_node(
        &manifest,
        &[
            SandboxNodeCandidate::offline(node_a),
            SandboxNodeCandidate::online(node_b.clone()),
        ],
        &SandboxPolicy::default(),
    )
    .expect("placement");
    assert_eq!(placement.node_id, node_b);
    assert_eq!(placement.reason, SandboxPlacementReason::AnyEligibleNode);
}

#[test]
fn sandbox_scheduler_prefers_local_owned_mount_owner() {
    let owner = NodeId::from_legacy_named("node_schedule_owner").expect("node");
    let other = NodeId::from_legacy_named("node_schedule_other").expect("node");
    let mut manifest = SandboxInstanceManifest::new(
        SandboxId::from_legacy_named("sandbox_schedule_owner").expect("sandbox"),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{owner}/files/project"),
        mount_path: SandboxPath::new("/project").expect("mount"),
        mode: SandboxMountMode::ReadOnly,
    });
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        ..SandboxPolicy::default()
    };
    let placement = select_sandbox_execution_node(
        &manifest,
        &[
            SandboxNodeCandidate::online(other),
            SandboxNodeCandidate::online(owner.clone()),
        ],
        &policy,
    )
    .expect("placement");
    assert_eq!(placement.node_id, owner.clone());
    assert_eq!(
        placement.reason,
        SandboxPlacementReason::LocalOwnedMountOwner
    );
    assert_eq!(placement.local_owned_mount_nodes, vec![owner]);
}

#[test]
fn sandbox_scheduler_reports_offline_local_owned_owner() {
    let owner = NodeId::from_legacy_named("node_schedule_offline_owner").expect("node");
    let other = NodeId::from_legacy_named("node_schedule_online_other").expect("node");
    let mut manifest = SandboxInstanceManifest::new(
        SandboxId::from_legacy_named("sandbox_schedule_offline_owner").expect("sandbox"),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{owner}/files/project"),
        mount_path: SandboxPath::new("/project").expect("mount"),
        mode: SandboxMountMode::ReadOnly,
    });
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        ..SandboxPolicy::default()
    };
    let error = select_sandbox_execution_node(
        &manifest,
        &[
            SandboxNodeCandidate::offline(owner),
            SandboxNodeCandidate::online(other),
        ],
        &policy,
    )
    .expect_err("owner offline");
    assert_eq!(error.code, "unavailable-offline");
}

#[test]
fn sandbox_scheduler_can_fallback_when_remote_local_owned_mounts_are_allowed() {
    let owner = NodeId::from_legacy_named("node_schedule_remote_owner").expect("node");
    let other = NodeId::from_legacy_named("node_schedule_remote_other").expect("node");
    let mut manifest = SandboxInstanceManifest::new(
        SandboxId::from_legacy_named("sandbox_schedule_remote_allowed").expect("sandbox"),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{owner}/files/project"),
        mount_path: SandboxPath::new("/project").expect("mount"),
        mode: SandboxMountMode::ReadOnly,
    });
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        allow_remote_local_owned_mounts: true,
        ..SandboxPolicy::default()
    };
    let placement = select_sandbox_execution_node(
        &manifest,
        &[
            SandboxNodeCandidate::offline(owner.clone()),
            SandboxNodeCandidate::online(other.clone()),
        ],
        &policy,
    )
    .expect("placement");
    assert_eq!(placement.node_id, other);
    assert_eq!(
        placement.reason,
        SandboxPlacementReason::RemoteLocalOwnedMountAllowed
    );
    assert_eq!(placement.local_owned_mount_nodes, vec![owner]);
}

#[test]
fn sandbox_scheduler_blocks_multiple_local_owned_owners_by_default() {
    let owner_a = NodeId::from_legacy_named("node_schedule_owner_a").expect("node");
    let owner_b = NodeId::from_legacy_named("node_schedule_owner_b").expect("node");
    let mut manifest = SandboxInstanceManifest::new(
        SandboxId::from_legacy_named("sandbox_schedule_multiple_owners").expect("sandbox"),
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{owner_a}/files/a"),
        mount_path: SandboxPath::new("/a").expect("mount"),
        mode: SandboxMountMode::ReadOnly,
    });
    manifest.mounts.push(SandboxMount {
        source_sifs_path: format!("/sinomo/nodes/{owner_b}/files/b"),
        mount_path: SandboxPath::new("/b").expect("mount"),
        mode: SandboxMountMode::ReadOnly,
    });
    let policy = SandboxPolicy {
        allowed_mount_prefixes: vec!["/sinomo/nodes".to_string()],
        ..SandboxPolicy::default()
    };
    let error = select_sandbox_execution_node(
        &manifest,
        &[
            SandboxNodeCandidate::online(owner_a),
            SandboxNodeCandidate::online(owner_b),
        ],
        &policy,
    )
    .expect_err("multiple owners blocked");
    assert_eq!(error.code, "policy-blocked");
}
