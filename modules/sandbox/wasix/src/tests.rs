use super::*;
use crate::internal::*;
use sinomo_dfs::{
    DfsMetadataShardId, LocalContentStore, LocalSobjMetadataBackend, NetworkedDfsService,
};
use sinomo_network::InProcessNetwork;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

type EvolutionJobId = Id;
type WorkspaceId = Id;

mod event_tests;
mod http_tests;
mod instance_filesystem_tests;
mod policy_tests;
mod posixtestsuite_tests;
mod scheduler_tests;
mod staged_runner_tests;
mod storage_tests;
mod support;
mod toolchain_tests;
use support::*;

#[test]
fn sandbox_run_passes_when_scope_and_commands_pass() {
    let workspace_id = WorkspaceId::from_legacy_named("workspace_candidate").expect("workspace");

    let result = SandboxHarness::new(NodeId::from_legacy_named("node_desktop").expect("node")).run(
        SandboxRunRequest {
            subject: SandboxRunSubject {
                subject_id: EvolutionJobId::from_legacy_named("evojob_candidate").expect("job"),
                workspace_id: Some(workspace_id.clone()),
                description: "improve trigger".to_string(),
            },
            candidate_artifact_id: ArtifactId::from_legacy_named("artifact_candidate")
                .expect("artifact"),
            workspace_snapshot: SandboxWorkspaceSnapshot {
                workspace_id,
                object_ids: Vec::new(),
                source_event_id: None,
            },
            limits: ModuleResourceLimits::default(),
            commands: vec![SandboxCommand {
                name: "unit tests".to_string(),
                command: "cargo test -p sinomo-agent".to_string(),
                expected_exit_code: 0,
                observed_exit_code: 0,
            }],
        },
    );

    assert_eq!(result.status, SandboxRunStatus::Passed);
}

#[test]
fn sandbox_paths_normalize_without_host_escape() {
    assert_eq!(
        SandboxPath::new("/work//src/./main.rs")
            .expect("path")
            .as_str(),
        "/work/src/main.rs"
    );
    assert_eq!(
        SandboxPath::new("/work/src/../Cargo.toml")
            .expect("path")
            .as_str(),
        "/work/Cargo.toml"
    );
    assert_eq!(SandboxPath::new("/").expect("root").as_str(), "/");
}

#[test]
fn sandbox_paths_reject_relative_and_clamp_absolute_parent_traversal() {
    assert!(matches!(
        SandboxPath::new("work/file").expect_err("relative"),
        SandboxPathError::Relative { .. }
    ));
    assert_eq!(
        SandboxPath::new("/../../etc/passwd")
            .expect("absolute paths clamp at sandbox root")
            .as_str(),
        "/etc/passwd"
    );
    assert_eq!(
        SandboxPath::new("/work")
            .expect("base")
            .join("../tmp/file")
            .expect("joined")
            .as_str(),
        "/tmp/file"
    );
}

#[test]
fn sandbox_manifest_uses_services_sandbox_root_and_default_tmp_policy() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_phase0").expect("sandbox");
    let toolchain_id =
        SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain");
    let now = OffsetDateTime::now_utc();
    let manifest =
        SandboxInstanceManifest::new(sandbox_id.clone(), toolchain_id, now).expect("manifest");

    assert_eq!(
        manifest.workspace_root,
        format!("/services/sandbox/instances/{sandbox_id}")
    );
    assert_eq!(manifest.cwd.as_str(), "/work");
    assert_eq!(manifest.temp_policy, SandboxTempPolicy::ClearOnSessionStart);
    assert_eq!(
        sandbox_manifest_path(&sandbox_id),
        format!("/services/sandbox/instances/{sandbox_id}/manifest.sobj")
    );
}

#[test]
fn sandbox_storage_paths_are_registered_by_sandbox_crate() {
    let mut layout = sinomo_storage::StorageLayout::new();
    register_storage_paths(&mut layout).expect("register");

    for path in [
        "sandbox/cache/toolchains/busybox.wasm",
        "sandbox/cache/modules/busybox.compiled",
        "sandbox/runtime/active.sobj",
        "sandbox/tmp/command-stdout.tmp",
    ] {
        let owner = layout.owner_for(path).expect("owner");
        let module = match owner {
            sinomo_storage::StoragePathOwner::File(file) => file.module.as_str(),
            sinomo_storage::StoragePathOwner::Escape(escape) => escape.module.as_str(),
        };
        assert_eq!(module, SANDBOX_STORAGE_MODULE);
    }
}

#[test]
fn sandbox_create_response_encodes_for_daemon_rpc() {
    let manifest = SandboxInstanceManifest::new(
        SandboxId::from_legacy_named("sandbox_rpc_create_response").expect("sandbox"),
        SandboxToolchainId::from_legacy_named("toolchain_rpc_create_response").expect("toolchain"),
        OffsetDateTime::now_utc(),
    )
    .expect("manifest");
    let response = SandboxCreateInstanceResponse { manifest };
    let encoded = sinomo_network::ObjectRpcResponse::encode_body(
        sinomo_network::SANDBOX_CREATE_ROUTE,
        &response,
    )
    .expect("encode response");
    let decoded: SandboxCreateInstanceResponse = encoded.decode_body().expect("decode response");
    assert_eq!(decoded.manifest.sandbox_id, response.manifest.sandbox_id);
}
