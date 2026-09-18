//! Explicit R1 runtime boundary facade.
//!
//! R2 service crates should treat this crate as the public foundation surface
//! for trusted runtime contracts. The `client` profile exposes only contracts
//! and local daemon clients; the default `host` profile aggregates the complete
//! R0/R1 implementation set. The facade must never depend on R2 service or
//! application crates, and it reaches R0 exclusively through `sinomo-core`.
//!
//! The crate does not add behavior. It makes the trust-ring cut visible in the
//! Cargo graph and gives architecture tests one concrete boundary to enforce.

#[cfg(not(feature = "client"))]
compile_error!("sinomo-runtime requires either the `client` or default `host` feature profile");

/// R0 cluster foundation, re-exported from the `sinomo-core` boundary facade.
///
/// These are the smallest durable contracts that must remain understandable
/// across standby handoff, rollback, recovery, and high-level automation: the
/// minimal set of crates that defines the cluster.
pub mod r0 {
    pub use sinomo_core as core;
    pub use sinomo_core::*;
    pub use sinomo_core::{
        base, cluster, events, kv, local, network, node, object, service, storage,
    };
}

/// R1 trusted-runtime services and contracts.
///
/// R1 owns the local runtime substrate above the cluster foundation: DFS
/// metadata, module loading, policy, registry, rollout, sandboxing,
/// supervision, and command evaluation. R2 services may build on these crates,
/// but R1 crates must not depend back on R2 service crates.
pub mod r1 {
    pub use sinomo_api as api;
    #[cfg(feature = "host")]
    pub use sinomo_build as build;
    #[cfg(feature = "cli-client")]
    pub use sinomo_cli_registry as cli_registry;
    pub use sinomo_config as config;
    #[cfg(feature = "host")]
    pub mod daemon {
        pub use sinomo_build_client::*;
        pub use sinomo_daemon::*;
        pub use sinomo_rollout_client::*;
    }
    #[cfg(not(feature = "host"))]
    pub use sinomo_api::dfs;
    #[cfg(all(feature = "module-client", not(feature = "host")))]
    pub use sinomo_api::module;
    #[cfg(feature = "device-client")]
    pub use sinomo_device as device;
    #[cfg(feature = "host")]
    pub use sinomo_dfs as dfs;
    #[cfg(feature = "host")]
    pub use sinomo_dfs_nfs as dfs_nfs;
    #[cfg(feature = "host")]
    pub use sinomo_eval as eval;
    #[cfg(feature = "host")]
    pub use sinomo_installer as installer;
    #[cfg(feature = "host")]
    pub use sinomo_module as module;
    pub use sinomo_module_api as module_api;
    #[cfg(feature = "host")]
    pub mod network_module {
        pub use sinomo_module::{
            network_module_capabilities, network_module_id, network_module_registration,
            network_module_surfaces,
        };
    }
    #[cfg(all(feature = "observe-client", not(feature = "host")))]
    pub use sinomo_api::observe;
    #[cfg(feature = "host")]
    pub use sinomo_observe as observe;
    #[cfg(feature = "host")]
    pub use sinomo_policy as policy;
    #[cfg(feature = "host")]
    pub use sinomo_registry as registry;
    #[cfg(feature = "host")]
    pub use sinomo_remote_management as remote_management;
    #[cfg(feature = "host")]
    pub use sinomo_rollout as rollout;
    #[cfg(feature = "host")]
    pub use sinomo_rollout_host as rollout_host;
    // The sandbox crate owns both its provider-neutral protocol and the typed
    // client that binds that protocol to the generic component loader.
    #[cfg(feature = "host")]
    pub use sinomo_sandbox as sandbox;
    #[cfg(feature = "host")]
    pub use sinomo_sandbox_simple as sandbox_simple;
    #[cfg(feature = "host")]
    pub use sinomo_supervision as supervision;
    #[cfg(feature = "host")]
    pub use sinomo_test as test;
    #[cfg(feature = "host")]
    pub mod vfs {
        pub use sinomo_vfs::*;
        pub use sinomo_vfs_sifs::{
            DaemonSifsVfsBackend, DfsCancellationFlag, MountedSifsOpenOptions,
            MountedSifsVfsBackend, daemon_sifs, daemon_sifs_on_node, daemon_sifs_with_timeout,
            mounted_sifs, mounted_sifs_with_cancellation,
        };
    }

    pub use sinomo_core::{cluster, events, kv, local, network, node, service};

    #[cfg(feature = "host")]
    pub mod storage {
        pub use sinomo_core::storage::*;
        pub use sinomo_daemon::registry::{
            ensure_sinomo_data_dir_is_registered, sinomo_data_dir_layout,
            sinomo_data_dir_layout_with_catalog, storage_registry, verify_sinomo_data_dir,
            verify_sinomo_data_dir_with_catalog,
        };
    }

    #[cfg(feature = "host")]
    pub mod modules {
        pub use sinomo_module::{
            ModuleRegistration, ModuleRegistry, ModuleRegistryError, ModuleRegistrySnapshot,
            ModuleRegistrySnapshotStore, module_object_rpc_route_descriptor_to_service,
            module_registration_object_rpc_route_descriptors,
            module_registry_object_rpc_route_descriptors,
            module_registry_snapshot_object_rpc_route_descriptors,
        };
    }
}

pub use r0::cluster;
pub use r0::{core, object};
#[cfg(feature = "cli-client")]
pub use r1::cli_registry;
#[cfg(feature = "device-client")]
pub use r1::device;
#[cfg(feature = "host")]
pub use r1::dfs_nfs;
#[cfg(feature = "module-client")]
pub use r1::module;
#[cfg(feature = "observe-client")]
pub use r1::observe;
pub use r1::{api, config, dfs, module_api};
#[cfg(feature = "host")]
pub use r1::{
    build, daemon, eval, installer, modules, network_module, policy, registry, remote_management,
    rollout, rollout_host, sandbox, sandbox_simple, storage, supervision, test, vfs,
};
pub use r1::{events, kv, local, network, node, service};

#[cfg(all(test, feature = "host"))]
mod tests {
    use super::storage::{StorageCoverage, StoragePathOwner, sinomo_data_dir_layout_with_catalog};
    use super::{build, daemon, dfs, module, network, observe, registry, rollout, sandbox};

    #[test]
    fn runtime_storage_layout_is_composed_from_owner_crates() {
        let catalog = module::ComponentCatalog::builder()
            .register_storage_contributor(
                build::SINOMO_BUILD_MODULE_LEGACY_ID,
                build::register_storage_paths,
            )
            .expect("build storage")
            .register_storage_contributor(
                dfs::SINOMO_DFS_MODULE_LEGACY_ID,
                dfs::register_storage_paths,
            )
            .expect("DFS storage")
            .register_storage_contributor(
                observe::SINOMO_OBSERVE_MODULE_LEGACY_ID,
                observe::register_storage_paths,
            )
            .expect("observe storage")
            .register_storage_contributor(
                rollout::SINOMO_ROLLOUT_MODULE_LEGACY_ID,
                rollout::register_storage_paths,
            )
            .expect("rollout storage")
            .register_storage_contributor(
                sandbox::SINOMO_SANDBOX_MODULE_LEGACY_ID,
                sandbox::register_storage_paths,
            )
            .expect("sandbox storage")
            .register_storage_contributor("sinomo-registry", registry::register_storage_paths)
            .expect("registry storage")
            .build()
            .expect("storage catalog");
        let layout = sinomo_data_dir_layout_with_catalog(&catalog).expect("layout");

        let Some(StoragePathOwner::File(cluster_state)) = layout.owner_for("cluster/state.sobj")
        else {
            panic!("expected cluster state contract");
        };
        assert_eq!(cluster_state.module, "sinomo-cluster");

        let Some(StoragePathOwner::Escape(kv_raft_node)) =
            layout.owner_for("cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/wal.sobjlog")
        else {
            panic!("expected dynamic raft node escape");
        };
        assert_eq!(kv_raft_node.module, "sinomo-kv");

        let Some(StoragePathOwner::File(kv_watch_events)) =
            layout.owner_for("cluster/watch-events.sobj")
        else {
            panic!("expected cluster kv watch events contract");
        };
        assert_eq!(kv_watch_events.module, "sinomo-kv");

        let Some(StoragePathOwner::Escape(kv_watch_registration)) =
            layout.owner_for("cluster/watches/watch-7.sobj")
        else {
            panic!("expected dynamic cluster kv watch registration escape");
        };
        assert_eq!(kv_watch_registration.module, "sinomo-kv");

        let report = layout
            .verify_relative_paths([
                "daemon/status.sobj",
                "dfs/metadata/root/records/filesystems/root/object.sobj",
                "dfs/metadata-replication-cursors/01kr4w089vqgdxpb84t6bq1qxn/root.json",
                "sandbox/runtime/active.sobj",
                "sandbox/module-artifacts/artifact_candidate.wasm",
                "sandbox/module-wasm-artifacts.sobj",
                "module/loader-state/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/loader-state/native-rust/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/lifecycle/unload-records/01kr3d54d45zkm0p0xgps8k7mt.sobj",
                "module/local-command-artifacts.sobj",
                "events/ops/operational-events.jsonl",
                "events/ops/logs/operational-log.jsonl.lock",
                "configs/rollout/telemetry/gates.json",
                "rollout/runtime-state.sobj",
                "rollout/telemetry-advance-state.sobj",
                "rollout/telemetry-gates.sobj",
                "registry/trusted-artifact-signers.json",
                "registry/artifact-evidence/release_verified.json",
                "registry/verified-release-material/release_verified.json",
                "bin/sinomo",
                "startup/sinomo-daemon.service",
            ])
            .expect("verify");
        assert!(report.unregistered.is_empty());
        assert!(
            report
                .covered
                .iter()
                .any(|path| path.coverage == StorageCoverage::Escape)
        );
        assert!(report.covered.iter().any(|path| {
            path.relative_path == "sandbox/runtime/active.sobj" && path.module == "sinomo-sandbox"
        }));
    }

    #[test]
    fn runtime_daemon_facade_exports_async_rpc_clients() {
        let _ = daemon::send_daemon_rpc_async;
        let _ = daemon::send_daemon_rpc_to_endpoint_async;
        let _ = daemon::fetch_daemon_status_from_endpoint_async;
        let _ = daemon::send_object_rpc_to_target_async;
        let request =
            network::ObjectRpcRequest::encode_body("/test/stream", "test", &()).expect("request");
        let _future = daemon::send_daemon_rpc_stream_async(
            "127.0.0.1:0",
            &request,
            network::DaemonRpcTimeouts::connect_and_idle(std::time::Duration::from_millis(1)),
            |_response| Ok(false),
        );
    }
}
