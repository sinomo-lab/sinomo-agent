//! Explicit R0/R1 runtime boundary facade.
//!
//! R2 service crates should treat this crate as the public foundation surface
//! for trusted runtime contracts. The facade intentionally aggregates only R0
//! and R1 crates; it must never depend on R2 service or application crates.
//!
//! The crate does not add behavior. It makes the trust-ring cut visible in the
//! Cargo graph and gives architecture tests one concrete boundary to enforce.

/// R0 continuity-root contracts.
///
/// These are the smallest durable contracts that must remain understandable
/// across standby handoff, rollback, recovery, and future self-evolution.
pub mod r0 {
    pub use sinomo_core as core;
    pub use sinomo_core::*;
}

/// R1 trusted-runtime services and contracts.
///
/// R1 owns the local runtime substrate: events, nodes, DFS metadata, module
/// loading, policy, registry, rollout, sandboxing, supervision, local paths,
/// and command evaluation. R2 services may build on these crates, but R1 crates
/// must not depend back on R2 service crates.
pub mod r1 {
    pub use sinomo_api as api;
    pub use sinomo_daemon as daemon;
    pub use sinomo_device as device;
    pub use sinomo_dfs as dfs;
    pub use sinomo_eval as eval;
    pub use sinomo_events as events;
    pub use sinomo_kv as kv;
    pub use sinomo_local as local;
    pub use sinomo_module as module;
    pub use sinomo_network as network;
    pub use sinomo_node as node;
    pub use sinomo_policy as policy;
    pub use sinomo_registry as registry;
    pub use sinomo_rollout as rollout;
    pub use sinomo_sandbox as sandbox;
    pub use sinomo_supervision as supervision;
    pub use sinomo_sync as sync;

    pub mod storage {
        pub use sinomo_storage::*;

        pub fn sinomo_data_dir_layout() -> Result<StorageLayout, StorageContractError> {
            let mut layout = StorageLayout::new();
            sinomo_network::register_storage_paths(&mut layout)?;
            sinomo_kv::register_storage_paths(&mut layout)?;
            sinomo_daemon::register_storage_paths(&mut layout)?;
            sinomo_rollout::register_storage_paths(&mut layout)?;
            sinomo_node::register_storage_paths(&mut layout)?;
            sinomo_dfs::register_storage_paths(&mut layout)?;
            sinomo_sandbox::register_storage_paths(&mut layout)?;
            sinomo_local::register_storage_paths(&mut layout)?;
            sinomo_events::register_storage_paths(&mut layout)?;
            Ok(layout)
        }

        pub fn storage_registry() -> Result<StorageRegistry, StorageContractError> {
            sinomo_data_dir_layout().map(StorageRegistry::from_layout)
        }

        pub fn verify_sinomo_data_dir(
            data_dir: impl AsRef<std::path::Path>,
        ) -> Result<StorageVerificationReport, StorageContractError> {
            sinomo_data_dir_layout()?.verify_data_dir(data_dir)
        }

        pub fn ensure_sinomo_data_dir_is_registered(
            data_dir: impl AsRef<std::path::Path>,
        ) -> Result<(), StorageContractError> {
            let report = verify_sinomo_data_dir(data_dir)?;
            if report.is_complete() {
                Ok(())
            } else {
                Err(StorageContractError::UnregisteredPaths {
                    paths: report.unregistered,
                })
            }
        }
    }
}

pub use r0::core;
pub use r1::{
    api, daemon, device, dfs, eval, events, kv, local, module, network, node, policy, registry,
    rollout, sandbox, storage, supervision, sync,
};

#[cfg(test)]
mod tests {
    use super::storage::{StorageCoverage, StoragePathOwner, sinomo_data_dir_layout};

    #[test]
    fn runtime_storage_layout_is_composed_from_owner_crates() {
        let layout = sinomo_data_dir_layout().expect("layout");

        let Some(StoragePathOwner::File(cluster_state)) = layout.owner_for("cluster/state.sobj")
        else {
            panic!("expected cluster state contract");
        };
        assert_eq!(cluster_state.module, "sinomo-network");

        let Some(StoragePathOwner::Escape(kv_raft_node)) =
            layout.owner_for("cluster/kv-raft/01kr3d54d45zkm0p0xgps8k7mt/wal.sobjlog")
        else {
            panic!("expected dynamic raft node escape");
        };
        assert_eq!(kv_raft_node.module, "sinomo-kv");

        let report = layout
            .verify_relative_paths([
                "daemon/status.sobj",
                "dfs/metadata/root/records/filesystems/root/object.sobj",
                "sandbox/runtime/active.sobj",
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
}
