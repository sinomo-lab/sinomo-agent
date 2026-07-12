//! DFS module registration, route surface metadata, and storage layout contract.

use crate::{
    DFS_DIRECT_READ_ROUTE, DFS_EXPOSE_CONFIG_ROUTE, DFS_INSPECTION_ROUTE,
    DFS_MESSAGE_DATA_PLANE_ROUTES, DFS_METADATA_JOURNAL_BATCH_ROUTE,
    DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_RPC_CATEGORY, DFS_SMOKE_ROUTE, DFS_STATUS_ROUTE,
};
use sinomo_core::{
    CapabilityName, FileSystemAccess, ModuleCapabilities, ModuleResourceLimits, NetworkAccess,
    TrustRing,
};

pub const DFS_PROTOCOL: &str = "sinomo.dfs.v1";
pub const SINOMO_DFS_MODULE_LEGACY_ID: &str = "module.dfs";
pub const SINOMO_DFS_MODULE_OWNER: &str = "sinomo-dfs";
pub const SINOMO_DFS_NATIVE_REGISTRATION_KEY: &str = "sinomo-dfs";
pub const DFS_POLICY_VIOLATION_EVENT_KIND: &str = "dfs.policy.violation";

pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::StorageRing;

    for (relative_path, reason) in [
        ("dfs/content/blobs", "content-addressed DFS blob bytes"),
        (
            "dfs/content/blob-packs",
            "packed small DFS blob segment files and index records",
        ),
        (
            "dfs/content/objects",
            "dynamic DFS object manifests stored as sinomo-object files",
        ),
        (
            "dfs/content/stats.sobj",
            "durable approximate DFS content-store counters",
        ),
        (
            "dfs/content/stats.lock",
            "advisory lock for DFS content-store counter updates",
        ),
        ("dfs/content/staging", "temporary DFS staging bytes"),
        (
            "dfs/metadata/records",
            "dynamic DFS metadata records stored as sinomo-object files",
        ),
        (
            "dfs/metadata/journal",
            "dynamic DFS metadata journal entries stored as sinomo-object files",
        ),
        (
            "dfs/metadata/root",
            "default DFS root metadata shard with object records and journal entries",
        ),
        (
            "dfs/metadata/shard-index",
            "dynamic DFS object-to-metadata-shard index records",
        ),
        (
            "dfs/metadata-replication-cursors",
            "durable per-replica DFS metadata journal replication cursors",
        ),
        (
            "dfs/anti-entropy-cursors",
            "durable per-filesystem DFS anti-entropy scan cursors",
        ),
        (
            "dfs/maintenance",
            "durable DFS maintenance cursors, retry ledgers, and suppression records",
        ),
        (
            "dfs/metadata",
            "dynamic DFS metadata shard directories with object records and journal entries",
        ),
        (
            "dfs/imports",
            "quarantined DFS metadata imported from migrated source clusters",
        ),
        (
            "dfs/operator",
            "dynamic DFS operator intent records stored as sinomo-object files",
        ),
        (
            "dfs/pins",
            "dynamic DFS pin records stored as sinomo-object files",
        ),
        ("files", "local-owned user files exposed into DFS"),
    ] {
        layout.register_escape_path("sinomo-dfs", StorageRing::R1, relative_path, reason)?;
    }
    Ok(())
}

pub fn dfs_module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named(SINOMO_DFS_MODULE_LEGACY_ID)
        .expect("valid DFS module id")
}

pub fn dfs_module_surfaces() -> sinomo_module::ModuleSurfaces {
    use sinomo_module::{ModuleObjectRpcRoute, ModuleRouteAuth, ModuleSurfaces};

    let mut surfaces = ModuleSurfaces::default()
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_STATUS_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.status.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_EXPOSE_CONFIG_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.expose.configure",
            ModuleRouteAuth::LocalCommand,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_INSPECTION_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.inspection.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_METADATA_JOURNAL_BATCH_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.metadata.replicate",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.metadata.repair",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_SMOKE_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.smoke.run",
            ModuleRouteAuth::LocalCommand,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_DIRECT_READ_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.object.transfer",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_event_kind_name(DFS_POLICY_VIOLATION_EVENT_KIND)
        .with_storage_owner(SINOMO_DFS_MODULE_OWNER);
    for route in DFS_MESSAGE_DATA_PLANE_ROUTES.iter().copied() {
        surfaces = surfaces.with_object_rpc_route(ModuleObjectRpcRoute::new(
            route,
            DFS_RPC_CATEGORY,
            "dfs.object.transfer",
            ModuleRouteAuth::LocalOrClusterMember,
        ));
    }
    surfaces.capabilities = dfs_module_capabilities();
    surfaces
}

pub fn dfs_module_registration() -> sinomo_module::ModuleRegistration {
    let mut registration = sinomo_module::ModuleRegistration::native_rust(
        dfs_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_DFS_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        DFS_PROTOCOL,
        TrustRing::R1TrustedRuntime,
        SINOMO_DFS_NATIVE_REGISTRATION_KEY,
    );
    registration.manifest.capabilities = dfs_module_capabilities();
    registration.manifest.limits = ModuleResourceLimits {
        network: NetworkAccess::Allowlisted,
        filesystem: FileSystemAccess::Scoped,
        ..ModuleResourceLimits::default()
    };
    registration.surfaces = dfs_module_surfaces();
    registration
}

pub fn dfs_module_capabilities() -> ModuleCapabilities {
    ModuleCapabilities {
        imports: vec![
            dfs_capability("storage.local.read"),
            dfs_capability("storage.local.write"),
            dfs_capability("network.object_rpc"),
            dfs_capability("cluster.kv.read"),
        ],
        exports: vec![
            dfs_capability("dfs.status.read"),
            dfs_capability("dfs.inspection.read"),
            dfs_capability("dfs.expose.configure"),
            dfs_capability("dfs.metadata.replicate"),
            dfs_capability("dfs.metadata.repair"),
            dfs_capability("dfs.smoke.run"),
            dfs_capability("dfs.object.transfer"),
            dfs_capability("sifs.path.access"),
        ],
    }
}

fn dfs_capability(name: &str) -> CapabilityName {
    CapabilityName::new(name).expect("valid DFS module capability")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DFS_DATA_READ_ROUTE, DFS_OBJECT_RPC_ROUTES};
    use sinomo_core::ModuleKind;
    use sinomo_core::storage::StorageLayout;
    use sinomo_module::{ModuleBackendRegistration, NativeRustBackendRegistration};

    #[test]
    fn dfs_registers_as_r1_native_rust_module() {
        let registration = dfs_module_registration();

        assert_eq!(registration.module_id(), &dfs_module_id());
        assert_eq!(registration.manifest.kind, ModuleKind::NativeRust);
        assert_eq!(
            registration.manifest.target_ring,
            TrustRing::R1TrustedRuntime
        );
        assert_eq!(registration.manifest.api_version, DFS_PROTOCOL);
        assert!(
            registration
                .manifest
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "dfs.metadata.replicate")
        );
        assert_eq!(
            registration.backend,
            ModuleBackendRegistration::NativeRust {
                registration: NativeRustBackendRegistration::new(
                    SINOMO_DFS_NATIVE_REGISTRATION_KEY,
                    SINOMO_DFS_MODULE_OWNER
                )
            }
        );
        assert_eq!(
            registration.surfaces.storage_owners,
            vec![SINOMO_DFS_MODULE_OWNER.to_string()]
        );
        assert!(
            registration
                .surfaces
                .event_kinds
                .iter()
                .any(|event_kind| event_kind.as_str() == DFS_POLICY_VIOLATION_EVENT_KIND)
        );
        assert!(
            registration
                .surfaces
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "dfs.object.transfer")
        );
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            DFS_OBJECT_RPC_ROUTES.len()
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .all(|route| route.route != "/dfs/message")
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .any(|route| route.route == DFS_DATA_READ_ROUTE
                    && route.operation == "dfs.object.transfer")
        );
        assert!(
            registration
                .surfaces
                .object_rpc_routes
                .iter()
                .all(|route| route.route != "/dfs/protocol/frame")
        );
    }

    #[test]
    fn dfs_storage_contract_covers_dynamic_metadata_shards() {
        let mut layout = StorageLayout::new();
        register_storage_paths(&mut layout).expect("register DFS storage paths");

        for path in [
            "dfs/content/blob-packs/index.sobj",
            "dfs/content/blob-packs/segments/0000000000000000.pack",
            "dfs/content/stats.sobj",
            "dfs/anti-entropy-cursors/sifs.sobj",
            "dfs/metadata/services/records/tables/materialized.slog",
            "dfs/metadata/users/journal/segments/00000000000000000001.slog",
            "dfs/metadata/custom-shard/records/objects/example.sobj",
        ] {
            assert!(
                layout.owner_for(path).is_some(),
                "storage contract should cover {path}"
            );
        }
    }
}
