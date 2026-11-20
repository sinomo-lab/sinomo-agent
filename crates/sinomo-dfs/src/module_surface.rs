//! DFS module registration, route surface metadata, and storage layout contract.

use crate::{
    DFS_DIRECT_READ_ROUTE, DFS_INSPECTION_ROUTE, DFS_MESSAGE_DATA_PLANE_ROUTES,
    DFS_METADATA_JOURNAL_BATCH_ROUTE, DFS_METADATA_SNAPSHOT_INSTALL_ROUTE, DFS_RPC_CATEGORY,
    DFS_SMOKE_ROUTE, DFS_STATUS_ROUTE,
};
#[cfg(not(windows))]
use crate::{sifs_mount_config_definition, sifs_mount_status_config_definition};
use sinomo_core::{
    CapabilityName, FileSystemAccess, ModuleCapabilities, ModuleResourceLimits, NetworkAccess,
    TrustRing,
};
use std::path::{Path, PathBuf};

pub const DFS_PROTOCOL: &str = "sinomo.dfs.v7";
pub use sinomo_api::dfs::module_surface::SINOMO_DFS_MODULE_LEGACY_ID;
pub use sinomo_api::dfs::module_surface::SINOMO_DFS_MODULE_OWNER;
pub const SINOMO_DFS_NATIVE_REGISTRATION_KEY: &str = "sinomo-dfs";
pub const SIFS_PATH_ACCESS_CAPABILITY: &str = "sifs.path.access";
pub const DFS_POLICY_VIOLATION_EVENT_KIND: &str = "dfs.policy.violation";

pub struct DfsMigrationQuarantinePlan {
    pub moves: Vec<(PathBuf, PathBuf)>,
    pub import_namespace: String,
}

pub fn dfs_migration_quarantine_plan(
    data_dir: &sinomo_core::local::SinomoDataDir,
    source_cluster_id: &sinomo_core::node::ClusterId,
) -> DfsMigrationQuarantinePlan {
    DfsMigrationQuarantinePlan {
        moves: vec![(
            crate::default_dfs_metadata_shards_root(data_dir),
            crate::dfs_import_metadata_root(data_dir, source_cluster_id),
        )],
        import_namespace: crate::dfs_import_namespace_root(source_cluster_id),
    }
}

pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::StorageRing;

    layout.register_binary(
        "sinomo-dfs",
        StorageRing::R1,
        "dfs/.dfs-content-initialization-{root_identity}.lock",
    )?;

    for (relative_path, reason) in [
        ("dfs/content/blobs", "content-addressed DFS blob bytes"),
        (
            "dfs/content/inline-bindings",
            "checksummed small-content bindings, sealed generations, activation and compaction progress",
        ),
        (
            "dfs/content/content-owner.lock",
            "inline content mutation and migration owner fencing",
        ),
        (
            "dfs/content/catalog-staging-owner.lock",
            "DFS content catalog staging ownership",
        ),
        (
            "dfs/content/objects",
            "dynamic DFS object manifests stored as sinomo-object files",
        ),
        (
            "dfs/content/catalog",
            "durable authoritative DFS content-root catalogs",
        ),
        (
            "dfs/content/content-reservation-reconciliation-cursor.sobj",
            "durable DFS content reservation reconciliation cursor",
        ),
        (
            "dfs/content/manifest-absence-observations",
            "durable first-absence observations for manifest collection",
        ),
        (
            "dfs/content/orphan-observations",
            "durable first-absence observations for orphan blob collection",
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
            "dfs/mounted-mutation-outbox",
            "crash-safe host-mount mutation ambiguity markers, leases, and daemon handoff receipts",
        ),
        (
            "dfs/metadata",
            "DFS shard journals, mixed-format metadata snapshots, and durable storage migration jobs",
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
    layout.register_binary(
        "sinomo-dfs",
        StorageRing::R1,
        "dfs/content/file-readers-v2.lock",
    )?;
    layout.register_binary(
        "sinomo-dfs",
        StorageRing::R1,
        "dfs/content/content-reservations/write.lock",
    )?;
    layout.register_sinomo_object(
        "sinomo-dfs",
        StorageRing::R1,
        "dfs/content/content-reservations/00/batch-format-v1.sobj",
        sinomo_core::storage::ObjectSchema::new("ContentStoreReservationBatchSentinel", 1)
            .field("format_version", "u32")
            .field("marker", "String")
            .field("store_incarnation", "String")
            .field("catalog_scan_epoch", "u64")
            .field("content_binding_layout", "String"),
    )?;
    layout.register_binary("sinomo-dfs", StorageRing::R1, "dfs/host-mount.lock")
}

pub fn dfs_module_id() -> sinomo_module::ModuleId {
    sinomo_module::ModuleId::from_legacy_named(SINOMO_DFS_MODULE_LEGACY_ID)
        .expect("valid DFS module id")
}

/// Whether the built-in DFS module should be materialized in this node's
/// runtime. DFS remains enabled by default for compatibility; an explicit
/// native-Rust module state can disable it on an individual node.
pub fn dfs_module_enabled(data_dir_root: impl AsRef<Path>) -> Result<bool, crate::DfsError> {
    sinomo_module::native_rust_module_enabled(data_dir_root.as_ref(), &dfs_module_id(), true)
        .map_err(|source| crate::DfsError::Metadata {
            message: format!("cannot read DFS module runtime state: {source}"),
        })
}

pub fn set_dfs_module_enabled(
    data_dir_root: impl AsRef<Path>,
    enabled: bool,
    source: impl Into<String>,
    reason: impl Into<String>,
) -> Result<PathBuf, crate::DfsError> {
    sinomo_module::set_native_rust_module_enabled(
        data_dir_root.as_ref(),
        &dfs_module_id(),
        enabled,
        source,
        reason,
    )
    .map_err(|source| crate::DfsError::Metadata {
        message: format!("cannot write DFS module runtime state: {source}"),
    })
}

pub fn dfs_module_surfaces() -> sinomo_module::ModuleSurfaces {
    use sinomo_module::{ModuleObjectRpcRoute, ModuleRouteAuth, ModuleSurfaces};

    let mut surfaces = ModuleSurfaces::default()
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            sinomo_api::dfs::DFS_REPAIR_RETRY_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.repair.retry",
            ModuleRouteAuth::Admin,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            crate::DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            crate::DFS_MOUNTED_MUTATION_RPC_CATEGORY,
            "dfs.mounted-mutation.handoff",
            ModuleRouteAuth::LocalCommand,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_STATUS_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.status.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                crate::DFS_CONTENT_ADVERTISEMENT_LIVE_ROUTE,
                DFS_RPC_CATEGORY,
                "dfs.status.read",
                ModuleRouteAuth::LocalOrClusterMember,
            ).require_exact_member_identity(),
        )
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            DFS_INSPECTION_ROUTE,
            DFS_RPC_CATEGORY,
            "dfs.inspection.read",
            ModuleRouteAuth::LocalOrClusterMember,
        ))
        .with_object_rpc_route(ModuleObjectRpcRoute::new(
            crate::DFS_CONFIGURATION_SNAPSHOT_ROUTE,
            DFS_RPC_CATEGORY,
            crate::DFS_CONFIGURATION_READ_CAPABILITY,
            ModuleRouteAuth::LocalCommand,
        ))
        .with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                crate::source_adoption::SOURCE_ADOPTION_ROUTE,
                DFS_RPC_CATEGORY,
                "dfs.metadata.source-adoption",
                ModuleRouteAuth::LocalOrClusterMember,
            )
            .require_exact_member_identity(),
        )
        .with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                DFS_METADATA_JOURNAL_BATCH_ROUTE,
                DFS_RPC_CATEGORY,
                "dfs.metadata.replicate",
                ModuleRouteAuth::LocalOrClusterMember,
            )
            .require_exact_member_identity(),
        )
        .with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                DFS_METADATA_SNAPSHOT_INSTALL_ROUTE,
                DFS_RPC_CATEGORY,
                "dfs.metadata.repair",
                ModuleRouteAuth::LocalOrClusterMember,
            )
            .require_exact_member_identity(),
        )
        .with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                crate::DFS_METADATA_TAKEOVER_CORROBORATION_ROUTE,
                DFS_RPC_CATEGORY,
                "dfs.metadata.replicate",
                ModuleRouteAuth::LocalOrClusterMember,
            )
            .require_exact_member_identity(),
        )
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
        surfaces = surfaces.with_object_rpc_route(
            ModuleObjectRpcRoute::new(
                route,
                DFS_RPC_CATEGORY,
                "dfs.object.transfer",
                ModuleRouteAuth::LocalOrClusterMember,
            )
            .require_exact_member_identity(),
        );
    }
    // Unix hosts mount SIFS through the built-in FUSE adapter, so the DFS
    // registration owns the shared mount settings there. Windows delegates
    // the adapter through the generic host-filesystem provider interface; the
    // active external component is the sole module-registry owner of these
    // keys. Registering both owners is rejected fail-closed.
    #[cfg(not(windows))]
    {
        surfaces = surfaces
            .with_config_definition(sifs_mount_config_definition())
            .expect("valid SIFS mount settings surface")
            .with_config_definition(sifs_mount_status_config_definition())
            .expect("valid SIFS mount status surface");
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
    )
    .with_display_name("Distributed Filesystem");
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
            dfs_capability("dfs.mounted-mutation.handoff"),
            dfs_capability("dfs.status.read"),
            dfs_capability("dfs.inspection.read"),
            dfs_capability(crate::DFS_CONFIGURATION_READ_CAPABILITY),
            dfs_capability("dfs.repair.retry"),
            dfs_capability("dfs.metadata.replicate"),
            dfs_capability("dfs.metadata.repair"),
            dfs_capability("dfs.metadata.source-adoption"),
            dfs_capability("dfs.smoke.run"),
            dfs_capability("dfs.object.transfer"),
            dfs_capability("sifs.path.access"),
            dfs_capability(crate::DFS_CORE_COMPONENT_ACCESS_OPERATION),
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
    use sinomo_core::storage::{StorageLayout, StoragePathOwner};
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
        for route in &registration.surfaces.object_rpc_routes {
            let expected_exact = route.route == crate::DFS_CONTENT_ADVERTISEMENT_LIVE_ROUTE
                || route.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                || route.route == crate::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE
                || route.route == crate::source_adoption::SOURCE_ADOPTION_ROUTE
                || route.route == crate::DFS_METADATA_TAKEOVER_CORROBORATION_ROUTE
                || crate::dfs_route_is_message_data_plane(&route.route);
            assert_eq!(
                route.exact_member_identity, expected_exact,
                "exact-member policy drift for {}",
                route.route
            );
        }
        assert_eq!(
            registration.surfaces.object_rpc_routes.len(),
            DFS_OBJECT_RPC_ROUTES.len() + 5
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

        let settings = registration
            .surfaces
            .config_definitions()
            .expect("decode DFS settings surfaces");
        #[cfg(not(windows))]
        assert_eq!(
            settings
                .iter()
                .map(|definition| definition.key.as_str())
                .collect::<Vec<_>>(),
            vec![
                crate::SIFS_MOUNT_CONFIG_KEY,
                crate::SIFS_MOUNT_STATUS_CONFIG_KEY,
            ],
            "the built-in DFS module owns mount settings on Unix"
        );
        #[cfg(windows)]
        assert_eq!(
            settings
                .iter()
                .map(|definition| definition.key.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new(),
            "the external Windows component owns mount settings"
        );
    }

    #[test]
    fn dfs_storage_contract_covers_dynamic_metadata_shards() {
        let mut layout = StorageLayout::new();
        register_storage_paths(&mut layout).expect("register DFS storage paths");

        for path in [
            "dfs/content/file-readers-v2.lock",
            "dfs/content/catalog-staging-owner.lock",
            "dfs/content/content-reservations/write.lock",
            "dfs/content/content-reservations/00/batch-format-v1.sobj",
            "dfs/content/catalog/metadata-content-roots.sobj",
            "dfs/content/content-reservation-reconciliation-cursor.sobj",
            "dfs/content/manifest-absence-observations/ab/abcdef.sobj",
            "dfs/content/orphan-observations/ab/abcdef.sobj",
            "dfs/content/stats.sobj",
            "dfs/host-mount.lock",
            "dfs/mounted-mutation-outbox/daemon-committed-work-latch.sobj",
            "dfs/mounted-mutation-outbox/operations/01hxy6qa9k0t9vr4g9n8d3q5bf/state.sobj",
            "dfs/mounted-mutation-outbox/sessions/01hxy6qa9k0t9vr4g9n8d3q5bf/lease.lock",
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
        assert!(matches!(
            layout.owner_for("dfs/host-mount.lock"),
            Some(StoragePathOwner::File(contract))
                if contract.module == SINOMO_DFS_MODULE_OWNER
        ));
    }
}
