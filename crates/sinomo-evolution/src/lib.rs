//! Autonomous self-evolution planning skeleton.

pub mod cli;
mod cli_runtime;
mod engine;
pub mod module_evidence;
pub mod persistence;
mod test_cases;
pub mod tools;
mod types;

pub use cli::*;
pub use cli_runtime::*;
pub use engine::*;
pub use module_evidence::*;
pub use persistence::*;
pub use test_cases::register_evolution_test_cases;
pub use tools::*;
pub use types::*;

pub type EvolutionJobId = sinomo_runtime::core::Id;
pub type TaskId = sinomo_runtime::core::Id;
pub type WorkspaceId = sinomo_runtime::core::Id;

pub const EVOLUTION_PROTOCOL: &str = "sinomo.evolution.v1";
pub const SINOMO_EVOLUTION_MODULE_LEGACY_ID: &str = "module.evolution";
pub const SINOMO_EVOLUTION_MODULE_OWNER: &str = "sinomo-evolution";
pub const SINOMO_EVOLUTION_NATIVE_REGISTRATION_KEY: &str = "sinomo-evolution";

pub const EVOLUTION_JOB_SCOPED_EVENT_KIND: &str = "evolution.job.scoped";
pub const EVOLUTION_TOOL_EXECUTED_EVENT_KIND: &str = "evolution.tool.executed";
pub const EVOLUTION_METRIC_REGRESSION_EVENT_KIND: &str = "evolution.metric.regression";
pub const EVOLUTION_REPLAY_FAILURE_EVENT_KIND: &str = "evolution.replay.failure";
pub const EVOLUTION_SCORECARD_STALE_EVENT_KIND: &str = "evolution.scorecard.stale";

pub const EVOLUTION_NATIVE_RUST_MODULE_BINDING: sinomo_runtime::module::NativeRustModuleBinding =
    sinomo_runtime::module::NativeRustModuleBinding::new(SINOMO_EVOLUTION_NATIVE_REGISTRATION_KEY)
        .with_cli_command(register_evolution_cli_command)
        .with_config_definitions(register_evolution_config_definitions)
        .with_storage_paths(register_storage_paths);

pub fn evolution_module_id() -> sinomo_runtime::module::ModuleId {
    sinomo_runtime::module::ModuleId::from_legacy_named(SINOMO_EVOLUTION_MODULE_LEGACY_ID)
        .expect("valid evolution module id")
}

pub fn evolution_module_registration() -> sinomo_runtime::module::ModuleRegistration {
    let mut registration = sinomo_runtime::module::ModuleRegistration::native_rust(
        evolution_module_id(),
        env!("CARGO_PKG_VERSION"),
        SINOMO_EVOLUTION_MODULE_OWNER,
        env!("CARGO_PKG_NAME"),
        EVOLUTION_PROTOCOL,
        sinomo_runtime::core::TrustRing::R2TrustedService,
        SINOMO_EVOLUTION_NATIVE_REGISTRATION_KEY,
    );
    registration.manifest.capabilities = evolution_module_capabilities();
    registration.manifest.limits = sinomo_runtime::core::ModuleResourceLimits {
        cpu_ms: 300_000,
        memory_mb: 2048,
        wall_time_ms: 600_000,
        network: sinomo_runtime::core::NetworkAccess::Allowlisted,
        filesystem: sinomo_runtime::core::FileSystemAccess::Scoped,
    };
    registration.surfaces = evolution_module_surfaces();
    registration
}

pub fn evolution_module_surfaces() -> sinomo_runtime::module::ModuleSurfaces {
    let mut surfaces = sinomo_runtime::module::ModuleSurfaces::default()
        .with_event_kind_name(EVOLUTION_JOB_SCOPED_EVENT_KIND)
        .with_event_kind_name(EVOLUTION_TOOL_EXECUTED_EVENT_KIND)
        .with_event_kind_name(EVOLUTION_METRIC_REGRESSION_EVENT_KIND)
        .with_event_kind_name(EVOLUTION_REPLAY_FAILURE_EVENT_KIND)
        .with_event_kind_name(EVOLUTION_SCORECARD_STALE_EVENT_KIND)
        .with_storage_owner(SINOMO_EVOLUTION_MODULE_OWNER);
    surfaces.capabilities = evolution_module_capabilities();
    surfaces
}

pub fn evolution_module_capabilities() -> sinomo_runtime::core::ModuleCapabilities {
    sinomo_runtime::core::ModuleCapabilities {
        imports: vec![
            evolution_capability("storage.local.read"),
            evolution_capability("storage.local.write"),
            evolution_capability("cluster.kv.read"),
            evolution_capability("cluster.kv.write"),
            evolution_capability("lm.invoke"),
            evolution_capability("agent.coding.run"),
            evolution_capability("sandbox.command.run"),
            evolution_capability("rollout.status.read"),
        ],
        exports: vec![
            evolution_capability("evolution.cli"),
            evolution_capability("evolution.job.propose"),
            evolution_capability("evolution.job.evaluate"),
            evolution_capability("evolution.job.approve"),
            evolution_capability("evolution.campaign.schedule"),
            evolution_capability("evolution.trusted-signers.sync"),
        ],
    }
}

fn evolution_capability(name: &str) -> sinomo_runtime::core::CapabilityName {
    sinomo_runtime::core::CapabilityName::new(name).expect("valid evolution module capability")
}

fn register_evolution_cli_command(
    registry: sinomo_runtime::cli_registry::CliCommandRegistry,
) -> sinomo_runtime::cli_registry::CliCommandRegistry {
    registry.register(evolve_cli_command_handler())
}

fn register_evolution_config_definitions(
    builder: sinomo_runtime::config::ConfigRegistryBuilder,
) -> Result<sinomo_runtime::config::ConfigRegistryBuilder, String> {
    persistence::register_config_definitions(builder).map_err(|error| error.to_string())
}

pub fn register_storage_paths(
    layout: &mut sinomo_runtime::storage::StorageLayout,
) -> Result<(), sinomo_runtime::storage::StorageContractError> {
    layout.register_escape_path(
        "sinomo-evolution",
        sinomo_runtime::storage::StorageRing::R2,
        "evolution",
        "self-evolution jobs, campaigns, logs, and candidate workspaces",
    )
}

#[cfg(test)]
mod module_declaration_tests {
    use super::*;

    #[test]
    fn evolution_module_registration_declares_catalog_surfaces() {
        let registration = evolution_module_registration();

        assert_eq!(registration.module_id(), &evolution_module_id());
        assert_eq!(
            registration.manifest.target_ring,
            sinomo_runtime::core::TrustRing::R2TrustedService
        );
        assert!(
            registration
                .surfaces
                .storage_owners
                .contains(&SINOMO_EVOLUTION_MODULE_OWNER.to_string())
        );
        assert!(
            registration
                .surfaces
                .event_kinds
                .iter()
                .any(|kind| kind.as_str() == EVOLUTION_JOB_SCOPED_EVENT_KIND)
        );
        assert!(
            registration
                .surfaces
                .capabilities
                .exports
                .iter()
                .any(|capability| capability.as_str() == "evolution.cli")
        );
    }

    #[test]
    fn evolution_native_binding_matches_registration_key() {
        let registration = evolution_module_registration();
        let sinomo_runtime::module::ModuleBackendRegistration::NativeRust { registration } =
            registration.backend
        else {
            panic!("expected native rust backend");
        };

        assert_eq!(
            registration.registration_key,
            EVOLUTION_NATIVE_RUST_MODULE_BINDING.registration_key
        );
        assert!(
            EVOLUTION_NATIVE_RUST_MODULE_BINDING
                .register_cli_command
                .is_some()
        );
        assert!(
            EVOLUTION_NATIVE_RUST_MODULE_BINDING
                .register_config_definitions
                .is_some()
        );
        assert!(
            EVOLUTION_NATIVE_RUST_MODULE_BINDING
                .register_storage_paths
                .is_some()
        );
    }
}
