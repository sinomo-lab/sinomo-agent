use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sinomo_module::{ActiveNativeComponent, ActiveNativeComponentResponse};
use sinomo_module_api::ServiceRequirement;

use sinomo_api::app::evolution::{
    EVOLUTION_CLI_OPERATION, EVOLUTION_INTERFACE_ID, EVOLUTION_INTERFACE_VERSION,
};

#[derive(Clone, Debug, Serialize)]
struct EvolutionCliRequest {
    args: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct EvolutionCliResponse {
    stdout: String,
}

#[test]
fn independently_built_evolution_dylib_serves_its_cli_contract() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let manifest = workspace.join("modules/evolution/sinomo-evolution/Cargo.toml");
    let target = workspace.join("target");
    let status = Command::new("cargo")
        .args(["build", "--manifest-path"])
        .arg(&manifest)
        .status()
        .expect("build evolution module");
    assert!(status.success(), "evolution dylib build failed");

    let requirement = ServiceRequirement::new(EVOLUTION_INTERFACE_ID, EVOLUTION_INTERFACE_VERSION)
        .from_service("evolution")
        .with_operation(EVOLUTION_CLI_OPERATION)
        .with_caller_capability(EVOLUTION_CLI_OPERATION);
    let component =
        unsafe { ActiveNativeComponent::load_verified(library_path(&target), &requirement) }
            .expect("load evolution component");
    let descriptor = component.descriptor();
    assert_eq!(descriptor.module_id, "module.evolution");
    assert_eq!(descriptor.surfaces.settings.len(), 1);
    let evolution_agent = descriptor
        .surfaces
        .agent_definitions
        .iter()
        .find(|agent| agent.id == "evolution-agent")
        .expect("evolution agent export");
    assert_eq!(
        evolution_agent.skills,
        [
            "evolution-orchestration",
            "evolution-planning",
            "evolution-review"
        ]
    );
    assert!(
        evolution_agent
            .visible_toolsets
            .contains(&"sinomo-evolution".to_string())
    );
    assert!(!evolution_agent.tools.is_empty());
    assert!(
        descriptor
            .surfaces
            .declarative_resources
            .iter()
            .all(|resource| resource.kind != "skill" || resource.target_agents.is_empty())
    );
    let cli_route = descriptor
        .surfaces
        .object_rpc_routes
        .iter()
        .find(|route| route.operation == EVOLUTION_CLI_OPERATION)
        .expect("evolution CLI route");
    assert_eq!(cli_route.concurrency_limit, Some(1));
    assert_eq!(cli_route.timeout_ms, Some(60 * 60 * 1_000));
    let response = component
        .invoke_sobj::<_, EvolutionCliResponse>(
            EVOLUTION_CLI_OPERATION,
            &EvolutionCliRequest {
                args: vec!["help".to_string()],
            },
            None,
        )
        .expect("invoke evolution CLI contract");
    let ActiveNativeComponentResponse::Value(response) = response else {
        panic!("unexpected evolution response: {response:?}");
    };
    assert!(response.stdout.contains("sinomo evolve propose"));
    assert!(component.unload().shutdown_ok);
}

fn library_path(target: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    let filename = "sinomo_evolution.dll";
    #[cfg(target_os = "macos")]
    let filename = "libsinomo_evolution.dylib";
    #[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
    let filename = "libsinomo_evolution.so";
    target.join("debug").join(filename)
}
