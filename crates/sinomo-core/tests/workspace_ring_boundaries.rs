use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Stage F of docs/plans/r0-r1-crate-boundary-refactor.md flips this to `true`,
/// at which point R1 crates must reach R0 exclusively through the `sinomo-core`
/// facade instead of depending on R0 crates directly.
const FACADE_MIGRATION_COMPLETE: bool = true;

/// Stable dylib ABI crates may depend directly on `sinomo-object` so native
/// modules do not have to link the whole R0 graph for the wire codec.
const ABI_OBJECT_EXEMPT: &[&str] = &["sinomo-module-api"];

/// The isolated UI helper is itself the consumer-side endpoint of the stable
/// module UI ABI. Keeping it on `sinomo-module-api` avoids pulling the complete
/// R1 client facade (including DFS contracts) into the disposable renderer
/// process. It remains forbidden from linking `sinomo-module` or any authority
/// implementation; the dedicated graphical-app test below enforces that cut.
const APPLICATION_ABI_EXEMPT: &[(&str, &str)] =
    &[("sinomo-ui-extension-host", "sinomo-module-api")];

#[derive(Clone, Debug)]
struct PackageManifest {
    name: String,
    ring: String,
    dependencies: Vec<String>,
    manifest_path: PathBuf,
}

fn ring_is_known(ring: &str) -> bool {
    matches!(
        ring,
        "r0" | "r0-boundary" | "r1" | "r1-boundary" | "r2" | "application"
    )
}

fn is_native_module_package(package: &PackageManifest) -> bool {
    package
        .manifest_path
        .components()
        .any(|component| component.as_os_str() == "modules")
}

#[test]
fn every_workspace_crate_declares_a_known_trust_ring() {
    let packages = read_workspace_packages();

    assert!(
        packages.contains_key("sinomo-core"),
        "workspace should expose the explicit sinomo-core R0 boundary crate"
    );
    assert!(
        packages.contains_key("sinomo-runtime"),
        "workspace should expose the explicit sinomo-runtime R1 boundary crate"
    );

    for package in packages.values() {
        assert!(
            ring_is_known(&package.ring),
            "{} must declare [package.metadata.sinomo] ring = \"r0\" | \"r0-boundary\" | \"r1\" | \"r1-boundary\" | \"r2\" | \"application\" in {} (found {:?})",
            package.name,
            package.manifest_path.display(),
            package.ring
        );
    }
}

#[test]
fn r0_crates_depend_only_on_r0_crates() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if package.ring != "r0" {
            continue;
        }
        for dependency in &package.dependencies {
            let Some(dependency_manifest) = packages.get(dependency) else {
                continue;
            };
            assert_eq!(
                dependency_manifest.ring, "r0",
                "{} (r0) must not depend on {} ({}); the R0 cluster foundation only builds on itself",
                package.name, dependency_manifest.name, dependency_manifest.ring
            );
        }
    }
}

#[test]
fn sinomo_core_is_the_only_r0_boundary_and_aggregates_only_r0() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if package.ring == "r0-boundary" {
            assert_eq!(
                package.name, "sinomo-core",
                "sinomo-core is the only crate allowed to declare ring = \"r0-boundary\""
            );
        }
    }

    let core = packages
        .get("sinomo-core")
        .expect("sinomo-core package should exist");
    assert_eq!(core.ring, "r0-boundary");
    assert!(
        !core.dependencies.is_empty(),
        "sinomo-core should explicitly aggregate the R0 crates"
    );
    for dependency in &core.dependencies {
        let dependency_manifest = packages.get(dependency).unwrap_or_else(|| {
            panic!("sinomo-core depends on unknown workspace crate {dependency}")
        });
        assert_eq!(
            dependency_manifest.ring, "r0",
            "sinomo-core must only depend on R0 crates, but depends on {} ({})",
            dependency_manifest.name, dependency_manifest.ring
        );
    }
}

#[test]
fn r1_crates_cross_the_r0_boundary_through_sinomo_core() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if package.ring != "r1" {
            continue;
        }

        for dependency in &package.dependencies {
            let Some(dependency_manifest) = packages.get(dependency) else {
                continue;
            };
            let dependency_ring = dependency_manifest.ring.as_str();

            match dependency_ring {
                "r1" | "r0-boundary" => {}
                "r0" => {
                    if dependency == "sinomo-object"
                        && ABI_OBJECT_EXEMPT.contains(&package.name.as_str())
                    {
                        continue;
                    }
                    if FACADE_MIGRATION_COMPLETE {
                        panic!(
                            "{} (r1) must depend on sinomo-core instead of directly on {} (r0)",
                            package.name, dependency_manifest.name
                        );
                    }
                }
                "r1-boundary" => {
                    // Native-dylib module packages under modules/ are leaf
                    // plugin artifacts, not part of the runtime aggregation;
                    // they may build against the runtime facade.
                    assert!(
                        is_native_module_package(package),
                        "{} (r1) must not depend on {} ({}); only native module packages under modules/ may build against the runtime facade",
                        package.name,
                        dependency_manifest.name,
                        dependency_ring
                    );
                }
                _ => panic!(
                    "{} (r1) must not depend on {} ({})",
                    package.name, dependency_manifest.name, dependency_ring
                ),
            }
        }
    }
}

#[test]
fn sinomo_runtime_is_the_only_r1_boundary_and_aggregates_core_plus_r1() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if package.ring == "r1-boundary" {
            assert_eq!(
                package.name, "sinomo-runtime",
                "sinomo-runtime is the only crate allowed to declare ring = \"r1-boundary\""
            );
        }
    }

    let runtime = packages
        .get("sinomo-runtime")
        .expect("sinomo-runtime package should exist");
    assert_eq!(runtime.ring, "r1-boundary");
    assert!(
        !runtime.dependencies.is_empty(),
        "sinomo-runtime should explicitly aggregate sinomo-core and the R1 crates"
    );
    for dependency in &runtime.dependencies {
        let dependency_manifest = packages.get(dependency).unwrap_or_else(|| {
            panic!("sinomo-runtime depends on unknown workspace crate {dependency}")
        });
        assert!(
            matches!(dependency_manifest.ring.as_str(), "r0-boundary" | "r1"),
            "sinomo-runtime must only depend on sinomo-core and R1 crates, but depends on {} ({})",
            dependency_manifest.name,
            dependency_manifest.ring
        );
    }
}

#[test]
fn r2_and_application_crates_cross_the_boundary_through_sinomo_runtime() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if !matches!(package.ring.as_str(), "r2" | "application") {
            continue;
        }

        for dependency in &package.dependencies {
            let Some(dependency_manifest) = packages.get(dependency) else {
                continue;
            };
            if APPLICATION_ABI_EXEMPT
                .contains(&(package.name.as_str(), dependency_manifest.name.as_str()))
                || (is_native_module_package(package)
                    && dependency_manifest.name == "sinomo-module-api")
            {
                continue;
            }
            assert!(
                !matches!(
                    dependency_manifest.ring.as_str(),
                    "r0" | "r0-boundary" | "r1"
                ),
                "{} ({}) must depend on sinomo-runtime instead of directly depending on {} ({})",
                package.name,
                package.ring,
                dependency_manifest.name,
                dependency_manifest.ring
            );
        }
    }
}

#[test]
fn graphical_app_and_app_owned_daemon_remain_separate_artifacts() {
    let packages = read_workspace_packages();
    assert!(
        !packages.contains_key("sinomo-app-daemon"),
        "desktop sessions must reuse the sinomo CLI binary"
    );
    let app = packages
        .get("sinomo-app")
        .expect("sinomo-app package should exist");
    let daemon_host = packages
        .get("sinomo")
        .expect("sinomo CLI package should exist");
    let ui_extension_host = packages
        .get("sinomo-ui-extension-host")
        .expect("sinomo-ui-extension-host package should exist");
    let client = packages
        .get("sinomo-client")
        .expect("sinomo-client package should exist");

    assert_eq!(app.ring, "application");
    assert_eq!(daemon_host.ring, "application");
    assert_eq!(ui_extension_host.ring, "application");
    assert_eq!(client.ring, "r2");
    assert!(
        app.dependencies.contains(&client.name),
        "sinomo-app must reach daemon wire contracts through sinomo-client"
    );
    assert_eq!(
        client.dependencies,
        vec!["sinomo-runtime".to_string()],
        "sinomo-client must reach shared API contracts and foundations through sinomo-runtime"
    );
    assert!(
        !app.dependencies.contains(&daemon_host.name),
        "sinomo-app must discover the sibling daemon executable at runtime instead of linking its package"
    );
    assert!(
        !app.dependencies.contains(&ui_extension_host.name),
        "sinomo-app must discover the isolated UI extension helper at runtime instead of linking its Wasm host"
    );
    assert!(
        ui_extension_host
            .dependencies
            .contains(&"sinomo-module-api".to_string())
            && !ui_extension_host
                .dependencies
                .contains(&"sinomo-module".to_string())
            && !ui_extension_host
                .dependencies
                .contains(&"sinomo-runtime".to_string()),
        "the reviewed UI ABI helper may depend directly on sinomo-module-api but must not link module loading, runtime, or daemon implementations"
    );
    assert!(
        daemon_host.dependencies.contains(&"sinomo-cli".to_string())
            && daemon_host
                .dependencies
                .contains(&"sinomo-runtime".to_string()),
        "the CLI daemon host owns full product composition"
    );
}

#[test]
fn shared_contracts_are_modules_and_do_not_depend_on_their_hosts() {
    let packages = read_workspace_packages();
    for retired in [
        "sinomo-app-contract",
        "sinomo-lm-contract",
        "sinomo-dfs-api",
        "sinomo-module-client",
        "sinomo-observe-contract",
    ] {
        assert!(
            !packages.contains_key(retired),
            "{retired} was consolidated into shared API modules"
        );
    }
    let api = &packages["sinomo-api"];
    for host in [
        "sinomo-daemon",
        "sinomo-dfs",
        "sinomo-module",
        "sinomo-observe",
        "sinomo-runtime",
    ] {
        assert!(
            !api.dependencies.iter().any(|dependency| dependency == host),
            "shared API contracts must not depend on host {host}"
        );
    }
    let module_api = &packages["sinomo-module-api"];
    assert!(
        !module_api.dependencies.iter().any(|dependency| matches!(
            dependency.as_str(),
            "sinomo-api" | "sinomo-core" | "sinomo-runtime" | "sinomo-observe"
        )),
        "the module ABI and exporter values must stay independent of the runtime and recorder"
    );
}

#[test]
fn desktop_client_profiles_exclude_daemon_implementations_transitively() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    // Query profiles independently: workspace feature unification and app test
    // fixtures intentionally enable the host and cannot certify a GUI build.
    for (package, features) in [
        ("sinomo-api", ""),
        ("sinomo-client", ""),
        (
            "sinomo-runtime",
            "client,device-client,module-client,observe-client,cli-client",
        ),
        ("sinomo-lm", "cli"),
        ("sinomo-app", "desktop,native-devices"),
    ] {
        for target in [
            "x86_64-pc-windows-msvc",
            "x86_64-unknown-linux-gnu",
            "aarch64-apple-darwin",
        ] {
            let mut command = std::process::Command::new(env!("CARGO"));
            command.current_dir(workspace).args([
                "tree",
                "--locked",
                "-p",
                package,
                "--edges",
                "normal,build",
                "--prefix",
                "none",
                "--target",
                target,
            ]);
            if package == "sinomo-runtime" || package == "sinomo-lm" {
                command.arg("--no-default-features");
            }
            if !features.is_empty() {
                command.args(["--features", features]);
            }
            let output = command.output().expect("inspect client dependency graph");
            assert!(
                output.status.success(),
                "{package} ({target}): {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let tree = String::from_utf8_lossy(&output.stdout);
            let packages = tree
                .lines()
                .filter_map(|line| line.split_whitespace().next())
                .collect::<std::collections::BTreeSet<_>>();
            for forbidden in [
                "sinomo-daemon",
                "sinomo-dfs",
                "sinomo-dfs-nfs",
                "sinomo-module",
                "sinomo-observe",
                "sinomo-agent",
                "sinomo-cli",
                "sinomo-product-profile",
                "sinomo-build",
                "sinomo-rollout",
                "sinomo-sandbox",
                "sinomo-vfs-sifs",
            ] {
                assert!(
                    !packages.contains(forbidden),
                    "{package} ({target}) must not link daemon implementation {forbidden}"
                );
            }
        }
    }
}

#[test]
fn network_and_kv_stay_decoupled_through_the_cluster_crate() {
    // Optional dependencies are exempt only when every feature enabling them
    // is the dev-only `test-support` feature (or they are explicitly
    // allowlisted); see `optional_dependency_is_ring_exempt`.
    let packages = read_workspace_packages();

    let network = packages
        .get("sinomo-network")
        .expect("sinomo-network package should exist");
    for forbidden in ["sinomo-kv", "sinomo-cluster"] {
        assert!(
            !network.dependencies.contains(&forbidden.to_string()),
            "sinomo-network must stay a pure networking library and not depend on {forbidden}; \
             cluster state reaches routing through the ClusterDirectory provider seam"
        );
    }

    let kv = packages
        .get("sinomo-kv")
        .expect("sinomo-kv package should exist");
    for forbidden in ["sinomo-network", "sinomo-cluster"] {
        assert!(
            !kv.dependencies.contains(&forbidden.to_string()),
            "sinomo-kv must stay transport-free and not depend on {forbidden}; \
             sinomo-cluster is the only bridge between KV and the network layer"
        );
    }
}

#[test]
fn daemon_has_no_production_dependency_on_product_components() {
    let packages = read_workspace_packages();
    let daemon = packages
        .get("sinomo-daemon")
        .expect("sinomo-daemon package should exist");

    for forbidden in [
        "sinomo-build",
        "sinomo-dfs",
        "sinomo-dfs-nfs",
        "sinomo-observe",
        "sinomo-registry",
        "sinomo-rollout",
        "sinomo-sandbox",
    ] {
        assert!(
            !daemon.dependencies.contains(&forbidden.to_string()),
            "sinomo-daemon must consume {forbidden} through the product ComponentCatalog, not a production dependency"
        );
    }
}

#[test]
fn native_dylib_modules_are_not_default_workspace_build_members() {
    let default_members = read_workspace_default_members();

    assert!(
        !default_members.is_empty(),
        "workspace should declare default-members so heavy native modules stay opt-in"
    );
    assert!(
        default_members.contains(&"crates/sinomo-base".to_string()),
        "the base contracts crate should remain in default builds"
    );
    assert!(
        default_members.contains(&"crates/sinomo-module-api".to_string()),
        "the generic native module ABI crate should remain in default builds"
    );
    assert!(
        !default_members.contains(&"crates/sinomo-sandbox-executor-host".to_string())
            && !default_members
                .contains(&"crates/sinomo-host-filesystem-provider-host".to_string()),
        "generic component clients belong to their domain crates, not parallel host packages"
    );
    assert!(
        default_members
            .iter()
            .all(|member| !member.starts_with("modules/")),
        "native-dylib modules under modules/ must be built explicitly, not by default"
    );
    assert!(
        !default_members.contains(&"crates/sinomo-benchmarks".to_string()),
        "the old benchmark crate path must not remain in default-members"
    );
}

#[test]
fn evolution_is_an_external_module_not_a_host_dependency() {
    let packages = read_workspace_packages();
    let evolution = packages
        .get("sinomo-evolution")
        .expect("evolution module package should exist");
    assert!(
        is_native_module_package(evolution),
        "sinomo-evolution must live under modules/ as an independently built artifact"
    );
    for host in ["sinomo", "sinomo-cli", "sinomo-app"] {
        let host = packages.get(host).expect("host package should exist");
        assert!(
            !host.dependencies.contains(&evolution.name),
            "{} must reach evolution through its module contract, not link the implementation",
            host.name
        );
    }
}

#[test]
fn external_component_fixture_is_not_a_core_workspace_dependency() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let workspace_manifest =
        fs::read_to_string(workspace_root.join("Cargo.toml")).expect("read workspace manifest");
    assert!(
        !workspace_manifest.contains("fixtures/external-component"),
        "the external component fixture must remain outside workspace members and default-members"
    );
    assert!(
        workspace_root
            .join("fixtures/external-component/Cargo.toml")
            .exists(),
        "the independently built external component proof fixture must exist"
    );
    for package in read_workspace_packages().values() {
        let manifest = fs::read_to_string(&package.manifest_path).expect("read package manifest");
        assert!(
            !manifest.contains("sinomo-external-component-fixture"),
            "{} must not compile against the external component fixture",
            package.name
        );
    }
}

#[test]
fn provider_specific_api_crates_and_source_tree_module_discovery_are_absent() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let packages = read_workspace_packages();
    for removed in [
        "sinomo-dfs-cloud-files-api",
        "sinomo-sandbox-wasix-api",
        "sinomo-memory-extension-api",
        "sinomo-sandbox-wasix-host",
        "sinomo-host-filesystem-provider-host",
        "sinomo-sandbox-executor-host",
    ] {
        assert!(
            !packages.contains_key(removed),
            "provider-specific boundary crate {removed} must not return to the core workspace"
        );
    }
    for provider in [
        "sinomo-dfs-cloud-files",
        "sinomo-sandbox-wasix-module",
        "sinomo-network-iroh-module",
        "sinomo-remote-management-module",
        "sinomo-benchmarks",
    ] {
        let dependents = packages
            .values()
            .filter(|package| {
                package.name != provider
                    && package
                        .dependencies
                        .iter()
                        .any(|dependency| dependency == provider)
            })
            .map(|package| package.name.as_str())
            .collect::<Vec<_>>();
        assert!(
            dependents.is_empty(),
            "external provider {provider} must remain a dependency root, found dependents {dependents:?}"
        );
    }
    let runtime = fs::read_to_string(workspace_root.join("crates/sinomo-runtime/src/lib.rs"))
        .expect("runtime facade");
    for forbidden in [
        "dfs_cloud_files_api",
        "sandbox_wasix_api",
        "memory_extension_api",
        "host_filesystem_provider_host",
        "sandbox_executor_host",
    ] {
        assert!(
            !runtime.contains(forbidden),
            "runtime facade must not expose provider-specific API `{forbidden}`"
        );
    }
    let rollout_paths =
        fs::read_to_string(workspace_root.join("crates/sinomo-rollout/src/paths.rs"))
            .expect("rollout paths");
    assert!(
        !rollout_paths.contains("discover_module_package_manifests")
            && !rollout_paths.contains("package.metadata.sinomo.native-dylib"),
        "rollout must consume signed module profiles instead of scanning source-tree manifests"
    );
    assert!(
        workspace_root
            .join("distribution/module-profile.json")
            .is_file(),
        "the default external component set must be seeded as signed profile data"
    );
}

#[test]
fn wasix_native_module_runtime_feature_is_default_off() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let manifest_text = fs::read_to_string(
        workspace_root
            .join("modules")
            .join("sandbox")
            .join("wasix")
            .join("Cargo.toml"),
    )
    .expect("read WASIX module manifest");

    assert_eq!(
        parse_feature_value(&manifest_text, "default").as_deref(),
        Some("[]"),
        "WASIX native module runtime must stay opt-in"
    );
    assert_eq!(
        parse_feature_value(&manifest_text, "wasix-runtime").as_deref(),
        Some("["),
        "WASIX native module must expose the explicit runtime feature"
    );
}

fn read_workspace_packages() -> BTreeMap<String, PackageManifest> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");

    let mut packages = BTreeMap::new();
    for manifest_path in workspace_manifest_paths(workspace_root) {
        let manifest_text = fs::read_to_string(&manifest_path).expect("read manifest");
        let name = parse_package_name(&manifest_text)
            .unwrap_or_else(|| panic!("missing package name in {}", manifest_path.display()));
        let ring = parse_sinomo_ring(&manifest_text).unwrap_or_default();
        let dependencies = parse_workspace_dependencies(&manifest_text, &name);
        packages.insert(
            name.clone(),
            PackageManifest {
                name,
                ring,
                dependencies,
                manifest_path,
            },
        );
    }

    packages
}

fn workspace_manifest_paths(workspace_root: &Path) -> Vec<PathBuf> {
    let mut manifests = Vec::new();
    for entry in fs::read_dir(workspace_root.join("crates")).expect("read crates dir") {
        let entry = entry.expect("crate dir entry");
        let manifest_path = entry.path().join("Cargo.toml");
        if manifest_path.exists() {
            manifests.push(manifest_path);
        }
    }
    let modules_dir = workspace_root.join("modules");
    if modules_dir.exists() {
        for family in fs::read_dir(&modules_dir).expect("read modules dir") {
            let family = family.expect("module family dir entry");
            if !family.path().is_dir() {
                continue;
            }
            for module in fs::read_dir(family.path()).expect("read module family dir") {
                let module = module.expect("module dir entry");
                let manifest_path = module.path().join("Cargo.toml");
                if manifest_path.exists() {
                    manifests.push(manifest_path);
                }
            }
        }
    }
    manifests.sort();
    manifests
}

fn read_workspace_default_members() -> Vec<String> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let manifest_text =
        fs::read_to_string(workspace_root.join("Cargo.toml")).expect("read workspace manifest");

    let mut in_default_members = false;
    let mut default_members = Vec::new();

    for line in manifest_text.lines() {
        let trimmed = line.trim();
        if trimmed == "default-members = [" {
            in_default_members = true;
            continue;
        }
        if in_default_members && trimmed == "]" {
            break;
        }
        if !in_default_members || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        default_members.push(trimmed.trim_end_matches(',').trim_matches('"').to_string());
    }

    default_members.sort();
    default_members.dedup();
    default_members
}

fn parse_package_name(manifest_text: &str) -> Option<String> {
    let mut in_package = false;
    for line in manifest_text.lines() {
        let trimmed = line.trim();
        if trimmed == "[package]" {
            in_package = true;
            continue;
        }
        if in_package && trimmed.starts_with('[') {
            return None;
        }
        if in_package && let Some(value) = parse_string_value(trimmed, "name") {
            return Some(value);
        }
    }
    None
}

fn parse_sinomo_ring(manifest_text: &str) -> Option<String> {
    let mut in_metadata = false;
    for line in manifest_text.lines() {
        let trimmed = line.trim();
        if trimmed == "[package.metadata.sinomo]" {
            in_metadata = true;
            continue;
        }
        if in_metadata && trimmed.starts_with('[') {
            return None;
        }
        if in_metadata && let Some(value) = parse_string_value(trimmed, "ring") {
            return Some(value);
        }
    }
    None
}

/// Production feature-gated cross-ring dependencies that predate the R0/R1
/// facade sweep and are knowingly outside the ring rules (feature-off in
/// default builds). Additions require an architecture review; prefer routing
/// new surfaces through sinomo-core/sinomo-runtime instead.
const OPTIONAL_PRODUCTION_DEPENDENCY_ALLOWLIST: &[(&str, &str)] = &[
    ("sinomo-cli", "sinomo-dfs-fuse"),
    ("sinomo-cli", "sinomo-models"),
];

fn parse_workspace_dependencies(manifest_text: &str, package_name: &str) -> Vec<String> {
    let mut in_dependencies = false;
    let mut dependencies = Vec::new();

    for line in manifest_text.lines() {
        let trimmed = line.trim();
        if trimmed == "[dependencies]" {
            in_dependencies = true;
            continue;
        }
        if in_dependencies && trimmed.starts_with('[') {
            break;
        }
        if !in_dependencies || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((name, rest)) = trimmed.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if !(name.starts_with("sinomo-") && rest.contains("path")) {
            continue;
        }
        if rest.contains("optional = true")
            && optional_dependency_is_ring_exempt(manifest_text, package_name, name)
        {
            continue;
        }
        dependencies.push(name.to_string());
    }

    dependencies.sort();
    dependencies.dedup();
    dependencies
}

/// An optional dependency escapes the ring rules only when every feature that
/// activates it is the dev-only `test-support` feature (never enabled by a
/// production build), or when it is explicitly allowlisted above. Any other
/// optional dependency is checked like a plain one: a production feature must
/// not be a ring bypass.
fn optional_dependency_is_ring_exempt(
    manifest_text: &str,
    package_name: &str,
    dependency_name: &str,
) -> bool {
    if OPTIONAL_PRODUCTION_DEPENDENCY_ALLOWLIST.contains(&(package_name, dependency_name)) {
        return true;
    }
    let enabling = features_enabling_dependency(manifest_text, dependency_name);
    !enabling.is_empty()
        && enabling
            .iter()
            .all(|feature| feature == "test-support" || feature == "cluster-test-support")
}

/// Names of `[features]` entries whose activation list references
/// `dependency_name` (as `dep:<name>`, `<name>/feature`, or bare `<name>`).
fn features_enabling_dependency(manifest_text: &str, dependency_name: &str) -> Vec<String> {
    let mut in_features = false;
    let mut features = Vec::new();
    for line in manifest_text.lines() {
        let trimmed = line.trim();
        if trimmed == "[features]" {
            in_features = true;
            continue;
        }
        if in_features && trimmed.starts_with('[') {
            break;
        }
        if !in_features || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((feature_name, activation)) = trimmed.split_once('=') else {
            continue;
        };
        let activates_dependency =
            activation
                .split(['[', ']', ',', '"'])
                .map(str::trim)
                .any(|entry| {
                    entry == format!("dep:{dependency_name}")
                        || entry == dependency_name
                        || entry
                            .strip_prefix(dependency_name)
                            .is_some_and(|rest| rest.starts_with('/'))
                });
        if activates_dependency {
            features.push(feature_name.trim().to_string());
        }
    }
    features
}

fn parse_string_value(line: &str, key: &str) -> Option<String> {
    let (actual_key, raw_value) = line.split_once('=')?;
    if actual_key.trim() != key {
        return None;
    }
    let value = raw_value.trim().trim_matches('"');
    Some(value.to_string())
}

fn parse_feature_value(manifest_text: &str, key: &str) -> Option<String> {
    let mut in_features = false;
    for line in manifest_text.lines() {
        let trimmed = line.trim();
        if trimmed == "[features]" {
            in_features = true;
            continue;
        }
        if in_features && trimmed.starts_with('[') {
            return None;
        }
        if in_features
            && let Some((actual_key, raw_value)) = trimmed.split_once('=')
            && actual_key.trim() == key
        {
            return Some(raw_value.trim().to_string());
        }
    }
    None
}
