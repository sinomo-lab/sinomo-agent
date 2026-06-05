use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
struct PackageManifest {
    name: String,
    ring: String,
    dependencies: Vec<String>,
    manifest_path: PathBuf,
}

#[test]
fn every_workspace_crate_declares_a_trust_ring() {
    let packages = read_workspace_packages();

    assert!(
        packages.contains_key("sinomo-runtime"),
        "workspace should expose an explicit sinomo-runtime R0/R1 boundary crate"
    );

    for package in packages.values() {
        assert!(
            !package.ring.is_empty(),
            "{} must declare [package.metadata.sinomo] ring = \"...\" in {}",
            package.name,
            package.manifest_path.display()
        );
    }
}

#[test]
fn r0_r1_crates_do_not_depend_on_r2_or_application_crates() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if !matches!(package.ring.as_str(), "r0" | "r1" | "r0-r1-boundary") {
            continue;
        }

        for dependency in &package.dependencies {
            let Some(dependency_manifest) = packages.get(dependency) else {
                continue;
            };
            assert!(
                matches!(
                    dependency_manifest.ring.as_str(),
                    "r0" | "r1" | "r0-r1-boundary"
                ),
                "{} ({}) must not depend on {} ({})",
                package.name,
                package.ring,
                dependency_manifest.name,
                dependency_manifest.ring
            );
        }
    }
}

#[test]
fn r2_and_application_crates_cross_runtime_boundary_through_runtime_crate() {
    let packages = read_workspace_packages();

    for package in packages.values() {
        if !matches!(package.ring.as_str(), "r2" | "application") {
            continue;
        }

        for dependency in &package.dependencies {
            let Some(dependency_manifest) = packages.get(dependency) else {
                continue;
            };
            assert!(
                !matches!(dependency_manifest.ring.as_str(), "r0" | "r1"),
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
fn runtime_boundary_crate_exports_only_r0_r1_dependencies() {
    let packages = read_workspace_packages();
    let runtime = packages
        .get("sinomo-runtime")
        .expect("sinomo-runtime package should exist");

    assert_eq!(runtime.ring, "r0-r1-boundary");
    assert!(
        !runtime.dependencies.is_empty(),
        "sinomo-runtime should explicitly aggregate the R0/R1 crates"
    );

    for dependency in &runtime.dependencies {
        let dependency_manifest = packages.get(dependency).unwrap_or_else(|| {
            panic!("sinomo-runtime depends on unknown workspace crate {dependency}")
        });
        assert!(
            matches!(dependency_manifest.ring.as_str(), "r0" | "r1"),
            "sinomo-runtime must only depend on R0/R1 crates, but depends on {} ({})",
            dependency_manifest.name,
            dependency_manifest.ring
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
        default_members.contains(&"crates/sinomo-sandbox-wasix-api".to_string()),
        "the stable WASIX ABI crate should remain in default builds"
    );
    assert!(
        default_members.contains(&"crates/sinomo-module-api".to_string()),
        "the generic native module ABI crate should remain in default builds"
    );
    assert!(
        default_members.contains(&"crates/sinomo-sandbox-wasix-host".to_string()),
        "the lightweight WASIX host loader should remain in default builds"
    );
    assert!(
        !default_members.contains(&"modules/sandbox/wasix".to_string()),
        "the heavy WASIX native-dylib module must be built explicitly, not by default"
    );
    assert!(
        !default_members.contains(&"modules/benchmark/sinomo-benchmarks".to_string()),
        "the benchmark native-dylib module must be built explicitly, not by default"
    );
    assert!(
        !default_members.contains(&"crates/sinomo-benchmarks".to_string()),
        "the old benchmark crate path must not remain in default-members"
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
        let dependencies = parse_workspace_dependencies(&manifest_text);
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

fn parse_workspace_dependencies(manifest_text: &str) -> Vec<String> {
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
        if name.starts_with("sinomo-") && rest.contains("path") {
            dependencies.push(name.to_string());
        }
    }

    dependencies.sort();
    dependencies.dedup();
    dependencies
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
        if in_features && let Some((actual_key, raw_value)) = trimmed.split_once('=') {
            if actual_key.trim() == key {
                return Some(raw_value.trim().to_string());
            }
        }
    }
    None
}
