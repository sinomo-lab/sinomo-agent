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

fn read_workspace_packages() -> BTreeMap<String, PackageManifest> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let crates_dir = workspace_root.join("crates");

    let mut packages = BTreeMap::new();
    for entry in fs::read_dir(&crates_dir).expect("read crates dir") {
        let entry = entry.expect("crate dir entry");
        let manifest_path = entry.path().join("Cargo.toml");
        if !manifest_path.exists() {
            continue;
        }
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
