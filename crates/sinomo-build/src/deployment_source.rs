//! Prepare self-contained deployment source from committed repositories. Local
//! debug overrides remain untouched in the operator's working tree.
use crate::{BuildError, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Component, Path, PathBuf},
};
use toml_edit::{DocumentMut, Item, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeploymentDependencyPin {
    pub name: String,
    pub repository: PathBuf,
    pub revision: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeploymentSourcePins {
    pub schema_version: u32,
    pub dependencies: Vec<DeploymentDependencyPin>,
}

fn invalid(detail: impl Into<String>) -> BuildError {
    BuildError::InvalidValue {
        detail: detail.into(),
    }
}
fn io(path: &Path, error: std::io::Error) -> BuildError {
    BuildError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let output = crate::process::noninteractive_command("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|error| io(repo, error))?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!("git {}", args.join(" ")),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn manifests(repo: &Path) -> Result<Vec<PathBuf>> {
    Ok(git(
        repo,
        &[
            "ls-files",
            "-z",
            "--",
            "Cargo.toml",
            "**/Cargo.toml",
            ".cargo/config",
            ".cargo/config.toml",
        ],
    )?
    .split('\0')
    .filter(|path| !path.is_empty())
    .map(PathBuf::from)
    .collect())
}

fn dependency_base(repo: &Path, relative: &Path) -> PathBuf {
    let parent = relative.parent().unwrap_or(Path::new(""));
    if parent.file_name().is_some_and(|name| name == ".cargo") {
        repo.join(parent.parent().unwrap_or(Path::new("")))
    } else {
        repo.join(parent)
    }
}

fn normalize(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            component => result.push(component.as_os_str()),
        }
    }
    result
}

fn document(path: &Path) -> Result<DocumentMut> {
    fs::read_to_string(path)
        .map_err(|error| io(path, error))?
        .parse()
        .map_err(|error| invalid(format!("{}: {error}", path.display())))
}

// Visit path fields only inside dependency/patch declarations, including
// platform tables and inline declarations. Package metadata is unrelated.
fn dependency_paths(
    item: &mut Item,
    context: bool,
    patch: bool,
    visit: &mut impl FnMut(&mut Value) -> Result<()>,
) -> Result<()> {
    if let Some(table) = item.as_table_like_mut() {
        for (key, child) in table.iter_mut() {
            let key = key.get();
            if key == "paths" && !context {
                if let Some(paths) = child.as_array_mut() {
                    for path in paths.iter_mut() {
                        visit(path)?;
                    }
                }
            } else if context && key == "path" {
                if let Some(value) = child.as_value_mut() {
                    visit(value)?;
                }
            } else if key != "metadata" || context {
                dependency_paths(
                    child,
                    context
                        || patch
                        || matches!(
                            key,
                            "dependencies" | "dev-dependencies" | "build-dependencies"
                        ),
                    key == "patch",
                    visit,
                )?;
            }
        }
    }
    Ok(())
}

/// Called before a committed source bundle enters the build service. A fixed
/// root commit cannot make an external path dependency reproducible.
pub fn validate_deployment_source(repo: &Path) -> Result<()> {
    let root = dunce::canonicalize(repo).map_err(|error| io(repo, error))?;
    for relative in manifests(&root)? {
        let path = root.join(&relative);
        let mut doc = document(&path)?;
        dependency_paths(doc.as_item_mut(), false, false, &mut |value| {
            if let Some(dependency) = value.as_str() {
                let resolved = normalize(&dependency_base(&root, &relative).join(dependency));
                if !resolved.starts_with(&root) {
                    return Err(invalid(format!(
                        "{} has external dependency {dependency:?}; use `sinomo rollout prepare-source` with committed dependency pins",
                        relative.display()
                    )));
                }
                if resolved.exists()
                    && !dunce::canonicalize(&resolved)
                        .map_err(|error| io(&resolved, error))?
                        .starts_with(&root)
                {
                    return Err(invalid(format!(
                        "dependency {} escapes the source through a symlink",
                        resolved.display()
                    )));
                }
            }
            Ok(())
        })?;
    }
    let pin_path = root.join("rollout-source.json");
    if pin_path.is_file() {
        let pins: DeploymentSourcePins =
            serde_json::from_slice(&fs::read(&pin_path).map_err(|error| io(&pin_path, error))?)
                .map_err(|error| invalid(error.to_string()))?;
        if pins.schema_version != 1 {
            return Err(invalid("unsupported rollout source pin schema"));
        }
        if !pins.dependencies.is_empty() {
            let provenance_path = root.join("deployment-source.json");
            let provenance: serde_json::Value = serde_json::from_slice(&fs::read(&provenance_path).map_err(|_| invalid("dependency pins require a prepared deployment-source.json; run `sinomo rollout prepare-source`"))?)
                .map_err(|error| invalid(error.to_string()))?;
            let index = git(&root, &["write-tree"])?;
            for pin in &pins.dependencies {
                let entry = provenance["dependencies"]
                    .as_array()
                    .and_then(|entries| entries.iter().find(|entry| entry["name"] == pin.name))
                    .ok_or_else(|| invalid(format!("missing prepared dependency {}", pin.name)))?;
                if pin.revision.len() != 40
                    || !pin.revision.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || entry["revision"] != pin.revision
                {
                    return Err(invalid(format!(
                        "prepared dependency {} does not match its fixed commit",
                        pin.name
                    )));
                }
                let prefix = format!("vendor/{}", pin.name);
                let tree = git(&root, &["rev-parse", &format!("{index}:{prefix}")])?;
                if entry["tree"] != tree {
                    return Err(invalid(format!(
                        "prepared tree for {} differs from recorded provenance",
                        pin.name
                    )));
                }
                git(&root, &["diff", "--quiet", "--", &prefix])?;
            }
        }
    }
    Ok(())
}

/// The output must be new. Failed preparation leaves that isolated checkout
/// available for diagnosis; it never edits or cleans an operator repository.
pub fn prepare_deployment_source(
    source: &Path,
    output: &Path,
    targets: &[String],
) -> Result<String> {
    let source = dunce::canonicalize(source).map_err(|error| io(source, error))?;
    if output.exists() {
        return Err(invalid(
            "deployment output already exists; choose a new directory",
        ));
    }
    let revision = crate::build_source_head(&source)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| io(parent, error))?;
    }
    let status = crate::process::noninteractive_command("git")
        .args(["clone", "--no-hardlinks", "--no-checkout", "--quiet"])
        .arg(&source)
        .arg(output)
        .status()
        .map_err(|error| io(output, error))?;
    if !status.success() {
        return Err(invalid("could not create isolated deployment checkout"));
    }
    git(output, &["config", "core.autocrlf", "false"])?;
    git(output, &["checkout", "--detach", &revision])?;
    let pin_path = output.join("rollout-source.json");
    let pins: DeploymentSourcePins =
        serde_json::from_slice(&fs::read(&pin_path).map_err(|error| io(&pin_path, error))?)
            .map_err(|error| invalid(format!("invalid rollout-source.json: {error}")))?;
    if pins.schema_version != 1 {
        return Err(invalid("unsupported deployment source pin schema"));
    }
    let mut pinned = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    let mut provenance = Vec::new();
    for pin in &pins.dependencies {
        if pin.name.is_empty()
            || !pin
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || !names.insert(pin.name.clone())
        {
            return Err(invalid(
                "dependency names must be unique simple path components",
            ));
        }
        if pin.revision.len() != 40 || !pin.revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid(format!(
                "dependency {} requires a full fixed Git commit",
                pin.name
            )));
        }
        let dependency_repo = dunce::canonicalize(source.join(&pin.repository))
            .map_err(|error| io(&pin.repository, error))?;
        let commit = crate::resolve_build_source_commit(&dependency_repo, &pin.revision)?;
        let tree = git(
            &dependency_repo,
            &["rev-parse", &format!("{commit}^{{tree}}")],
        )?;
        git(
            output,
            &[
                "fetch",
                "--no-tags",
                dependency_repo
                    .to_str()
                    .ok_or_else(|| invalid("non-Unicode dependency repository"))?,
                &commit,
            ],
        )?;
        let prefix = format!("vendor/{}/", pin.name);
        git(
            output,
            &["read-tree", &format!("--prefix={prefix}"), "-u", &commit],
        )?;
        pinned.push((dependency_repo, prefix.clone()));
        provenance.push(serde_json::json!({"name": pin.name, "revision": commit, "tree": tree, "prefix": prefix}));
    }
    // Only source manifests are rewritten. Vendored trees retain exact bytes,
    // modes and symlinks from the dependency commit.
    for relative in manifests(&source)? {
        let path = output.join(&relative);
        let original_parent = dependency_base(&source, &relative);
        let mut doc = document(&path)?;
        dependency_paths(doc.as_item_mut(), false, false, &mut |value| {
            let Some(raw) = value.as_str() else {
                return Ok(());
            };
            let original = normalize(&original_parent.join(raw));
            if original.starts_with(&source) {
                return Ok(());
            }
            let (dependency, prefix) = pinned
                .iter()
                .find(|(dependency, _)| original.starts_with(dependency))
                .ok_or_else(|| {
                    invalid(format!(
                        "unversioned external dependency {raw:?} in {}",
                        relative.display()
                    ))
                })?;
            let depth = original_parent
                .strip_prefix(&source)
                .expect("source-relative dependency base")
                .components()
                .count();
            let suffix = original
                .strip_prefix(dependency)
                .expect("checked prefix")
                .to_string_lossy()
                .replace('\\', "/");
            *value = Value::from(format!("{}{prefix}{suffix}", "../".repeat(depth)));
            Ok(())
        })?;
        if relative == Path::new("Cargo.toml") && !pinned.is_empty() {
            let exclude = &mut doc["workspace"]["exclude"];
            if exclude.is_none() {
                *exclude = toml_edit::value(toml_edit::Array::new());
            }
            let exclude = exclude
                .as_array_mut()
                .ok_or_else(|| invalid("workspace.exclude must be an array"))?;
            for (_, prefix) in &pinned {
                exclude.push(prefix.trim_end_matches('/'));
            }
        }
        fs::write(&path, doc.to_string()).map_err(|error| io(&path, error))?;
    }
    let provenance_path = output.join("deployment-source.json");
    fs::write(
        &provenance_path,
        serde_json::to_vec_pretty(
            &serde_json::json!({"source_revision": revision, "dependencies": provenance}),
        )
        .map_err(|error| invalid(error.to_string()))?,
    )
    .map_err(|error| io(&provenance_path, error))?;
    validate_deployment_source(output)?;
    let platforms: Vec<Option<&str>> = if targets.is_empty() {
        vec![None]
    } else {
        targets.iter().map(|target| Some(target.as_str())).collect()
    };
    for platform in platforms {
        let mut command = crate::process::noninteractive_command("cargo");
        command
            .current_dir(output)
            .args(["metadata", "--locked", "--format-version", "1"]);
        if let Some(platform) = platform {
            command.args(["--filter-platform", platform]);
        }
        let result = command.output().map_err(|error| io(output, error))?;
        if !result.status.success() {
            return Err(invalid(format!(
                "prepared metadata validation failed: {}",
                String::from_utf8_lossy(&result.stderr)
            )));
        }
    }
    git(output, &["add", "--all"])?;
    let index = git(output, &["write-tree"])?;
    for dependency in &provenance {
        let prefix = dependency["prefix"]
            .as_str()
            .expect("prefix")
            .trim_end_matches('/');
        if git(output, &["rev-parse", &format!("{index}:{prefix}")])?
            != dependency["tree"].as_str().expect("tree")
        {
            return Err(invalid(format!(
                "vendored tree {prefix} differs from its pinned commit"
            )));
        }
    }
    let date = git(&source, &["show", "-s", "--format=%cI", &revision])?;
    let result = crate::process::noninteractive_command("git")
        .arg("-C")
        .arg(output)
        .args([
            "-c",
            "user.name=Sinomo rollout",
            "-c",
            "user.email=rollout@sinomo.local",
            "commit",
            "--no-verify",
            "--no-gpg-sign",
            "-m",
            &format!("Prepare deployment of {revision}"),
        ])
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date)
        .output()
        .map_err(|error| io(output, error))?;
    if !result.status.success() {
        return Err(invalid(
            String::from_utf8_lossy(&result.stderr).into_owned(),
        ));
    }
    crate::build_source_head(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(repo: &Path) -> String {
        git(repo, &["add", "--all"]).unwrap();
        git(
            repo,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--no-gpg-sign",
                "-m",
                "fixture",
            ],
        )
        .unwrap();
        crate::build_source_head(repo).unwrap()
    }

    fn fixture(root: &Path) -> (PathBuf, PathBuf, String) {
        let dep = root.join("dependency");
        let source = root.join("source");
        for dir in [&dep, &source] {
            fs::create_dir_all(dir.join("src")).unwrap();
            git(dir, &["init", "--quiet"]).unwrap();
            fs::write(dir.join("src/lib.rs"), "pub fn committed() {}\n").unwrap();
        }
        fs::write(
            dep.join("Cargo.toml"),
            "[package]\nname='fixture-dep'\nversion='0.1.0'\nedition='2024'\n",
        )
        .unwrap();
        let revision = commit(&dep);
        fs::write(source.join("Cargo.toml"), "[workspace]\nmembers=['.']\n[package]\nname='fixture-app'\nversion='0.1.0'\nedition='2024'\n[dependencies]\nfixture-dep={path='../dependency'}\n").unwrap();
        fs::write(source.join("Cargo.lock"), "version = 4\n\n[[package]]\nname = \"fixture-app\"\nversion = \"0.1.0\"\ndependencies = [\"fixture-dep\"]\n\n[[package]]\nname = \"fixture-dep\"\nversion = \"0.1.0\"\n").unwrap();
        let pins = DeploymentSourcePins {
            schema_version: 1,
            dependencies: vec![DeploymentDependencyPin {
                name: "dep".into(),
                repository: "../dependency".into(),
                revision: revision.clone(),
            }],
        };
        fs::write(
            source.join("rollout-source.json"),
            serde_json::to_vec_pretty(&pins).unwrap(),
        )
        .unwrap();
        commit(&source);
        (source, dep, revision)
    }

    #[test]
    fn deployment_preparation_excludes_dirty_dependencies_and_is_repeatable() {
        let root = sinomo_testdir::TestDir::new("deployment-committed-pins");
        let (source, dep, revision) = fixture(&root);
        assert!(
            validate_deployment_source(&source)
                .unwrap_err()
                .to_string()
                .contains("external dependency")
        );
        fs::write(dep.join("src/lib.rs"), "dirty dependency\n").unwrap();
        fs::write(source.join("src/lib.rs"), "dirty application\n").unwrap();
        let first = root.join("prepared-a");
        let second = root.join("prepared-b");
        let first_commit = prepare_deployment_source(&source, &first, &[]).unwrap();
        let second_commit = prepare_deployment_source(&source, &second, &[]).unwrap();
        assert_eq!(first_commit, second_commit);
        assert_eq!(
            fs::read_to_string(first.join("vendor/dep/src/lib.rs")).unwrap(),
            "pub fn committed() {}\n"
        );
        assert_eq!(
            fs::read_to_string(first.join("src/lib.rs")).unwrap(),
            "pub fn committed() {}\n"
        );
        assert_eq!(
            fs::read_to_string(dep.join("src/lib.rs")).unwrap(),
            "dirty dependency\n"
        );
        assert_eq!(
            git(&first, &["rev-parse", "HEAD:vendor/dep"]).unwrap(),
            git(&dep, &["rev-parse", &format!("{revision}^{{tree}}")]).unwrap()
        );
        validate_deployment_source(&first).unwrap();
        fs::write(first.join("vendor/dep/src/lib.rs"), "tampered\n").unwrap();
        assert!(validate_deployment_source(&first).is_err());
    }

    #[test]
    fn deployment_preparation_rejects_a_lockfile_from_a_different_dependency_revision() {
        let root = sinomo_testdir::TestDir::new("deployment-mismatched-lockfile");
        let (source, dependency, _) = fixture(&root);
        let lock_path = source.join("Cargo.lock");
        let stale_lock = fs::read_to_string(&lock_path).unwrap().replace(
            "name = \"fixture-dep\"\nversion = \"0.1.0\"",
            "name = \"fixture-dep\"\nversion = \"0.2.0\"",
        );
        fs::write(&lock_path, &stale_lock).unwrap();
        let source_revision = commit(&source);
        let dependency_manifest = fs::read(dependency.join("Cargo.toml")).unwrap();
        let output = root.join("prepared");
        let error =
            prepare_deployment_source(&source, &output, &["x86_64-pc-windows-msvc".to_string()])
                .expect_err("prepared dependency graph must match the committed lockfile");
        assert!(
            error
                .to_string()
                .contains("prepared metadata validation failed"),
            "{error}"
        );
        assert_eq!(fs::read_to_string(&lock_path).unwrap(), stale_lock);
        assert_eq!(
            fs::read(dependency.join("Cargo.toml")).unwrap(),
            dependency_manifest
        );
        assert_eq!(
            crate::build_source_head(&output).unwrap(),
            source_revision,
            "failed validation must not publish a prepared commit"
        );
    }

    #[test]
    fn dependency_paths_handle_workspace_target_and_patch_tables() {
        let mut doc: DocumentMut = "[workspace.dependencies]\na={path='../a'}\n[target.'cfg(unix)'.dependencies]\nb={path='../b'}\n[patch.'https://example.invalid/repo']\nc={path='../c'}\n[package.metadata]\npath='../unrelated'\n".parse().unwrap();
        let mut found = Vec::new();
        dependency_paths(doc.as_item_mut(), false, false, &mut |value| {
            found.push(value.as_str().unwrap().to_owned());
            Ok(())
        })
        .unwrap();
        assert_eq!(found, ["../a", "../b", "../c"]);
    }
}
