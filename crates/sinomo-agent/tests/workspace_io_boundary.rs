//! Workspace content must reach the host only through the VFS.
//!
//! Clippy enforces this with `disallowed-methods` (see `clippy.toml`). This
//! test enforces the same boundary in ordinary `cargo test` runs: the
//! workspace-facing modules keep their deny attribute and contain no direct
//! filesystem calls outside their test modules. `host_probe.rs` is the one
//! documented exception.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::fs;
use std::path::{Path, PathBuf};

const DENY_ATTRIBUTE: &str = "#![deny(clippy::disallowed_methods, clippy::disallowed_types)]";

/// Modules that declare the deny attribute themselves; their submodules
/// inherit it.
const GUARDED_ROOTS: &[&str] = &[
    "src/tools/local_workspace/mod.rs",
    "src/tools/workspace_files.rs",
    "src/tools/hosted_model.rs",
    "src/agent/context.rs",
];

const ALLOWED_EXCEPTIONS: &[&str] = &["src/tools/local_workspace/host_probe.rs"];

const FORBIDDEN: &[&str] = &[
    "std::fs",
    "fs::",
    "File::",
    "OpenOptions",
    "DirBuilder",
    ".exists()",
    ".try_exists(",
    ".is_file()",
    ".is_dir()",
    ".is_symlink()",
    ".canonicalize(",
    ".read_dir(",
    ".read_link(",
    ".symlink_metadata(",
];

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn relative(path: &Path) -> String {
    path.strip_prefix(crate_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn guarded_files() -> Vec<PathBuf> {
    let root = crate_root();
    let mut files = GUARDED_ROOTS
        .iter()
        .map(|path| root.join(path))
        .collect::<Vec<_>>();
    let local_workspace = root.join("src/tools/local_workspace");
    let mut submodules = fs::read_dir(&local_workspace)
        .expect("list local_workspace")
        .map(|entry| entry.expect("local_workspace entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .filter(|path| path.file_name().is_some_and(|name| name != "mod.rs"))
        .filter(|path| !ALLOWED_EXCEPTIONS.contains(&relative(path).as_str()))
        .collect::<Vec<_>>();
    submodules.sort();
    files.extend(submodules);
    files
}

/// Source up to the inline test module, which may build fixtures directly.
fn production_source(source: &str) -> &str {
    let mut end = source.len();
    for marker in ["#[cfg(test)]\nmod tests", "#[cfg(test)]\n#[allow("] {
        if let Some(index) = source.replace("\r\n", "\n").find(marker) {
            end = end.min(index);
        }
    }
    &source[..end.min(source.len())]
}

fn token_occurs(line: &str, token: &str) -> bool {
    line.match_indices(token).any(|(index, _)| {
        // `fs::` must be a path segment, not the tail of `workspace_files::`.
        if token == "fs::" {
            let before = line[..index].chars().next_back();
            return !before.is_some_and(|ch| ch.is_alphanumeric() || ch == '_');
        }
        true
    })
}

#[test]
fn guarded_modules_declare_the_disallowed_filesystem_lints() {
    for path in GUARDED_ROOTS {
        let source = fs::read_to_string(crate_root().join(path)).expect("read guarded module");
        assert!(
            source.contains(DENY_ATTRIBUTE),
            "{path} must keep `{DENY_ATTRIBUTE}`"
        );
    }
}

#[test]
fn workspace_modules_do_not_touch_the_host_filesystem_directly() {
    let mut violations = Vec::new();
    for path in guarded_files() {
        let source = fs::read_to_string(&path)
            .expect("read module")
            .replace("\r\n", "\n");
        for (number, line) in production_source(&source).lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            for token in FORBIDDEN {
                if token_occurs(code, token) {
                    violations.push(format!(
                        "{}:{}: `{token}` in `{}`",
                        relative(&path),
                        number + 1,
                        line.trim()
                    ));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "workspace content must go through tools::workspace_files (VFS); move host probes to host_probe.rs:\n{}",
        violations.join("\n")
    );
}

#[test]
fn host_probe_is_the_only_explicit_exception() {
    for path in ALLOWED_EXCEPTIONS {
        let source = fs::read_to_string(crate_root().join(path)).expect("read exception module");
        assert!(
            source.contains("#![allow(clippy::disallowed_methods, clippy::disallowed_types)]"),
            "{path} must state its exception explicitly"
        );
    }
}
