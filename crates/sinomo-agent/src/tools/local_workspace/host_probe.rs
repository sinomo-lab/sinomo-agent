//! Host introspection for choosing how commands run.
//!
//! These probes inspect the host itself (installed programs, the Git
//! repository that encloses a workspace), not workspace content, so they are
//! the one place in the workspace tools allowed to touch the host filesystem
//! directly. They only answer yes/no questions and never read or write
//! workspace files on a tool's behalf.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub(super) fn current_path_dirs() -> Vec<PathBuf> {
    env::var_os("PATH")
        .map(|value| env::split_paths(&value).collect())
        .unwrap_or_default()
}

#[cfg(not(windows))]
pub(super) fn bash_is_installed() -> bool {
    Path::new("/bin/bash").exists() || Path::new("/usr/bin/bash").exists()
}

/// Git discovers repositories by walking up from the working directory, which
/// can leave the workspace grant; this mirrors that walk on the host.
pub(super) fn workspace_has_git_metadata(root: &Path) -> bool {
    let mut path = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    if path.is_file() {
        path.pop();
    }
    loop {
        if is_git_metadata(&path.join(".git")) {
            return true;
        }
        if !path.pop() {
            return false;
        }
    }
}

/// Mirrors git discovery closely enough that stray empty `.git` entries in an
/// ancestor (for example `/tmp/.git`) do not enable git helpers that would fail.
fn is_git_metadata(dot_git: &Path) -> bool {
    match fs::metadata(dot_git) {
        Ok(metadata) if metadata.is_dir() => dot_git.join("HEAD").is_file(),
        // Worktrees and submodules use a `gitdir: <path>` link file.
        Ok(metadata) if metadata.is_file() => {
            fs::read(dot_git).is_ok_and(|contents| contents.starts_with(b"gitdir:"))
        }
        _ => false,
    }
}

pub(super) fn program_is_available(program: &str) -> bool {
    let program = program.trim();
    if program.is_empty() {
        return false;
    }
    let path = Path::new(program);
    if path.is_absolute() || path.components().count() > 1 {
        return program_path_is_available(path);
    }
    current_path_dirs()
        .into_iter()
        .any(|directory| program_path_is_available(&directory.join(path)))
}

fn program_path_is_available(path: &Path) -> bool {
    if executable_program_file(path) {
        return true;
    }
    #[cfg(windows)]
    if path.extension().is_none() {
        let extensions = env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
        return extensions.split(';').any(|extension| {
            let extension = extension.trim().trim_start_matches('.');
            !extension.is_empty() && executable_program_file(&path.with_extension(extension))
        });
    }
    false
}

fn executable_program_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}
