//! Embeds build metadata only into the final `sinomo` binary.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

const ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV: &str = "SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE";
const WINDOWS_MAIN_STACK_BYTES: usize = 8 * 1024 * 1024;

fn main() {
    configure_windows_main_stack();
    println!("cargo:rerun-if-env-changed={ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV}");
    let commit = std::env::var(ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SINOMO_GIT_COMMIT={commit}");
    watch_git_head();
}

fn configure_windows_main_stack() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    let Some(link_arg) = windows_main_stack_link_arg(&target_os, &target_env) else {
        return;
    };
    println!("cargo:rustc-link-arg-bin=sinomo={link_arg}");
}

fn windows_main_stack_link_arg(target_os: &str, target_env: &str) -> Option<String> {
    if target_os != "windows" {
        return None;
    }
    match target_env {
        "msvc" => Some(format!("/STACK:{WINDOWS_MAIN_STACK_BYTES}")),
        "gnu" => Some(format!("-Wl,--stack,{WINDOWS_MAIN_STACK_BYTES}")),
        _ => None,
    }
}

fn git_commit() -> Option<String> {
    let hash = run(&["rev-parse", "--short=12", "HEAD"])?;
    let hash = hash.trim();
    if hash.is_empty() {
        return None;
    }
    let dirty = Command::new("git")
        .args(["diff", "--quiet", "HEAD"])
        .status()
        .map(|status| !status.success())
        .unwrap_or(false);
    Some(if dirty {
        format!("{hash}-dirty")
    } else {
        hash.to_string()
    })
}

fn watch_git_head() {
    let Some(git_dir) = run(&["rev-parse", "--git-dir"]) else {
        return;
    };
    let git_dir = PathBuf::from(git_dir.trim());
    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display()
    );

    let Ok(head) = fs::read_to_string(git_dir.join("HEAD")) else {
        return;
    };
    let Some(reference) = head.strip_prefix("ref:").map(str::trim) else {
        return;
    };
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join(reference).display()
    );
}

fn run(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_main_stack_link_args_cover_supported_toolchains() {
        assert_eq!(
            windows_main_stack_link_arg("windows", "msvc").as_deref(),
            Some("/STACK:8388608")
        );
        assert_eq!(
            windows_main_stack_link_arg("windows", "gnu").as_deref(),
            Some("-Wl,--stack,8388608")
        );
    }

    #[test]
    fn windows_main_stack_link_arg_does_not_change_native_unix_links() {
        assert_eq!(windows_main_stack_link_arg("linux", "gnu"), None);
        assert_eq!(windows_main_stack_link_arg("macos", ""), None);
        assert_eq!(windows_main_stack_link_arg("windows", "unknown"), None);
    }
}
