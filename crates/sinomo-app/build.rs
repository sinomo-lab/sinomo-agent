//! Embeds build metadata and target-specific application metadata into the
//! final `sinomo-app` binary.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

const ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV: &str = "SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE";

fn main() {
    embed_macos_privacy_plist();
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

fn embed_macos_privacy_plist() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"),
    );
    let plist = manifest_dir.join("assets").join("macos").join("Info.plist");
    println!("cargo:rerun-if-changed={}", plist.display());
    println!(
        "cargo:rustc-link-arg-bin=sinomo-app=-Wl,-sectcreate,__TEXT,__info_plist,{}",
        plist.display()
    );
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
