use std::path::PathBuf;
use std::process::Command;

const ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV: &str = "SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE";

fn main() {
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

fn git_commit() -> Option<String> {
    let hash = run_git(&["rev-parse", "--short=12", "HEAD"])?;
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
    let Some(git_dir) = run_git(&["rev-parse", "--git-dir"]) else {
        return;
    };
    let git_dir = PathBuf::from(git_dir.trim());
    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display()
    );
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
}
