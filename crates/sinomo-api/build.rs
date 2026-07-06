//! Embeds the git commit for daemon API status contracts.

use std::env;
use std::process::Command;

const ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV: &str = "SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE";

fn main() {
    println!("cargo:rerun-if-env-changed={ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV}");
    let commit = env::var(ROLLOUT_BUILD_COMMIT_OVERRIDE_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SINOMO_GIT_COMMIT={commit}");

    if let Some(git_dir) = run(&["rev-parse", "--git-dir"]) {
        let git_dir = git_dir.trim();
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        println!("cargo:rerun-if-changed={git_dir}/index");
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

fn run(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
