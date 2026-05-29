//! Embeds the git commit the daemon was built from into `SINOMO_GIT_COMMIT`.
//!
//! The package version stays `0.1.0` across self-rollout builds, so the commit
//! hash is what distinguishes one build from another in `sinomo cluster health`.
//! Falls back to "unknown" when git is unavailable or the source is not a repo.

use std::process::Command;

fn main() {
    let commit = git_commit().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SINOMO_GIT_COMMIT={commit}");

    // Rebuild when HEAD moves so the embedded commit stays current.
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
    // `git diff --quiet` exits non-zero when tracked files differ from HEAD;
    // untracked files (e.g. target/) are ignored, so a clean checkout is "clean".
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
