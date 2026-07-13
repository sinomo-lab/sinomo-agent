use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=SINOMO_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=SINOMO_GIT_DIRTY");

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR");
    let manifest_dir = Path::new(&manifest_dir);
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("Cargo.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("src").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("sifs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("build.rs").display()
    );
    watch_git_path(manifest_dir, "HEAD");
    watch_git_path(manifest_dir, "index");
    if let Some(head_ref) = git_stdout(manifest_dir, &["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(manifest_dir, &head_ref);
    }

    let commit = env::var("SINOMO_GIT_COMMIT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| git_stdout(manifest_dir, &["rev-parse", "HEAD"]));
    if let Some(commit) = commit {
        println!("cargo:rustc-env=SINOMO_GIT_COMMIT={commit}");
    }

    let dirty = env::var("SINOMO_GIT_DIRTY")
        .ok()
        .filter(|value| matches!(value.as_str(), "true" | "false"))
        .or_else(|| {
            git_stdout(
                manifest_dir,
                &[
                    "status",
                    "--porcelain",
                    "--untracked-files=normal",
                    "--",
                    ".",
                ],
            )
            .map(|status| (!status.is_empty()).to_string())
        });
    if let Some(dirty) = dirty {
        println!("cargo:rustc-env=SINOMO_GIT_DIRTY={dirty}");
    }
}

fn watch_git_path(directory: &Path, name: &str) {
    let Some(path) = git_stdout(directory, &["rev-parse", "--git-path", name]) else {
        return;
    };
    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        directory.join(path)
    };
    println!("cargo:rerun-if-changed={}", path.display());
}

fn git_stdout(directory: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(directory)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
}
