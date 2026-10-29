//! Embeds build metadata only into the final `sinomo` binary.

use std::fs;
use std::path::{Path, PathBuf};
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
    let hash = run(&["rev-parse", "HEAD"])?;
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
    let mut paths = git_head_watch_paths(|path| run(&["rev-parse", "--git-path", path]));
    if let Some(root) = run(&["rev-parse", "--show-toplevel"])
        && let Some(files) = run(&["-C", root.trim(), "ls-files", "-z"])
    {
        // Commit metadata also includes the tracked dirty state. Keep unstaged
        // edits observable when the Git metadata itself has not changed.
        paths.extend(git_tracked_source_paths(Path::new(root.trim()), &files));
    }
    paths.sort();
    paths.dedup();
    for path in paths {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git_tracked_source_paths(root: &Path, files: &str) -> Vec<PathBuf> {
    files
        .split('\0')
        .filter(|file| !file.is_empty())
        .map(|file| root.join(file))
        .collect()
}

fn git_head_watch_paths(mut resolve: impl FnMut(&str) -> Option<String>) -> Vec<PathBuf> {
    let Some(head_path) = resolve("HEAD").map(|path| PathBuf::from(path.trim())) else {
        return Vec::new();
    };
    let mut paths = vec![head_path.clone()];
    if let Some(index) = resolve("index") {
        paths.push(PathBuf::from(index.trim()));
    }
    // Linked worktrees keep refs in the common Git directory. A nonexistent
    // rerun-if-changed input makes Cargo rerun and relink on every build.
    if let Some(packed) = resolve("packed-refs") {
        let packed = PathBuf::from(packed.trim());
        if packed.is_file() {
            paths.push(packed);
        }
    }
    if let Ok(head) = fs::read_to_string(head_path)
        && let Some(reference) = head.strip_prefix("ref:").map(str::trim)
        && let Some(path) = resolve(reference)
    {
        paths.push(PathBuf::from(path.trim()));
    }
    for path in &mut paths {
        // A packed or unborn branch has no loose ref yet. Watching its nearest
        // existing parent still notices creation of the loose ref on a commit.
        while !path.exists() && path.pop() {}
    }
    paths.retain(|path| path.exists());
    paths.sort();
    paths.dedup();
    paths
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
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct GitFixture(PathBuf);

    impl GitFixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "sinomo-build-git-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            let fixture = Self(path);
            fixture.git(&fixture.0, &["init", "--initial-branch=main"]);
            fixture
        }

        fn git(&self, directory: &Path, args: &[&str]) -> String {
            let output = Command::new("git")
                .current_dir(directory)
                .args([
                    "-c",
                    "user.name=Sinomo build test",
                    "-c",
                    "user.email=build-test@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                ])
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        }

        fn commit(&self) {
            self.git(&self.0, &["commit", "--allow-empty", "-m", "fixture"]);
        }

        fn resolved(&self, directory: &Path, path: &str) -> PathBuf {
            PathBuf::from(
                self.git(
                    directory,
                    &["rev-parse", "--path-format=absolute", "--git-path", path],
                )
                .trim(),
            )
        }

        fn watched(&self, directory: &Path) -> Vec<PathBuf> {
            git_head_watch_paths(|path| {
                Some(self.git(
                    directory,
                    &["rev-parse", "--path-format=absolute", "--git-path", path],
                ))
            })
        }
    }

    impl Drop for GitFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn git_watches_existing_metadata_and_loose_head_without_packed_refs() {
        let fixture = GitFixture::new();
        fixture.commit();
        let watched = fixture.watched(&fixture.0);
        for path in ["HEAD", "refs/heads/main"] {
            assert!(watched.contains(&fixture.resolved(&fixture.0, path)));
        }
        assert!(!watched.contains(&fixture.resolved(&fixture.0, "packed-refs")));
        assert!(watched.iter().all(|path| path.exists()));
    }

    #[test]
    fn git_linked_worktree_watches_common_refs_and_private_head_and_index() {
        let fixture = GitFixture::new();
        fixture.commit();
        let worktree = fixture.0.join("linked");
        fixture.git(
            &fixture.0,
            &[
                "worktree",
                "add",
                "-b",
                "linked",
                worktree.to_str().unwrap(),
            ],
        );
        fixture.git(&fixture.0, &["pack-refs", "--all", "--prune"]);
        let watched = fixture.watched(&worktree);
        assert!(watched.contains(&fixture.resolved(&worktree, "HEAD")));
        assert!(watched.contains(&fixture.resolved(&worktree, "index")));
        assert!(watched.contains(&fixture.0.join(".git/packed-refs")));
        assert!(watched.contains(&fixture.0.join(".git/refs/heads")));
        assert!(watched.iter().all(|path| path.exists()));
    }

    #[test]
    fn git_packed_head_watches_loose_ref_creation_then_the_new_ref() {
        let fixture = GitFixture::new();
        fixture.commit();
        fixture.git(&fixture.0, &["pack-refs", "--all", "--prune"]);
        let loose = fixture.resolved(&fixture.0, "refs/heads/main");
        assert!(!loose.exists());
        let watched = fixture.watched(&fixture.0);
        assert!(watched.iter().any(|path| path == loose.parent().unwrap()));
        assert!(watched.contains(&fixture.resolved(&fixture.0, "packed-refs")));
        fixture.commit();
        assert!(fixture.watched(&fixture.0).contains(&loose));
    }

    #[test]
    fn git_detached_worktree_does_not_watch_missing_private_packed_refs() {
        let fixture = GitFixture::new();
        fixture.commit();
        let worktree = fixture.0.join("detached");
        fixture.git(
            &fixture.0,
            &["worktree", "add", "--detach", worktree.to_str().unwrap()],
        );
        let watched = fixture.watched(&worktree);
        assert!(watched.contains(&fixture.resolved(&worktree, "HEAD")));
        assert!(watched.contains(&fixture.resolved(&worktree, "index")));
        assert!(!watched.iter().any(|path| path.ends_with("packed-refs")));
        assert!(watched.iter().all(|path| path.exists()));
    }

    #[test]
    fn git_unborn_head_watches_metadata_creation_without_missing_inputs() {
        let fixture = GitFixture::new();
        let watched = fixture.watched(&fixture.0);
        assert!(watched.contains(&fixture.resolved(&fixture.0, "HEAD")));
        assert!(watched.contains(&fixture.0.join(".git")));
        assert!(watched.contains(&fixture.0.join(".git/refs/heads")));
        assert!(watched.iter().all(|path| path.exists()));
    }

    #[test]
    fn git_unavailable_has_no_watch_inputs() {
        assert!(git_head_watch_paths(|_| None).is_empty());
    }

    #[test]
    fn git_tracked_source_watches_include_spaces_and_deleted_paths() {
        let fixture = GitFixture::new();
        fs::write(fixture.0.join("source with spaces.rs"), "// original\n").unwrap();
        fs::write(fixture.0.join("deleted.rs"), "// original\n").unwrap();
        fixture.git(&fixture.0, &["add", "."]);
        fixture.commit();
        fs::remove_file(fixture.0.join("deleted.rs")).unwrap();
        let files = fixture.git(&fixture.0, &["ls-files", "-z"]);
        let watched = git_tracked_source_paths(&fixture.0, &files);
        assert!(watched.contains(&fixture.0.join("source with spaces.rs")));
        assert!(watched.contains(&fixture.0.join("deleted.rs")));
        assert_eq!(watched.len(), 2);
    }

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
