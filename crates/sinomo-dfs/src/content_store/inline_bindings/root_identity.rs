//! Noncreating identities for a content root before and after layout creation.

use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

const MAX_SYMLINK_TRAVERSALS: usize = 40;

/// Resolve existing directory ancestors and symlinks, then normalize missing
/// components without creating them. Parent components are applied after a
/// symlink's target has been resolved, rather than being collapsed lexically
/// before resolution. Every error except an absent component is preserved.
pub(super) fn normalized_root_key(root: &Path) -> io::Result<PathBuf> {
    let absolute = if root.is_absolute() {
        root.to_owned()
    } else {
        std::env::current_dir()?.join(root)
    };
    if !absolute.is_absolute() {
        // In particular, do not invent another drive's current directory for
        // a Windows drive-relative path such as C:content.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "content root did not resolve to an absolute path",
        ));
    }
    resolve_directory_components(&absolute, &mut 0)
}

/// Mutation and activation require an existing, fully canonical directory.
/// This never creates the root and never falls back after a filesystem error.
pub(super) fn canonical_existing_root(root: &Path) -> io::Result<PathBuf> {
    let canonical = fs::canonicalize(root)?;
    require_directory(&fs::metadata(&canonical)?)?;
    Ok(canonical)
}

fn require_directory(metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "content root component is not a directory",
        ))
    }
}

fn resolve_directory_components(path: &Path, traversals: &mut usize) -> io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            Component::RootDir => resolved.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::ParentDir => {
                // `resolved` has already followed every existing symlink. A
                // missing child may be canceled by .. without being created.
                // PathBuf cannot pop above a filesystem root.
                resolved.pop();
            }
            Component::Normal(name) => {
                let candidate = resolved.join(name);
                match fs::symlink_metadata(&candidate) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        *traversals += 1;
                        if *traversals > MAX_SYMLINK_TRAVERSALS {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "too many symbolic links in content root",
                            ));
                        }
                        let target = fs::read_link(&candidate)?;
                        let target = if target.is_absolute() {
                            target
                        } else {
                            resolved.join(target)
                        };
                        resolved = resolve_directory_components(&target, traversals)?;
                    }
                    Ok(metadata) => {
                        require_directory(&metadata)?;
                        // Preserve platform canonical spelling and reparse
                        // semantics. A race/error after the observation must
                        // fail rather than silently create a second identity.
                        resolved = fs::canonicalize(&candidate)?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        resolved = candidate;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_root_key_is_noncreating_and_matches_after_creation() {
        let fixture = sinomo_testdir::TestDir::new("content-root-key-before-create");
        let missing = fixture.join("missing").join("content");
        let before = normalized_root_key(&missing).expect("key before creation");
        assert!(!fixture.join("missing").exists());
        assert_eq!(
            before,
            fs::canonicalize(fixture.path())
                .unwrap()
                .join("missing/content")
        );
        fs::create_dir_all(&missing).unwrap();
        assert_eq!(before, normalized_root_key(&missing).unwrap());
        assert_eq!(before, canonical_existing_root(&missing).unwrap());
    }

    #[test]
    fn relative_missing_parent_components_do_not_create_paths() {
        let fixture = sinomo_testdir::TestDir::new("content-root-key-relative");
        let component = fixture.path().file_name().expect("unique component");
        let cwd = std::env::current_dir().unwrap();
        let missing = cwd.join(component);
        assert!(!missing.exists());
        let spelling = Path::new(".").join(component).join("..").join(".");
        assert_eq!(
            normalized_root_key(&spelling).unwrap(),
            fs::canonicalize(&cwd).unwrap()
        );
        assert!(
            !missing.exists(),
            "normalizing a missing suffix must not initialize it"
        );
    }

    #[test]
    fn existing_root_rejects_absence_and_regular_file_components() {
        let fixture = sinomo_testdir::TestDir::new("content-root-key-real-io");
        let missing = fixture.join("missing");
        assert_eq!(
            canonical_existing_root(&missing).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(!missing.exists());
        let blocker = fixture.join("regular-file");
        fs::write(&blocker, b"not a directory").unwrap();
        for path in [
            blocker.clone(),
            blocker.join("child"),
            blocker.join("..").join("other"),
        ] {
            assert_eq!(
                normalized_root_key(&path).unwrap_err().kind(),
                io::ErrorKind::NotADirectory
            );
        }
        assert_eq!(
            canonical_existing_root(&blocker).unwrap_err().kind(),
            io::ErrorKind::NotADirectory
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_ancestor_key_is_stable_across_root_creation() {
        use std::os::unix::fs::symlink;
        let fixture = sinomo_testdir::TestDir::new("content-root-key-symlink-create");
        let real = fixture.join("real");
        fs::create_dir(&real).unwrap();
        let alias = fixture.join("alias");
        symlink(&real, &alias).unwrap();
        let aliased_root = alias.join("new-content");
        let before = normalized_root_key(&aliased_root).unwrap();
        assert_eq!(
            before,
            normalized_root_key(&real.join("new-content")).unwrap()
        );
        assert!(!aliased_root.exists());
        fs::create_dir_all(real.join("new-content")).unwrap();
        assert_eq!(before, canonical_existing_root(&aliased_root).unwrap());
        assert_eq!(before, normalized_root_key(&aliased_root).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn parent_component_follows_symlink_target_before_popping() {
        use std::os::unix::fs::symlink;
        let fixture = sinomo_testdir::TestDir::new("content-root-key-symlink-parent");
        let real_parent = fixture.join("real");
        fs::create_dir_all(real_parent.join("child")).unwrap();
        let alias = fixture.join("alias");
        symlink("real/child", &alias).unwrap();
        let spelling = alias.join("..").join("future");
        let expected = fs::canonicalize(&real_parent).unwrap().join("future");
        assert_eq!(normalized_root_key(&spelling).unwrap(), expected);
        assert_ne!(expected, fixture.join("future"));
        assert!(!real_parent.join("future").exists());
        assert!(!fixture.join("future").exists());
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_target_is_normalized_without_creation() {
        use std::os::unix::fs::symlink;
        let fixture = sinomo_testdir::TestDir::new("content-root-key-dangling-link");
        let alias = fixture.join("alias");
        symlink("not-yet-created", &alias).unwrap();
        let spelling = alias.join("content");
        let before = normalized_root_key(&spelling).unwrap();
        assert_eq!(
            before,
            fs::canonicalize(fixture.path())
                .unwrap()
                .join("not-yet-created/content")
        );
        assert!(!fixture.join("not-yet-created").exists());
        fs::create_dir_all(fixture.join("not-yet-created/content")).unwrap();
        assert_eq!(before, canonical_existing_root(&spelling).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_loop_is_an_error() {
        use std::os::unix::fs::symlink;
        let fixture = sinomo_testdir::TestDir::new("content-root-key-link-loop");
        symlink("second", fixture.join("first")).unwrap();
        symlink("first", fixture.join("second")).unwrap();
        assert!(normalized_root_key(&fixture.join("first/content")).is_err());
    }
}
