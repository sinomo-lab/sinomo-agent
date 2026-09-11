//! POSIX path normalization and containment helpers.

use crate::DfsObjectId;
use crate::posix_model::{DfsPosixError, Result};

pub(crate) fn parent_posix_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

pub(crate) fn join_posix_path(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{child}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), child)
    }
}

pub(crate) fn append_posix_components(path: String, components: &[String]) -> Result<String> {
    let mut current = path;
    for component in components {
        current = join_posix_path(&current, component);
    }
    normalize_posix_walk_path(&current)
}

pub(crate) fn resolve_posix_symlink_target(link_path: &str, target: &str) -> Result<String> {
    validate_posix_symlink_target(target)?;
    if target.starts_with('/') {
        return normalize_posix_walk_path(target);
    }
    let parent = parent_posix_path(link_path).unwrap_or_else(|| "/".to_string());
    normalize_posix_walk_path(&join_posix_path(&parent, target))
}

/// Returns the canonical POSIX path key used to index objects by path, or
/// `None` when the path cannot be normalized. This must match the normalization
/// applied when looking objects up by path so the index and lookups agree.
pub(crate) fn normalized_posix_path_key(path: &str) -> Option<String> {
    normalize_posix_path(path).ok()
}

pub(crate) fn normalize_posix_path(path: &str) -> Result<String> {
    if !path.starts_with('/') {
        return Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        });
    }
    if path.bytes().any(|byte| byte == 0) {
        return Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        });
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

pub(crate) fn normalize_posix_path_with_trailing_directory(path: &str) -> Result<(String, bool)> {
    let normalized = normalize_posix_walk_path(path)?;
    let requires_directory = normalized != "/" && path.ends_with('/');
    Ok((normalized, requires_directory))
}

pub(crate) fn posix_path_has_dot_components(path: &str) -> bool {
    path.split('/').any(|part| matches!(part, "." | ".."))
}

/// Route dot-containing requests by their resolved path while preserving the
/// trailing-directory requirement. Canonical requests retain their exact wire
/// spelling, including for durable operation-id retries.
pub(crate) fn path_after_dot_resolution<'a>(
    path: &'a str,
    resolved: &str,
) -> std::borrow::Cow<'a, str> {
    if !posix_path_has_dot_components(path) {
        return std::borrow::Cow::Borrowed(path);
    }
    std::borrow::Cow::Owned(if path.ends_with('/') && resolved != "/" {
        format!("{resolved}/")
    } else {
        resolved.to_string()
    })
}

/// Validate an operation path without discarding components that must be
/// interpreted after following symlinks. Metadata keys still use the lexical
/// normalizer, but only after the operation's component walk has completed.
pub(crate) fn normalize_posix_walk_path(path: &str) -> Result<String> {
    if !path.starts_with('/') || path.bytes().any(|byte| byte == 0) {
        return Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        });
    }
    Ok(format!(
        "/{}",
        path.split('/')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("/")
    ))
}

pub(crate) fn validate_posix_symlink_target(target: &str) -> Result<()> {
    if target.is_empty() || target.bytes().any(|byte| byte == 0) {
        return Err(DfsPosixError::InvalidPath {
            path: target.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn subtree_prefix(path: &str) -> String {
    if path == "/" {
        "/".to_string()
    } else {
        format!("{}/", path.trim_end_matches('/'))
    }
}

pub(crate) fn posix_path_is_at_or_under_prefix(path: &str, prefix: &str) -> bool {
    let path = lenient_posix_prefix_path(path);
    let prefix = lenient_posix_prefix_path(prefix);
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(&prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn hidden_alias_tombstone_path(object_id: &DfsObjectId) -> String {
    format!("/.sinomo/deleted-hardlinks/{object_id}")
}

fn lenient_posix_prefix_path(path: &str) -> String {
    let normalized = path.trim_matches('/');
    if normalized.is_empty() {
        "/".to_string()
    } else {
        format!("/{normalized}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_check_normalizes_slashes() {
        assert!(posix_path_is_at_or_under_prefix("/docs/readme.md", "/docs"));
        assert!(posix_path_is_at_or_under_prefix("docs/readme.md", "docs/"));
        assert!(posix_path_is_at_or_under_prefix("/docs", "/docs"));
        assert!(!posix_path_is_at_or_under_prefix(
            "/docs2/readme.md",
            "/docs"
        ));
    }

    #[test]
    fn strict_path_normalization_rejects_relative_paths() {
        assert_eq!(
            normalize_posix_path("/docs/./a/../b").expect("normalized"),
            "/docs/b"
        );
        assert!(matches!(
            normalize_posix_path("docs/readme.md"),
            Err(DfsPosixError::InvalidPath { .. })
        ));
    }
}
