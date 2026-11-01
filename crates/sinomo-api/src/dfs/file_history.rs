//! Explicitly retained file content. Commit generations are not file history.

use crate::dfs::{
    DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsObjectVersionRecord,
    DfsVersion,
};
use serde::{Deserialize, Serialize};
pub use sinomo_core::DfsNamespaceObjectPrecondition;

/// Retain captures within the age window OR among the newest configured count.
/// Zero for both fields disables automatic capture; independent pins still apply.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRetention {
    #[serde(default)]
    pub keep_last: u32,
    #[serde(default)]
    pub keep_for_seconds: u64,
}

impl DfsFileHistoryRetention {
    pub fn is_disabled(&self) -> bool {
        self.keep_last == 0 && self.keep_for_seconds == 0
    }

    /// `newer_captures` counts other policy captures for the same object.
    pub fn retains(&self, captured_at: u64, now: u64, newer_captures: usize) -> bool {
        newer_captures < self.keep_last as usize
            || (self.keep_for_seconds != 0
                && now.saturating_sub(captured_at)
                    < self.keep_for_seconds.saturating_mul(1_000_000_000))
    }
}

/// A compact, explicitly configured subtree rule. The longest matching path wins.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryRule {
    pub path: String,
    pub retention: DfsFileHistoryRetention,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<DfsFileHistoryRule>,
}

impl DfsFileHistoryPolicy {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn retention_for_path(&self, path: &str) -> DfsFileHistoryRetention {
        self.rules
            .iter()
            .filter(|rule| {
                rule.path == "/"
                    || path == rule.path
                    || path
                        .strip_prefix(&rule.path)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
            .max_by_key(|rule| rule.path.len())
            .map(|rule| rule.retention.clone())
            .unwrap_or_default()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 256 {
            return Err("file history policy has more than 256 rules".into());
        }
        let mut paths = std::collections::BTreeSet::new();
        for rule in &self.rules {
            if !rule.path.starts_with('/')
                || rule.path.contains('\0')
                || (rule.path != "/"
                    && rule.path[1..]
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == ".."))
                || rule.path.len() > 4096
            {
                return Err("file history rule requires a normalized absolute path".into());
            }
            if !paths.insert(&rule.path) {
                return Err("file history policy repeats a path".into());
            }
        }
        Ok(())
    }
}

/// Why bytes were explicitly preserved. A write rejection is never a capture.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsFileVersionSource {
    Checkpoint {
        operation_id: String,
    },
    RetentionRule,
    /// A caller explicitly preserves staged local-operation bytes. This does
    /// not publish an alternative current object or mark DFS as conflicted.
    LocalOperation {
        operation_id: String,
        base_version: Option<DfsVersion>,
    },
}

/// Content durability and canonical namespace publication are separate effects.
/// A retained write is always an explicit request, never a conflict fallback.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsWritePublication {
    #[default]
    Current,
    ConditionalCurrent {
        precondition: Box<DfsCurrentWritePrecondition>,
    },
    Retained {
        capture: Box<DfsRetainedWritePublication>,
    },
    /// Internal ownership of repairs for content already explicitly retained.
    /// This is never an incoming write or permission to create a capture.
    RetainedRepair,
}

pub type DfsRetainedWritePublication = crate::dfs::lifecycle::DfsRetainedContentPublication;

impl DfsWritePublication {
    /// Only the legacy default may be omitted from a serialized publication.
    pub fn is_default_current(&self) -> bool {
        matches!(self, Self::Current)
    }

    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current | Self::ConditionalCurrent { .. })
    }

    pub fn current_precondition(&self) -> Option<&DfsCurrentWritePrecondition> {
        match self {
            Self::ConditionalCurrent { precondition } => Some(precondition),
            _ => None,
        }
    }

    pub fn is_retained_repair(&self) -> bool {
        matches!(self, Self::RetainedRepair)
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::ConditionalCurrent { precondition } => precondition.validate()?,
            Self::Retained { capture } => capture.validate()?,
            Self::Current | Self::RetainedRepair => {}
        }
        Ok(())
    }
}

/// Original namespace predicates consumed by one current publication.
/// These values do not supply a writer, a policy decision, or shard authority.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsCurrentWritePrecondition {
    Scoped {
        root: String,
        path: String,
        directories: Vec<DfsDirectoryWritePrecondition>,
        target: DfsWriteTargetPrecondition,
    },
    /// The exact directory capability opened for this operation. Comparisons
    /// cover the supplied root through the target parent; effects must never
    /// resolve an outside prefix again after that open.
    OpenedScope {
        root: String,
        path: String,
        directories: Vec<DfsDirectoryWritePrecondition>,
        target: DfsWriteTargetPrecondition,
    },
    /// An explicit opened-directory contract without an inferred scoped walk.
    ParentOnly {
        path: String,
        parent: DfsNamespaceObjectPrecondition,
    },
    CompareAndSwap {
        path: String,
        expected: DfsNamespaceObjectPrecondition,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsDirectoryWritePrecondition {
    pub path: String,
    pub expected: DfsNamespaceObjectPrecondition,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsWriteTargetPrecondition {
    Absent {},
    Match {
        expected: DfsNamespaceObjectPrecondition,
    },
}

impl DfsCurrentWritePrecondition {
    pub fn path(&self) -> &str {
        match self {
            Self::Scoped { path, .. }
            | Self::OpenedScope { path, .. }
            | Self::ParentOnly { path, .. }
            | Self::CompareAndSwap { path, .. } => path,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_write_precondition_path(self.path())?;
        if self.path() == "/" {
            return Err("a write precondition requires a non-root target".into());
        }
        if let Self::Scoped {
            root,
            path,
            directories,
            ..
        }
        | Self::OpenedScope {
            root,
            path,
            directories,
            ..
        } = self
        {
            validate_write_precondition_path(root)?;
            // The bounded target path determines the entire bounded chain.
            // Requiring exact length/order rejects omitted and extra ancestors
            // without imposing a separate depth limit on valid native paths.
            let (parent, _) = path.rsplit_once('/').expect("absolute target");
            let mut expected_paths = vec!["/".to_string()];
            let mut prefix = String::new();
            for component in parent.split('/').filter(|component| !component.is_empty()) {
                prefix.push('/');
                prefix.push_str(component);
                expected_paths.push(prefix.clone());
            }
            let root_index = expected_paths
                .iter()
                .position(|directory| directory == root)
                .ok_or("scoped root must be a directory ancestor of the target")?;
            let expected_paths = match self {
                Self::OpenedScope { .. } => &expected_paths[root_index..],
                _ => expected_paths.as_slice(),
            };
            if directories.len() != expected_paths.len()
                || directories
                    .iter()
                    .zip(expected_paths)
                    .any(|(directory, expected)| &directory.path != expected)
            {
                return Err(match self {
                    Self::OpenedScope { .. } => "opened scope requires its exact ordered directory chain from its root through the target parent".into(),
                    _ => "scoped write requires its exact ordered directory chain from / through the target parent, including its root".into(),
                });
            }
        }
        Ok(())
    }
}

/// Native observation proving why an original conditional effect was refused.
/// Persisting the cause distinguishes this outcome from a caller's abort.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsWritePreconditionFailure {
    Directory {
        path: String,
        expected: DfsNamespaceObjectPrecondition,
        actual: Option<DfsNamespaceObjectPrecondition>,
        actual_kind: Option<DfsObjectKind>,
    },
    Target {
        path: String,
        expected: Option<DfsNamespaceObjectPrecondition>,
        actual: Option<DfsNamespaceObjectPrecondition>,
        actual_kind: Option<DfsObjectKind>,
    },
}

impl DfsWritePreconditionFailure {
    pub fn path(&self) -> &str {
        match self {
            Self::Directory { path, .. } | Self::Target { path, .. } => path,
        }
    }

    pub fn reason(&self) -> String {
        match self {
            Self::Directory {
                path,
                expected,
                actual,
                actual_kind,
            } => format!(
                "directory precondition failed at {path}: expected object {} version {}, found {actual:?} ({actual_kind:?})",
                expected.object_id, expected.version,
            ),
            Self::Target {
                path,
                expected,
                actual,
                actual_kind,
            } => format!(
                "target precondition failed at {path}: expected {expected:?}, found {actual:?} ({actual_kind:?})"
            ),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_write_precondition_path(self.path())?;
        let (actual, actual_kind) = match self {
            Self::Directory {
                actual,
                actual_kind,
                ..
            }
            | Self::Target {
                actual,
                actual_kind,
                ..
            } => (actual, actual_kind),
        };
        if actual.is_some() != actual_kind.is_some() {
            return Err("precondition failure requires coherent actual identity and kind".into());
        }
        Ok(())
    }
}

fn validate_write_precondition_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/')
        || path.len() > 4096
        || path.contains('\0')
        || (path != "/"
            && path[1..]
                .split('/')
                .any(|component| component.is_empty() || matches!(component, "." | "..")))
    {
        return Err("write precondition requires a bounded canonical absolute path".into());
    }
    Ok(())
}

/// The explicit rule evaluated when the current content was superseded.
/// Changing configuration affects future captures, not existing guarantees.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileVersionPolicyCapture {
    /// Assigned in authoritative object order, never inferred from wall time.
    pub order: u64,
    pub captured_at_unix_nanos: u64,
    pub retention: DfsFileHistoryRetention,
}

/// History's qualified view of one immutable retained revision.
/// The module projects this from generic grants and its declared provenance.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetainedFileVersion {
    pub object: Box<DfsObjectMetadata>,
    pub captured_at_unix_nanos: u64,
    pub source: DfsFileVersionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_capture: Option<DfsFileVersionPolicyCapture>,
}

impl DfsRetainedFileVersion {
    /// Validate nested identity before any journal/snapshot payload is applied.
    pub fn validate_for_version(&self, version: &DfsObjectVersionRecord) -> Result<(), String> {
        let object = &self.object;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || object.version != version.version
            || object.content_hash != version.content_hash
            || object.content_size_bytes != version.size_bytes
            || version.content_hash.is_none()
            || version.size_bytes.is_none()
            || object.tombstone_provenance.is_some()
            || !matches!(
                object.kind,
                DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
            )
        {
            return Err(
                "retained file version does not match its immutable content identity".into(),
            );
        }
        DfsFileHistoryPolicy {
            rules: vec![DfsFileHistoryRule {
                path: object.path.clone(),
                retention: DfsFileHistoryRetention::default(),
            }],
        }
        .validate()?;
        let operation = match &self.source {
            DfsFileVersionSource::Checkpoint { operation_id }
            | DfsFileVersionSource::LocalOperation { operation_id, .. } => Some(operation_id),
            DfsFileVersionSource::RetentionRule => None,
        };
        if operation.is_some_and(|id| id.trim().is_empty() || id.len() > 256 || id.contains('\0')) {
            return Err("file version capture requires a bounded operation identity".into());
        }
        if self.policy_capture.as_ref().is_some_and(|capture| {
            capture.retention.is_disabled()
                || capture.order == 0
                || capture.order > version.retention_order(DFS_HISTORY_METADATA_NAMESPACE)
        }) || (matches!(self.source, DfsFileVersionSource::RetentionRule)
            && self.policy_capture.is_none())
        {
            return Err("policy capture requires an enabled explicit retention rule".into());
        }
        Ok(())
    }
}

pub type DfsFileVersionIdentity = crate::dfs::lifecycle::DfsRevisionReference;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryEntry {
    pub identity: DfsFileVersionIdentity,
    pub captured: DfsRetainedFileVersion,
    pub content_hash: String,
    pub size_bytes: u64,
    pub pins: Vec<String>,
    pub is_current: bool,
    pub current_path: String,
    /// The object's current namespace entry is a deletion. Its retained
    /// content can be recovered only to an unoccupied path.
    pub deleted: bool,
}

/// The page cursor is tied to its query and authority generation by the server.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryCursor {
    pub shard_id: String,
    pub shard_epoch: u64,
    pub object_id: Option<DfsObjectId>,
    pub version: Option<DfsVersion>,
    pub topology: String,
    pub query: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryListRequest {
    pub file_system_id: DfsFileSystemId,
    /// Captured paths make deleted and replaced objects discoverable. An
    /// object selector follows identity across rename and hardlink aliases.
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub after: Option<DfsFileHistoryCursor>,
    pub limit: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryListResponse {
    pub entries: Vec<DfsFileHistoryEntry>,
    pub next_cursor: Option<DfsFileHistoryCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsFileHistoryPinOperation {
    /// Capture the exact current generation. The first pin is mandatory.
    Capture {
        pin_id: String,
    },
    /// Add an independent pin to a version already explicitly retained.
    Pin {
        pin_id: String,
    },
    Unpin {
        pin_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPinRequest {
    /// Routing hint; the authority must also check the stable object identity.
    pub path: String,
    pub identity: DfsFileVersionIdentity,
    pub operation: DfsFileHistoryPinOperation,
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryPinResponse {
    pub identity: DfsFileVersionIdentity,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryReadRequest {
    pub identity: DfsFileVersionIdentity,
    pub offset: u64,
    pub max_bytes: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryReadResponse {
    pub identity: DfsFileVersionIdentity,
    pub offset: u64,
    pub size_bytes: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsFileHistoryReadOutcome {
    Found {
        data: DfsFileHistoryReadResponse,
    },
    NotRetained {
        identity: DfsFileVersionIdentity,
    },
    ContentUnavailable {
        identity: DfsFileVersionIdentity,
        detail: String,
    },
}

pub type DfsFileHistoryRestoreRequest = crate::dfs::lifecycle::DfsContentCopyRequest;

pub type DfsFileHistoryRestoreResponse = crate::dfs::lifecycle::DfsContentCopyResponse;

/// Explicit preservation of caller-owned local-operation content. This is a
/// separate action from the failed/stale write whose base is recorded here.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPreserveRequest {
    /// Canonical lookup hint; it cannot substitute a different object identity.
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub local_operation_id: String,
    pub base_version: Option<DfsVersion>,
    pub pin_id: String,
    pub operation_id: String,
}

pub type DfsFileHistoryPreserveOutcome = crate::dfs::lifecycle::DfsPreservedContentOutcome;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileHistoryPolicyUpdateRequest {
    pub file_system_id: DfsFileSystemId,
    /// Compare-and-set prevents an administrator overwriting another edit.
    pub expected: DfsFileHistoryPolicy,
    pub policy: DfsFileHistoryPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileHistoryPolicyRequest {
    pub file_system_id: DfsFileSystemId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_is_disabled_without_an_explicit_rule() {
        let policy = DfsFileHistoryPolicy::default();
        assert!(policy.retention_for_path("/users/a/file").is_disabled());
        assert!(!DfsFileHistoryRetention::default().retains(0, 0, 0));
    }

    #[test]
    fn retention_keeps_the_union_of_count_and_time() {
        let retention = DfsFileHistoryRetention {
            keep_last: 2,
            keep_for_seconds: 10,
        };
        assert!(retention.retains(0, 100_000_000_000, 1));
        assert!(retention.retains(0, 9_000_000_000, 20));
        assert!(!retention.retains(0, 10_000_000_000, 2));
        assert!(
            retention.retains(10, 0, 20),
            "clock rollback retains conservatively"
        );
    }

    #[test]
    fn retention_rules_use_path_components_and_allow_explicit_disable() {
        let policy = DfsFileHistoryPolicy {
            rules: vec![
                DfsFileHistoryRule {
                    path: "/users".into(),
                    retention: DfsFileHistoryRetention {
                        keep_last: 3,
                        keep_for_seconds: 0,
                    },
                },
                DfsFileHistoryRule {
                    path: "/users/cache".into(),
                    retention: DfsFileHistoryRetention::default(),
                },
            ],
        };
        policy.validate().unwrap();
        assert_eq!(policy.retention_for_path("/users/a").keep_last, 3);
        assert!(policy.retention_for_path("/users/cache/file").is_disabled());
        assert!(policy.retention_for_path("/users2/a").is_disabled());
        let mut invalid = policy;
        invalid.rules[0].path = "/users/../services".into();
        assert!(invalid.validate().is_err());
    }

    fn identity(name: &str) -> DfsNamespaceObjectPrecondition {
        DfsNamespaceObjectPrecondition {
            object_id: DfsObjectId::from_legacy_named(format!("conditional-object-{name}"))
                .unwrap(),
            version: DfsVersion::from_legacy_named(format!("conditional-version-{name}")).unwrap(),
        }
    }

    fn scoped() -> DfsCurrentWritePrecondition {
        DfsCurrentWritePrecondition::Scoped {
            root: "/guard".into(),
            path: "/guard/note.md".into(),
            directories: vec![
                DfsDirectoryWritePrecondition {
                    path: "/".into(),
                    expected: identity("root"),
                },
                DfsDirectoryWritePrecondition {
                    path: "/guard".into(),
                    expected: identity("parent"),
                },
            ],
            target: DfsWriteTargetPrecondition::Absent {},
        }
    }

    fn wire_bytes(value: &impl Serialize) -> Vec<u8> {
        sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/conditional-publication",
            "test",
            value,
        )
        .unwrap()
        .body
    }

    #[test]
    fn conditional_current_roundtrips_without_default_omission() {
        #[derive(Serialize, Deserialize)]
        struct Envelope {
            #[serde(
                default,
                skip_serializing_if = "DfsWritePublication::is_default_current"
            )]
            publication: DfsWritePublication,
        }
        let publication = DfsWritePublication::ConditionalCurrent {
            precondition: Box::new(scoped()),
        };
        publication.validate().unwrap();
        assert!(publication.is_current());
        assert!(!publication.is_default_current());
        assert_eq!(
            publication.current_precondition().unwrap().path(),
            "/guard/note.md"
        );
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/conditional-publication",
            "test",
            &publication,
        )
        .unwrap();
        assert_eq!(
            request.decode_body::<DfsWritePublication>().unwrap(),
            publication
        );
        assert_eq!(
            serde_json::to_string(&Envelope {
                publication: DfsWritePublication::Current
            })
            .unwrap(),
            "{}"
        );
        let value = serde_json::to_value(Envelope {
            publication: publication.clone(),
        })
        .unwrap();
        assert_eq!(value["publication"]["kind"], "conditional-current");
        assert_eq!(
            serde_json::from_value::<Envelope>(value)
                .unwrap()
                .publication,
            publication
        );
    }

    #[test]
    fn conditional_scope_rejects_incomplete_reordered_and_escaping_walks() {
        let original = scoped();
        original.validate().unwrap();
        for bad_path in [
            "",
            "guard/note.md",
            "/",
            "/guard//note.md",
            "/guard/../note.md",
            "/guard/./note.md",
            "/guard/note.md/",
            "/guard/\0note.md",
        ] {
            let mut bad = original.clone();
            let DfsCurrentWritePrecondition::Scoped { path, .. } = &mut bad else {
                unreachable!()
            };
            *path = bad_path.into();
            assert!(bad.validate().is_err(), "{bad_path:?}");
        }
        let mut too_long = original.clone();
        let DfsCurrentWritePrecondition::Scoped { path, .. } = &mut too_long else {
            unreachable!()
        };
        *path = format!("/{}", "x".repeat(4096));
        assert!(too_long.validate().is_err());
        for change in 0..5 {
            let mut bad = original.clone();
            let DfsCurrentWritePrecondition::Scoped {
                root, directories, ..
            } = &mut bad
            else {
                unreachable!()
            };
            match change {
                0 => {
                    directories.remove(0);
                }
                1 => {
                    directories.pop();
                }
                2 => {
                    directories.reverse();
                }
                3 => {
                    directories.push(directories[1].clone());
                }
                4 => {
                    *root = "/guard/note.md".into();
                }
                _ => unreachable!(),
            }
            assert!(bad.validate().is_err(), "changed walk {change}");
        }
        let mut deep = original;
        let DfsCurrentWritePrecondition::Scoped {
            root,
            path,
            directories,
            ..
        } = &mut deep
        else {
            unreachable!()
        };
        *root = "/".into();
        *path = String::new();
        directories.clear();
        directories.push(DfsDirectoryWritePrecondition {
            path: "/".into(),
            expected: identity("root"),
        });
        for index in 0..32 {
            path.push_str(&format!("/d{index}"));
            directories.push(DfsDirectoryWritePrecondition {
                path: path.clone(),
                expected: identity(&index.to_string()),
            });
        }
        path.push_str("/note.md");
        deep.validate().unwrap();
        DfsCurrentWritePrecondition::Scoped {
            root: "/".into(),
            path: "/note.md".into(),
            directories: vec![DfsDirectoryWritePrecondition {
                path: "/".into(),
                expected: identity("root"),
            }],
            target: DfsWriteTargetPrecondition::Match {
                expected: identity("file"),
            },
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn opened_parent_and_compare_swap_keep_their_explicit_contracts() {
        for precondition in [
            DfsCurrentWritePrecondition::ParentOnly {
                path: "/guard/note.md".into(),
                parent: identity("parent"),
            },
            DfsCurrentWritePrecondition::CompareAndSwap {
                path: "/guard/note.md".into(),
                expected: identity("file"),
            },
        ] {
            precondition.validate().unwrap();
            let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
                "/test/conditional-publication",
                "test",
                &precondition,
            )
            .unwrap();
            assert_eq!(
                rpc.decode_body::<DfsCurrentWritePrecondition>().unwrap(),
                precondition
            );
        }
    }

    #[test]
    fn absent_target_keeps_prior_unit_json_and_native_codec_bytes() {
        #[derive(Serialize)]
        #[serde(
            rename = "DfsWriteTargetPrecondition",
            tag = "kind",
            rename_all = "kebab-case",
            deny_unknown_fields
        )]
        enum PriorUnitTarget {
            Absent,
        }
        let target = DfsWriteTargetPrecondition::Absent {};
        let json = serde_json::to_vec(&target).unwrap();
        assert_eq!(json, br#"{"kind":"absent"}"#);
        assert_eq!(json, serde_json::to_vec(&PriorUnitTarget::Absent).unwrap());
        assert_eq!(wire_bytes(&target), wire_bytes(&PriorUnitTarget::Absent));
        assert_eq!(
            serde_json::from_slice::<DfsWriteTargetPrecondition>(&json).unwrap(),
            target
        );
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/conditional-publication",
            "test",
            &target,
        )
        .unwrap();
        assert_eq!(
            request.decode_body::<DfsWriteTargetPrecondition>().unwrap(),
            target
        );
    }

    #[test]
    fn conditional_guards_and_failure_dtos_reject_unknown_wire_fields() {
        let mut condition = serde_json::to_value(scoped()).unwrap();
        condition["unguarded_fallback"] = true.into();
        assert!(serde_json::from_value::<DfsCurrentWritePrecondition>(condition).is_err());
        let mut directory = serde_json::to_value(DfsDirectoryWritePrecondition {
            path: "/".into(),
            expected: identity("root"),
        })
        .unwrap();
        directory["mode"] = 0o700.into();
        assert!(serde_json::from_value::<DfsDirectoryWritePrecondition>(directory).is_err());
        assert!(
            serde_json::from_value::<DfsWriteTargetPrecondition>(
                serde_json::json!({"kind":"absent","fallback":true})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<DfsWriteTargetPrecondition>(
                serde_json::json!({"kind":"future-target"})
            )
            .is_err()
        );
        let mut publication = serde_json::to_value(DfsWritePublication::ConditionalCurrent {
            precondition: Box::new(scoped()),
        })
        .unwrap();
        publication["fallback"] = true.into();
        assert!(serde_json::from_value::<DfsWritePublication>(publication).is_err());
        let mut failure = serde_json::to_value(DfsWritePreconditionFailure::Target {
            path: "/guard/note.md".into(),
            expected: None,
            actual: Some(identity("file")),
            actual_kind: Some(DfsObjectKind::File),
        })
        .unwrap();
        failure["possibly_applied"] = false.into();
        assert!(serde_json::from_value::<DfsWritePreconditionFailure>(failure).is_err());
    }

    #[test]
    fn legacy_publication_and_unit_terminal_bytes_remain_exact() {
        #[derive(Serialize, Deserialize)]
        #[serde(
            rename = "DfsWritePublication",
            tag = "kind",
            rename_all = "kebab-case",
            deny_unknown_fields
        )]
        enum LegacyPublication {
            Current,
            Retained {
                capture: Box<DfsRetainedWritePublication>,
            },
            RetainedRepair,
        }
        assert_eq!(
            serde_json::to_vec(&DfsWritePublication::Current).unwrap(),
            br#"{"kind":"current"}"#
        );
        assert_eq!(
            wire_bytes(&DfsWritePublication::Current),
            wire_bytes(&LegacyPublication::Current)
        );
        assert_eq!(
            wire_bytes(&DfsWritePublication::RetainedRepair),
            wire_bytes(&LegacyPublication::RetainedRepair)
        );
        assert!(
            serde_json::from_value::<LegacyPublication>(
                serde_json::to_value(DfsWritePublication::ConditionalCurrent {
                    precondition: Box::new(scoped())
                })
                .unwrap()
            )
            .is_err()
        );

        #[derive(Serialize, Deserialize)]
        #[serde(rename = "DfsWriteIntentState", rename_all = "kebab-case")]
        enum LegacyState {
            AwaitingReceipts,
            Committed,
            Rejected,
            Aborted,
        }
        for (old, current) in [
            (
                LegacyState::AwaitingReceipts,
                crate::dfs::DfsWriteIntentState::AwaitingReceipts,
            ),
            (
                LegacyState::Committed,
                crate::dfs::DfsWriteIntentState::Committed,
            ),
            (
                LegacyState::Rejected,
                crate::dfs::DfsWriteIntentState::Rejected,
            ),
            (
                LegacyState::Aborted,
                crate::dfs::DfsWriteIntentState::Aborted,
            ),
        ] {
            assert_eq!(wire_bytes(&old), wire_bytes(&current));
            assert_eq!(
                serde_json::to_vec(&old).unwrap(),
                serde_json::to_vec(&current).unwrap()
            );
        }
        #[derive(Serialize, Deserialize)]
        #[serde(rename = "DfsWriteIntentStatus", rename_all = "kebab-case")]
        enum LegacyStatus {
            Staging,
            ReceiptsPending,
            Committed,
            Rejected,
            Aborted,
        }
        for (old, current) in [
            (
                LegacyStatus::Staging,
                crate::dfs::DfsWriteIntentStatus::Staging,
            ),
            (
                LegacyStatus::ReceiptsPending,
                crate::dfs::DfsWriteIntentStatus::ReceiptsPending,
            ),
            (
                LegacyStatus::Committed,
                crate::dfs::DfsWriteIntentStatus::Committed,
            ),
            (
                LegacyStatus::Rejected,
                crate::dfs::DfsWriteIntentStatus::Rejected,
            ),
            (
                LegacyStatus::Aborted,
                crate::dfs::DfsWriteIntentStatus::Aborted,
            ),
        ] {
            assert_eq!(wire_bytes(&old), wire_bytes(&current));
            assert_eq!(
                serde_json::to_vec(&old).unwrap(),
                serde_json::to_vec(&current).unwrap()
            );
        }
        let failure = DfsWritePreconditionFailure::Directory {
            path: "/guard".into(),
            expected: identity("parent"),
            actual: None,
            actual_kind: None,
        };
        assert!(
            serde_json::from_value::<LegacyState>(
                serde_json::to_value(crate::dfs::DfsWriteIntentState::PreconditionFailed {
                    failure: failure.clone()
                })
                .unwrap()
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<LegacyStatus>(
                serde_json::to_value(crate::dfs::DfsWriteIntentStatus::PreconditionFailed {
                    failure
                })
                .unwrap()
            )
            .is_err()
        );

        #[derive(Serialize)]
        #[serde(rename = "DfsNamespaceObjectPrecondition")]
        struct LegacyLeaf {
            object_id: DfsObjectId,
            version: DfsVersion,
        }
        let leaf = identity("same-leaf");
        assert_eq!(
            wire_bytes(&leaf),
            wire_bytes(&LegacyLeaf {
                object_id: leaf.object_id.clone(),
                version: leaf.version.clone()
            })
        );
    }

    #[test]
    fn typed_precondition_failure_roundtrips_without_becoming_user_abort() {
        let failure = DfsWritePreconditionFailure::Directory {
            path: "/guard".into(),
            expected: identity("parent"),
            actual: Some(identity("replacement")),
            actual_kind: Some(DfsObjectKind::Directory),
        };
        failure.validate().unwrap();
        assert_eq!(failure.path(), "/guard");
        assert!(failure.reason().contains("directory precondition failed"));
        let status = crate::dfs::DfsWriteIntentStatus::PreconditionFailed {
            failure: failure.clone(),
        };
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/conditional-publication",
            "test",
            &status,
        )
        .unwrap();
        assert_eq!(
            request
                .decode_body::<crate::dfs::DfsWriteIntentStatus>()
                .unwrap(),
            status
        );
        let state = crate::dfs::DfsWriteIntentState::PreconditionFailed {
            failure: failure.clone(),
        };
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/conditional-publication",
            "test",
            &state,
        )
        .unwrap();
        assert_eq!(
            request
                .decode_body::<crate::dfs::DfsWriteIntentState>()
                .unwrap(),
            state
        );
        assert_ne!(state, crate::dfs::DfsWriteIntentState::Aborted);
        assert_ne!(status, crate::dfs::DfsWriteIntentStatus::Aborted);
        let incoherent = DfsWritePreconditionFailure::Target {
            path: "/guard/note.md".into(),
            expected: Some(identity("file")),
            actual: None,
            actual_kind: Some(DfsObjectKind::File),
        };
        assert!(incoherent.validate().is_err());
    }

    fn opened_scope() -> DfsCurrentWritePrecondition {
        DfsCurrentWritePrecondition::OpenedScope {
            root: "/services/cap".into(),
            path: "/services/cap/notes/n.md".into(),
            directories: vec![
                DfsDirectoryWritePrecondition {
                    path: "/services/cap".into(),
                    expected: identity("opened-root"),
                },
                DfsDirectoryWritePrecondition {
                    path: "/services/cap/notes".into(),
                    expected: identity("opened-parent"),
                },
            ],
            target: DfsWriteTargetPrecondition::Absent {},
        }
    }

    #[test]
    fn opened_scope_requires_its_exact_root_relative_directory_chain() {
        let condition = opened_scope();
        condition.validate().unwrap();
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/opened-scope",
            "test",
            &condition,
        )
        .unwrap();
        assert_eq!(
            request
                .decode_body::<DfsCurrentWritePrecondition>()
                .unwrap(),
            condition
        );
        for alteration in 0..6 {
            let mut bad = condition.clone();
            let DfsCurrentWritePrecondition::OpenedScope {
                root,
                path,
                directories,
                ..
            } = &mut bad
            else {
                unreachable!()
            };
            match alteration {
                0 => {
                    directories.remove(0);
                }
                1 => directories.reverse(),
                2 => directories.push(directories[1].clone()),
                3 => directories.insert(
                    0,
                    DfsDirectoryWritePrecondition {
                        path: "/".into(),
                        expected: identity("outside-prefix"),
                    },
                ),
                4 => *path = "/services/cap-other/notes/n.md".into(),
                5 => *root = "/services/cap/notes/n.md".into(),
                _ => unreachable!(),
            }
            assert!(bad.validate().is_err(), "altered opened scope {alteration}");
        }
        let mut unknown = serde_json::to_value(condition).unwrap();
        unknown["unguarded_fallback"] = true.into();
        assert!(serde_json::from_value::<DfsCurrentWritePrecondition>(unknown).is_err());
    }

    #[test]
    fn opened_scope_preserves_legacy_absolute_scoped_bytes_and_closed_readers() {
        #[derive(Serialize, Deserialize)]
        #[serde(
            rename = "DfsCurrentWritePrecondition",
            tag = "kind",
            rename_all = "kebab-case",
            deny_unknown_fields
        )]
        enum BeforeOpenedScope {
            Scoped {
                root: String,
                path: String,
                directories: Vec<DfsDirectoryWritePrecondition>,
                target: DfsWriteTargetPrecondition,
            },
            ParentOnly {
                path: String,
                parent: DfsNamespaceObjectPrecondition,
            },
            CompareAndSwap {
                path: String,
                expected: DfsNamespaceObjectPrecondition,
            },
        }
        let original = scoped();
        let DfsCurrentWritePrecondition::Scoped {
            root,
            path,
            directories,
            target,
        } = original.clone()
        else {
            unreachable!()
        };
        let old = BeforeOpenedScope::Scoped {
            root,
            path,
            directories,
            target,
        };
        assert_eq!(wire_bytes(&original), wire_bytes(&old));
        assert_eq!(
            serde_json::to_vec(&original).unwrap(),
            serde_json::to_vec(&old).unwrap()
        );
        original.validate().unwrap();
        let opened = opened_scope();
        assert!(
            serde_json::from_value::<BeforeOpenedScope>(serde_json::to_value(&opened).unwrap())
                .is_err()
        );
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/opened-scope",
            "test",
            &opened,
        )
        .unwrap();
        assert!(request.decode_body::<BeforeOpenedScope>().is_err());
        let mut legacy_with_shortened_walk = serde_json::to_value(opened).unwrap();
        legacy_with_shortened_walk["kind"] = "scoped".into();
        assert!(
            serde_json::from_value::<DfsCurrentWritePrecondition>(legacy_with_shortened_walk)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}

/// Namespace used by History's independently versioned persisted payloads.
pub const DFS_HISTORY_METADATA_NAMESPACE: &str = "module.dfs.history";
