use super::types::{
    IndexRoot, LocalScanCursor, LocalScanFrame, MemoryFileRef, MemoryFileVersionRef,
    MemorySourceConnector, SourceCandidate, SourcePage, VerifiedSourceSnapshot,
};
use crate::MemoryError;
use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::ambient_authority;
use cap_std::fs::{
    Dir as CapabilityDir, File as CapabilityFile, Metadata as CapabilityMetadata,
    OpenOptions as CapabilityOpenOptions, ReadDir as CapabilityReadDir,
};
use sinomo_runtime::core::Id;
use std::collections::HashMap;
use std::ffi::OsString;
#[cfg(test)]
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

const MAX_LOCAL_SCAN_PAGE_SIZE: usize = 256;
const MAX_LOCAL_SCAN_DEPTH: usize = 256;
const MAX_LOCAL_SCAN_SESSIONS: usize = 128;
const LOCAL_SCAN_SESSION_TTL: Duration = Duration::from_secs(60 * 60);
const LOCAL_SCAN_CURSOR_PREFIX: &str = "sinomo-local-scan-v2:";

type SharedLocalScanSession = Arc<Mutex<LocalScanSession>>;

struct LocalScanSession {
    root: IndexRoot,
    root_dir: CapabilityDir,
    sequence: u64,
    directories: Vec<LocalDirectoryFrame>,
}

struct LocalDirectoryFrame {
    relative_dir: String,
    dir: CapabilityDir,
    entries: CapabilityReadDir,
}

struct LocalScanSessionEntry {
    session: SharedLocalScanSession,
    last_used: Instant,
}

#[derive(Default)]
struct LocalScanSessionRegistry {
    sessions: HashMap<String, LocalScanSessionEntry>,
}

static LOCAL_SCAN_SESSIONS: OnceLock<Mutex<LocalScanSessionRegistry>> = OnceLock::new();

#[cfg(test)]
std::thread_local! {
    static LAST_LOCAL_SCAN_EXAMINED_ENTRIES: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalSourceConnector;

impl MemorySourceConnector for LocalSourceConnector {
    fn descriptor_identity_is_current(
        &self,
        root: &IndexRoot,
        source: &super::types::SourceDescriptor,
    ) -> Result<bool, MemoryError> {
        Ok(descriptor_identity_is_current(root, source))
    }

    fn scan_page(
        &self,
        root: &IndexRoot,
        cursor: Option<&LocalScanCursor>,
        limit: usize,
    ) -> Result<SourcePage, MemoryError> {
        if limit == 0 || limit > MAX_LOCAL_SCAN_PAGE_SIZE {
            return Err(MemoryError::InvalidMemoryPageLimit {
                surface: "local source scan",
                limit,
                max: MAX_LOCAL_SCAN_PAGE_SIZE,
            });
        }

        let (token, expected_sequence, session) = match cursor {
            Some(cursor) => {
                let (token, sequence) = parse_scan_cursor(cursor)?;
                let session = lookup_scan_session(&token)?;
                (token, sequence, session)
            }
            None => {
                let token = format!("{LOCAL_SCAN_CURSOR_PREFIX}{}", Id::generate());
                let session = Arc::new(Mutex::new(LocalScanSession::new(root)?));
                insert_scan_session(token.clone(), Arc::clone(&session))?;
                (token, 0, session)
            }
        };

        let result = {
            let mut session = session.lock().map_err(|_| MemoryError::LockPoisoned {
                detail: "local scan session lock is poisoned".to_string(),
            })?;
            if session.sequence != expected_sequence {
                return Err(MemoryError::InvalidMemoryContinuation {
                    detail: format!(
                        "local scan cursor sequence {expected_sequence} is stale; expected {}",
                        session.sequence
                    ),
                });
            }
            if session.root != *root {
                return Err(MemoryError::InvalidMemoryContinuation {
                    detail: "local scan cursor belongs to a different or reconfigured root"
                        .to_string(),
                });
            }
            scan_session_page(&mut session, limit).and_then(|(sources, examined)| {
                let complete = session.directories.is_empty();
                if !complete {
                    session.sequence = session.sequence.checked_add(1).ok_or_else(|| {
                        MemoryError::InvalidMemoryContinuation {
                            detail: "local scan cursor sequence overflowed".to_string(),
                        }
                    })?;
                }
                Ok((sources, examined, complete, session.sequence))
            })
        };

        match result {
            Ok((sources, examined, complete, sequence)) => {
                record_examined_entries(examined);
                if complete {
                    remove_scan_session(&token)?;
                }
                Ok(SourcePage {
                    sources,
                    continuation: (!complete).then(|| scan_cursor(&token, sequence)),
                })
            }
            Err(error) => {
                let _ = remove_scan_session(&token);
                Err(error)
            }
        }
    }

    fn acquire_snapshot(
        &self,
        root: &IndexRoot,
        source: &SourceCandidate,
    ) -> Result<VerifiedSourceSnapshot, MemoryError> {
        let (mut file, before) = open_validated_candidate(root, source)?;
        let before_identity = platform_identity(&before);
        let before_hint = source_version_hint(&before, &before_identity)?;
        if before_identity != source.platform_identity || before_hint != source.source_version_hint
        {
            return Err(MemoryError::SourceChanged {
                path: source.absolute_path.display().to_string(),
            });
        }
        if before.len() > root.config.max_source_bytes {
            return Err(MemoryError::SourceTooLarge {
                path: source.absolute_path.display().to_string(),
                bytes: before.len(),
                limit: root.config.max_source_bytes,
            });
        }

        let capacity = usize::try_from(before.len())
            .unwrap_or(usize::MAX)
            .min(1024 * 1024);
        let mut bytes = Vec::with_capacity(capacity);
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(|error| MemoryError::Io {
                path: source.absolute_path.display().to_string(),
                source: error,
            })?;
            if read == 0 {
                break;
            }
            let next_len = bytes.len().saturating_add(read);
            if u64::try_from(next_len).unwrap_or(u64::MAX) > root.config.max_source_bytes {
                return Err(MemoryError::SourceTooLarge {
                    path: source.absolute_path.display().to_string(),
                    bytes: u64::try_from(next_len).unwrap_or(u64::MAX),
                    limit: root.config.max_source_bytes,
                });
            }
            hasher.update(&buffer[..read]);
            bytes.extend_from_slice(&buffer[..read]);
        }

        let after = file.metadata().map_err(|error| MemoryError::Io {
            path: source.absolute_path.display().to_string(),
            source: error,
        })?;
        let after_identity = platform_identity(&after);
        let after_hint = source_version_hint(&after, &after_identity)?;
        if before_identity != after_identity || before_hint != after_hint {
            return Err(MemoryError::SourceChanged {
                path: source.absolute_path.display().to_string(),
            });
        }
        // Reopen through a freshly established capability after reading. This
        // ensures that the registered namespace still names the same source;
        // a rename or symlink/reparse replacement cannot make an old open
        // handle authorize a new path.
        let _ = open_validated_candidate(root, source)?;

        let content_hash = hasher.finalize().to_hex().to_string();
        let version_fingerprint = format!("{}\0{}", source.file_ref.object_id, content_hash);
        Ok(VerifiedSourceSnapshot {
            candidate: source.clone(),
            version: MemoryFileVersionRef {
                file_ref: source.file_ref.clone(),
                version_id: Id::derived("memory-file-version", version_fingerprint.as_bytes()),
                content_hash,
                byte_length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            },
            bytes,
        })
    }
}

impl LocalScanSession {
    fn new(root: &IndexRoot) -> Result<Self, MemoryError> {
        let root_dir = open_registered_root(root)?;
        let frame_dir = root_dir.try_clone().map_err(|source| MemoryError::Io {
            path: root.config.path.display().to_string(),
            source,
        })?;
        let entries = frame_dir.entries().map_err(|source| MemoryError::Io {
            path: root.config.path.display().to_string(),
            source,
        })?;
        Ok(Self {
            root: root.clone(),
            root_dir,
            sequence: 0,
            directories: vec![LocalDirectoryFrame {
                relative_dir: String::new(),
                dir: frame_dir,
                entries,
            }],
        })
    }
}

fn scan_session_page(
    session: &mut LocalScanSession,
    work_budget: usize,
) -> Result<(Vec<SourceCandidate>, usize), MemoryError> {
    validate_scan_session_namespace(session)?;

    let mut examined = 0usize;
    let mut sources = Vec::with_capacity(work_budget.min(MAX_LOCAL_SCAN_PAGE_SIZE));
    while examined < work_budget {
        let Some(frame_index) = session.directories.len().checked_sub(1) else {
            break;
        };
        let relative_dir = session.directories[frame_index].relative_dir.clone();
        let entry = match session.directories[frame_index].entries.next() {
            Some(entry) => entry.map_err(|source| MemoryError::Io {
                path: join_relative(&session.root.config.path, &relative_dir)
                    .unwrap_or_else(|_| session.root.config.path.clone())
                    .display()
                    .to_string(),
                source,
            })?,
            None => {
                examined += 1;
                session.directories.pop();
                continue;
            }
        };
        examined += 1;

        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_safe_entry_name(&name) {
            continue;
        }
        let relative = if relative_dir.is_empty() {
            name
        } else {
            format!("{relative_dir}/{name}")
        };
        let path = join_relative(&session.root.config.path, &relative)?;
        let file_type = entry.file_type().map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            if session.directories.len() >= MAX_LOCAL_SCAN_DEPTH {
                return Err(MemoryError::InvalidIndexRoot {
                    path: path.display().to_string(),
                    detail: format!(
                        "local scan exceeds the maximum directory depth of {MAX_LOCAL_SCAN_DEPTH}"
                    ),
                });
            }
            let dir = session.directories[frame_index]
                .dir
                .open_dir_nofollow(entry.file_name())
                .map_err(|source| MemoryError::InvalidIndexRoot {
                    path: path.display().to_string(),
                    detail: format!(
                        "local scan directory is not an authorized non-symlink directory: {source}"
                    ),
                })?;
            let entries = dir.entries().map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?;
            session.directories.push(LocalDirectoryFrame {
                relative_dir: relative,
                dir,
                entries,
            });
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if !is_included(&session.root, &path) {
            continue;
        }
        let (_, metadata) = open_regular_file_for_scan(
            &session.directories[frame_index].dir,
            entry.file_name(),
            &path,
        )?;
        if metadata.len() > session.root.config.max_source_bytes {
            continue;
        }
        let platform_identity = platform_identity(&metadata);
        let source_version_hint = source_version_hint(&metadata, &platform_identity)?;
        let object_fingerprint = format!("{}\0{}", session.root.root_id, platform_identity);
        sources.push(SourceCandidate {
            file_ref: MemoryFileRef {
                connector_id: session.root.connector_id.clone(),
                root_id: session.root.root_id.clone(),
                object_id: Id::derived("memory-file", object_fingerprint.as_bytes()),
                path_hint: relative,
            },
            absolute_path: path.clone(),
            platform_identity,
            source_version_hint,
            byte_length: metadata.len(),
            media_type: media_type_for_path(&path).to_string(),
        });
    }
    Ok((sources, examined))
}

fn scan_session_registry() -> &'static Mutex<LocalScanSessionRegistry> {
    LOCAL_SCAN_SESSIONS.get_or_init(|| Mutex::new(LocalScanSessionRegistry::default()))
}

fn insert_scan_session(token: String, session: SharedLocalScanSession) -> Result<(), MemoryError> {
    let now = Instant::now();
    let mut registry = scan_session_registry()
        .lock()
        .map_err(|_| MemoryError::LockPoisoned {
            detail: "local scan session registry is poisoned".to_string(),
        })?;
    registry.sessions.retain(|_, entry| {
        Arc::strong_count(&entry.session) > 1
            || now.duration_since(entry.last_used) <= LOCAL_SCAN_SESSION_TTL
    });
    if registry.sessions.len() >= MAX_LOCAL_SCAN_SESSIONS
        && let Some(oldest) = registry
            .sessions
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.session) == 1)
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(token, _)| token.clone())
    {
        registry.sessions.remove(&oldest);
    }
    if registry.sessions.len() >= MAX_LOCAL_SCAN_SESSIONS {
        return Err(MemoryError::InvalidMemoryContinuation {
            detail: format!(
                "the {MAX_LOCAL_SCAN_SESSIONS} active local scan session limit is exhausted"
            ),
        });
    }
    if registry.sessions.contains_key(&token) {
        return Err(MemoryError::InvalidMemoryContinuation {
            detail: "generated a duplicate local scan session token".to_string(),
        });
    }
    registry.sessions.insert(
        token,
        LocalScanSessionEntry {
            session,
            last_used: now,
        },
    );
    Ok(())
}

fn lookup_scan_session(token: &str) -> Result<SharedLocalScanSession, MemoryError> {
    let now = Instant::now();
    let mut registry = scan_session_registry()
        .lock()
        .map_err(|_| MemoryError::LockPoisoned {
            detail: "local scan session registry is poisoned".to_string(),
        })?;
    registry.sessions.retain(|_, entry| {
        Arc::strong_count(&entry.session) > 1
            || now.duration_since(entry.last_used) <= LOCAL_SCAN_SESSION_TTL
    });
    let entry =
        registry
            .sessions
            .get_mut(token)
            .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
                detail: "local scan cursor is unknown or expired; scans never restart implicitly"
                    .to_string(),
            })?;
    entry.last_used = now;
    Ok(Arc::clone(&entry.session))
}

fn remove_scan_session(token: &str) -> Result<(), MemoryError> {
    scan_session_registry()
        .lock()
        .map_err(|_| MemoryError::LockPoisoned {
            detail: "local scan session registry is poisoned".to_string(),
        })?
        .sessions
        .remove(token);
    Ok(())
}

fn scan_cursor(token: &str, sequence: u64) -> LocalScanCursor {
    LocalScanCursor {
        frames: vec![LocalScanFrame {
            relative_dir: token.to_string(),
            after_name: Some(sequence.to_string()),
        }],
    }
}

fn parse_scan_cursor(cursor: &LocalScanCursor) -> Result<(String, u64), MemoryError> {
    let [frame] = cursor.frames.as_slice() else {
        return Err(MemoryError::InvalidMemoryContinuation {
            detail: "local scan cursor must contain exactly one opaque session frame".to_string(),
        });
    };
    let Some(token_body) = frame.relative_dir.strip_prefix(LOCAL_SCAN_CURSOR_PREFIX) else {
        return Err(MemoryError::InvalidMemoryContinuation {
            detail: "local scan cursor has an unsupported session token".to_string(),
        });
    };
    if token_body.is_empty()
        || token_body.len() > 128
        || !token_body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(MemoryError::InvalidMemoryContinuation {
            detail: "local scan cursor has an invalid session token".to_string(),
        });
    }
    let sequence = frame
        .after_name
        .as_deref()
        .ok_or_else(|| MemoryError::InvalidMemoryContinuation {
            detail: "local scan cursor is missing its sequence".to_string(),
        })?
        .parse::<u64>()
        .map_err(|_| MemoryError::InvalidMemoryContinuation {
            detail: "local scan cursor has an invalid sequence".to_string(),
        })?;
    Ok((frame.relative_dir.clone(), sequence))
}

fn record_examined_entries(examined: usize) {
    #[cfg(test)]
    LAST_LOCAL_SCAN_EXAMINED_ENTRIES.with(|value| value.set(examined));
    #[cfg(not(test))]
    let _ = examined;
}

fn is_safe_entry_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

fn join_relative(root: &Path, relative: &str) -> Result<PathBuf, MemoryError> {
    let mut path = root.to_path_buf();
    for component in Path::new(relative).components() {
        match component {
            std::path::Component::Normal(segment) => path.push(segment),
            _ => {
                return Err(MemoryError::InvalidMemoryContinuation {
                    detail: format!("local scan cursor contains unsafe directory `{relative}`"),
                });
            }
        }
    }
    Ok(path)
}

fn absolute_root_parts(path: &Path) -> Result<(PathBuf, Vec<OsString>), MemoryError> {
    let mut components = path.components();
    let mut anchor = PathBuf::new();
    match components.next() {
        Some(Component::Prefix(prefix)) => {
            anchor.push(prefix.as_os_str());
            if !matches!(components.next(), Some(Component::RootDir)) {
                return Err(invalid_root_path(
                    path,
                    "registered local root is not absolute",
                ));
            }
            anchor.push(std::path::MAIN_SEPARATOR_STR);
        }
        Some(Component::RootDir) => anchor.push(std::path::MAIN_SEPARATOR_STR),
        _ => {
            return Err(invalid_root_path(
                path,
                "registered local root is not absolute",
            ));
        }
    }

    let mut segments = Vec::new();
    for component in components {
        match component {
            Component::Normal(segment) => segments.push(segment.to_os_string()),
            _ => {
                return Err(invalid_root_path(
                    path,
                    "registered local root must be a normalized absolute path",
                ));
            }
        }
    }
    Ok((anchor, segments))
}

fn invalid_root_path(path: &Path, detail: impl Into<String>) -> MemoryError {
    MemoryError::InvalidIndexRoot {
        path: path.display().to_string(),
        detail: detail.into(),
    }
}

/// Establish a capability from the filesystem anchor and walk every root
/// component independently with no-follow semantics. The only ambient open is
/// the platform filesystem anchor itself; the registered path and all of its
/// ancestors are therefore unable to redirect the walk through a symlink or
/// Windows reparse point.
fn open_registered_root(root: &IndexRoot) -> Result<CapabilityDir, MemoryError> {
    let path = &root.config.path;
    let (anchor, segments) = absolute_root_parts(path)?;
    let mut dir =
        CapabilityDir::open_ambient_dir(&anchor, ambient_authority()).map_err(|source| {
            invalid_root_path(
                path,
                format!("failed to open filesystem anchor without ambient descendants: {source}"),
            )
        })?;
    let mut current_path = anchor;
    for segment in segments {
        current_path.push(&segment);
        dir = dir.open_dir_nofollow(&segment).map_err(|source| {
            invalid_root_path(
                path,
                format!(
                    "registered root component {} is not an accessible non-symlink directory: {source}",
                    current_path.display()
                ),
            )
        })?;
    }
    Ok(dir)
}

fn directory_identity(dir: &CapabilityDir) -> std::io::Result<(u64, u64)> {
    let metadata = dir.dir_metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

fn validate_scan_session_namespace(session: &LocalScanSession) -> Result<(), MemoryError> {
    let path = &session.root.config.path;
    let mut current = open_registered_root(&session.root)?;
    let expected_root = directory_identity(&session.root_dir).map_err(|source| {
        invalid_root_path(
            path,
            format!("failed to stat retained root capability: {source}"),
        )
    })?;
    let current_root = directory_identity(&current).map_err(|source| {
        invalid_root_path(
            path,
            format!("failed to stat current root capability: {source}"),
        )
    })?;
    if current_root != expected_root {
        return Err(invalid_root_path(
            path,
            "registered local root was replaced after this scan began",
        ));
    }

    let mut parent_relative = "";
    for (depth, frame) in session.directories.iter().enumerate() {
        if depth == 0 {
            if !frame.relative_dir.is_empty() {
                return Err(MemoryError::InvalidMemoryContinuation {
                    detail: "local scan session has an invalid root frame".to_string(),
                });
            }
        } else {
            let prefix = if parent_relative.is_empty() {
                "".to_string()
            } else {
                format!("{parent_relative}/")
            };
            let child = frame.relative_dir.strip_prefix(&prefix).ok_or_else(|| {
                MemoryError::InvalidMemoryContinuation {
                    detail: "local scan session directory frames are not an ancestor chain"
                        .to_string(),
                }
            })?;
            if !is_safe_entry_name(child) {
                return Err(MemoryError::InvalidMemoryContinuation {
                    detail: "local scan session contains an unsafe directory frame".to_string(),
                });
            }
            current = current.open_dir_nofollow(child).map_err(|source| {
                invalid_root_path(
                    path,
                    format!(
                        "active scan directory {} was replaced or became a symlink: {source}",
                        frame.relative_dir
                    ),
                )
            })?;
        }
        let current_identity = directory_identity(&current).map_err(|source| {
            invalid_root_path(
                path,
                format!("failed to stat active scan directory: {source}"),
            )
        })?;
        let retained_identity = directory_identity(&frame.dir).map_err(|source| {
            invalid_root_path(
                path,
                format!("failed to stat retained scan directory: {source}"),
            )
        })?;
        if current_identity != retained_identity {
            return Err(invalid_root_path(
                path,
                format!(
                    "active scan directory {} was replaced after this scan began",
                    frame.relative_dir
                ),
            ));
        }
        parent_relative = &frame.relative_dir;
    }
    Ok(())
}

fn read_only_nofollow_options() -> CapabilityOpenOptions {
    let mut options = CapabilityOpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options
}

fn open_regular_file_for_scan(
    parent: &CapabilityDir,
    name: OsString,
    path: &Path,
) -> Result<(CapabilityFile, CapabilityMetadata), MemoryError> {
    let file = parent
        .open_with(name, &read_only_nofollow_options())
        .map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
    let metadata = file.metadata().map_err(|source| MemoryError::Io {
        path: path.display().to_string(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(MemoryError::InvalidSourceCandidate {
            path: path.display().to_string(),
            detail: "scan entry changed and is not a regular non-symlink file".to_string(),
        });
    }
    Ok((file, metadata))
}

fn safe_relative_segments(relative: &str) -> Option<Vec<OsString>> {
    let mut segments = Vec::new();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(segment) => segments.push(segment.to_os_string()),
            _ => return None,
        }
    }
    (!segments.is_empty()).then_some(segments)
}

fn open_relative_regular_file(
    root: &IndexRoot,
    relative: &str,
    expected_path: &Path,
) -> Result<(CapabilityFile, CapabilityMetadata), MemoryError> {
    let segments =
        safe_relative_segments(relative).ok_or_else(|| MemoryError::InvalidSourceCandidate {
            path: relative.to_string(),
            detail: "source path is not a non-empty safe relative path".to_string(),
        })?;
    let (file_name, parents) = segments.split_last().expect("segments are non-empty");
    let mut dir = open_registered_root(root)?;
    for parent in parents {
        dir = dir.open_dir_nofollow(parent).map_err(|source| {
            MemoryError::InvalidSourceCandidate {
                path: expected_path.display().to_string(),
                detail: format!(
                    "source ancestor is missing, replaced, or a symlink/reparse point: {source}"
                ),
            }
        })?;
    }
    let file = dir
        .open_with(file_name, &read_only_nofollow_options())
        .map_err(|source| MemoryError::InvalidSourceCandidate {
            path: expected_path.display().to_string(),
            detail: format!(
                "source is missing, replaced, or not a regular non-symlink file: {source}"
            ),
        })?;
    let metadata = file
        .metadata()
        .map_err(|source| MemoryError::InvalidSourceCandidate {
            path: expected_path.display().to_string(),
            detail: format!("failed to stat source through its capability: {source}"),
        })?;
    if !metadata.is_file() {
        return Err(MemoryError::InvalidSourceCandidate {
            path: expected_path.display().to_string(),
            detail: "source is not a regular file".to_string(),
        });
    }
    Ok((file, metadata))
}

fn open_validated_candidate(
    root: &IndexRoot,
    source: &SourceCandidate,
) -> Result<(CapabilityFile, CapabilityMetadata), MemoryError> {
    let expected_path =
        join_relative(&root.config.path, &source.file_ref.path_hint).map_err(|_| {
            MemoryError::InvalidSourceCandidate {
                path: source.file_ref.path_hint.clone(),
                detail: "source path is not a safe relative path".to_string(),
            }
        })?;
    if source.file_ref.root_id != root.root_id
        || source.file_ref.connector_id != root.connector_id
        || source.absolute_path != expected_path
        || !is_included(root, &expected_path)
    {
        return Err(MemoryError::InvalidSourceCandidate {
            path: source.absolute_path.display().to_string(),
            detail: "candidate identity, path, connector, or extension does not match its root"
                .to_string(),
        });
    }
    let (file, metadata) =
        open_relative_regular_file(root, &source.file_ref.path_hint, &expected_path)?;
    let identity = platform_identity(&metadata);
    let hint = source_version_hint(&metadata, &identity)?;
    let expected_object_id = Id::derived(
        "memory-file",
        format!("{}\0{}", root.root_id, identity).as_bytes(),
    );
    if source.file_ref.object_id != expected_object_id
        || source.platform_identity != identity
        || source.source_version_hint != hint
        || source.byte_length != metadata.len()
        || source.media_type != media_type_for_path(&expected_path)
    {
        return Err(MemoryError::SourceChanged {
            path: expected_path.display().to_string(),
        });
    }
    Ok((file, metadata))
}

fn is_included(root: &IndexRoot, path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
        return false;
    };
    root.config
        .include_extensions
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(extension))
}

fn media_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some(extension) if extension.eq_ignore_ascii_case("md") => "text/markdown",
        Some(extension) if extension.eq_ignore_ascii_case("markdown") => "text/markdown",
        _ => "text/plain",
    }
}

fn platform_identity(metadata: &CapabilityMetadata) -> String {
    format!("file:{}:{}", metadata.dev(), metadata.ino())
}

fn source_version_hint(
    metadata: &CapabilityMetadata,
    identity: &str,
) -> Result<String, MemoryError> {
    let modified = metadata.modified().map_err(|source| MemoryError::Io {
        path: identity.to_string(),
        source,
    })?;
    let duration = modified
        .into_std()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(format!(
        "{}:{}:{}:{}",
        identity,
        metadata.len(),
        duration.as_secs(),
        duration.subsec_nanos()
    ))
}

pub(super) fn descriptor_identity_is_current(
    root: &IndexRoot,
    source: &super::types::SourceDescriptor,
) -> bool {
    if source.version.file_ref.root_id != root.root_id
        || source.version.file_ref.connector_id != root.connector_id
    {
        return false;
    }
    let Ok(path) = join_relative(&root.config.path, &source.version.file_ref.path_hint) else {
        return false;
    };
    if !is_included(root, &path) {
        return false;
    }
    let Ok((_, metadata)) =
        open_relative_regular_file(root, &source.version.file_ref.path_hint, &path)
    else {
        return false;
    };
    metadata.len() <= root.config.max_source_bytes
        && platform_identity(&metadata) == source.platform_identity
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed::{IndexRootPolicy, LocalIndexRootConfig, SourceRetentionCapability};
    use time::OffsetDateTime;

    struct TestRoot {
        base: PathBuf,
        source: PathBuf,
    }

    impl TestRoot {
        fn new(name: &str) -> Self {
            let base = std::env::temp_dir()
                .join(format!("sinomo-local-connector-{name}-{}", Id::generate()));
            let source = base.join("source");
            fs::create_dir_all(&source).expect("create source root");
            Self { base, source }
        }

        fn index_root(&self, max_source_bytes: u64) -> IndexRoot {
            let canonical = fs::canonicalize(&self.source).expect("canonical source root");
            let mut config =
                LocalIndexRootConfig::new(&canonical, IndexRootPolicy::cluster_local());
            config.max_source_bytes = max_source_bytes;
            IndexRoot {
                root_id: Id::derived(
                    "test-memory-index-root",
                    canonical.to_string_lossy().as_bytes(),
                ),
                authorization_epoch: Id::derived(
                    "test-memory-index-root-authorization",
                    canonical.to_string_lossy().as_bytes(),
                ),
                connector_id: "sinomo.memory.local/v1".to_string(),
                config,
                retention: SourceRetentionCapability::CurrentOnly,
                registered_at: OffsetDateTime::UNIX_EPOCH,
            }
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.base);
        }
    }

    fn last_examined_entries() -> usize {
        LAST_LOCAL_SCAN_EXAMINED_ENTRIES.with(std::cell::Cell::get)
    }

    #[test]
    fn scan_page_bounds_examined_excluded_and_oversized_entries() {
        let fixture = TestRoot::new("bounded-work");
        for index in 0..20 {
            fs::write(
                fixture.source.join(format!("excluded-{index:02}.bin")),
                b"x",
            )
            .expect("write excluded source");
            fs::write(
                fixture.source.join(format!("oversized-{index:02}.md")),
                b"too large",
            )
            .expect("write oversized source");
        }
        fs::write(fixture.source.join("valid.md"), b"ok").expect("write valid source");
        let root = fixture.index_root(4);
        let connector = LocalSourceConnector;

        let first = connector.scan_page(&root, None, 5).expect("first page");
        assert_eq!(last_examined_entries(), 5);
        assert!(first.continuation.is_some());

        let mut candidates = first.sources;
        let mut continuation = first.continuation;
        let mut pages = 1usize;
        while let Some(cursor) = continuation {
            let page = connector
                .scan_page(&root, Some(&cursor), 5)
                .expect("continued page");
            assert!(last_examined_entries() <= 5);
            candidates.extend(page.sources);
            continuation = page.continuation;
            pages += 1;
        }
        assert!(pages > 1);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].file_ref.path_hint, "valid.md");
    }

    #[cfg(unix)]
    #[test]
    fn registered_root_symlink_replacement_fails_closed() {
        use std::os::unix::fs::symlink;

        let fixture = TestRoot::new("root-symlink");
        let outside = fixture.base.join("outside");
        fs::create_dir_all(&outside).expect("create outside directory");
        fs::write(outside.join("secret.md"), b"outside").expect("write outside source");
        let root = fixture.index_root(1024);
        let original = fixture.base.join("source-original");
        fs::rename(&fixture.source, &original).expect("move registered root");
        symlink(&outside, &fixture.source).expect("replace root with symlink");

        assert!(matches!(
            LocalSourceConnector.scan_page(&root, None, 1),
            Err(MemoryError::InvalidIndexRoot { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn resumed_scan_rejects_intermediate_directory_symlink_replacement() {
        use std::os::unix::fs::symlink;

        let fixture = TestRoot::new("intermediate-symlink");
        let nested = fixture.source.join("nested");
        let outside = fixture.base.join("outside");
        fs::create_dir_all(&nested).expect("create nested directory");
        fs::create_dir_all(&outside).expect("create outside directory");
        fs::write(nested.join("inside.md"), b"inside").expect("write inside source");
        fs::write(outside.join("secret.md"), b"outside").expect("write outside source");
        let root = fixture.index_root(1024);

        let first = LocalSourceConnector
            .scan_page(&root, None, 1)
            .expect("open nested scan frame");
        let cursor = first.continuation.expect("nested scan continuation");
        let original = fixture.source.join("nested-original");
        fs::rename(&nested, &original).expect("move nested directory");
        symlink(&outside, &nested).expect("replace nested directory with symlink");

        assert!(matches!(
            LocalSourceConnector.scan_page(&root, Some(&cursor), 1),
            Err(MemoryError::InvalidIndexRoot { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_rejects_candidate_after_intermediate_symlink_replacement() {
        use std::os::unix::fs::symlink;

        let fixture = TestRoot::new("candidate-symlink");
        let nested = fixture.source.join("nested");
        let outside = fixture.base.join("outside");
        fs::create_dir_all(&nested).expect("create nested directory");
        fs::create_dir_all(&outside).expect("create outside directory");
        fs::write(nested.join("inside.md"), b"inside").expect("write inside source");
        fs::write(outside.join("inside.md"), b"outside").expect("write outside source");
        let root = fixture.index_root(1024);
        let candidate = LocalSourceConnector
            .scan_page(&root, None, 8)
            .expect("scan candidate")
            .sources
            .into_iter()
            .next()
            .expect("inside candidate");

        let original = fixture.source.join("nested-original");
        fs::rename(&nested, &original).expect("move nested directory");
        symlink(&outside, &nested).expect("replace nested directory with symlink");

        assert!(matches!(
            LocalSourceConnector.acquire_snapshot(&root, &candidate),
            Err(MemoryError::InvalidSourceCandidate { .. })
        ));
    }
}
