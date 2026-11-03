//! Host-local working copies. Only the editor helper holds the writeback queue;
//! DFS reads and mutations always go through the authenticated browser client.
mod lifetime;
pub use lifetime::monitor;

use fs2::FileExt;
use notify::Watcher;
use serde::{Deserialize, Serialize};
use sinomo_client::{
    SifsBrowser,
    api::{
        DaemonDfsBrowserDurableReplaceOutcome as Outcome,
        DaemonDfsBrowserDurableReplaceRequest as Request,
    },
};
use sinomo_runtime::{
    core::storage::durable_file::{self as durable, DurableAtomicFileOptions},
    dfs::{SifsMountPlatform, SifsMountRuntimePhase, SifsMountRuntimeStatus},
    file_sync::*,
    local::SinomoDataDir,
};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

const OWNER: &str = "module.files.external-edit";
const LIMIT: u64 = sinomo_client::api::DAEMON_DFS_BROWSER_DURABLE_REPLACE_MAX_BYTES;
type Result<T> = std::result::Result<T, String>;
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn digest(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}
fn id() -> String {
    sinomo_runtime::core::Id::generate().to_string()
}
fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    durable::write_file_atomic(
        path,
        bytes,
        DurableAtomicFileOptions::new("edit", "publish").sync_all(),
    )
    .map_err(err)
}
fn link(m: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        m.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        m.file_type().is_symlink()
    }
}
fn checked_dir(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(err)?;
    if !m.is_dir() || link(&m) {
        return Err(format!(
            "Not a private working directory: {}",
            path.display()
        ));
    }
    Ok(())
}
fn private_dir(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).is_ok() {
        checked_dir(path)?;
    }
    durable::ensure_private_directory(path).map_err(err)?;
    checked_dir(path)
}
fn read(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let m = fs::symlink_metadata(path).map_err(err)?;
    if !m.is_file() || link(&m) || m.len() > limit {
        return Err("Working file is unavailable, linked, or exceeds the editor limit".into());
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options.open(path).map_err(err)?;
    let opened = file.metadata().map_err(err)?;
    if !opened.is_file()
        || link(&opened)
        || opened.len() != m.len()
        || opened.modified().ok() != m.modified().ok()
    {
        return Err("Source changed during open".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.nlink() != 1 || opened.dev() != m.dev() || opened.ino() != m.ino() {
            return Err("Working copy must be a single unlinked file identity".into());
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        // The owned file handle and the output structure are live for this call.
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0
            || info.nNumberOfLinks != 1
        {
            return Err("Working copy must have one file identity".into());
        }
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes).map_err(err)?;
    let after = fs::symlink_metadata(path).map_err(err)?;
    if link(&after)
        || bytes.len() as u64 != m.len()
        || after.len() != m.len()
        || m.modified().ok() != after.modified().ok()
    {
        return Err("File changed during capture; waiting for a stable save".into());
    }
    Ok(bytes)
}
fn scoped_id(node: &str, object: &str) -> String {
    blake3::hash(format!("{node}\0{object}").as_bytes())
        .to_hex()
        .to_string()
}
fn valid_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}
fn root(data: &SinomoDataDir) -> PathBuf {
    data.module_runtime_dir().join(OWNER).join("sessions")
}
fn work_name(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or("file");
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_control() || "<>:\"/\\|?*".contains(c) {
                '_'
            } else {
                c
            }
        })
        .collect();
    let safe = safe.trim_end_matches(['.', ' ']);
    let base = safe.split('.').next().unwrap_or("").to_ascii_uppercase();
    if safe.is_empty()
        || matches!(base.as_str(), "CON" | "NUL" | "PRN" | "AUX")
        || base
            .strip_prefix("COM")
            .or_else(|| base.strip_prefix("LPT"))
            .is_some_and(|n| {
                matches!(
                    n,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
    {
        format!("file-{safe}")
    } else {
        safe.to_owned()
    }
}
fn local_mount(data: &SinomoDataDir, node: &str, path: &str) -> Option<PathBuf> {
    use sinomo_client::api::{DaemonPaths, DaemonProbeStatus, probe_daemon_status};
    // Resolve the local process from its DATA ownership evidence. Never treat
    // a remote node's reported mount point as a path on the UI's computer.
    let DaemonProbeStatus::Running(status) =
        probe_daemon_status(&DaemonPaths::new(data), Duration::from_secs(1))
    else {
        return None;
    };
    if status.data_dir.canonicalize().ok()? != data.root().canonicalize().ok()? {
        return None;
    }
    let local = SifsBrowser::open_daemon(status.address).ok()?;
    if local.node_id() != node {
        return None;
    }
    mounted(&local.live_mount_status().ok()?, path)
}
fn mounted(status: &SifsMountRuntimeStatus, path: &str) -> Option<PathBuf> {
    if status.platform != SifsMountPlatform::current()
        || status.read_only
        || status.unregister_pending
    {
        return None;
    }
    match status.platform {
        SifsMountPlatform::ExternalProvider if status.phase==SifsMountRuntimePhase::Connected && status.sync_local_changes && crate::backend::mounts::validate_connected_host_filesystem_provider_status_at(status,time::OffsetDateTime::now_utc()).is_ok() => {},
        SifsMountPlatform::UnixFuse if status.phase==SifsMountRuntimePhase::Mounted => {}, _ => return None,
    }
    let root = status.root_path.trim_end_matches('/');
    let relative = path.strip_prefix(root)?.strip_prefix('/')?;
    let mount = Path::new(status.resolved_mount_point.as_deref()?);
    if !mount.is_absolute() {
        return None;
    }
    let mount = mount.canonicalize().ok()?;
    let mut candidate = mount.clone();
    for part in relative.split('/') {
        if part.is_empty() || part == "." || part == ".." || work_name(part) != part {
            return None;
        }
        candidate.push(part);
    }
    let real = candidate.canonicalize().ok()?;
    (real.starts_with(&mount) && real.is_file()).then_some(candidate)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Session {
    address: String,
    node: String,
    path: String,
    object: String,
    version: String,
    hash: String,
    finished: bool,
    #[serde(default)]
    last_action: Option<String>,
    status: String,
}
impl Session {
    fn summary(&self, dir: &Path) -> String {
        let mut text = match &self.last_action {
            Some(action) => format!("{} · Last action: {action}", self.status),
            None => self.status.clone(),
        };
        if lifetime::ambiguous(dir) {
            text.push_str(" · Untracked editor handoff. Finish manually or keep until reboot.");
        }
        text
    }

    fn store(&self, dir: &Path) -> Result<()> {
        write(
            &dir.join("session.sobj"),
            &sinomo_runtime::module_api::encode_sobj(self).map_err(err)?,
        )
    }
    fn load(dir: &Path) -> Result<Self> {
        checked_dir(dir)?;
        let state: Self =
            sinomo_runtime::module_api::decode_sobj(&read(&dir.join("session.sobj"), 128 * 1024)?)
                .map_err(err)?;
        if dir.file_name().and_then(|n| n.to_str())
            != Some(scoped_id(&state.node, &state.object).as_str())
        {
            return Err("External edit identity mismatch".into());
        }
        Ok(state)
    }
    fn working(&self, dir: &Path) -> PathBuf {
        dir.join("working").join(work_name(&self.path))
    }
    fn status(&mut self, dir: &Path, text: String) -> Result<()> {
        if self.status != text {
            self.status = text;
            self.store(dir)?;
        }
        Ok(())
    }
}
fn snapshot(browser: &SifsBrowser, path: &str) -> Result<(String, String, String, Vec<u8>)> {
    let d = browser.stat(path)?;
    if d.kind != "file" || d.mode & 0o222 == 0 {
        return Err("Select a writable regular SIFS file".into());
    }
    let size = d.size_bytes.ok_or("Missing file size")?;
    if size > LIMIT {
        return Err("External working-copy limit is 32 MiB".into());
    }
    let hash = d.content_hash.ok_or("Missing SIFS content hash")?;
    let bytes = browser.read_object_version(&d.object_id, &d.version, &hash, size)?;
    if bytes.len() as u64 != size || digest(&bytes) != hash {
        return Err("Working-copy snapshot failed verification".into());
    }
    Ok((d.object_id, d.version, hash, bytes))
}
fn launch(path: &Path, folder: bool) -> Result<()> {
    #[cfg(windows)]
    if !folder {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::{
            System::Com::{COINIT_APARTMENTTHREADED, CoInitializeEx, CoUninitialize},
            UI::Shell::{OAIF_EXEC, OPENASINFO, SHOpenWithDialog},
        };
        let name: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        // This runs on the Files operations thread. The chooser receives a local
        // filename, never a shell command or an executable supplied by SIFS.
        let initialized =
            unsafe { CoInitializeEx(std::ptr::null(), COINIT_APARTMENTTHREADED as u32) };
        if initialized < 0 {
            return Err(format!(
                "Cannot initialize the editor chooser: {initialized:#x}"
            ));
        }
        let info = OPENASINFO {
            pcszFile: name.as_ptr(),
            pcszClass: std::ptr::null(),
            oaifInFlags: OAIF_EXEC,
        };
        let result = unsafe { SHOpenWithDialog(std::ptr::null_mut(), &info) };
        unsafe { CoUninitialize() };
        return if result >= 0 {
            Ok(())
        } else {
            Err(format!(
                "External editor selection was cancelled or failed ({result:#x})"
            ))
        };
    }
    #[cfg(windows)]
    let mut command = Command::new(
        PathBuf::from(std::env::var_os("SystemRoot").ok_or("Missing Windows system directory")?)
            .join("explorer.exe"),
    );
    #[cfg(target_os = "macos")]
    let mut command = Command::new("open");
    #[cfg(all(not(windows), not(target_os = "macos")))]
    let mut command = Command::new("xdg-open");
    let _ = folder;
    command.arg(path).spawn().map_err(err)?;
    Ok(())
}
fn spawn_helper(data: &SinomoDataDir, key: &str) -> Result<()> {
    sinomo_runtime::local::ensure_deployment_enabled(&data.root()).map_err(err)?;
    let lease = durable::open_file_for_append(&root(data).join(key).join("helper.lock"))
        .map_err(err)?
        .file;
    match lease.try_lock_exclusive() {
        Ok(()) => drop(lease),
        Err(e) if e.raw_os_error() == fs2::lock_contended_error().raw_os_error() => return Ok(()),
        Err(e) => return Err(err(e)),
    }
    let mut cmd = Command::new(std::env::current_exe().map_err(err)?);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    cmd.arg("--external-edit-worker")
        .arg(key)
        .env("SINOMO_DATA_DIR", data.root())
        .spawn()
        .map_err(err)?;
    Ok(())
}
pub fn resume_sessions() {
    let Ok(data) = SinomoDataDir::from_env_or_home() else {
        return;
    };
    let Some(entries) = session_directories(&data) else {
        return;
    };
    for e in entries.flatten() {
        let key = e.file_name();
        let Some(key) = key.to_str() else { continue };
        if valid_id(key)
            && Session::load(&e.path())
                .is_ok_and(|s| !s.finished || e.path().join("retiring").exists())
        {
            let _ = spawn_helper(&data, key);
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    Open,
    Status,
    Reveal,
    Retry,
    Reload,
    Finish,
}
impl Action {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Open => "Open externally",
            Self::Status => "Working-copy status",
            Self::Reveal => "Reveal working copy",
            Self::Retry => "Retry synchronization",
            Self::Reload => "Keep a copy and reload remote",
            Self::Finish => "Finish external editing",
        }
    }
}
fn session_directories(data: &SinomoDataDir) -> Option<fs::ReadDir> {
    checked_dir(&data.module_runtime_dir().join(OWNER)).ok()?;
    checked_dir(&root(data)).ok()?;
    fs::read_dir(root(data)).ok()
}
fn find_session(
    data: &SinomoDataDir,
    address: &str,
    path: &str,
    object: Option<&str>,
    node: Option<&str>,
) -> Result<Option<(PathBuf, Session)>> {
    let Some(entries) = session_directories(data) else {
        return Ok(None);
    };
    let mut matching = entries
        .flatten()
        .filter_map(|e| Session::load(&e.path()).ok().map(|s| (e.path(), s)))
        .filter(|(_, s)| {
            !s.finished
                && s.address == address
                && s.path == path
                && object.is_none_or(|id| s.object == id)
                && node.is_none_or(|id| s.node == id)
        });
    let selected = matching.next();
    if matching.next().is_some() {
        return Err(
            "Multiple working copies match this path. Choose the copy under Storage health.".into(),
        );
    }
    Ok(selected)
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Summary {
    pub key: String,
    pub path: String,
    pub status: String,
}
pub(crate) fn active(data: &SinomoDataDir) -> Vec<Summary> {
    let Some(entries) = session_directories(data) else {
        return Vec::new();
    };
    let mut sessions: Vec<_> = entries
        .flatten()
        .filter_map(|e| {
            let key = e.file_name().into_string().ok()?;
            if !valid_id(&key) {
                return None;
            }
            let s = Session::load(&e.path()).ok()?;
            (!s.finished).then_some(Summary {
                key,
                status: s.summary(&e.path()),
                path: s.path,
            })
        })
        .collect();
    sessions.sort_by(|a, b| a.path.cmp(&b.path).then(a.key.cmp(&b.key)));
    sessions
}
pub(crate) fn act_on_session(data: &SinomoDataDir, key: &str, action: Action) -> Result<String> {
    if !valid_id(key) {
        return Err("Invalid session".into());
    }
    checked_dir(&data.module_runtime_dir().join(OWNER))?;
    checked_dir(&root(data))?;
    let dir = root(data).join(key);
    let s = Session::load(&dir)?;
    if s.finished {
        return Err("External editing is already finished".into());
    }
    manage(data, &dir, s, action)
}
fn manage(data: &SinomoDataDir, dir: &Path, state: Session, action: Action) -> Result<String> {
    let key = scoped_id(&state.node, &state.object);
    let path = &state.path;
    let working = state.working(&dir);
    match action {
        Action::Open => {
            checked_dir(&dir.join("working"))?;
            read(&working, LIMIT)?;
            let _lock = lifetime::lock(dir)?;
            let current = Session::load(dir)?;
            if current.finished {
                return Err("Working copy has closed; open the SIFS file again".into());
            }
            lifetime::initialize_locked(dir)?;
            lifetime::open_locked(data, dir, &key)?;
        }
        Action::Status => {}
        Action::Reveal => launch(&dir.join("working"), true)?,
        other => {
            let command = match other {
                Action::Reload => "reload",
                Action::Retry => "retry",
                Action::Finish => "finish",
                _ => unreachable!(),
            };
            write(&dir.join("control"), command.as_bytes())?;
            spawn_helper(data, &key)?;
            return Ok(format!("Requested {command} for {path}"));
        }
    }
    Ok(format!(
        "Last status: {} · {}",
        state.summary(dir),
        working.display()
    ))
}
pub(crate) fn act(
    data: &SinomoDataDir,
    address: &str,
    path: &str,
    action: Action,
) -> Result<String> {
    // An existing working copy belongs to its object, not to a reusable pathname.
    let connected = (action == Action::Open).then(|| SifsBrowser::open_daemon(address));
    let head = match connected.as_ref() {
        Some(Ok(browser)) => Some(browser.stat(path)?),
        _ => None,
    };
    if let Some((dir, state)) = find_session(
        data,
        address,
        path,
        head.as_ref().map(|d| d.object_id.as_str()),
        connected
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .map(|b| b.node_id()),
    )? {
        return manage(data, &dir, state, action);
    }
    if action != Action::Open {
        return Err("No active working copy for this file".into());
    }
    let browser = connected.ok_or("Missing open request")??;
    let details = head.ok_or("Missing file metadata")?;
    if details.kind != "file" || details.mode & 0o222 == 0 {
        return Err("Select a writable regular SIFS file".into());
    }
    if let Some(local) = local_mount(data, browser.node_id(), path) {
        launch(&local, false)?;
        return Ok(format!("Opened through SIFS mount: {}", local.display()));
    }
    let (object, version, hash, bytes) = snapshot(&browser, path)?;
    let key = scoped_id(browser.node_id(), &object);
    let probe = Request {
        expected_node_id: browser.node_id().into(),
        path: path.into(),
        expected_object_id: object.clone(),
        expected_version: version.clone(),
        operation_id: id(),
        content_hash: hash.clone(),
        size_bytes: bytes.len() as u64,
        bytes: None,
    };
    if !matches!(browser.durable_replace(&probe)?, Outcome::NotApplied) {
        return Err("DFS cannot currently confirm durable writeback capability".into());
    }
    durable::ensure_directory(&data.module_runtime_dir()).map_err(err)?;
    private_dir(&data.module_runtime_dir().join(OWNER))?;
    private_dir(&root(data))?;
    let dir = root(data).join(&key);
    private_dir(&dir)?;
    let lock = durable::open_file_for_append(&dir.join("prepare.lock"))
        .map_err(err)?
        .file;
    lock.lock_exclusive().map_err(err)?;
    private_dir(&dir.join("working"))?;
    let mut state = Session {
        address: address.into(),
        node: browser.node_id().into(),
        path: path.into(),
        object,
        version,
        hash,
        finished: false,
        last_action: None,
        status: "Synced".into(),
    };
    let working = state.working(&dir);
    let mut reopen_lease = None;
    if dir.join("session.sobj").exists() {
        let prior = Session::load(&dir)?;
        if prior.path != path || prior.address != address {
            return Err(
                "This object already has a working copy at another path or connection".into(),
            );
        }
        if !prior.finished {
            // Another opener won preparation; never overwrite the helper's manifest.
            read(&working, LIMIT)?;
            lifetime::initialize_locked(&dir)?;
            lifetime::open_locked(data, &dir, &key)?;
            return Ok(format!(
                "Opened existing working copy: {}",
                working.display()
            ));
        }
        // Wait for a finishing helper to release ownership before reactivating the session.
        let lease = durable::open_file_for_append(&dir.join("helper.lock"))
            .map_err(err)?
            .file;
        lease
            .try_lock_exclusive()
            .map_err(|_| "External editing is finishing. Retry opening the file.".to_string())?;
        reopen_lease = Some(lease);
        recover_claim(&dir, &prior)?;
        lifetime::clear(&dir)?;
        if working.exists() && digest(&read(&working, LIMIT)?) != prior.hash {
            state = prior;
            state.finished = false;
        } else {
            write(&working, &bytes)?;
        }
    } else if working.exists() {
        if digest(&read(&working, LIMIT)?) != state.hash {
            return Err(format!(
                "Unclaimed working bytes retained at {}",
                working.display()
            ));
        }
    } else {
        durable::write_file_create_new_atomic(
            &working,
            &bytes,
            DurableAtomicFileOptions::new("working", "checkout").sync_all(),
        )
        .map_err(err)?;
    }
    state.store(&dir)?;
    drop(reopen_lease);
    lifetime::open_locked(data, &dir, &key)?;
    Ok(format!("Opened working copy: {}", working.display()))
}

trait EditRemote {
    fn identity(&self) -> &str;
    fn replace(&self, request: &Request) -> Result<Outcome>;
    fn head(&self, path: &str) -> Result<(String, String)>;
    fn checkout(&self, path: &str) -> Result<(String, String, String, Vec<u8>)>;
}
impl EditRemote for SifsBrowser {
    fn identity(&self) -> &str {
        self.node_id()
    }
    fn replace(&self, request: &Request) -> Result<Outcome> {
        self.durable_replace(request)
    }
    fn head(&self, path: &str) -> Result<(String, String)> {
        let head = self.stat(path)?;
        Ok((head.object_id, head.version))
    }
    fn checkout(&self, path: &str) -> Result<(String, String, String, Vec<u8>)> {
        snapshot(self, path)
    }
}
struct Adapter<'a>(&'a dyn EditRemote);
impl Adapter<'_> {
    fn call(&self, c: &WritebackDfsMutationCommand, apply: bool) -> Result<Outcome> {
        let source = c
            .preconditions
            .source
            .as_ref()
            .ok_or("Missing base version")?;
        let content = c.staged_content.as_ref().ok_or("Missing staged content")?;
        self.0.replace(&Request {
            expected_node_id: self.0.identity().into(),
            path: c.source_path.clone(),
            expected_object_id: source.object_id.clone(),
            expected_version: source.version.clone(),
            operation_id: c.operation_id.clone(),
            content_hash: content.descriptor.content_hash.clone(),
            size_bytes: content.descriptor.length,
            bytes: if apply {
                Some(read(&content.blob_path, LIMIT)?)
            } else {
                None
            },
        })
    }
}
fn evidence(
    c: &WritebackDfsMutationCommand,
    operation_id: String,
    r: sinomo_client::api::DaemonDfsBrowserWriteReceipt,
) -> Result<WritebackRemoteMutationEvidence> {
    let source = c.preconditions.source.as_ref().ok_or("Missing source")?;
    let content = c.staged_content.as_ref().ok_or("Missing staged content")?;
    if operation_id != c.operation_id
        || r.object_id != source.object_id
        || r.content_hash.as_deref() != Some(content.descriptor.content_hash.as_str())
        || r.size_bytes != Some(content.descriptor.length)
        || r.version.is_empty()
    {
        return Err("DFS receipt does not match staged replacement".into());
    }
    Ok(WritebackRemoteMutationEvidence {
        operation_id: c.operation_id.clone(),
        object_id: Some(r.object_id),
        version: Some(r.version),
        content_hash: r.content_hash,
        content_size_bytes: r.size_bytes,
        kind: Some("file".into()),
        detail: "Version committed".into(),
    })
}
fn conflict(detail: String) -> WritebackConflictEvidence {
    WritebackConflictEvidence {
        code: "external-edit-conflict".into(),
        detail,
        observed_object_id: None,
        observed_version: None,
        observed_content_hash: None,
    }
}
impl WritebackDfsMutationAdapter for Adapter<'_> {
    fn apply(&mut self, c: &WritebackDfsMutationCommand) -> Result<WritebackDfsMutationOutcome> {
        Ok(match self.call(c, true)? {
            Outcome::Applied {
                operation_id,
                receipt,
                ..
            } => WritebackDfsMutationOutcome::Applied(evidence(c, operation_id, receipt)?),
            Outcome::Conflict { detail } => WritebackDfsMutationOutcome::Conflict(conflict(detail)),
            Outcome::Unknown { detail } => WritebackDfsMutationOutcome::Ambiguous { detail },
            Outcome::NotApplied => WritebackDfsMutationOutcome::Ambiguous {
                detail: "Write has no receipt yet".into(),
            },
        })
    }
    fn reconcile(
        &mut self,
        c: &WritebackDfsMutationCommand,
    ) -> Result<WritebackDfsMutationReconciliation> {
        Ok(match self.call(c, false)? {
            Outcome::Applied {
                operation_id,
                receipt,
                ..
            } => WritebackDfsMutationReconciliation::Applied(evidence(c, operation_id, receipt)?),
            Outcome::Conflict { detail } => {
                WritebackDfsMutationReconciliation::Conflict(conflict(detail))
            }
            Outcome::Unknown { detail } => WritebackDfsMutationReconciliation::Ambiguous { detail },
            Outcome::NotApplied => WritebackDfsMutationReconciliation::NotApplied,
        })
    }
}
struct Complete<'a>(&'a Path, &'a mut Session);
impl WritebackLocalCompletionEffect for Complete<'_> {
    fn complete(
        &mut self,
        _: &WritebackDfsMutationCommand,
        e: &WritebackRemoteMutationEvidence,
    ) -> Result<WritebackLocalCompletionOutcome> {
        self.1.version = e.version.clone().ok_or("Missing version receipt")?;
        self.1.hash = e.content_hash.clone().ok_or("Missing hash receipt")?;
        self.1.store(self.0)?;
        Ok(WritebackLocalCompletionOutcome::Completed)
    }
}
fn pending(q: &DurableWritebackQueue) -> Option<WritebackLocalOperation> {
    q.operations()
        .find(|op| {
            !matches!(
                op.status,
                WritebackLocalOperationStatus::Completed
                    | WritebackLocalOperationStatus::Superseded
            )
        })
        .cloned()
}
// A rename claims exactly the file being verified. After a crash a claim is
// either restored to an active session or removed against its durable receipt.
fn recover_claim(dir: &Path, s: &Session) -> Result<()> {
    let claim = dir.join("retiring");
    if !claim.try_exists().map_err(err)? {
        return Ok(());
    }
    let bytes = read(&claim, LIMIT)?;
    if !(s.finished && digest(&bytes) == s.hash) {
        let working = s.working(dir);
        let dest = if working.try_exists().map_err(err)? {
            dir.join("working").join(format!("preserved-{}", id()))
        } else {
            working
        };
        durable::write_file_create_new_atomic(
            &dest,
            &bytes,
            DurableAtomicFileOptions::new("working", "restore").sync_all(),
        )
        .map_err(err)?;
    }
    durable::remove_file_if_exists_and_sync_parent(&claim).map_err(err)?;
    Ok(())
}

fn finish_copy(
    dir: &Path,
    s: &mut Session,
    q: &DurableWritebackQueue,
    automatic: bool,
) -> Result<Option<String>> {
    let lock = durable::open_file_for_append(&dir.join("prepare.lock"))
        .map_err(err)?
        .file;
    if lock.try_lock_exclusive().is_err() {
        return Ok(None);
    }
    if automatic && !lifetime::automatic(dir)? {
        return Ok(None);
    }
    if pending(q).is_some() {
        return Ok(None);
    }
    let working = s.working(dir);
    recover_claim(dir, s)?;
    if digest(&read(&working, LIMIT)?) != s.hash {
        return Ok(None);
    }
    let claim = dir.join("retiring");
    fs::rename(&working, &claim).map_err(err)?;
    if digest(&read(&claim, LIMIT)?) != s.hash || working.try_exists().map_err(err)? {
        recover_claim(dir, s)?;
        return Ok(None);
    }
    let mut closed = s.clone();
    closed.finished = true;
    closed.status = "Finished; synchronized working copy removed".into();
    if let Err(error) = closed.store(dir) {
        recover_claim(dir, s)?;
        return Err(error);
    }
    *s = closed;
    recover_claim(dir, s)?;
    lifetime::clear(dir)?;
    Ok(Some(s.status.clone()))
}

fn tick<B: EditRemote>(
    connect: impl FnOnce(&str) -> Result<B>,
    dir: &Path,
    s: &mut Session,
    q: &mut DurableWritebackQueue,
) -> Result<String> {
    checked_dir(&dir.join("working"))?;
    let working = s.working(dir);
    let control = dir.join("control");
    let cmd = if control.exists() {
        String::from_utf8(read(&control, 64)?).map_err(err)?
    } else {
        String::new()
    };
    if !cmd.is_empty() {
        durable::remove_file_if_exists_and_sync_parent(&control).map_err(err)?;
    }
    let result = sync_step(connect, dir, s, q, &working, &cmd);
    if !cmd.is_empty() {
        s.last_action = Some(match &result {
            Ok(msg) | Err(msg) => msg.clone(),
        });
        s.store(dir)?;
    }
    result
}

fn sync_step<B: EditRemote>(
    connect: impl FnOnce(&str) -> Result<B>,
    dir: &Path,
    s: &mut Session,
    q: &mut DurableWritebackQueue,
    working: &Path,
    cmd: &str,
) -> Result<String> {
    if cmd == "finish" {
        return finish_copy(dir, s, q, false)?.ok_or_else(|| {
            "Finish deferred: outstanding local changes, open request or conflict".into()
        });
    }
    if cmd.is_empty()
        && lifetime::automatic(dir)?
        && let Some(msg) = finish_copy(dir, s, q, true)?
    {
        return Ok(msg);
    }
    if cmd != "reload" && pending(q).is_none() {
        let bytes = read(&working, LIMIT)?;
        let hash = digest(&bytes);
        if hash != s.hash {
            std::thread::sleep(Duration::from_millis(350));
            if read(&working, LIMIT)? != bytes {
                return Err("Save still changing; waiting".into());
            }
            let staged = q.stage_content(&bytes).map_err(err)?;
            q.enqueue(WritebackLocalOperationRequest {
                operation_id: id(),
                capture_id: None,
                mount: scoped_id(&s.node, &s.object),
                root: "/".into(),
                kind: WritebackLocalOperationKind::ReplaceFile,
                source_path: s.path.clone(),
                destination_path: None,
                preconditions: WritebackLocalOperationPreconditions {
                    source: Some(WritebackExpectedObjectState {
                        object_id: s.object.clone(),
                        version: s.version.clone(),
                        content_hash: Some(s.hash.clone()),
                    }),
                    destination: Default::default(),
                },
                staged_content: Some(staged),
            })
            .map_err(err)?;
            s.status(dir, "Saved locally; syncing".into())?;
        }
    }
    let browser = connect(&s.address).map_err(|e| format!("Offline · {e}"))?;
    if browser.identity() != s.node {
        return Err("Daemon identity changed; writeback paused".into());
    }
    if cmd == "reload" {
        let op = pending(q);
        if op.as_ref().is_some_and(|o| {
            !matches!(
                o.status,
                WritebackLocalOperationStatus::Conflict | WritebackLocalOperationStatus::Failed
            )
        }) {
            return Err("Resolve pending write outcome before reloading".into());
        }
        let (object, v, h, bytes) = browser.checkout(&s.path)?;
        if object != s.object {
            return Err("Path now names another object; local copy retained".into());
        }
        let backup = dir
            .join("working")
            .join(format!("preserved-{}-{}", id(), work_name(&s.path)));
        read(&working, LIMIT)?;
        fs::rename(&working, &backup).map_err(err)?;
        durable::write_file_create_new_atomic(
            &working,
            &bytes,
            DurableAtomicFileOptions::new("working", "reload").sync_all(),
        )
        .map_err(|e| format!("Previous file retained at {}: {e}", backup.display()))?;
        s.version = v;
        s.hash = h;
        s.store(dir)?;
        if let Some(op) = op {
            q.retire_terminal_operation(&op.request.operation_id)
                .map_err(err)?;
        }
        return Ok(format!(
            "Reloaded remote version; previous bytes at {}",
            backup.display()
        ));
    }
    if let Some(op) = pending(q) {
        if matches!(
            op.status,
            WritebackLocalOperationStatus::Conflict | WritebackLocalOperationStatus::Failed
        ) {
            return Ok(format!(
                "Writeback blocked · {}",
                op.conflict
                    .map(|e| e.detail)
                    .or(op.last_error)
                    .unwrap_or_else(|| "Inspect the local copy before reloading".into())
            ));
        }
        let progress = execute_writeback_operation(
            q,
            &op.request.operation_id,
            &mut Adapter(&browser),
            &mut Complete(dir, s),
        )
        .map_err(err)?;
        match progress {
            WritebackSyncExecutionProgress::Completed { .. } => (),
            WritebackSyncExecutionProgress::AwaitingReconciliation { detail, .. } => {
                return Ok(format!("Waiting for DFS confirmation · {detail}"));
            }
            WritebackSyncExecutionProgress::CompletionPending { detail, .. } => {
                return Ok(format!("Finishing local synchronization · {detail}"));
            }
            WritebackSyncExecutionProgress::Conflict { evidence, .. } => {
                return Ok(format!("Conflict · {}", evidence.detail));
            }
            WritebackSyncExecutionProgress::Noop { status, .. } => {
                return Ok(format!("Writeback stopped · {status:?}"));
            }
        }
        q.compact_and_prune().map_err(err)?;
        if digest(&read(&working, LIMIT)?) != s.hash {
            return Ok("Newer local save pending".into());
        }
    }
    let head = browser.head(&s.path)?;
    if head.0 != s.object || head.1 != s.version {
        return Ok("Remote version changed; reload or preserve your edits".into());
    }
    // This adapter projects exactly one file. Its authoritative head matches
    // the durably installed base, so successful older receipts can be retired.
    let settled: Vec<_> = q
        .operations()
        .filter(|op| {
            matches!(
                op.status,
                WritebackLocalOperationStatus::Completed
                    | WritebackLocalOperationStatus::Superseded
            ) && op.remote_evidence.is_some()
                && op.conflict.is_none()
                && op.last_error.is_none()
        })
        .take(128)
        .map(|op| op.request.operation_id.clone())
        .collect();
    if !settled.is_empty() {
        q.retire_terminal_operations(&settled).map_err(err)?;
        q.acknowledge_reconciled_retirements(&settled)
            .map_err(err)?;
    }
    if lifetime::automatic(dir)?
        && let Some(msg) = finish_copy(dir, s, q, true)?
    {
        return Ok(msg);
    }
    Ok("Synced".into())
}

pub fn run_helper() -> Result<()> {
    let key = std::env::args().nth(2).ok_or("Missing session id")?;
    if !valid_id(&key) {
        return Err("Invalid session id".into());
    }
    let data = SinomoDataDir::from_env_or_home().map_err(err)?;
    let dir = root(&data).join(&key);
    sinomo_runtime::local::ensure_deployment_enabled(&data.root()).map_err(err)?;
    checked_dir(&data.module_runtime_dir().join(OWNER))?;
    checked_dir(&root(&data))?;
    checked_dir(&dir)?;
    let lease = durable::open_file_for_append(&dir.join("helper.lock"))
        .map_err(err)?
        .file;
    match lease.try_lock_exclusive() {
        Ok(()) => (),
        Err(e) if e.raw_os_error() == fs2::lock_contended_error().raw_os_error() => return Ok(()),
        Err(e) => return Err(err(e)),
    }
    let mut queue = match DurableWritebackQueue::open(OWNER, &data, &key, "/") {
        Ok(q) => q,
        Err(e) if e.code == "queue-busy" => return Ok(()),
        Err(e) => return Err(err(e)),
    };
    let mut s = Session::load(&dir)?;
    {
        let _lock = lifetime::lock(&dir)?;
        recover_claim(&dir, &s)?;
    }
    if s.finished {
        return Ok(());
    }
    lifetime::initialize(&dir)?;
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let working = s.working(&dir);
    let control = dir.join("control");
    let opens = dir.join("opens");
    let _timer_sender = tx.clone();
    let _watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if event.map_or(true, |e| {
            !matches!(e.kind, notify::EventKind::Access(_))
                && (e.paths.is_empty()
                    || e.paths.iter().any(|p| {
                        p == &working
                            || p.parent() == Some(opens.as_path())
                            || (p == &control && control.exists())
                    }))
        }) {
            let _ = tx.try_send(());
        }
    })
    .ok()
    .and_then(|mut watcher| {
        watcher
            .watch(&dir, notify::RecursiveMode::Recursive)
            .ok()
            .map(|()| watcher)
    });
    while !s.finished {
        sinomo_runtime::local::ensure_deployment_enabled(&data.root()).map_err(err)?;
        let msg = tick(
            |address| SifsBrowser::open_daemon(address),
            &dir,
            &mut s,
            &mut queue,
        )
        .unwrap_or_else(|e| e);
        s.status(&dir, msg)?;
        if !s.finished {
            let _ = rx.recv_timeout(Duration::from_secs(30));
            while rx.try_recv().is_ok() {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_client::api::DaemonDfsBrowserWriteReceipt as Receipt;
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

    #[derive(Clone)]
    struct Remote(Rc<RefCell<Server>>);
    struct Server {
        version: usize,
        bytes: Vec<u8>,
        receipts: BTreeMap<String, Receipt>,
        lose_reply: bool,
        writes: usize,
    }
    impl EditRemote for Remote {
        fn identity(&self) -> &str {
            "node-a"
        }
        fn head(&self, _: &str) -> Result<(String, String)> {
            Ok(("object-a".into(), format!("v{}", self.0.borrow().version)))
        }
        fn checkout(&self, _: &str) -> Result<(String, String, String, Vec<u8>)> {
            let s = self.0.borrow();
            Ok((
                "object-a".into(),
                format!("v{}", s.version),
                digest(&s.bytes),
                s.bytes.clone(),
            ))
        }
        fn replace(&self, r: &Request) -> Result<Outcome> {
            let mut s = self.0.borrow_mut();
            if let Some(receipt) = s.receipts.get(&r.operation_id) {
                return Ok(Outcome::Applied {
                    operation_id: r.operation_id.clone(),
                    receipt: receipt.clone(),
                    maintenance_warning: None,
                });
            }
            let Some(bytes) = r.bytes.as_ref() else {
                return Ok(Outcome::NotApplied);
            };
            assert_eq!(digest(bytes), r.content_hash);
            assert_eq!(bytes.len() as u64, r.size_bytes);
            if r.expected_object_id != "object-a" || r.expected_version != format!("v{}", s.version)
            {
                return Ok(Outcome::Conflict {
                    detail: "Base version changed".into(),
                });
            }
            s.writes += 1;
            s.version += 1;
            s.bytes = bytes.clone();
            let receipt = Receipt {
                object_id: "object-a".into(),
                version: format!("v{}", s.version),
                content_hash: Some(r.content_hash.clone()),
                size_bytes: Some(r.size_bytes),
            };
            s.receipts.insert(r.operation_id.clone(), receipt.clone());
            if std::mem::take(&mut s.lose_reply) {
                return Err("lost response".into());
            }
            Ok(Outcome::Applied {
                operation_id: r.operation_id.clone(),
                receipt,
                maintenance_warning: None,
            })
        }
    }
    #[test]
    fn external_edit_recovers_lost_reply_and_never_overwrites_remote_conflicts() {
        let temp = std::env::temp_dir().join(format!("se-{}", id()));
        let data = SinomoDataDir::new(&temp);
        let key = scoped_id("node-a", "object-a");
        let dir = root(&data).join(&key);
        std::fs::create_dir_all(dir.join("working")).unwrap();
        let mut s = Session {
            address: "test".into(),
            node: "node-a".into(),
            object: "object-a".into(),
            path: "/services/file.bin".into(),
            version: "v1".into(),
            hash: digest(b"base"),
            finished: false,
            last_action: None,
            status: "Synced".into(),
        };
        s.store(&dir).unwrap();
        let local = s.working(&dir);
        let mut q = DurableWritebackQueue::open(OWNER, &data, &key, "/").unwrap();
        write(&local, b"first\0edit").unwrap();
        let offline: Result<String> = tick(
            |_| Err::<Remote, _>("unreachable".into()),
            &dir,
            &mut s,
            &mut q,
        );
        assert!(offline.is_err());
        let operation = pending(&q).unwrap().request.operation_id;
        assert_eq!(q.read_staged_content(&operation).unwrap(), b"first\0edit");
        write(&local, b"second edit").unwrap();
        let remote = Remote(Rc::new(RefCell::new(Server {
            version: 1,
            bytes: b"base".to_vec(),
            receipts: BTreeMap::new(),
            lose_reply: true,
            writes: 0,
        })));
        let _ = tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q);
        assert_eq!(remote.0.borrow().writes, 1);
        assert_eq!(s.version, "v1");
        drop(q);
        let mut q = DurableWritebackQueue::open(OWNER, &data, &key, "/").unwrap();
        let mut s = Session::load(&dir).unwrap();
        tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).unwrap();
        assert_eq!(s.version, "v2");
        assert_eq!(remote.0.borrow().writes, 1);
        assert_eq!(
            tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).unwrap(),
            "Synced"
        );
        assert_eq!(remote.0.borrow().bytes, b"second edit");
        remote.0.borrow_mut().version = 4;
        remote.0.borrow_mut().bytes = b"remote change".to_vec();
        write(&local, b"local change").unwrap();
        tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).unwrap();
        assert_eq!(
            pending(&q).unwrap().status,
            WritebackLocalOperationStatus::Conflict
        );
        assert_eq!(read(&local, LIMIT).unwrap(), b"local change");
        assert_eq!(remote.0.borrow().bytes, b"remote change");
        write(&dir.join("control"), b"finish").unwrap();
        assert!(tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).is_err());
        assert!(!s.finished);
        write(&dir.join("control"), b"reload").unwrap();
        tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).unwrap();
        assert_eq!(read(&local, LIMIT).unwrap(), b"remote change");
        assert!(
            fs::read_dir(dir.join("working"))
                .unwrap()
                .flatten()
                .any(
                    |e| e.file_name().to_string_lossy().starts_with("preserved-")
                        && fs::read(e.path()).unwrap() == b"local change"
                )
        );
        fs::remove_file(&local).unwrap();
        assert!(tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).is_err());
        assert_eq!(remote.0.borrow().writes, 2);
        write(&local, b"remote change").unwrap();
        write(&dir.join("control"), b"finish").unwrap();
        tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).unwrap();
        assert!(s.finished);
        assert!(!local.exists());
        drop(q);
        fs::remove_dir_all(temp).unwrap();
    }
    #[test]
    fn external_edit_automatic_cleanup_waits_for_the_final_write_receipt() {
        let temp = std::env::temp_dir().join(format!("final-{}", id()));
        let data = SinomoDataDir::new(&temp);
        let key = scoped_id("node-a", "object-a");
        let dir = root(&data).join(&key);
        fs::create_dir_all(dir.join("working")).unwrap();
        fs::create_dir(dir.join("opens")).unwrap();
        fs::write(dir.join("opens/editor.closed"), b"unknown").unwrap();
        let mut s = Session {
            address: "test".into(),
            node: "node-a".into(),
            object: "object-a".into(),
            path: "/file.txt".into(),
            version: "v1".into(),
            hash: digest(b"base"),
            finished: false,
            last_action: None,
            status: "Synced".into(),
        };
        s.store(&dir).unwrap();
        let path = s.working(&dir);
        write(&path, b"last save").unwrap();
        fs::rename(&path, dir.join("retiring")).unwrap();
        recover_claim(&dir, &s).unwrap();
        let mut q = DurableWritebackQueue::open(OWNER, &data, &key, "/").unwrap();
        assert!(tick(|_| Err::<Remote, _>("offline".into()), &dir, &mut s, &mut q).is_err());
        assert!(pending(&q).is_some());
        assert_eq!(read(&path, LIMIT).unwrap(), b"last save");
        let remote = Remote(Rc::new(RefCell::new(Server {
            version: 1,
            bytes: b"base".to_vec(),
            receipts: BTreeMap::new(),
            lose_reply: true,
            writes: 0,
        })));
        let _ = tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q);
        assert!(path.is_file() && !s.finished);
        tick(|_| Ok(remote.clone()), &dir, &mut s, &mut q).unwrap();
        assert!(s.finished && !path.exists());
        assert_eq!(remote.0.borrow().writes, 1);
        assert_eq!(remote.0.borrow().bytes, b"last save");
        drop(q);
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn external_edit_mount_uses_live_writable_subtree_only() {
        use sinomo_runtime::dfs::SifsMountWritebackMode;
        let dir = std::env::temp_dir().join(format!("em-{}", id()));
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("note.txt"), b"mounted").unwrap();
        let platform = SifsMountPlatform::current();
        if platform == SifsMountPlatform::Unsupported {
            fs::remove_dir_all(dir).unwrap();
            return;
        }
        let mut status = SifsMountRuntimeStatus {
            platform,
            phase: if platform == SifsMountPlatform::ExternalProvider {
                SifsMountRuntimePhase::Connected
            } else {
                SifsMountRuntimePhase::Mounted
            },
            read_only: false,
            sync_local_changes: true,
            writeback_mode: SifsMountWritebackMode::Active,
            provider_registered: true,
            provider_connected: true,
            worker_ready: true,
            worker_alive: true,
            worker_health_generation: 2,
            last_worker_heartbeat_at: Some(
                time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
            ),
            root_path: "/docs".into(),
            resolved_mount_point: Some(dir.to_string_lossy().into()),
            ..Default::default()
        };
        assert!(mounted(&status, "/docs/note.txt").is_some());
        for bad in [
            "/docset/note.txt",
            "/docs/../note.txt",
            "/docs/foo\\note.txt",
            "/docs/C:note.txt",
        ] {
            assert!(mounted(&status, bad).is_none());
        }
        status.read_only = true;
        assert!(mounted(&status, "/docs/note.txt").is_none());
        status.read_only = false;
        status.phase = SifsMountRuntimePhase::Stopping;
        assert!(mounted(&status, "/docs/note.txt").is_none());
        fs::remove_dir_all(dir).unwrap();
    }
}
