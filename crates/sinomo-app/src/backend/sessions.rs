//! Lightweight per-chat persistence for the chat tab.
//!
//! Chats are stored in SIFS under `/services/sinomo-app/...`.
//!
//! The app must not treat `$DATA/app/sessions` as authoritative: chats can be
//! created or updated on another node, and DFS/SIFS owns caching. App-local files
//! may exist from older builds, but reads and writes in this module use SIFS as
//! the source of truth.

use serde::{Deserialize, Serialize};
use sinomo_cli::app_api::SifsBrowser;
use sinomo_runtime::local::SinomoDataDir;

pub const SIFS_SESSIONS_ROOT: &str = "/services/sinomo-app/chat/sessions";
const SIFS_SESSION_FILE_PREFIX: &str = "/services/sinomo-app-chat-session-";
const SIFS_SESSION_FILE_NAME_PREFIX: &str = "sinomo-app-chat-session-";
const SIFS_SESSION_FILE_SUFFIX: &str = ".json";

#[derive(Clone)]
pub struct SifsDaemonAccess {
    address: String,
    runtime: tokio::runtime::Handle,
}

impl SifsDaemonAccess {
    pub fn new(address: impl Into<String>, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            address: address.into(),
            runtime: runtime.clone(),
        }
    }
}

/// Sidebar metadata for a stored chat.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionMeta {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub updated_unix: i64,
    #[serde(default)]
    pub message_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_unix: Option<i64>,
}

/// One stored message.
///
/// `role` is the presentation role used by older app builds (`user` or `agent`).
/// `source` and `kind` distinguish model text from app-generated information
/// and errors without feeding those system messages back as assistant history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredMessage {
    pub role: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl StoredMessage {
    pub fn new(
        role: impl Into<String>,
        text: impl Into<String>,
        source: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            role: role.into(),
            text: text.into(),
            source: Some(source.into()),
            kind: Some(kind.into()),
        }
    }

    pub fn legacy(role: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            text: text.into(),
            source: None,
            kind: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StoredTranscript {
    #[serde(default)]
    pub messages: Vec<StoredMessage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSessionRecord {
    meta: SessionMeta,
    transcript: StoredTranscript,
}

/// Persist a chat's metadata + transcript.
pub fn save(
    data_dir: &SinomoDataDir,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<()> {
    save_with_report(data_dir, id, title, messages)
}

/// Persist a chat to SIFS. SIFS is the source of truth; failure to write SIFS
/// is a chat-save failure.
pub fn save_with_report(
    data_dir: &SinomoDataDir,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<()> {
    save_with_report_using(data_dir, None, id, title, messages)
}

pub fn save_with_report_via_daemon(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<()> {
    let daemon = daemon.ok_or_else(daemon_required_error)?;
    save_with_report_using(data_dir, Some(daemon), id, title, messages)
}

fn save_with_report_using(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<()> {
    if id.is_empty() {
        return Ok(());
    }
    if !valid_session_id(id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid chat id",
        ));
    }
    let existing = load_sifs_record_using(data_dir, daemon, id);
    let (title, title_source) = match existing.as_ref().map(|record| &record.meta) {
        Some(meta) if meta.deleted_unix.is_some() => return Ok(()),
        Some(meta) if meta.title_source.as_deref() == Some("user") => {
            (meta.title.clone(), Some("user".to_string()))
        }
        _ => (title.to_string(), Some("auto".to_string())),
    };
    let meta = SessionMeta {
        id: id.to_string(),
        title,
        updated_unix: now_unix(),
        message_count: messages.len(),
        title_source,
        deleted_unix: None,
    };
    let transcript = StoredTranscript {
        messages: messages.to_vec(),
    };
    save_sifs(data_dir, daemon, id, &meta, &transcript)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))?;
    Ok(())
}

/// Rename a stored chat without changing its transcript.
pub fn rename(data_dir: &SinomoDataDir, id: &str, title: &str) -> std::io::Result<()> {
    rename_using(data_dir, None, id, title)
}

pub fn rename_via_daemon(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
    title: &str,
) -> std::io::Result<()> {
    let daemon = daemon.ok_or_else(daemon_required_error)?;
    rename_using(data_dir, Some(daemon), id, title)
}

fn rename_using(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
    title: &str,
) -> std::io::Result<()> {
    if !valid_session_id(id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid chat id",
        ));
    }
    let title = title.trim();
    if title.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "chat title cannot be empty",
        ));
    }
    let mut record = load_sifs_record_using(data_dir, daemon, id)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "chat not found"))?;
    if record.meta.deleted_unix.is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "chat not found",
        ));
    }
    record.meta.title = title.chars().take(80).collect();
    record.meta.title_source = Some("user".to_string());
    record.meta.updated_unix = now_unix();
    save_sifs(data_dir, daemon, id, &record.meta, &record.transcript)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))
}

/// Hide a chat from listings and future loads. SIFS has no exposed unlink operation, so deletion
/// is represented by a tombstone record at the chat path.
pub fn delete(data_dir: &SinomoDataDir, id: &str) -> std::io::Result<()> {
    delete_using(data_dir, None, id)
}

pub fn delete_via_daemon(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
) -> std::io::Result<()> {
    let daemon = daemon.ok_or_else(daemon_required_error)?;
    delete_using(data_dir, Some(daemon), id)
}

fn delete_using(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
) -> std::io::Result<()> {
    if !valid_session_id(id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid chat id",
        ));
    }
    let browser = open_sifs_browser(data_dir, daemon)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))?;
    let deleted_unix = now_unix();
    let meta = SessionMeta {
        id: id.to_string(),
        title: "Deleted chat".to_string(),
        updated_unix: deleted_unix,
        message_count: 0,
        title_source: None,
        deleted_unix: Some(deleted_unix),
    };
    let transcript = StoredTranscript::default();
    save_sifs_with_browser(&browser, id, &meta, &transcript)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))
}

/// List stored chats from SIFS (metadata only), newest first.
pub fn list(data_dir: &SinomoDataDir) -> Vec<SessionMeta> {
    let mut metas = list_sifs(data_dir, None).unwrap_or_default();
    metas.sort_by_key(|m| std::cmp::Reverse(m.updated_unix));
    metas
}

pub fn list_via_daemon(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
) -> Vec<SessionMeta> {
    let Some(daemon) = daemon else {
        return Vec::new();
    };
    let mut metas = list_sifs(data_dir, Some(daemon)).unwrap_or_default();
    metas.sort_by_key(|m| std::cmp::Reverse(m.updated_unix));
    metas
}

pub fn list_with_report_via_daemon(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
) -> Result<Vec<SessionMeta>, String> {
    let daemon =
        daemon.ok_or_else(|| "chat persistence requires a daemon connection".to_string())?;
    let mut metas = list_sifs(data_dir, Some(daemon))?;
    metas.sort_by_key(|m| std::cmp::Reverse(m.updated_unix));
    Ok(metas)
}

/// Load a chat's transcript on demand.
pub fn load_transcript(data_dir: &SinomoDataDir, id: &str) -> Option<StoredTranscript> {
    load_transcript_using(data_dir, None, id)
}

pub fn load_transcript_via_daemon(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
) -> Option<StoredTranscript> {
    let daemon = daemon?;
    load_transcript_using(data_dir, Some(daemon), id)
}

fn load_transcript_using(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
) -> Option<StoredTranscript> {
    if !valid_session_id(id) {
        return None;
    }
    load_sifs_record_using(data_dir, daemon, id)
        .filter(|record| record.meta.deleted_unix.is_none())
        .map(|record| record.transcript)
}

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn save_sifs(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
    meta: &SessionMeta,
    transcript: &StoredTranscript,
) -> Result<(), String> {
    let browser = open_sifs_browser(data_dir, daemon)?;
    save_sifs_with_browser(&browser, id, meta, transcript)
}

fn save_sifs_with_browser(
    browser: &SifsBrowser,
    id: &str,
    meta: &SessionMeta,
    transcript: &StoredTranscript,
) -> Result<(), String> {
    let record = StoredSessionRecord {
        meta: meta.clone(),
        transcript: transcript.clone(),
    };
    browser.write_file(
        &sifs_session_file(id)?,
        &serde_json::to_vec(&record).map_err(|error| error.to_string())?,
    )?;
    Ok(())
}

fn list_sifs(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
) -> Result<Vec<SessionMeta>, String> {
    let browser = open_sifs_browser(data_dir, daemon)?;
    let mut metas = Vec::new();
    for meta in list_flat_sifs_records(&browser)
        .into_iter()
        .chain(list_legacy_sifs(&browser))
    {
        merge_session_meta(&mut metas, meta);
    }
    metas.retain(|meta| meta.deleted_unix.is_none());
    Ok(metas)
}

fn merge_session_meta(metas: &mut Vec<SessionMeta>, meta: SessionMeta) {
    match metas.iter_mut().find(|existing| existing.id == meta.id) {
        Some(existing) if meta.updated_unix >= existing.updated_unix => *existing = meta,
        Some(_) => {}
        None => metas.push(meta),
    }
}

fn list_flat_sifs_records(browser: &SifsBrowser) -> Vec<SessionMeta> {
    let Ok(entries) = browser.list_if_exists("/services") else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter(|entry| !entry.is_dir)
        .filter_map(|entry| {
            let id = entry
                .name
                .strip_prefix(SIFS_SESSION_FILE_NAME_PREFIX)?
                .strip_suffix(SIFS_SESSION_FILE_SUFFIX)?;
            if !valid_session_id(id) {
                return None;
            }
            let bytes = browser.read_file(&entry.path).ok()?;
            let record = serde_json::from_slice::<StoredSessionRecord>(&bytes).ok()?;
            (record.meta.id == id).then_some(record.meta)
        })
        .collect()
}

fn list_legacy_sifs(browser: &SifsBrowser) -> Vec<SessionMeta> {
    let Ok(entries) = browser.list_if_exists(SIFS_SESSIONS_ROOT) else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter(|entry| entry.is_dir)
        .filter_map(|entry| {
            if !valid_session_id(&entry.name) {
                return None;
            }
            let bytes = browser
                .read_file(&format!("{}/meta.json", entry.path.trim_end_matches('/')))
                .ok()?;
            serde_json::from_slice::<SessionMeta>(&bytes)
                .ok()
                .filter(|meta| meta.deleted_unix.is_none())
        })
        .collect()
}

#[cfg(test)]
fn load_sifs_record(data_dir: &SinomoDataDir, id: &str) -> Option<StoredSessionRecord> {
    load_sifs_record_using(data_dir, None, id)
}

fn load_sifs_record_using(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
    id: &str,
) -> Option<StoredSessionRecord> {
    let browser = open_sifs_browser(data_dir, daemon).ok()?;
    load_sifs_record_from_browser(&browser, id)
}

fn open_sifs_browser(
    data_dir: &SinomoDataDir,
    daemon: Option<&SifsDaemonAccess>,
) -> Result<SifsBrowser, String> {
    match daemon {
        Some(daemon) => SifsBrowser::open_daemon_on_runtime(&daemon.address, &daemon.runtime),
        None => open_local_sifs_browser_for_tests(data_dir),
    }
}

#[cfg(test)]
fn open_local_sifs_browser_for_tests(data_dir: &SinomoDataDir) -> Result<SifsBrowser, String> {
    SifsBrowser::open(data_dir)
}

#[cfg(not(test))]
fn open_local_sifs_browser_for_tests(_data_dir: &SinomoDataDir) -> Result<SifsBrowser, String> {
    Err("SIFS access requires a daemon connection".to_string())
}

fn daemon_required_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "chat persistence requires a daemon connection",
    )
}

fn load_sifs_record_from_browser(browser: &SifsBrowser, id: &str) -> Option<StoredSessionRecord> {
    if let Some(record) = load_flat_sifs_record(browser, id) {
        return Some(record);
    }
    load_legacy_sifs_record(browser, id)
}

fn load_flat_sifs_record(browser: &SifsBrowser, id: &str) -> Option<StoredSessionRecord> {
    let bytes = browser.read_file(&sifs_session_file(id).ok()?).ok()?;
    serde_json::from_slice::<StoredSessionRecord>(&bytes).ok()
}

fn load_legacy_sifs_record(browser: &SifsBrowser, id: &str) -> Option<StoredSessionRecord> {
    let dir = sifs_session_dir(id).ok()?;
    let transcript_bytes = browser.read_file(&format!("{dir}/transcript.json")).ok()?;
    let transcript = serde_json::from_slice::<StoredTranscript>(&transcript_bytes).ok()?;
    let meta = browser
        .read_file(&format!("{dir}/meta.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<SessionMeta>(&bytes).ok())
        .unwrap_or_else(|| SessionMeta {
            id: id.to_string(),
            title: "New chat".to_string(),
            updated_unix: 0,
            message_count: transcript.messages.len(),
            title_source: None,
            deleted_unix: None,
        });
    Some(StoredSessionRecord { meta, transcript })
}

fn sifs_session_file(id: &str) -> Result<String, String> {
    if !valid_session_id(id) {
        return Err("invalid chat id".to_string());
    }
    Ok(format!(
        "{SIFS_SESSION_FILE_PREFIX}{id}{SIFS_SESSION_FILE_SUFFIX}"
    ))
}

fn sifs_session_dir(id: &str) -> Result<String, String> {
    if !valid_session_id(id) {
        return Err("invalid chat id".to_string());
    }
    Ok(format!("{SIFS_SESSIONS_ROOT}/{id}"))
}

fn valid_session_id(id: &str) -> bool {
    !id.is_empty()
        && !id.contains('/')
        && !id.contains('\\')
        && !id.contains("..")
        && !id.bytes().any(|byte| byte == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn sessions_round_trip_through_sifs() {
        let root = temp_data_dir("sifs-roundtrip");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d4";
        let messages = vec![
            StoredMessage::new("user", "hello", "user", "message"),
            StoredMessage::new("agent", "hi", "model", "message"),
            StoredMessage::new("agent", "Error: provider unavailable", "system", "error"),
        ];

        save(&data_dir, id, "hello", &messages).expect("save");

        let listed = list(&data_dir);
        assert!(listed.iter().any(|meta| meta.id == id));
        let transcript = load_transcript(&data_dir, id).expect("sifs transcript");
        assert_eq!(transcript.messages.len(), 3);
        assert_eq!(transcript.messages[1].text, "hi");
        assert_eq!(transcript.messages[2].source.as_deref(), Some("system"));
        assert_eq!(transcript.messages[2].kind.as_deref(), Some("error"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sifs_listing_recovers_flat_session_records_without_index() {
        let root = temp_data_dir("sifs-flat-listing");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d5";
        let meta = SessionMeta {
            id: id.to_string(),
            title: "flat record".to_string(),
            updated_unix: 42,
            message_count: 1,
            title_source: Some("auto".to_string()),
            deleted_unix: None,
        };
        let record = StoredSessionRecord {
            meta: meta.clone(),
            transcript: StoredTranscript {
                messages: vec![StoredMessage::new("user", "hello", "user", "message")],
            },
        };
        let browser = SifsBrowser::open(&data_dir).expect("open sifs");
        browser
            .write_file(
                &sifs_session_file(id).expect("session path"),
                &serde_json::to_vec(&record).expect("record json"),
            )
            .expect("write flat record");

        let listed = list(&data_dir);
        assert!(
            listed
                .iter()
                .any(|session| session.id == meta.id && session.title == meta.title),
            "flat session records should be listed even when the index is absent"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sifs_transcript_wins_over_stale_app_local_files() {
        let root = temp_data_dir("sifs-first");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d5";
        let sifs_messages = vec![
            StoredMessage::new("user", "sifs question", "user", "message"),
            StoredMessage::new("agent", "sifs answer", "model", "message"),
        ];
        save(&data_dir, id, "sifs", &sifs_messages).expect("save SIFS session");
        write_app_local_session(&root, id, "local", "local question", "local answer");

        let loaded = load_transcript(&data_dir, id).expect("SIFS transcript");
        assert_eq!(loaded.messages[0].text, "sifs question");
        assert_eq!(loaded.messages[1].text, "sifs answer");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sifs_listing_wins_over_app_local_files() {
        let root = temp_data_dir("sifs-list");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d6";
        let messages = vec![StoredMessage::new("user", "hello", "user", "message")];

        save(&data_dir, id, "sifs title", &messages).expect("save SIFS session");
        write_app_local_session(&root, id, "local title", "local question", "local answer");

        let listed = list(&data_dir);
        let meta = listed
            .iter()
            .find(|meta| meta.id == id)
            .expect("SIFS session metadata");
        assert_eq!(meta.title, "sifs title");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rename_preserves_transcript_and_user_title_across_auto_saves() {
        let root = temp_data_dir("sifs-rename");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d7";
        let messages = vec![
            StoredMessage::new("user", "original prompt", "user", "message"),
            StoredMessage::new("agent", "answer", "model", "message"),
        ];
        save(&data_dir, id, "original prompt", &messages).expect("save");

        rename(&data_dir, id, "Renamed session").expect("rename");
        save(&data_dir, id, "auto title should not win", &messages).expect("resave");

        let listed = list(&data_dir);
        let meta = listed
            .iter()
            .find(|meta| meta.id == id)
            .expect("renamed session listed");
        assert_eq!(meta.title, "Renamed session");
        assert_eq!(meta.title_source.as_deref(), Some("user"));
        let transcript = load_transcript(&data_dir, id).expect("transcript still loads");
        assert_eq!(transcript.messages[0].text, "original prompt");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_hides_session_from_listing_and_loading() {
        let root = temp_data_dir("sifs-delete");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d8";
        let messages = vec![StoredMessage::new("user", "remove me", "user", "message")];
        save(&data_dir, id, "remove me", &messages).expect("save");

        delete(&data_dir, id).expect("delete");

        assert!(
            list(&data_dir).iter().all(|meta| meta.id != id),
            "deleted sessions are hidden from the sidebar list"
        );
        assert!(
            load_transcript(&data_dir, id).is_none(),
            "deleted sessions do not reopen"
        );
        let tombstone = load_sifs_record(&data_dir, id).expect("delete writes tombstone");
        assert!(tombstone.meta.deleted_unix.is_some());
        assert!(tombstone.transcript.messages.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    fn write_app_local_session(root: &Path, id: &str, title: &str, user: &str, agent: &str) {
        let dir = root.join("app").join("sessions").join(id);
        fs::create_dir_all(&dir).expect("create stale local session dir");
        let meta = SessionMeta {
            id: id.to_string(),
            title: title.to_string(),
            updated_unix: i64::MAX,
            message_count: 2,
            title_source: Some("auto".to_string()),
            deleted_unix: None,
        };
        let transcript = StoredTranscript {
            messages: vec![
                StoredMessage::legacy("user", user),
                StoredMessage::legacy("agent", agent),
            ],
        };
        fs::write(
            dir.join("meta.json"),
            serde_json::to_vec_pretty(&meta).expect("encode stale local meta"),
        )
        .expect("write stale local meta");
        fs::write(
            dir.join("transcript.json"),
            serde_json::to_vec(&transcript).expect("encode stale local transcript"),
        )
        .expect("write stale local transcript");
    }

    fn temp_data_dir(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-app-sessions-{name}-{}-{unique}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        root
    }
}
