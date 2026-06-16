//! Lightweight per-session persistence for the chat tab.
//!
//! Sessions live under `$DATA/app/sessions/<id>/` with a small `meta.json`
//! (listed cheaply at startup) and a `transcript.json` loaded **on demand** when
//! a session is opened — so a long history never has to be parsed up front.
//! When SIFS is available, the same files are mirrored at
//! `/services/sinomo-app/chat/sessions/<id>/`, making the app's chat history a
//! cluster-visible service state instead of node-local UI state.

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sinomo_cli::app_api::SifsBrowser;
use sinomo_runtime::local::SinomoDataDir;

pub const SIFS_SESSIONS_ROOT: &str = "/services/sinomo-app/chat/sessions";
const SIFS_SESSIONS_INDEX: &str = "/services/sinomo-app-chat-sessions-index.json";
const SIFS_SESSION_FILE_PREFIX: &str = "/services/sinomo-app-chat-session-";
const SIFS_SESSION_FILE_SUFFIX: &str = ".json";

/// Sidebar metadata for a stored session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionMeta {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub updated_unix: i64,
    #[serde(default)]
    pub message_count: usize,
}

/// One stored message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredMessage {
    pub role: String,
    pub text: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StoredTranscript {
    #[serde(default)]
    pub messages: Vec<StoredMessage>,
}

#[derive(Clone, Debug)]
pub struct SessionSaveReport {
    pub sifs: Result<(), String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSessionRecord {
    meta: SessionMeta,
    transcript: StoredTranscript,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredSessionIndex {
    #[serde(default)]
    sessions: Vec<SessionMeta>,
}

fn sessions_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.root().join("app").join("sessions")
}

fn session_dir(data_dir: &SinomoDataDir, id: &str) -> PathBuf {
    sessions_root(data_dir).join(id)
}

/// Persist a session's metadata + transcript.
pub fn save(
    data_dir: &SinomoDataDir,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<()> {
    save_with_report(data_dir, id, title, messages).map(|_| ())
}

/// Persist a session and report whether the cluster-visible SIFS mirror also
/// succeeded. Local disk persistence remains authoritative for offline UI use.
pub fn save_with_report(
    data_dir: &SinomoDataDir,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<SessionSaveReport> {
    if id.is_empty() {
        return Ok(SessionSaveReport { sifs: Ok(()) });
    }
    if !valid_session_id(id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid session id",
        ));
    }
    let dir = session_dir(data_dir, id);
    fs::create_dir_all(&dir)?;
    let meta = SessionMeta {
        id: id.to_string(),
        title: title.to_string(),
        updated_unix: now_unix(),
        message_count: messages.len(),
    };
    fs::write(dir.join("meta.json"), serde_json::to_vec_pretty(&meta)?)?;
    let transcript = StoredTranscript {
        messages: messages.to_vec(),
    };
    fs::write(
        dir.join("transcript.json"),
        serde_json::to_vec(&transcript)?,
    )?;
    let sifs = save_sifs(data_dir, id, &meta, &transcript);
    Ok(SessionSaveReport { sifs })
}

/// List stored sessions (metadata only), newest first.
pub fn list(data_dir: &SinomoDataDir) -> Vec<SessionMeta> {
    let mut metas = list_local(data_dir);
    for meta in list_sifs(data_dir) {
        match metas.iter_mut().find(|existing| existing.id == meta.id) {
            Some(existing) if meta.updated_unix > existing.updated_unix => *existing = meta,
            Some(_) => {}
            None => metas.push(meta),
        }
    }
    metas.sort_by_key(|m| std::cmp::Reverse(m.updated_unix));
    metas
}

/// Load a session's transcript on demand.
pub fn load_transcript(data_dir: &SinomoDataDir, id: &str) -> Option<StoredTranscript> {
    if !valid_session_id(id) {
        return None;
    }
    if let Some(transcript) = load_sifs_transcript(data_dir, id) {
        let title = list(data_dir)
            .into_iter()
            .find(|meta| meta.id == id)
            .map(|meta| meta.title)
            .unwrap_or_else(|| "New chat".to_string());
        let _ = save_local(data_dir, id, &title, &transcript);
        return Some(transcript);
    }
    load_local_transcript(data_dir, id)
}

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn save_local(
    data_dir: &SinomoDataDir,
    id: &str,
    title: &str,
    transcript: &StoredTranscript,
) -> std::io::Result<()> {
    let dir = session_dir(data_dir, id);
    fs::create_dir_all(&dir)?;
    let meta = SessionMeta {
        id: id.to_string(),
        title: title.to_string(),
        updated_unix: now_unix(),
        message_count: transcript.messages.len(),
    };
    fs::write(dir.join("meta.json"), serde_json::to_vec_pretty(&meta)?)?;
    fs::write(dir.join("transcript.json"), serde_json::to_vec(transcript)?)?;
    Ok(())
}

fn list_local(data_dir: &SinomoDataDir) -> Vec<SessionMeta> {
    let root = sessions_root(data_dir);
    let Ok(entries) = fs::read_dir(&root) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            let meta_path = entry.path().join("meta.json");
            let bytes = fs::read(meta_path).ok()?;
            serde_json::from_slice::<SessionMeta>(&bytes).ok()
        })
        .collect()
}

fn load_local_transcript(data_dir: &SinomoDataDir, id: &str) -> Option<StoredTranscript> {
    let bytes = fs::read(session_dir(data_dir, id).join("transcript.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_sifs(
    data_dir: &SinomoDataDir,
    id: &str,
    meta: &SessionMeta,
    transcript: &StoredTranscript,
) -> Result<(), String> {
    let browser = SifsBrowser::open(data_dir)?;
    let record = StoredSessionRecord {
        meta: meta.clone(),
        transcript: transcript.clone(),
    };
    browser.write_file(
        &sifs_session_file(id)?,
        &serde_json::to_vec(&record).map_err(|error| error.to_string())?,
    )?;
    let mut index = read_sifs_index(&browser).unwrap_or_default();
    match index.sessions.iter_mut().find(|existing| existing.id == id) {
        Some(existing) => *existing = meta.clone(),
        None => index.sessions.push(meta.clone()),
    }
    index
        .sessions
        .sort_by_key(|meta| std::cmp::Reverse(meta.updated_unix));
    browser.write_file(
        SIFS_SESSIONS_INDEX,
        &serde_json::to_vec(&index).map_err(|error| error.to_string())?,
    )
}

fn list_sifs(data_dir: &SinomoDataDir) -> Vec<SessionMeta> {
    let Ok(browser) = SifsBrowser::open(data_dir) else {
        return Vec::new();
    };
    let mut metas = read_sifs_index(&browser)
        .map(|index| index.sessions)
        .unwrap_or_default();
    for meta in list_legacy_sifs(&browser) {
        match metas.iter_mut().find(|existing| existing.id == meta.id) {
            Some(existing) if meta.updated_unix > existing.updated_unix => *existing = meta,
            Some(_) => {}
            None => metas.push(meta),
        }
    }
    metas
}

fn list_legacy_sifs(browser: &SifsBrowser) -> Vec<SessionMeta> {
    let Ok(entries) = browser.list(SIFS_SESSIONS_ROOT) else {
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
            serde_json::from_slice::<SessionMeta>(&bytes).ok()
        })
        .collect()
}

fn load_sifs_transcript(data_dir: &SinomoDataDir, id: &str) -> Option<StoredTranscript> {
    let browser = SifsBrowser::open(data_dir).ok()?;
    if let Some(transcript) = load_flat_sifs_transcript(&browser, id) {
        return Some(transcript);
    }
    load_legacy_sifs_transcript(&browser, id)
}

fn read_sifs_index(browser: &SifsBrowser) -> Result<StoredSessionIndex, String> {
    let bytes = browser.read_file(SIFS_SESSIONS_INDEX)?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn load_flat_sifs_transcript(browser: &SifsBrowser, id: &str) -> Option<StoredTranscript> {
    let bytes = browser.read_file(&sifs_session_file(id).ok()?).ok()?;
    serde_json::from_slice::<StoredSessionRecord>(&bytes)
        .ok()
        .map(|record| record.transcript)
}

fn load_legacy_sifs_transcript(browser: &SifsBrowser, id: &str) -> Option<StoredTranscript> {
    let dir = sifs_session_dir(id).ok()?;
    let bytes = browser.read_file(&format!("{dir}/transcript.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn sifs_session_file(id: &str) -> Result<String, String> {
    if !valid_session_id(id) {
        return Err("invalid session id".to_string());
    }
    Ok(format!(
        "{SIFS_SESSION_FILE_PREFIX}{id}{SIFS_SESSION_FILE_SUFFIX}"
    ))
}

fn sifs_session_dir(id: &str) -> Result<String, String> {
    if !valid_session_id(id) {
        return Err("invalid session id".to_string());
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

    #[test]
    fn sessions_round_trip_through_sifs_when_local_cache_is_missing() {
        let root = temp_data_dir("sifs-roundtrip");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d4";
        let messages = vec![
            StoredMessage {
                role: "user".to_string(),
                text: "hello".to_string(),
            },
            StoredMessage {
                role: "agent".to_string(),
                text: "hi".to_string(),
            },
        ];

        save(&data_dir, id, "hello", &messages).expect("save");
        fs::remove_dir_all(sessions_root(&data_dir)).expect("remove local cache");

        let listed = list(&data_dir);
        assert!(listed.iter().any(|meta| meta.id == id));
        let transcript = load_transcript(&data_dir, id).expect("sifs transcript");
        assert_eq!(transcript.messages.len(), 2);
        assert_eq!(transcript.messages[1].text, "hi");

        let _ = fs::remove_dir_all(root);
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
