//! Lightweight per-session persistence for the chat tab.
//!
//! Sessions are stored in SIFS under `/services/sinomo-app/...`.
//!
//! The app must not treat `$DATA/app/sessions` as authoritative: sessions can be
//! created or updated on another node, and DFS/SIFS owns caching. App-local files
//! may exist from older builds, but reads and writes in this module use SIFS as
//! the source of truth.

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

/// Persist a session's metadata + transcript.
pub fn save(
    data_dir: &SinomoDataDir,
    id: &str,
    title: &str,
    messages: &[StoredMessage],
) -> std::io::Result<()> {
    save_with_report(data_dir, id, title, messages)
}

/// Persist a session to SIFS. SIFS is the source of truth; failure to write SIFS
/// is a session-save failure.
pub fn save_with_report(
    data_dir: &SinomoDataDir,
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
            "invalid session id",
        ));
    }
    let meta = SessionMeta {
        id: id.to_string(),
        title: title.to_string(),
        updated_unix: now_unix(),
        message_count: messages.len(),
    };
    let transcript = StoredTranscript {
        messages: messages.to_vec(),
    };
    save_sifs(data_dir, id, &meta, &transcript)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))?;
    Ok(())
}

/// List stored sessions from SIFS (metadata only), newest first.
pub fn list(data_dir: &SinomoDataDir) -> Vec<SessionMeta> {
    let mut metas = list_sifs(data_dir);
    metas.sort_by_key(|m| std::cmp::Reverse(m.updated_unix));
    metas
}

/// Load a session's transcript on demand.
pub fn load_transcript(data_dir: &SinomoDataDir, id: &str) -> Option<StoredTranscript> {
    if !valid_session_id(id) {
        return None;
    }
    load_sifs_record(data_dir, id).map(|record| record.transcript)
}

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
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

fn load_sifs_record(data_dir: &SinomoDataDir, id: &str) -> Option<StoredSessionRecord> {
    let browser = SifsBrowser::open(data_dir).ok()?;
    if let Some(record) = load_flat_sifs_record(&browser, id) {
        return Some(record);
    }
    load_legacy_sifs_record(&browser, id)
}

fn read_sifs_index(browser: &SifsBrowser) -> Result<StoredSessionIndex, String> {
    let bytes = browser.read_file(SIFS_SESSIONS_INDEX)?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
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
        });
    Some(StoredSessionRecord { meta, transcript })
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
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn sessions_round_trip_through_sifs() {
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

        let listed = list(&data_dir);
        assert!(listed.iter().any(|meta| meta.id == id));
        let transcript = load_transcript(&data_dir, id).expect("sifs transcript");
        assert_eq!(transcript.messages.len(), 2);
        assert_eq!(transcript.messages[1].text, "hi");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sifs_transcript_wins_over_stale_app_local_files() {
        let root = temp_data_dir("sifs-first");
        let data_dir = SinomoDataDir::new(&root);
        let id = "01hxy6qa9k0t9vr4g9n8d3q5d5";
        let sifs_messages = vec![
            StoredMessage {
                role: "user".to_string(),
                text: "sifs question".to_string(),
            },
            StoredMessage {
                role: "agent".to_string(),
                text: "sifs answer".to_string(),
            },
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
        let messages = vec![StoredMessage {
            role: "user".to_string(),
            text: "hello".to_string(),
        }];

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

    fn write_app_local_session(root: &Path, id: &str, title: &str, user: &str, agent: &str) {
        let dir = root.join("app").join("sessions").join(id);
        fs::create_dir_all(&dir).expect("create stale local session dir");
        let meta = SessionMeta {
            id: id.to_string(),
            title: title.to_string(),
            updated_unix: i64::MAX,
            message_count: 2,
        };
        let transcript = StoredTranscript {
            messages: vec![
                StoredMessage {
                    role: "user".to_string(),
                    text: user.to_string(),
                },
                StoredMessage {
                    role: "agent".to_string(),
                    text: agent.to_string(),
                },
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
