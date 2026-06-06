//! Lightweight per-session persistence for the chat tab.
//!
//! Sessions live under `$DATA/app/sessions/<id>/` with a small `meta.json`
//! (listed cheaply at startup) and a `transcript.json` loaded **on demand** when
//! a session is opened — so a long history never has to be parsed up front.

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sinomo_runtime::local::SinomoDataDir;

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
    if id.is_empty() {
        return Ok(());
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
    Ok(())
}

/// List stored sessions (metadata only), newest first.
pub fn list(data_dir: &SinomoDataDir) -> Vec<SessionMeta> {
    let root = sessions_root(data_dir);
    let Ok(entries) = fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut metas: Vec<SessionMeta> = entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            let meta_path = entry.path().join("meta.json");
            let bytes = fs::read(meta_path).ok()?;
            serde_json::from_slice::<SessionMeta>(&bytes).ok()
        })
        .collect();
    metas.sort_by_key(|m| std::cmp::Reverse(m.updated_unix));
    metas
}

/// Load a session's transcript on demand.
pub fn load_transcript(data_dir: &SinomoDataDir, id: &str) -> Option<StoredTranscript> {
    let bytes = fs::read(session_dir(data_dir, id).join("transcript.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}
