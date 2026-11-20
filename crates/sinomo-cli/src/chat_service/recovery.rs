//! Durable command admission. A transport disconnect never cancels execution; ambiguous
//! started commands retain receipts and are never blindly replayed after a crash.
use super::wire;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_runtime::storage::durable_file::{DurableAtomicFileOptions, write_file_atomic};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum CommandState {
    Pending,
    Running,
    Complete,
    Interrupted,
    Rejected(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    id: String,
    command: Option<wire::ChatSessionCommand>,
    command_hash: String,
    state: CommandState,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Journal {
    execution_id: String,
    schema_version: u32,
    bootstrap: Vec<wire::ChatSessionCommand>,
    entries: Vec<Entry>,
}

pub(super) type RecoveredCommands = (
    Vec<wire::ChatSessionCommand>,
    Vec<(String, wire::ChatSessionCommand)>,
    bool,
);

#[derive(Clone)]
pub(super) struct CommandJournal {
    path: PathBuf,
    data_dir: PathBuf,
    _owner: Arc<fs::File>,
    state: Arc<Mutex<Journal>>,
}

impl CommandJournal {
    pub(super) fn open(data_dir: &Path, id: &str) -> Result<Self, String> {
        super::validate_client_id(id)?;
        let path = data_dir
            .join("events/agent/chat-hosts")
            .join(format!("{}.json", blake3::hash(id.as_bytes()).to_hex()));
        let parent = path.parent().ok_or("chat host has no parent")?;
        fs::create_dir_all(parent.parent().ok_or("chat host has no owner")?)
            .map_err(|e| e.to_string())?;
        sinomo_runtime::storage::durable_file::ensure_private_directory(parent)
            .map_err(|e| e.to_string())?;
        let owner = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))
            .map_err(|e| e.to_string())?;
        owner
            .try_lock_exclusive()
            .map_err(|_| "chat execution host already has a live owner".to_string())?;
        let mut state = match fs::read(&path) {
            Ok(bytes) => {
                let state: Journal = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                if state.schema_version != 1 {
                    return Err("unsupported chat host journal version".into());
                }
                state
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Journal::default(),
            Err(e) => return Err(e.to_string()),
        };
        if state.execution_id.is_empty() {
            state.execution_id = id.into();
        }
        if state.execution_id != id {
            return Err("chat execution identity mismatch".into());
        }
        let mut interrupted = false;
        for entry in &mut state.entries {
            if matches!(entry.state, CommandState::Pending | CommandState::Running)
                && entry.command.is_none()
            {
                return Err("pending chat command is missing its input".into());
            }
            if entry.state == CommandState::Running {
                entry.state = CommandState::Interrupted;
                interrupted = true;
            }
        }
        let journal = Self {
            path,
            data_dir: data_dir.to_path_buf(),
            _owner: Arc::new(owner),
            state: Arc::new(Mutex::new(state)),
        };
        // Persist the uncertainty before any network access. A second crash
        // must see the same outcome even if this view could not be restored.
        if interrupted {
            journal.change(|_| Ok(()))?;
        }
        Ok(journal)
    }

    pub(super) fn data_dir(&self) -> PathBuf {
        self.data_dir.clone()
    }

    fn change<T>(&self, f: impl FnOnce(&mut Journal) -> Result<T, String>) -> Result<T, String> {
        let mut guard = self.state.lock().map_err(|_| "chat journal poisoned")?;
        let mut staged = guard.clone();
        let result = f(&mut staged)?;
        staged.schema_version = 1;
        let bytes = serde_json::to_vec(&staged).map_err(|e| e.to_string())?;
        let parent = self.path.parent().ok_or("chat journal has no parent")?;
        fs::create_dir_all(parent.parent().ok_or("chat journal has no owner")?)
            .map_err(|e| e.to_string())?;
        sinomo_runtime::storage::durable_file::ensure_private_directory(parent)
            .map_err(|e| e.to_string())?;
        write_file_atomic(
            &self.path,
            &bytes,
            DurableAtomicFileOptions::new("chat-host", "command").sync_all(),
        )
        .map_err(|e| e.to_string())?;
        *guard = staged;
        Ok(result)
    }

    pub(super) fn admit(
        &self,
        id: &str,
        command: &wire::ChatSessionCommand,
    ) -> Result<bool, String> {
        super::validate_client_id(id)?;
        let command_hash = blake3::hash(&serde_json::to_vec(command).map_err(|e| e.to_string())?)
            .to_hex()
            .to_string();
        self.change(|state| {
            if let Some(entry) = state.entries.iter().find(|entry| entry.id == id) {
                if entry.command_hash != command_hash { return Err("command identity reused for different input".into()); }
                if let CommandState::Rejected(error) = &entry.state { return Err(error.clone()); }
                if entry.state == CommandState::Interrupted { return Err("command outcome requires reconciliation after daemon restart; it was not replayed".into()); }
                return Ok(false);
            }
            // Keep receipts for the host's lifetime; dropping a receipt would make an old retry
            // executable again. Bound admission instead of silently weakening idempotency.
            if state.entries.len() >= 16_384 { return Err("chat host command journal is full; open a new execution host".into()); }
            state.entries.push(Entry { id: id.into(), command: Some(command.clone()), command_hash, state: CommandState::Pending });
            Ok(true)
        })
    }

    pub(super) fn start(&self, id: &str) -> Result<(), String> {
        self.change(|state| {
            let entry = state
                .entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or("missing command receipt")?;
            if entry.state != CommandState::Pending {
                return Err("command is not pending".into());
            }
            entry.state = CommandState::Running;
            Ok(())
        })
    }

    /// The selected peer explicitly declined ownership before admission. Keep
    /// the original payload/identity pending for the caller's existing reroute.
    /// Never use this transition for a timeout or uncertain delivery result.
    pub(super) fn retry_after_not_owner(&self, id: &str) -> Result<(), String> {
        self.change(|state| {
            let entry = state
                .entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or("missing command receipt")?;
            if entry.state != CommandState::Running {
                return Err("only a running forward can be declined before admission".into());
            }
            entry.state = CommandState::Pending;
            Ok(())
        })
    }

    pub(super) fn reject(&self, id: &str, error: String) -> Result<(), String> {
        self.change(|state| {
            state
                .entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or("missing command receipt")?
                .state = CommandState::Rejected(error);
            Ok(())
        })
    }

    /// A queued input cancelled before its effect boundary must not become
    /// executable again after a host restart. Started work keeps its existing
    /// interrupted/outcome reconciliation contract.
    pub(super) fn reject_if_pending(&self, id: &str, error: &str) -> Result<(), String> {
        let pending = self
            .state
            .lock()
            .map_err(|_| "chat journal poisoned")?
            .entries
            .iter()
            .any(|entry| entry.id == id && entry.state == CommandState::Pending);
        if !pending {
            return Ok(());
        }
        self.change(|state| {
            let entry = state
                .entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or("missing command receipt")?;
            if entry.state == CommandState::Pending {
                entry.state = CommandState::Rejected(error.into());
            }
            Ok(())
        })
    }

    pub(super) fn complete(
        &self,
        id: &str,
        resume: Option<wire::ChatSessionCommand>,
    ) -> Result<(), String> {
        self.change(|state| {
            let entry = state
                .entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or("missing command receipt")?;
            entry.state = CommandState::Complete;
            entry.command = None;
            if let Some(command) = resume {
                let key = std::mem::discriminant(&command);
                state
                    .bootstrap
                    .retain(|old| std::mem::discriminant(old) != key);
                state.bootstrap.push(command);
            }
            Ok(())
        })
    }

    pub(super) fn journal_paths(data_dir: &Path) -> Result<Vec<PathBuf>, String> {
        let directory = data_dir.join("events/agent/chat-hosts");
        let files = match fs::read_dir(directory) {
            Ok(files) => files,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e.to_string()),
        };
        let mut paths = Vec::new();
        for file in files {
            let path = file.map_err(|e| e.to_string())?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                paths.push(path);
            }
        }
        Ok(paths)
    }

    pub(super) fn recoverable_execution(path: &Path) -> Result<Option<String>, String> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.to_string()),
        };
        let state: Journal = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if !state
            .entries
            .iter()
            .any(|e| matches!(e.state, CommandState::Pending | CommandState::Running))
        {
            return Ok(None);
        }
        super::validate_client_id(&state.execution_id)?;
        let key = blake3::hash(state.execution_id.as_bytes()).to_hex();
        if path.file_stem().and_then(|s| s.to_str()) != Some(key.as_str()) {
            return Err(
                "chat recovery journal filename does not match its execution identity".into(),
            );
        }
        Ok(Some(state.execution_id))
    }

    pub(super) fn recovery(&self) -> Result<RecoveredCommands, String> {
        let state = self.state.lock().map_err(|_| "chat journal poisoned")?;
        Ok((
            state.bootstrap.clone(),
            state
                .entries
                .iter()
                .filter(|e| e.state == CommandState::Pending)
                .filter_map(|e| e.command.clone().map(|command| (e.id.clone(), command)))
                .collect(),
            state
                .entries
                .iter()
                .any(|e| e.state == CommandState::Interrupted),
        ))
    }
}

pub(super) fn resume_command(
    command: &wire::ChatSessionCommand,
    session_id: String,
) -> Option<wire::ChatSessionCommand> {
    use wire::ChatSessionCommand::*;
    match command {
        SetModel { .. } => Some(command.clone()),
        Send { .. }
        | Resume { .. }
        | NewSession { .. }
        | RealtimeHandoff { .. }
        | SetAgent { .. }
        | SetToolRuntime { .. }
        | SetWorkspace { .. }
        | UpdateWorkspaces { .. }
        | SetToolRuntimeNode { .. } => Some(Resume {
            session_id,
            history: vec![],
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interrupted_command_is_durable_before_the_view_can_attach() {
        let dir = sinomo_testdir::TestDir::new("chat-failed-attach");
        let command = wire::ChatSessionCommand::Send {
            session_id: "chat".into(),
            text: "once".into(),
            attachments: vec![],
        };
        let journal = CommandJournal::open(&dir, "view").unwrap();
        journal.admit("original", &command).unwrap();
        journal.start("original").unwrap();
        drop(journal);
        let path = CommandJournal::journal_paths(&dir).unwrap().pop().unwrap();
        assert_eq!(
            CommandJournal::recoverable_execution(&path)
                .unwrap()
                .as_deref(),
            Some("view")
        );
        let reopened = CommandJournal::open(&dir, "view").unwrap();
        assert!(reopened.recovery().unwrap().2);
        drop(reopened);
        assert_eq!(CommandJournal::recoverable_execution(&path).unwrap(), None);
        assert!(
            CommandJournal::open(&dir, "view")
                .unwrap()
                .admit("original", &command)
                .is_err()
        );
    }

    #[test]
    fn only_explicit_non_admission_remains_pending_after_reopen() {
        let dir = sinomo_testdir::TestDir::new("chat-not-owner-recovery");
        let journal = CommandJournal::open(&dir, "origin").unwrap();
        let command = wire::ChatSessionCommand::Send {
            session_id: "chat".into(),
            text: "original payload".into(),
            attachments: vec![],
        };
        for id in ["declined", "uncertain", "complete"] {
            journal.admit(id, &command).unwrap();
            journal.start(id).unwrap();
        }
        journal.retry_after_not_owner("declined").unwrap();
        journal.complete("complete", None).unwrap();
        assert!(journal.retry_after_not_owner("complete").is_err());
        drop(journal);

        let reopened = CommandJournal::open(&dir, "origin").unwrap();
        let (_, pending, interrupted) = reopened.recovery().unwrap();
        assert_eq!(pending, vec![("declined".into(), command.clone())]);
        assert!(interrupted, "the uncertain original remains unreplayed");
        assert!(reopened.admit("uncertain", &command).is_err());
        assert!(reopened.retry_after_not_owner("uncertain").is_err());
        assert!(
            reopened
                .admit("declined", &wire::ChatSessionCommand::FetchAgents)
                .is_err()
        );
        reopened.start("declined").unwrap();
        reopened.complete("declined", None).unwrap();
        assert!(!reopened.admit("declined", &command).unwrap());
    }

    #[test]
    fn workspace_management_recovery_resumes_without_replaying_completed_grants() {
        let dir = sinomo_testdir::TestDir::new("multi-workspace-recovery");
        let journal = CommandJournal::open(&dir, "host").unwrap();
        let command = wire::ChatSessionCommand::UpdateWorkspaces {
            session_id: "session".into(),
            request_id: "ui-request".into(),
            change: wire::ChatWorkspaceChange::AddFolder {
                path: "C:/project".into(),
            },
        };
        assert!(journal.admit("operation", &command).unwrap());
        assert!(!journal.admit("operation", &command).unwrap());
        journal.start("operation").unwrap();
        journal
            .complete("operation", resume_command(&command, "session".into()))
            .unwrap();
        drop(journal);
        let recovered = CommandJournal::open(&dir, "host").unwrap();
        let (bootstrap, pending, interrupted) = recovered.recovery().unwrap();
        assert!(matches!(
            bootstrap.as_slice(),
            [wire::ChatSessionCommand::Resume { .. }]
        ));
        assert!(pending.is_empty());
        assert!(!interrupted);
        assert!(!recovered.admit("operation", &command).unwrap());
    }
    #[test]
    fn execution_owner_is_exclusive_until_all_live_handles_are_dropped() {
        let dir = sinomo_testdir::TestDir::new("chat-host-owner");
        let first = CommandJournal::open(&dir, "host").unwrap();
        let worker = first.clone();
        drop(first);
        assert!(CommandJournal::open(&dir, "host").is_err());
        drop(worker);
        assert!(CommandJournal::open(&dir, "host").is_ok());
    }
    #[test]
    fn completed_receipts_survive_restart_without_retaining_or_replaying_input() {
        let dir = sinomo_testdir::TestDir::new("chat-complete-receipt");
        let journal = CommandJournal::open(&dir, "host").unwrap();
        let command = wire::ChatSessionCommand::SetWorkspace {
            session_id: "session".into(),
            workspace_root: Some("/work".into()),
        };
        journal.admit("operation", &command).unwrap();
        journal.start("operation").unwrap();
        journal
            .complete("operation", resume_command(&command, "session".into()))
            .unwrap();
        assert!(journal.state.lock().unwrap().entries[0].command.is_none());
        drop(journal);
        let recovered = CommandJournal::open(&dir, "host").unwrap();
        assert!(!recovered.admit("operation", &command).unwrap());
        let (bootstrap, pending, interrupted) = recovered.recovery().unwrap();
        assert!(!interrupted);
        assert!(pending.is_empty());
        assert!(matches!(
            bootstrap.as_slice(),
            [wire::ChatSessionCommand::Resume { .. }]
        ));
    }

    #[test]
    fn rejected_and_unpersisted_commands_cannot_become_successful_retries() {
        let dir = sinomo_testdir::TestDir::new("chat-rejected-receipt");
        let mut journal = CommandJournal::open(&dir, "host").unwrap();
        let command = wire::ChatSessionCommand::FetchAgents;
        let original = journal.path.clone();
        let blocked_parent = dir.join("not-a-directory");
        fs::write(&blocked_parent, "blocked").unwrap();
        journal.path = blocked_parent.join("state.json");
        assert!(journal.admit("operation", &command).is_err());
        journal.path = original;
        assert!(journal.admit("operation", &command).unwrap());
        journal.start("operation").unwrap();
        journal.reject("operation", "policy denied".into()).unwrap();
        assert_eq!(
            journal.admit("operation", &command).unwrap_err(),
            "policy denied"
        );
    }
    #[test]
    fn durable_admission_replays_only_unstarted_commands_and_rejects_changed_retries() {
        let dir = sinomo_testdir::TestDir::new("chat-command-recovery");
        let journal = CommandJournal::open(&dir, "host").unwrap();
        let command = wire::ChatSessionCommand::Send {
            session_id: "session".into(),
            text: "do work".into(),
            attachments: vec![],
        };
        assert!(journal.admit("one", &command).unwrap());
        assert!(!journal.admit("one", &command).unwrap());
        assert!(
            journal
                .admit("one", &wire::ChatSessionCommand::FetchAgents)
                .is_err()
        );
        assert!(journal.admit("two", &command).unwrap());
        journal.start("one").unwrap();
        drop(journal);
        let reopened = CommandJournal::open(&dir, "host").unwrap();
        let (_, pending, interrupted) = reopened.recovery().unwrap();
        assert!(interrupted);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "two");
        assert!(reopened.admit("one", &command).is_err());
    }
}
