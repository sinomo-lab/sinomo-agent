use crate::internal::*;
use fs2::FileExt;
use sinomo_core::storage::durable_file::{
    DurableAtomicFileOptions, remove_file_if_exists_and_sync_parent, write_file_atomic,
};
use std::ffi::OsStr;
use std::io::Read;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const SANDBOX_LOCAL_EXECUTION_OBJECT_VERSION: u16 = 1;
const SANDBOX_LOCAL_EXECUTION_FILE: &str = "local-execution.sobj";
const SANDBOX_LOCAL_EXECUTION_LOCK_FILE: &str = "local-execution.lock";
const SANDBOX_LOCAL_EXECUTION_STDOUT_FILE: &str = "local-execution.stdout";
const SANDBOX_LOCAL_EXECUTION_STDERR_FILE: &str = "local-execution.stderr";
const SANDBOX_EXTERNAL_CLEANUP_WAIT_MS: u64 = 5_000;
const SANDBOX_EXTERNAL_COMMAND_WAIT_MS: u64 = 750;
const SANDBOX_EXTERNAL_ABSENCE_STABILITY_MS: u64 = 500;
const SANDBOX_EXTERNAL_OUTPUT_LIMIT: usize = 64 * 1024;

/// Cross-process exclusive ownership of one local sandbox execution workspace.
/// The native runner keeps this guard alive from recovery through terminal
/// settlement; observers use a non-blocking acquisition and surface `Busy`
/// instead of mistaking a live `GuestActive` marker for a crashed host.
#[derive(Debug)]
pub struct SandboxLocalExecutionLease {
    sandbox_id: SandboxId,
    _file: fs::File,
}

impl Drop for SandboxLocalExecutionLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self._file);
    }
}

impl SandboxLocalExecutionLease {
    pub(crate) fn ensure_scope(&self, sandbox_id: &SandboxId) -> Result<(), SandboxStoreError> {
        if self.sandbox_id == *sandbox_id {
            Ok(())
        } else {
            Err(SandboxStoreError::Io {
                path: SANDBOX_LOCAL_EXECUTION_LOCK_FILE.to_string(),
                detail: format!(
                    "local execution lease for {} cannot authorize sandbox {sandbox_id}",
                    self.sandbox_id
                ),
            })
        }
    }
}

pub fn try_acquire_sandbox_local_execution_lease(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
) -> Result<SandboxLocalExecutionLease, SandboxStoreError> {
    // The anchor deliberately lives outside the replaceable instance tree.
    // Destroy/recreate of an instance must continue contending on the same
    // inode while an older owner is still alive.
    let locks_root = store.runtime_root().join("locks");
    fs::create_dir_all(&locks_root)
        .map_err(|source| local_execution_io_error(&locks_root, source))?;
    let path = locks_root.join(format!("{}.lock", sandbox_id.as_str()));
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| local_execution_io_error(&path, source))?;
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(SandboxLocalExecutionLease {
            sandbox_id: sandbox_id.clone(),
            _file: file,
        }),
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
            Err(SandboxStoreError::LocalExecutionBusy {
                sandbox_id: sandbox_id.clone(),
            })
        }
        Err(source) => Err(local_execution_io_error(&path, source)),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxLocalExecutionOutputRef {
    pub file_name: String,
    pub length: u64,
    pub blake3: String,
}

/// Durable identity for work that can outlive the sandbox host process. The
/// execution journal must not be released until this owner is proven absent;
/// otherwise a late writer could mutate files after recovery reports the
/// command terminal.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxLocalExecutionLinuxProcessGroup {
    pub process_group_id: i32,
    pub leader_process_id: u32,
    pub leader_start_time_ticks: u64,
}

impl SandboxLocalExecutionLinuxProcessGroup {
    #[cfg(target_os = "linux")]
    pub fn capture(process_group_id: i32, leader_process_id: u32) -> Result<Self, String> {
        let leader_start_time_ticks = linux_process_start_time_ticks(leader_process_id)
            .ok_or_else(|| {
                format!(
                    "could not capture Linux process start identity for pid {leader_process_id}"
                )
            })?;
        Ok(Self {
            process_group_id,
            leader_process_id,
            leader_start_time_ticks,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SandboxLocalExecutionExternalOwner {
    Container {
        runtime_command: String,
        container_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        launcher: Option<SandboxLocalExecutionLinuxProcessGroup>,
    },
    LinuxProcessGroup {
        process_group: SandboxLocalExecutionLinuxProcessGroup,
    },
}

impl SandboxLocalExecutionExternalOwner {
    pub fn container(
        runtime_command: impl Into<String>,
        container_name: impl Into<String>,
    ) -> Self {
        Self::Container {
            runtime_command: runtime_command.into(),
            container_name: container_name.into(),
            launcher: None,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn linux_process_group(
        process_group_id: i32,
        leader_process_id: u32,
    ) -> Result<Self, String> {
        Ok(Self::LinuxProcessGroup {
            process_group: SandboxLocalExecutionLinuxProcessGroup::capture(
                process_group_id,
                leader_process_id,
            )?,
        })
    }

    pub fn with_container_launcher(
        self,
        launcher: SandboxLocalExecutionLinuxProcessGroup,
    ) -> Result<Self, String> {
        match self {
            Self::Container {
                runtime_command,
                container_name,
                ..
            } => Ok(Self::Container {
                runtime_command,
                container_name,
                launcher: Some(launcher),
            }),
            Self::LinuxProcessGroup { .. } => {
                Err("cannot attach a container launcher to a process-group owner".to_string())
            }
        }
    }
}

/// Durable local-only execution journal. This marker never authorizes cluster
/// copy-back, checkpoint restore, or filesystem mutation; it exists solely to
/// make daemonless guest liveness cleanup and terminal audit recovery durable
/// after host failure.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxLocalExecutionJournal {
    pub sandbox_id: SandboxId,
    pub phase: SandboxLocalExecutionJournalPhase,
    pub outcome: SandboxLocalExecutionOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_command_record: Option<SandboxCommandRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_stdout: Option<SandboxLocalExecutionOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_stderr: Option<SandboxLocalExecutionOutputRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_owner: Option<SandboxLocalExecutionExternalOwner>,
    #[serde(default)]
    pub external_owner_cleanup_completed: bool,
    #[serde(default)]
    pub audit_completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_failure: Option<SandboxLocalExecutionCleanupFailure>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl SandboxLocalExecutionJournal {
    pub fn preparing(manifest: &SandboxInstanceManifest, now: OffsetDateTime) -> Self {
        Self {
            sandbox_id: manifest.sandbox_id.clone(),
            phase: SandboxLocalExecutionJournalPhase::Preparing,
            outcome: SandboxLocalExecutionOutcome::InProgress,
            pending_command_record: None,
            pending_stdout: None,
            pending_stderr: None,
            external_owner: None,
            external_owner_cleanup_completed: false,
            audit_completed: false,
            cleanup_failure: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn transition(&mut self, phase: SandboxLocalExecutionJournalPhase, now: OffsetDateTime) {
        self.phase = phase;
        self.cleanup_failure = None;
        self.updated_at = now;
    }

    pub fn settle_primary(
        &mut self,
        outcome: SandboxLocalExecutionOutcome,
        command_record: SandboxCommandRecord,
        stdout: Option<SandboxLocalExecutionOutputRef>,
        stderr: Option<SandboxLocalExecutionOutputRef>,
        now: OffsetDateTime,
    ) {
        self.outcome = outcome;
        self.pending_command_record = Some(command_record);
        self.pending_stdout = stdout;
        self.pending_stderr = stderr;
        self.phase = SandboxLocalExecutionJournalPhase::OutcomeRecorded;
        self.cleanup_failure = None;
        self.updated_at = now;
    }

    pub fn mark_audit_completed(&mut self, now: OffsetDateTime) {
        self.audit_completed = true;
        self.phase = SandboxLocalExecutionJournalPhase::OutcomeRecorded;
        self.cleanup_failure = None;
        self.updated_at = now;
    }

    pub fn record_cleanup_failure(
        &mut self,
        stage: SandboxLocalExecutionCleanupStage,
        detail: impl Into<String>,
        now: OffsetDateTime,
    ) {
        self.phase = SandboxLocalExecutionJournalPhase::CleanupPending;
        self.cleanup_failure = Some(SandboxLocalExecutionCleanupFailure {
            stage,
            detail: detail.into(),
        });
        self.updated_at = now;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxLocalExecutionJournalPhase {
    Preparing,
    GuestActive,
    OutcomeRecorded,
    CleanupPending,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum SandboxLocalExecutionOutcome {
    InProgress,
    Succeeded,
    Cancelled,
    Failed { detail: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxLocalExecutionCleanupFailure {
    pub stage: SandboxLocalExecutionCleanupStage,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxLocalExecutionCleanupStage {
    ExternalOwner,
    AuditRecord,
    CommandHistory,
    OutputDelete,
    MarkerDelete,
}

pub fn sandbox_local_execution_journal_path(layout: &LocalSandboxInstanceLayout) -> PathBuf {
    layout.metadata.join(SANDBOX_LOCAL_EXECUTION_FILE)
}

pub fn read_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
) -> Result<Option<SandboxLocalExecutionJournal>, SandboxStoreError> {
    let path = sandbox_local_execution_journal_path(&store.layout(sandbox_id));
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(local_execution_io_error(&path, source)),
    };
    let record =
        sinomo_object_serde::from_bytes(&bytes).map_err(|source| SandboxStoreError::Object {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(Some(record))
}

pub fn write_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    record: &SandboxLocalExecutionJournal,
) -> Result<(), SandboxStoreError> {
    lease.ensure_scope(&record.sandbox_id)?;
    let layout = store.layout(&record.sandbox_id);
    fs::create_dir_all(&layout.metadata)
        .map_err(|source| local_execution_io_error(&layout.metadata, source))?;
    let path = sandbox_local_execution_journal_path(&layout);
    let bytes = sinomo_object_serde::to_bytes(
        record,
        SANDBOX_LOCAL_EXECUTION_OBJECT_VERSION,
        LengthMode::U32,
    )
    .map_err(|source| SandboxStoreError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    write_file_atomic(
        &path,
        &bytes,
        DurableAtomicFileOptions::new(SANDBOX_LOCAL_EXECUTION_FILE, "local-execution").sync_all(),
    )
    .map_err(|source| local_execution_io_error(&path, source))
}

pub fn begin_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    manifest: &SandboxInstanceManifest,
    now: OffsetDateTime,
) -> Result<SandboxLocalExecutionJournal, SandboxStoreError> {
    let journal = SandboxLocalExecutionJournal::preparing(manifest, now);
    write_sandbox_local_execution_journal(store, lease, &journal)?;
    Ok(journal)
}

/// Persist the command identity needed to audit a host crash before the guest
/// can become active. Normal settlement replaces this provisional failed record
/// with the actual terminal record; crash recovery completes it exactly once.
pub fn prepare_sandbox_local_execution_audit_identity(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut SandboxLocalExecutionJournal,
    command_record: SandboxCommandRecord,
    now: OffsetDateTime,
) -> Result<(), SandboxStoreError> {
    lease.ensure_scope(&journal.sandbox_id)?;
    journal.pending_command_record = Some(command_record);
    journal.updated_at = now;
    write_sandbox_local_execution_journal(store, lease, journal)
}

/// Persist an engine/container or process-group identity before launching it.
/// Recovery treats the identity as potentially live even when the launch call
/// itself never reports success.
pub fn prepare_sandbox_local_execution_external_owner(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut SandboxLocalExecutionJournal,
    owner: SandboxLocalExecutionExternalOwner,
    now: OffsetDateTime,
) -> Result<(), SandboxStoreError> {
    lease.ensure_scope(&journal.sandbox_id)?;
    journal.external_owner = Some(owner);
    journal.external_owner_cleanup_completed = false;
    journal.updated_at = now;
    write_sandbox_local_execution_journal(store, lease, journal)
}

/// Stop and prove absence of external work before releasing the execution
/// journal. Failure is durable and leaves the marker quarantining the local
/// workspace for a later recovery attempt, without reverting visible writes.
pub fn cleanup_sandbox_local_execution_external_owner(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut SandboxLocalExecutionJournal,
    now: OffsetDateTime,
) -> Result<(), SandboxStoreError> {
    lease.ensure_scope(&journal.sandbox_id)?;
    if journal.external_owner_cleanup_completed || journal.external_owner.is_none() {
        return Ok(());
    }
    let owner = journal.external_owner.clone().expect("owner checked above");
    if let Err(detail) = cleanup_external_owner(&owner) {
        journal.record_cleanup_failure(
            SandboxLocalExecutionCleanupStage::ExternalOwner,
            detail.clone(),
            now,
        );
        write_sandbox_local_execution_journal(store, lease, journal)?;
        return Err(SandboxStoreError::Io {
            path: sandbox_local_execution_journal_path(&store.layout(&journal.sandbox_id))
                .display()
                .to_string(),
            detail,
        });
    }
    journal.external_owner_cleanup_completed = true;
    if matches!(
        journal
            .cleanup_failure
            .as_ref()
            .map(|failure| &failure.stage),
        Some(SandboxLocalExecutionCleanupStage::ExternalOwner)
    ) {
        journal.cleanup_failure = None;
    }
    journal.updated_at = now;
    write_sandbox_local_execution_journal(store, lease, journal)
}

pub fn activate_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut SandboxLocalExecutionJournal,
    now: OffsetDateTime,
) -> Result<(), SandboxStoreError> {
    journal.transition(SandboxLocalExecutionJournalPhase::GuestActive, now);
    write_sandbox_local_execution_journal(store, lease, journal)
}

#[allow(clippy::too_many_arguments)]
pub fn settle_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &mut SandboxLocalExecutionJournal,
    outcome: SandboxLocalExecutionOutcome,
    command_record: SandboxCommandRecord,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    now: OffsetDateTime,
) -> Result<(), SandboxStoreError> {
    lease.ensure_scope(&journal.sandbox_id)?;
    if journal.external_owner.is_some() && !journal.external_owner_cleanup_completed {
        return Err(SandboxStoreError::Io {
            path: sandbox_local_execution_journal_path(&store.layout(&journal.sandbox_id))
                .display()
                .to_string(),
            detail: "cannot settle local execution before external owner absence is confirmed"
                .to_string(),
        });
    }
    let (stdout_ref, stderr_ref) = if command_record.response.is_some() {
        (
            Some(write_local_execution_output(
                store,
                &journal.sandbox_id,
                SANDBOX_LOCAL_EXECUTION_STDOUT_FILE,
                &stdout,
            )?),
            Some(write_local_execution_output(
                store,
                &journal.sandbox_id,
                SANDBOX_LOCAL_EXECUTION_STDERR_FILE,
                &stderr,
            )?),
        )
    } else {
        (None, None)
    };
    journal.settle_primary(outcome, command_record, stdout_ref, stderr_ref, now);
    write_sandbox_local_execution_journal(store, lease, journal)
}

pub fn remove_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    sandbox_id: &SandboxId,
) -> Result<bool, SandboxStoreError> {
    lease.ensure_scope(sandbox_id)?;
    let layout = store.layout(sandbox_id);
    for path in [
        layout.metadata.join(SANDBOX_LOCAL_EXECUTION_STDOUT_FILE),
        layout.metadata.join(SANDBOX_LOCAL_EXECUTION_STDERR_FILE),
    ] {
        remove_file_if_exists_and_sync_parent(&path)
            .map_err(|source| local_execution_io_error(&path, source))?;
    }
    let path = sandbox_local_execution_journal_path(&layout);
    remove_file_if_exists_and_sync_parent(&path)
        .map_err(|source| local_execution_io_error(&path, source))
}

/// Recover and settle any daemonless execution journal before another
/// observer or mutator accesses the local sandbox. The operation is idempotent:
/// external-owner termination and the retryable audit payload are durable,
/// command history is appended only after its record exists, and cleanup
/// failures leave the marker quarantining the workspace for the next retry.
/// Recovery preserves all workspace writes that happened before the external
/// owner was stopped.
pub fn recover_sandbox_local_execution_journal(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    now: OffsetDateTime,
) -> Result<bool, SandboxStoreError> {
    let lease = try_acquire_sandbox_local_execution_lease(store, sandbox_id)?;
    recover_sandbox_local_execution_journal_with_lease(store, &lease, sandbox_id, now)
}

pub fn recover_sandbox_local_execution_journal_with_lease(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    sandbox_id: &SandboxId,
    now: OffsetDateTime,
) -> Result<bool, SandboxStoreError> {
    lease.ensure_scope(sandbox_id)?;
    let Some(mut journal) = read_sandbox_local_execution_journal(store, sandbox_id)? else {
        return Ok(false);
    };
    if matches!(journal.outcome, SandboxLocalExecutionOutcome::InProgress) {
        let detail =
            "sandbox host stopped before the local guest reported a terminal outcome".to_string();
        journal.outcome = SandboxLocalExecutionOutcome::Failed {
            detail: detail.clone(),
        };
        if let Some(command_record) = journal.pending_command_record.as_mut() {
            command_record.status = SandboxCommandRecordStatus::Failed;
            command_record.completed_at = Some(now);
            command_record.response = None;
            command_record.error = Some(detail);
        }
        journal.transition(SandboxLocalExecutionJournalPhase::OutcomeRecorded, now);
        write_sandbox_local_execution_journal(store, lease, &journal)?;
    }

    cleanup_sandbox_local_execution_external_owner(
        store,
        lease,
        &mut journal,
        OffsetDateTime::now_utc(),
    )?;

    if !journal.audit_completed {
        if let Err(error) = persist_sandbox_local_execution_journal_command_record(store, &journal)
        {
            journal.record_cleanup_failure(
                SandboxLocalExecutionCleanupStage::AuditRecord,
                error.to_string(),
                now,
            );
            write_sandbox_local_execution_journal(store, lease, &journal)?;
            return Err(error);
        }
        if let Some(command_record) = journal.pending_command_record.as_ref()
            && let Err(error) = record_local_command_history(
                store,
                &journal.sandbox_id,
                &command_record.command_id,
                command_record.completed_at.unwrap_or(journal.updated_at),
            )
        {
            journal.record_cleanup_failure(
                SandboxLocalExecutionCleanupStage::CommandHistory,
                error.to_string(),
                now,
            );
            write_sandbox_local_execution_journal(store, lease, &journal)?;
            return Err(error);
        }
        journal.mark_audit_completed(now);
        write_sandbox_local_execution_journal(store, lease, &journal)?;
    }
    journal.record_cleanup_failure(
        SandboxLocalExecutionCleanupStage::MarkerDelete,
        "terminal cleanup is removing the durable local execution marker",
        now,
    );
    write_sandbox_local_execution_journal(store, lease, &journal)?;
    if let Err(error) = remove_sandbox_local_execution_journal(store, lease, sandbox_id) {
        journal.record_cleanup_failure(
            SandboxLocalExecutionCleanupStage::OutputDelete,
            error.to_string(),
            now,
        );
        write_sandbox_local_execution_journal(store, lease, &journal)?;
        return Err(error);
    }
    Ok(true)
}

fn cleanup_external_owner(owner: &SandboxLocalExecutionExternalOwner) -> Result<(), String> {
    match owner {
        SandboxLocalExecutionExternalOwner::Container {
            runtime_command,
            container_name,
            launcher,
        } => {
            if let Some(launcher) = launcher {
                cleanup_linux_process_group(launcher)?;
            }
            cleanup_container_owner(runtime_command, container_name)
        }
        SandboxLocalExecutionExternalOwner::LinuxProcessGroup { process_group } => {
            cleanup_linux_process_group(process_group)
        }
    }
}

fn cleanup_container_owner(runtime_command: &str, container_name: &str) -> Result<(), String> {
    if runtime_command.is_empty() || container_name.is_empty() {
        return Err("container cleanup identity is incomplete".to_string());
    }
    let deadline = Instant::now() + Duration::from_millis(SANDBOX_EXTERNAL_CLEANUP_WAIT_MS);
    let mut absent_since = None;
    let mut last_detail = "container absence has not been checked".to_string();
    while Instant::now() < deadline {
        match run_bounded_external_command(
            OsStr::new(runtime_command),
            &["rm", "-f", container_name],
            Duration::from_millis(SANDBOX_EXTERNAL_COMMAND_WAIT_MS),
        ) {
            Ok((status, _, stderr)) if !status.success() => {
                last_detail = format!(
                    "container removal exited with {status}: {}",
                    String::from_utf8_lossy(&stderr).trim()
                );
            }
            Err(detail) => last_detail = detail,
            _ => {}
        }

        let filter = format!("name=^/{container_name}$");
        match run_bounded_external_command(
            OsStr::new(runtime_command),
            &["ps", "--all", "--filter", &filter, "--format", "{{.Names}}"],
            Duration::from_millis(SANDBOX_EXTERNAL_COMMAND_WAIT_MS),
        ) {
            Ok((status, stdout, _stderr)) if status.success() => {
                let present = String::from_utf8_lossy(&stdout)
                    .lines()
                    .any(|name| name.trim() == container_name);
                if present {
                    absent_since = None;
                    last_detail = format!("container `{container_name}` is still present");
                } else {
                    let absent_since = absent_since.get_or_insert_with(Instant::now);
                    if absent_since.elapsed()
                        >= Duration::from_millis(SANDBOX_EXTERNAL_ABSENCE_STABILITY_MS)
                    {
                        return Ok(());
                    }
                }
            }
            Ok((status, _, stderr)) => {
                absent_since = None;
                last_detail = format!(
                    "container absence probe exited with {status}: {}",
                    String::from_utf8_lossy(&stderr).trim()
                );
            }
            Err(detail) => {
                absent_since = None;
                last_detail = detail;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "could not confirm container `{container_name}` absent within {SANDBOX_EXTERNAL_CLEANUP_WAIT_MS}ms: {last_detail}"
    ))
}

fn run_bounded_external_command(
    executable: &OsStr,
    args: &[&str],
    timeout: Duration,
) -> Result<(ExitStatus, Vec<u8>, Vec<u8>), String> {
    let mut child = Command::new(executable)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| {
            format!(
                "failed to start external cleanup command `{}`: {source}",
                executable.to_string_lossy()
            )
        })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "external cleanup command did not provide a stdout pipe".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "external cleanup command did not provide a stderr pipe".to_string())?;
    let stdout = spawn_bounded_external_reader(stdout);
    let stderr = spawn_bounded_external_reader(stderr);
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|source| format!("failed waiting for external cleanup command: {source}"))?
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "external cleanup command `{}` did not finish within {}ms",
                executable.to_string_lossy(),
                timeout.as_millis()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout
        .recv_timeout(Duration::from_millis(SANDBOX_EXTERNAL_COMMAND_WAIT_MS))
        .map_err(|_| "external cleanup stdout did not close".to_string())?;
    let stderr = stderr
        .recv_timeout(Duration::from_millis(SANDBOX_EXTERNAL_COMMAND_WAIT_MS))
        .map_err(|_| "external cleanup stderr did not close".to_string())?;
    Ok((status, stdout, stderr))
}

fn spawn_bounded_external_reader<R: Read + Send + 'static>(
    mut reader: R,
) -> mpsc::Receiver<Vec<u8>> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut captured = Vec::new();
        let mut buffer = [0_u8; 4 * 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    let keep = SANDBOX_EXTERNAL_OUTPUT_LIMIT
                        .saturating_sub(captured.len())
                        .min(read);
                    captured.extend_from_slice(&buffer[..keep]);
                }
            }
        }
        let _ = sender.send(captured);
    });
    receiver
}

#[cfg(target_os = "linux")]
fn linux_process_start_time_ticks(process_id: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).ok()?;
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
    fields.nth(19)?.parse().ok()
}

#[cfg(target_os = "linux")]
fn linux_process_state_and_group(stat: &str) -> Option<(char, i32)> {
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
    let state = fields.next()?.chars().next()?;
    fields.next()?;
    let process_group_id = fields.next()?.parse().ok()?;
    Some((state, process_group_id))
}

#[cfg(target_os = "linux")]
fn linux_process_group_has_live_members(process_group_id: i32) -> Option<bool> {
    let entries = fs::read_dir("/proc").ok()?;
    for entry in entries {
        let entry = entry.ok()?;
        let Some(process_id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let stat = match fs::read_to_string(format!("/proc/{process_id}/stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        let (state, observed_process_group_id) = linux_process_state_and_group(&stat)?;
        if observed_process_group_id == process_group_id && !matches!(state, 'Z' | 'X' | 'x') {
            return Some(true);
        }
    }
    Some(false)
}

#[cfg(not(target_os = "linux"))]
fn linux_process_start_time_ticks(_process_id: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn cleanup_linux_process_group(
    process_group: &SandboxLocalExecutionLinuxProcessGroup,
) -> Result<(), String> {
    let process_group_id = process_group.process_group_id;
    let leader_process_id = process_group.leader_process_id;
    let leader_start_time_ticks = process_group.leader_start_time_ticks;
    if process_group_id <= 0 || leader_process_id == 0 {
        return Err("Linux process-group cleanup identity is invalid".to_string());
    }
    if let Some(observed) = linux_process_start_time_ticks(leader_process_id)
        && leader_start_time_ticks != observed
    {
        // The original group leader is gone and its PID has been reused. A
        // signal to the recycled group would target unrelated work.
        return Ok(());
    }
    let group = -process_group_id;
    let probe = unsafe { libc::kill(group, 0) };
    if probe != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    if linux_process_group_has_live_members(process_group_id) == Some(false) {
        return Ok(());
    }
    unsafe {
        libc::kill(group, libc::SIGKILL);
    }
    let deadline = Instant::now() + Duration::from_millis(SANDBOX_EXTERNAL_CLEANUP_WAIT_MS);
    loop {
        let result = unsafe { libc::kill(group, 0) };
        if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        if linux_process_group_has_live_members(process_group_id) == Some(false) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "could not confirm Linux process group {process_group_id} absent within {SANDBOX_EXTERNAL_CLEANUP_WAIT_MS}ms"
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(not(target_os = "linux"))]
fn cleanup_linux_process_group(
    process_group: &SandboxLocalExecutionLinuxProcessGroup,
) -> Result<(), String> {
    let process_group_id = process_group.process_group_id;
    Err(format!(
        "cannot verify Linux process group {process_group_id} on this platform"
    ))
}

pub fn persist_sandbox_local_execution_audit(
    store: &LocalSandboxInstanceStore,
    lease: &SandboxLocalExecutionLease,
    journal: &SandboxLocalExecutionJournal,
) -> Result<(), SandboxStoreError> {
    lease.ensure_scope(&journal.sandbox_id)?;
    persist_sandbox_local_execution_journal_command_record(store, journal)?;
    let Some(command_record) = journal.pending_command_record.as_ref() else {
        return Ok(());
    };
    record_local_command_history(
        store,
        &journal.sandbox_id,
        &command_record.command_id,
        command_record.completed_at.unwrap_or(journal.updated_at),
    )
}

fn persist_sandbox_local_execution_journal_command_record(
    store: &LocalSandboxInstanceStore,
    journal: &SandboxLocalExecutionJournal,
) -> Result<(), SandboxStoreError> {
    let Some(command_record) = journal.pending_command_record.as_ref() else {
        return Ok(());
    };
    let mut sandbox_fs = LocalSandboxFileSystem::new(store.layout(&journal.sandbox_id));
    if command_record.response.is_some() {
        let stdout = read_local_execution_output(
            store,
            &journal.sandbox_id,
            journal.pending_stdout.as_ref(),
            SANDBOX_LOCAL_EXECUTION_STDOUT_FILE,
        )?;
        let stderr = read_local_execution_output(
            store,
            &journal.sandbox_id,
            journal.pending_stderr.as_ref(),
            SANDBOX_LOCAL_EXECUTION_STDERR_FILE,
        )?;
        persist_command_outputs(&mut sandbox_fs, command_record, &stdout, &stderr)?;
    } else {
        persist_command_record(&mut sandbox_fs, command_record)?;
    }
    Ok(())
}

fn write_local_execution_output(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    file_name: &'static str,
    bytes: &[u8],
) -> Result<SandboxLocalExecutionOutputRef, SandboxStoreError> {
    let path = store.layout(sandbox_id).metadata.join(file_name);
    write_file_atomic(
        &path,
        bytes,
        DurableAtomicFileOptions::new(file_name, "local-execution-output").sync_all(),
    )
    .map_err(|source| local_execution_io_error(&path, source))?;
    Ok(SandboxLocalExecutionOutputRef {
        file_name: file_name.to_string(),
        length: bytes.len() as u64,
        blake3: blake3::hash(bytes).to_hex().to_string(),
    })
}

fn read_local_execution_output(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    output_ref: Option<&SandboxLocalExecutionOutputRef>,
    expected_file_name: &'static str,
) -> Result<Vec<u8>, SandboxStoreError> {
    let output_ref = output_ref.ok_or_else(|| SandboxStoreError::Object {
        path: expected_file_name.to_string(),
        detail: "durable command output reference is missing".to_string(),
    })?;
    if output_ref.file_name != expected_file_name {
        return Err(SandboxStoreError::Object {
            path: output_ref.file_name.clone(),
            detail: format!(
                "unexpected local execution output reference; expected {expected_file_name}"
            ),
        });
    }
    let path = store.layout(sandbox_id).metadata.join(expected_file_name);
    let bytes = fs::read(&path).map_err(|source| local_execution_io_error(&path, source))?;
    let actual_hash = blake3::hash(&bytes).to_hex().to_string();
    if bytes.len() as u64 != output_ref.length || actual_hash != output_ref.blake3 {
        return Err(SandboxStoreError::Object {
            path: path.display().to_string(),
            detail: format!(
                "durable command output integrity mismatch: expected {} bytes {}, found {} bytes {}",
                output_ref.length,
                output_ref.blake3,
                bytes.len(),
                actual_hash
            ),
        });
    }
    Ok(bytes)
}

fn local_execution_io_error(path: &Path, source: impl std::fmt::Display) -> SandboxStoreError {
    SandboxStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_process_group_liveness_ignores_zombies_but_not_live_members() {
        assert_eq!(
            linux_process_state_and_group("123 (sleep worker) Z 1 77 77 0"),
            Some(('Z', 77))
        );
        assert_eq!(
            linux_process_state_and_group("124 (sleep worker) S 1 77 77 0"),
            Some(('S', 77))
        );
        let current_process_group = unsafe { libc::getpgrp() };
        assert_eq!(
            linux_process_group_has_live_members(current_process_group),
            Some(true)
        );
        assert_eq!(linux_process_group_has_live_members(i32::MAX), Some(false));
    }

    fn create_sandbox(
        root: &sinomo_testdir::TestDir,
        suffix: &str,
    ) -> (
        LocalSandboxInstanceStore,
        SandboxInstanceManifest,
        LocalSandboxInstanceLayout,
    ) {
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let sandbox_id =
            SandboxId::from_legacy_named(format!("local_execution_{suffix}")).expect("sandbox");
        let (manifest, layout) = store
            .create(
                sandbox_id,
                SandboxToolchainId::from_legacy_named("local_execution_toolchain")
                    .expect("toolchain"),
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("create sandbox");
        (store, manifest, layout)
    }

    fn activate_execution_journal(
        store: &LocalSandboxInstanceStore,
        manifest: &SandboxInstanceManifest,
    ) -> (SandboxLocalExecutionLease, SandboxLocalExecutionJournal) {
        let lease = try_acquire_sandbox_local_execution_lease(store, &manifest.sandbox_id)
            .expect("acquire lease");
        let mut journal = begin_sandbox_local_execution_journal(
            store,
            &lease,
            manifest,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("begin journal");
        prepare_sandbox_local_execution_audit_identity(
            store,
            &lease,
            &mut journal,
            cancelled_command_record(manifest),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("prepare crash audit identity");
        activate_sandbox_local_execution_journal(
            store,
            &lease,
            &mut journal,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("activate journal");
        (lease, journal)
    }

    fn cancelled_command_record(manifest: &SandboxInstanceManifest) -> SandboxCommandRecord {
        SandboxCommandRecord {
            sandbox_id: manifest.sandbox_id.clone(),
            command_id: SandboxCommandId::from_legacy_named("local_execution_command")
                .expect("command"),
            status: SandboxCommandRecordStatus::Cancelled,
            workspace_root: "/services/sandbox/instances/local".to_string(),
            toolchain_id: manifest.toolchain_id.clone(),
            argv: vec!["sh".to_string()],
            cwd: SandboxPath::new("/work").expect("cwd"),
            env: BTreeMap::new(),
            started_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: Some(OffsetDateTime::UNIX_EPOCH),
            response: None,
            error: Some("cancelled".to_string()),
        }
    }

    #[test]
    fn local_execution_journal_round_trips_durably() {
        let root = sinomo_testdir::TestDir::new("sandbox-local-execution-journal");
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let sandbox_id = SandboxId::from_legacy_named("local_journal").expect("sandbox");
        let (manifest, _) = store
            .create(
                sandbox_id.clone(),
                SandboxToolchainId::from_legacy_named("local_journal_toolchain")
                    .expect("toolchain"),
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("create sandbox");
        let mut record =
            SandboxLocalExecutionJournal::preparing(&manifest, OffsetDateTime::UNIX_EPOCH);
        record.transition(
            SandboxLocalExecutionJournalPhase::GuestActive,
            OffsetDateTime::UNIX_EPOCH,
        );
        record.record_cleanup_failure(
            SandboxLocalExecutionCleanupStage::ExternalOwner,
            "injected external-owner cleanup failure",
            OffsetDateTime::UNIX_EPOCH,
        );

        let lease =
            try_acquire_sandbox_local_execution_lease(&store, &sandbox_id).expect("acquire lease");
        write_sandbox_local_execution_journal(&store, &lease, &record).expect("write record");
        assert_eq!(
            read_sandbox_local_execution_journal(&store, &sandbox_id).expect("read record"),
            Some(record)
        );
        assert!(
            remove_sandbox_local_execution_journal(&store, &lease, &sandbox_id)
                .expect("remove record")
        );
    }

    #[test]
    fn recovery_preserves_partial_workspace_state_after_host_crash() {
        let root = sinomo_testdir::TestDir::new("sandbox-local-execution-guest-crash");
        let (store, manifest, layout) = create_sandbox(&root, "guest_crash");
        let path = SandboxPath::new("/work/state.txt").expect("path");
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        sandbox_fs
            .write(&path, b"baseline")
            .expect("write baseline");
        let (lease, journal) = activate_execution_journal(&store, &manifest);
        let crash_command_id = journal
            .pending_command_record
            .as_ref()
            .expect("crash audit identity")
            .command_id
            .clone();
        sandbox_fs
            .write(&path, b"partial guest effect")
            .expect("write partial effect");
        drop(lease);

        assert!(
            recover_sandbox_local_execution_journal(
                &store,
                &manifest.sandbox_id,
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("recover guest crash")
        );
        assert_eq!(
            sandbox_fs.read(&path, None).expect("read partial state"),
            b"partial guest effect"
        );
        assert_eq!(
            read_sandbox_local_execution_journal(&store, &manifest.sandbox_id)
                .expect("read marker"),
            None
        );
        let recovered_manifest = store
            .load(&manifest.sandbox_id)
            .expect("load recovered manifest")
            .0;
        assert_eq!(
            recovered_manifest
                .command_history
                .iter()
                .filter(|command_id| **command_id == crash_command_id)
                .count(),
            1
        );
        let mut recovered_fs = LocalSandboxFileSystem::new(store.layout(&manifest.sandbox_id));
        let crash_record =
            load_command_record(&mut recovered_fs, &crash_command_id).expect("load crash audit");
        assert_eq!(crash_record.status, SandboxCommandRecordStatus::Failed);
        assert!(
            crash_record
                .error
                .as_deref()
                .is_some_and(|detail| detail.contains("host stopped"))
        );
        assert!(
            !recover_sandbox_local_execution_journal(
                &store,
                &manifest.sandbox_id,
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("idempotent second recovery")
        );
    }

    #[cfg(unix)]
    #[test]
    fn recovery_waits_for_late_container_writer_before_releasing_journal() {
        use std::os::unix::fs::PermissionsExt;

        let root = sinomo_testdir::TestDir::new("sandbox-local-execution-late-container");
        let (store, manifest, layout) = create_sandbox(&root, "late_container");
        let state_path = layout.work.join("state.txt");
        let active_path = root.join("engine-active");
        let pid_path = root.join("engine-pid");
        let runtime_path = root.join("fake-engine.sh");
        fs::write(&state_path, b"baseline").expect("write baseline");
        fs::write(
            &runtime_path,
            format!(
                r#"#!/bin/sh
active='{}'
pid_file='{}'
name='late-container'
case "$1" in
  rm)
    if [ -f "$active" ]; then
      kill "$(cat "$pid_file")" 2>/dev/null || true
      rm -f "$active"
      exit 0
    fi
    exit 1
    ;;
  ps)
    if [ -f "$active" ]; then
      echo "$name"
    fi
    exit 0
    ;;
esac
exit 2
"#,
                active_path.display(),
                pid_path.display(),
            ),
        )
        .expect("write fake engine");
        let mut permissions = fs::metadata(&runtime_path)
            .expect("fake engine metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&runtime_path, permissions).expect("fake engine permissions");

        let (lease, mut journal) = activate_execution_journal(&store, &manifest);
        prepare_sandbox_local_execution_external_owner(
            &store,
            &lease,
            &mut journal,
            SandboxLocalExecutionExternalOwner::container(
                runtime_path.display().to_string(),
                "late-container",
            ),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("persist container identity");
        drop(lease);

        let mut late_writer = Command::new("/bin/sh")
            .args([
                "-c",
                &format!(
                    "sleep 0.2; echo $$ > '{}'; printf stale-writer > '{}'; touch '{}'; while [ -f '{}' ]; do sleep 0.02; done",
                    pid_path.display(),
                    state_path.display(),
                    active_path.display(),
                    active_path.display(),
                ),
            ])
            .spawn()
            .expect("start delayed engine writer");

        recover_sandbox_local_execution_journal(
            &store,
            &manifest.sandbox_id,
            OffsetDateTime::now_utc(),
        )
        .expect("recover after late container creation");
        let _ = late_writer.wait();
        std::thread::sleep(Duration::from_millis(150));
        let state_after_recovery = fs::read(&state_path).expect("read preserved state");
        assert_eq!(state_after_recovery, b"stale-writer");
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            fs::read(&state_path).expect("read stable state"),
            state_after_recovery,
            "external writer must not mutate the workspace after recovery"
        );
        assert!(!active_path.exists(), "engine owner must be absent");
        assert_eq!(
            read_sandbox_local_execution_journal(&store, &manifest.sandbox_id)
                .expect("read marker"),
            None
        );
    }

    #[test]
    fn audit_failure_does_not_publish_history_and_retries_from_durable_payload() {
        let root = sinomo_testdir::TestDir::new("sandbox-local-execution-audit-failure");
        let (store, manifest, layout) = create_sandbox(&root, "audit_failure");
        let (lease, mut journal) = activate_execution_journal(&store, &manifest);
        let command_record = cancelled_command_record(&manifest);
        let command_id = command_record.command_id.clone();
        settle_sandbox_local_execution_journal(
            &store,
            &lease,
            &mut journal,
            SandboxLocalExecutionOutcome::Succeeded,
            command_record,
            Vec::new(),
            Vec::new(),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("settle primary");
        drop(lease);
        fs::remove_dir_all(&layout.logs).expect("remove logs directory");
        fs::write(&layout.logs, b"injected non-directory").expect("block logs directory");

        recover_sandbox_local_execution_journal(
            &store,
            &manifest.sandbox_id,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect_err("audit persistence must fail");
        let marker = read_sandbox_local_execution_journal(&store, &manifest.sandbox_id)
            .expect("read marker")
            .expect("marker remains");
        assert_eq!(
            marker.cleanup_failure.expect("cleanup failure").stage,
            SandboxLocalExecutionCleanupStage::AuditRecord
        );
        assert!(
            !store
                .load(&manifest.sandbox_id)
                .expect("load manifest")
                .0
                .command_history
                .contains(&command_id),
            "history must not reference an audit record that was not persisted"
        );

        fs::remove_file(&layout.logs).expect("remove audit blocker");
        fs::create_dir_all(&layout.logs).expect("restore logs directory");
        recover_sandbox_local_execution_journal(
            &store,
            &manifest.sandbox_id,
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("retry audit cleanup");
        assert!(
            store
                .load(&manifest.sandbox_id)
                .expect("load manifest")
                .0
                .command_history
                .contains(&command_id)
        );
        let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
        assert_eq!(
            load_command_record(&mut sandbox_fs, &command_id)
                .expect("load recovered command record")
                .command_id,
            command_id
        );
        assert_eq!(
            read_sandbox_local_execution_journal(&store, &manifest.sandbox_id)
                .expect("read marker"),
            None
        );
    }
}
