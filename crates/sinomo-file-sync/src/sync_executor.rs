//! Platform-neutral executor for durable filesystem mutations.
//!
//! Concrete DFS calls remain behind [`WritebackDfsMutationAdapter`]. The
//! executor's ordering is the production contract: durable `InFlight` before
//! send, reconciliation before any ambiguous retry, durable `RemoteApplied`
//! before local finalization, and `Completed` only after that idempotent local
//! effect succeeds.

use crate::local_change_queue::{
    WritebackConflictEvidence, WritebackLocalChangeQueue, WritebackLocalChangeQueueError,
    WritebackLocalOperation, WritebackLocalOperationKind, WritebackLocalOperationPreconditions,
    WritebackLocalOperationRequest, WritebackLocalOperationStatus, WritebackRemoteMutationEvidence,
    WritebackStagedContent,
};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WritebackDfsMutationContent {
    pub descriptor: WritebackStagedContent,
    /// Hash-verified immutable queue blob. Adapters reopen only this path and
    /// stream it into DFS; the mutable Cloud Files path is never reread.
    pub blob_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WritebackDfsMutationCommand {
    pub operation_id: String,
    pub request_digest: String,
    pub kind: WritebackLocalOperationKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub preconditions: WritebackLocalOperationPreconditions,
    pub staged_content: Option<WritebackDfsMutationContent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WritebackDfsMutationOutcome {
    Applied(WritebackRemoteMutationEvidence),
    Conflict(WritebackConflictEvidence),
    Expired { detail: String },
    Ambiguous { detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WritebackDfsMutationReconciliation {
    Applied(WritebackRemoteMutationEvidence),
    NotApplied,
    Conflict(WritebackConflictEvidence),
    Expired { detail: String },
    Ambiguous { detail: String },
}

/// Narrow adapter for future DFS stable-ID/CAS operations.
pub trait WritebackDfsMutationAdapter {
    fn apply(
        &mut self,
        command: &WritebackDfsMutationCommand,
    ) -> Result<WritebackDfsMutationOutcome, String>;

    fn reconcile(
        &mut self,
        command: &WritebackDfsMutationCommand,
    ) -> Result<WritebackDfsMutationReconciliation, String>;
}

/// Idempotent local effect performed only after the DFS mutation is durably
/// known to have applied. A Windows adapter will update/suppress its local
/// placeholder here; tests use deterministic in-memory effects.
pub trait WritebackLocalCompletionEffect {
    fn complete(
        &mut self,
        command: &WritebackDfsMutationCommand,
        evidence: &WritebackRemoteMutationEvidence,
    ) -> Result<WritebackLocalCompletionOutcome, String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WritebackLocalCompletionOutcome {
    Completed,
    /// The local effect observed bytes that are newer than the just-applied
    /// namespace mutation (notably an offline dirty+rename). Those immutable
    /// bytes must be durably queued before the parent mutation can complete.
    EnqueueFollowUp(Box<WritebackLocalOperationRequest>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WritebackSyncExecutionProgress {
    Completed {
        operation_id: String,
    },
    AwaitingReconciliation {
        operation_id: String,
        detail: String,
    },
    CompletionPending {
        operation_id: String,
        detail: String,
    },
    Conflict {
        operation_id: String,
        evidence: WritebackConflictEvidence,
    },
    Noop {
        operation_id: String,
        status: WritebackLocalOperationStatus,
    },
}

#[derive(Debug)]
pub struct WritebackSyncExecutorError {
    pub code: &'static str,
    pub detail: String,
}

impl std::fmt::Display for WritebackSyncExecutorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for WritebackSyncExecutorError {}

pub fn execute_writeback_operation<A, E>(
    queue: &mut WritebackLocalChangeQueue,
    operation_id: &str,
    dfs: &mut A,
    completion: &mut E,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError>
where
    A: WritebackDfsMutationAdapter,
    E: WritebackLocalCompletionEffect,
{
    let operation = queue
        .operation(operation_id)
        .cloned()
        .ok_or_else(|| executor_error("operation-not-found", operation_id))?;
    match operation.status {
        WritebackLocalOperationStatus::Queued => apply_operation(queue, operation, dfs, completion),
        WritebackLocalOperationStatus::InFlight | WritebackLocalOperationStatus::Retrying => {
            reconcile_operation(queue, operation, dfs, completion)
        }
        WritebackLocalOperationStatus::RemoteApplied => {
            finalize_operation(queue, operation, completion)
        }
        status @ (WritebackLocalOperationStatus::Completed
        | WritebackLocalOperationStatus::Conflict
        | WritebackLocalOperationStatus::Failed
        | WritebackLocalOperationStatus::Superseded) => Ok(WritebackSyncExecutionProgress::Noop {
            operation_id: operation.request.operation_id,
            status,
        }),
    }
}

pub fn execute_ready_writeback_operations<A, E>(
    queue: &mut WritebackLocalChangeQueue,
    dfs: &mut A,
    completion: &mut E,
) -> Result<Vec<WritebackSyncExecutionProgress>, WritebackSyncExecutorError>
where
    A: WritebackDfsMutationAdapter,
    E: WritebackLocalCompletionEffect,
{
    execute_ready_writeback_operations_bounded(queue, dfs, completion, usize::MAX)
}

/// Execute at most `max_operations` durable requests in one cooperative pass.
///
/// The remaining requests stay in their current durable states and are picked
/// up by a later pass. This keeps cancellation and lifecycle progress bounded
/// without changing stable operation IDs or retry ordering.
pub fn execute_ready_writeback_operations_bounded<A, E>(
    queue: &mut WritebackLocalChangeQueue,
    dfs: &mut A,
    completion: &mut E,
    max_operations: usize,
) -> Result<Vec<WritebackSyncExecutionProgress>, WritebackSyncExecutorError>
where
    A: WritebackDfsMutationAdapter,
    E: WritebackLocalCompletionEffect,
{
    let operation_ids = queue
        .operations()
        .filter(|operation| {
            matches!(
                operation.status,
                WritebackLocalOperationStatus::Queued
                    | WritebackLocalOperationStatus::InFlight
                    | WritebackLocalOperationStatus::Retrying
                    | WritebackLocalOperationStatus::RemoteApplied
            )
        })
        .map(|operation| operation.request.operation_id.clone())
        .take(max_operations)
        .collect::<Vec<_>>();
    operation_ids
        .into_iter()
        .map(|operation_id| execute_writeback_operation(queue, &operation_id, dfs, completion))
        .collect()
}

fn reconcile_operation<A, E>(
    queue: &mut WritebackLocalChangeQueue,
    operation: WritebackLocalOperation,
    dfs: &mut A,
    completion: &mut E,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError>
where
    A: WritebackDfsMutationAdapter,
    E: WritebackLocalCompletionEffect,
{
    let command = command_for_operation(queue, &operation)?;
    match dfs.reconcile(&command) {
        Ok(WritebackDfsMutationReconciliation::Applied(evidence)) => {
            accept_remote_application(queue, operation, command, evidence, completion)
        }
        Ok(WritebackDfsMutationReconciliation::NotApplied) => {
            apply_operation(queue, operation, dfs, completion)
        }
        Ok(WritebackDfsMutationReconciliation::Conflict(evidence)) => {
            persist_conflict(queue, &command.operation_id, evidence)
        }
        Ok(WritebackDfsMutationReconciliation::Expired { detail }) => persist_conflict(
            queue,
            &command.operation_id,
            terminal_reconciliation_conflict("operation-receipt-expired", detail),
        ),
        Ok(WritebackDfsMutationReconciliation::Ambiguous { detail }) => {
            persist_ambiguous(queue, &command.operation_id, detail)
        }
        Err(detail) => persist_ambiguous(
            queue,
            &command.operation_id,
            format!("reconcile stable DFS operation ID: {detail}"),
        ),
    }
}

fn apply_operation<A, E>(
    queue: &mut WritebackLocalChangeQueue,
    operation: WritebackLocalOperation,
    dfs: &mut A,
    completion: &mut E,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError>
where
    A: WritebackDfsMutationAdapter,
    E: WritebackLocalCompletionEffect,
{
    // Materialize and verify the immutable queue blob before marking the
    // request in flight. No mutable source path is read by this executor.
    let command = command_for_operation(queue, &operation)?;
    queue
        .mark_in_flight(&command.operation_id)
        .map_err(queue_executor_error)?;
    match dfs.apply(&command) {
        Ok(WritebackDfsMutationOutcome::Applied(evidence)) => {
            accept_remote_application(queue, operation, command, evidence, completion)
        }
        Ok(WritebackDfsMutationOutcome::Conflict(evidence)) => {
            persist_conflict(queue, &command.operation_id, evidence)
        }
        Ok(WritebackDfsMutationOutcome::Expired { detail }) => persist_conflict(
            queue,
            &command.operation_id,
            terminal_reconciliation_conflict("operation-receipt-expired", detail),
        ),
        Ok(WritebackDfsMutationOutcome::Ambiguous { detail }) => {
            persist_ambiguous(queue, &command.operation_id, detail)
        }
        Err(detail) => persist_ambiguous(
            queue,
            &command.operation_id,
            format!("DFS mutation transport returned without a definitive outcome: {detail}"),
        ),
    }
}

fn accept_remote_application<E>(
    queue: &mut WritebackLocalChangeQueue,
    operation: WritebackLocalOperation,
    command: WritebackDfsMutationCommand,
    evidence: WritebackRemoteMutationEvidence,
    completion: &mut E,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError>
where
    E: WritebackLocalCompletionEffect,
{
    if evidence.operation_id != command.operation_id {
        return persist_ambiguous(
            queue,
            &command.operation_id,
            format!(
                "DFS returned evidence for operation `{}` while reconciling `{}`",
                evidence.operation_id, command.operation_id
            ),
        );
    }
    queue
        .mark_remote_applied_with_evidence(evidence)
        .map_err(queue_executor_error)?;
    let remote_applied = queue
        .operation(&operation.request.operation_id)
        .cloned()
        .ok_or_else(|| executor_error("operation-not-found", &operation.request.operation_id))?;
    finalize_operation(queue, remote_applied, completion)
}

fn finalize_operation<E>(
    queue: &mut WritebackLocalChangeQueue,
    operation: WritebackLocalOperation,
    completion: &mut E,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError>
where
    E: WritebackLocalCompletionEffect,
{
    let operation_id = operation.request.operation_id.clone();
    let command = command_for_operation(queue, &operation)?;
    let evidence = operation.remote_evidence.as_ref().ok_or_else(|| {
        executor_error(
            "missing-remote-evidence",
            format!("remote-applied operation `{operation_id}` has no durable evidence"),
        )
    })?;
    let completion = match completion.complete(&command, evidence) {
        Ok(completion) => completion,
        Err(detail) => {
            return Ok(WritebackSyncExecutionProgress::CompletionPending {
                operation_id,
                detail,
            });
        }
    };
    if let WritebackLocalCompletionOutcome::EnqueueFollowUp(request) = completion {
        let mut request = *request;
        if let Some(capture_id) = request.capture_id.as_deref()
            && let Some(existing) = queue.operation_id_for_capture(capture_id)
        {
            request.operation_id = existing.to_string();
        }
        queue.enqueue(request).map_err(queue_executor_error)?;
    }
    queue
        .mark_completed(&operation_id)
        .map_err(queue_executor_error)?;
    Ok(WritebackSyncExecutionProgress::Completed { operation_id })
}

fn command_for_operation(
    queue: &WritebackLocalChangeQueue,
    operation: &WritebackLocalOperation,
) -> Result<WritebackDfsMutationCommand, WritebackSyncExecutorError> {
    let staged_content = operation
        .request
        .staged_content
        .as_ref()
        .map(|descriptor| {
            queue
                .staged_content_path(&operation.request.operation_id)
                .map(|blob_path| WritebackDfsMutationContent {
                    descriptor: descriptor.clone(),
                    blob_path,
                })
        })
        .transpose()
        .map_err(queue_executor_error)?;
    Ok(WritebackDfsMutationCommand {
        operation_id: operation.request.operation_id.clone(),
        request_digest: operation.request_digest.clone(),
        kind: operation.request.kind.clone(),
        source_path: operation.request.source_path.clone(),
        destination_path: operation.request.destination_path.clone(),
        preconditions: operation.request.preconditions.clone(),
        staged_content,
    })
}

fn persist_conflict(
    queue: &mut WritebackLocalChangeQueue,
    operation_id: &str,
    evidence: WritebackConflictEvidence,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError> {
    queue
        .mark_conflict(operation_id, evidence.clone())
        .map_err(queue_executor_error)?;
    Ok(WritebackSyncExecutionProgress::Conflict {
        operation_id: operation_id.to_string(),
        evidence,
    })
}

fn persist_ambiguous(
    queue: &mut WritebackLocalChangeQueue,
    operation_id: &str,
    detail: impl Into<String>,
) -> Result<WritebackSyncExecutionProgress, WritebackSyncExecutorError> {
    let detail = detail.into();
    queue
        .mark_retrying(operation_id, detail.clone())
        .map_err(queue_executor_error)?;
    Ok(WritebackSyncExecutionProgress::AwaitingReconciliation {
        operation_id: operation_id.to_string(),
        detail,
    })
}

fn terminal_reconciliation_conflict(
    code: impl Into<String>,
    detail: impl Into<String>,
) -> WritebackConflictEvidence {
    WritebackConflictEvidence {
        code: code.into(),
        detail: detail.into(),
        observed_object_id: None,
        observed_version: None,
        observed_content_hash: None,
    }
}

fn queue_executor_error(error: WritebackLocalChangeQueueError) -> WritebackSyncExecutorError {
    WritebackSyncExecutorError {
        code: "queue",
        detail: error.to_string(),
    }
}

fn executor_error(code: &'static str, detail: impl Into<String>) -> WritebackSyncExecutorError {
    WritebackSyncExecutorError {
        code,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use sinomo_core::local::SinomoDataDir;

    use super::*;
    use crate::local_change_queue::{
        WritebackDestinationPrecondition, WritebackExpectedObjectState,
        WritebackLocalOperationRequest,
    };

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(name: &str) -> Self {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "sinomo-cloud-files-executor-{name}-{}-{suffix}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("test root");
            Self(path)
        }

        fn data_dir(&self) -> SinomoDataDir {
            SinomoDataDir::new(&self.0)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct FakeDfs {
        applies: Vec<WritebackDfsMutationCommand>,
        reconciles: Vec<WritebackDfsMutationCommand>,
        apply_outcomes: VecDeque<Result<WritebackDfsMutationOutcome, String>>,
        reconcile_outcomes: VecDeque<Result<WritebackDfsMutationReconciliation, String>>,
    }

    impl WritebackDfsMutationAdapter for FakeDfs {
        fn apply(
            &mut self,
            command: &WritebackDfsMutationCommand,
        ) -> Result<WritebackDfsMutationOutcome, String> {
            self.applies.push(command.clone());
            self.apply_outcomes
                .pop_front()
                .expect("configured apply outcome")
        }

        fn reconcile(
            &mut self,
            command: &WritebackDfsMutationCommand,
        ) -> Result<WritebackDfsMutationReconciliation, String> {
            self.reconciles.push(command.clone());
            self.reconcile_outcomes
                .pop_front()
                .expect("configured reconcile outcome")
        }
    }

    #[derive(Default)]
    struct FakeCompletion {
        calls: Vec<(WritebackDfsMutationCommand, WritebackRemoteMutationEvidence)>,
        fail_once: bool,
        follow_up: Option<WritebackLocalOperationRequest>,
    }

    impl WritebackLocalCompletionEffect for FakeCompletion {
        fn complete(
            &mut self,
            command: &WritebackDfsMutationCommand,
            evidence: &WritebackRemoteMutationEvidence,
        ) -> Result<WritebackLocalCompletionOutcome, String> {
            self.calls.push((command.clone(), evidence.clone()));
            if std::mem::take(&mut self.fail_once) {
                return Err("simulated crash after local effect".to_string());
            }
            Ok(self
                .follow_up
                .clone()
                .map(Box::new)
                .map(WritebackLocalCompletionOutcome::EnqueueFollowUp)
                .unwrap_or(WritebackLocalCompletionOutcome::Completed))
        }
    }

    fn follow_up_request(
        queue: &WritebackLocalChangeQueue,
        operation_id: &str,
    ) -> WritebackLocalOperationRequest {
        let content = queue
            .stage_content(b"newer renamed bytes")
            .expect("stage follow-up");
        WritebackLocalOperationRequest {
            operation_id: operation_id.to_string(),
            capture_id: Some("rename-follow-up:stable-capture".to_string()),
            mount: "mount-a".to_string(),
            root: "/services".to_string(),
            kind: WritebackLocalOperationKind::ReplaceFile,
            source_path: "/services/renamed.txt".to_string(),
            destination_path: None,
            preconditions: WritebackLocalOperationPreconditions {
                source: Some(WritebackExpectedObjectState {
                    object_id: "01remoteobject".to_string(),
                    version: "01remoteversion".to_string(),
                    content_hash: Some("blake3:remote".to_string()),
                }),
                destination: WritebackDestinationPrecondition::Unspecified,
            },
            staged_content: Some(content),
        }
    }

    fn evidence(operation_id: &str) -> WritebackRemoteMutationEvidence {
        WritebackRemoteMutationEvidence {
            operation_id: operation_id.to_string(),
            object_id: Some("01remoteobject".to_string()),
            version: Some("01remoteversion".to_string()),
            content_hash: Some("blake3:remote".to_string()),
            content_size_bytes: Some(12),
            kind: Some("file".to_string()),
            detail: "applied".to_string(),
        }
    }

    fn queue_with_write(
        root: &TestRoot,
        operation_id: &str,
        bytes: &[u8],
    ) -> WritebackLocalChangeQueue {
        let mut queue = WritebackLocalChangeQueue::open(
            "module.dfs.cloud-files",
            &root.data_dir(),
            "mount-a",
            "/services",
        )
        .expect("open queue");
        let content = queue.stage_content(bytes).expect("stage");
        queue
            .enqueue(WritebackLocalOperationRequest {
                operation_id: operation_id.to_string(),
                capture_id: None,
                mount: "mount-a".to_string(),
                root: "/services".to_string(),
                kind: WritebackLocalOperationKind::ReplaceFile,
                source_path: "/services/report.txt".to_string(),
                destination_path: None,
                preconditions: WritebackLocalOperationPreconditions {
                    source: Some(WritebackExpectedObjectState {
                        object_id: "01oldobject".to_string(),
                        version: "01oldversion".to_string(),
                        content_hash: Some("blake3:old".to_string()),
                    }),
                    destination: WritebackDestinationPrecondition::Unspecified,
                },
                staged_content: Some(content),
            })
            .expect("enqueue");
        queue
    }

    #[test]
    fn successful_execution_persists_each_ordered_state() {
        let root = TestRoot::new("success");
        let mut queue = queue_with_write(&root, "op-success", b"stable bytes");
        let mut dfs = FakeDfs {
            apply_outcomes: VecDeque::from([Ok(WritebackDfsMutationOutcome::Applied(evidence(
                "op-success",
            )))]),
            ..FakeDfs::default()
        };
        let mut completion = FakeCompletion::default();

        assert_eq!(
            execute_writeback_operation(&mut queue, "op-success", &mut dfs, &mut completion,)
                .expect("execute"),
            WritebackSyncExecutionProgress::Completed {
                operation_id: "op-success".to_string()
            }
        );
        let operation = queue.operation("op-success").expect("operation");
        assert_eq!(operation.status, WritebackLocalOperationStatus::Completed);
        assert_eq!(operation.attempts, 1);
        assert_eq!(dfs.applies[0].operation_id, "op-success");
        assert_eq!(
            std::fs::read(
                &dfs.applies[0]
                    .staged_content
                    .as_ref()
                    .expect("content")
                    .blob_path
            )
            .expect("read immutable command blob"),
            b"stable bytes"
        );
        assert_eq!(completion.calls.len(), 1);
    }

    #[test]
    fn follow_up_is_durable_before_parent_completion_and_reuses_capture_after_restart() {
        let root = TestRoot::new("rename-follow-up-crash");
        let mut queue = queue_with_write(&root, "op-parent", b"old bytes");
        queue
            .mark_remote_applied_with_evidence(evidence("op-parent"))
            .expect("parent remote applied");
        let first_follow_up = follow_up_request(&queue, "cf_01k00000000000000000000000");
        queue
            .enqueue(first_follow_up.clone())
            .expect("durable follow-up before crash");
        drop(queue);

        let mut reopened = WritebackLocalChangeQueue::open(
            "module.dfs.cloud-files",
            &root.data_dir(),
            "mount-a",
            "/services",
        )
        .expect("reopen after follow-up enqueue");
        let mut no_dfs = FakeDfs::default();
        let mut completion = FakeCompletion {
            // A retry may generate a fresh candidate ID, but the durable
            // capture binding must select the original operation.
            follow_up: Some(follow_up_request(
                &reopened,
                "cf_01k11111111111111111111111",
            )),
            ..FakeCompletion::default()
        };
        assert_eq!(
            execute_writeback_operation(&mut reopened, "op-parent", &mut no_dfs, &mut completion,)
                .expect("finish parent after restart"),
            WritebackSyncExecutionProgress::Completed {
                operation_id: "op-parent".to_string(),
            }
        );
        assert_eq!(
            reopened
                .operation("cf_01k00000000000000000000000")
                .expect("original follow-up survives")
                .status,
            WritebackLocalOperationStatus::Queued
        );
        assert!(
            reopened
                .operation("cf_01k11111111111111111111111")
                .is_none()
        );
    }

    #[test]
    fn bounded_pass_leaves_later_durable_operations_for_the_next_pass() {
        let root = TestRoot::new("bounded-pass");
        let mut queue = queue_with_write(&root, "op-first", b"first");
        let content = queue.stage_content(b"second").expect("stage second");
        let mut second = queue
            .operation("op-first")
            .expect("first operation")
            .request
            .clone();
        second.operation_id = "op-second".to_string();
        second.source_path = "/services/second.txt".to_string();
        second.staged_content = Some(content);
        queue.enqueue(second).expect("enqueue second");
        let mut dfs = FakeDfs {
            apply_outcomes: VecDeque::from([
                Ok(WritebackDfsMutationOutcome::Applied(evidence("op-first"))),
                Ok(WritebackDfsMutationOutcome::Applied(evidence("op-second"))),
            ]),
            ..FakeDfs::default()
        };
        let mut completion = FakeCompletion::default();

        let first =
            execute_ready_writeback_operations_bounded(&mut queue, &mut dfs, &mut completion, 1)
                .expect("first bounded pass");
        assert_eq!(first.len(), 1);
        assert_eq!(dfs.applies.len(), 1);
        assert_eq!(
            queue
                .operation("op-second")
                .expect("second operation")
                .status,
            WritebackLocalOperationStatus::Queued
        );

        let second =
            execute_ready_writeback_operations_bounded(&mut queue, &mut dfs, &mut completion, 1)
                .expect("second bounded pass");
        assert_eq!(second.len(), 1);
        assert_eq!(dfs.applies.len(), 2);
        assert_eq!(
            queue
                .operation("op-second")
                .expect("second operation")
                .status,
            WritebackLocalOperationStatus::Completed
        );
    }

    #[test]
    fn restart_after_local_effect_replays_completion_without_resending_dfs() {
        let root = TestRoot::new("completion-crash");
        let mut queue = queue_with_write(&root, "op-completion", b"stable bytes");
        let mut dfs = FakeDfs {
            apply_outcomes: VecDeque::from([Ok(WritebackDfsMutationOutcome::Applied(evidence(
                "op-completion",
            )))]),
            ..FakeDfs::default()
        };
        let mut completion = FakeCompletion {
            fail_once: true,
            ..FakeCompletion::default()
        };
        assert!(matches!(
            execute_writeback_operation(&mut queue, "op-completion", &mut dfs, &mut completion,)
                .expect("execute"),
            WritebackSyncExecutionProgress::CompletionPending { .. }
        ));
        assert_eq!(
            queue.operation("op-completion").expect("operation").status,
            WritebackLocalOperationStatus::RemoteApplied
        );
        drop(queue);

        let mut reopened = WritebackLocalChangeQueue::open(
            "module.dfs.cloud-files",
            &root.data_dir(),
            "mount-a",
            "/services",
        )
        .expect("reopen");
        let mut no_resend = FakeDfs::default();
        let mut recovered_completion = FakeCompletion::default();
        assert!(matches!(
            execute_writeback_operation(
                &mut reopened,
                "op-completion",
                &mut no_resend,
                &mut recovered_completion,
            )
            .expect("resume completion"),
            WritebackSyncExecutionProgress::Completed { .. }
        ));
        assert!(no_resend.applies.is_empty());
        assert!(no_resend.reconciles.is_empty());
        assert_eq!(recovered_completion.calls.len(), 1);
    }

    #[test]
    fn ambiguous_send_is_reconciled_before_same_id_and_blob_are_retried() {
        let root = TestRoot::new("ambiguous");
        let mut queue = queue_with_write(&root, "op-ambiguous", b"immutable request");
        let mut first_dfs = FakeDfs {
            apply_outcomes: VecDeque::from([Ok(WritebackDfsMutationOutcome::Ambiguous {
                detail: "response lost".to_string(),
            })]),
            ..FakeDfs::default()
        };
        let mut completion = FakeCompletion::default();
        assert!(matches!(
            execute_writeback_operation(
                &mut queue,
                "op-ambiguous",
                &mut first_dfs,
                &mut completion,
            )
            .expect("first attempt"),
            WritebackSyncExecutionProgress::AwaitingReconciliation { .. }
        ));

        let mut retry_dfs = FakeDfs {
            reconcile_outcomes: VecDeque::from([Ok(
                WritebackDfsMutationReconciliation::NotApplied,
            )]),
            apply_outcomes: VecDeque::from([Ok(WritebackDfsMutationOutcome::Applied(evidence(
                "op-ambiguous",
            )))]),
            ..FakeDfs::default()
        };
        assert!(matches!(
            execute_writeback_operation(
                &mut queue,
                "op-ambiguous",
                &mut retry_dfs,
                &mut completion,
            )
            .expect("reconciled retry"),
            WritebackSyncExecutionProgress::Completed { .. }
        ));
        assert_eq!(retry_dfs.reconciles.len(), 1);
        assert_eq!(retry_dfs.applies.len(), 1);
        assert_eq!(
            retry_dfs.reconciles[0].operation_id,
            retry_dfs.applies[0].operation_id
        );
        assert_eq!(
            retry_dfs.reconciles[0].staged_content,
            retry_dfs.applies[0].staged_content
        );
    }

    #[test]
    fn ambiguous_reconciliation_never_resends_the_mutation() {
        let root = TestRoot::new("ambiguous-reconciliation");
        let mut queue = queue_with_write(&root, "op-still-ambiguous", b"immutable request");
        queue
            .mark_retrying("op-still-ambiguous", "prior ambiguous send")
            .expect("retrying");
        let mut dfs = FakeDfs {
            reconcile_outcomes: VecDeque::from([Ok(
                WritebackDfsMutationReconciliation::Ambiguous {
                    detail: "authority cannot yet prove the result".to_string(),
                },
            )]),
            ..FakeDfs::default()
        };
        let mut completion = FakeCompletion::default();

        assert!(matches!(
            execute_writeback_operation(
                &mut queue,
                "op-still-ambiguous",
                &mut dfs,
                &mut completion,
            )
            .expect("keep reconciling"),
            WritebackSyncExecutionProgress::AwaitingReconciliation { .. }
        ));
        assert_eq!(dfs.reconciles.len(), 1);
        assert!(dfs.applies.is_empty());
        assert_eq!(
            queue
                .operation("op-still-ambiguous")
                .expect("operation")
                .status,
            WritebackLocalOperationStatus::Retrying
        );
    }

    #[test]
    fn expired_reconciliation_becomes_terminal_conflict_without_resend() {
        let root = TestRoot::new("expired");
        let mut queue = queue_with_write(&root, "op-expired", b"immutable request");
        queue
            .mark_retrying("op-expired", "prior ambiguous send")
            .expect("retrying");
        let mut dfs = FakeDfs {
            reconcile_outcomes: VecDeque::from([Ok(WritebackDfsMutationReconciliation::Expired {
                detail: "dedupe receipt aged out".to_string(),
            })]),
            ..FakeDfs::default()
        };
        let mut completion = FakeCompletion::default();

        let progress =
            execute_writeback_operation(&mut queue, "op-expired", &mut dfs, &mut completion)
                .expect("expired conflict");
        assert!(matches!(
            progress,
            WritebackSyncExecutionProgress::Conflict { .. }
        ));
        assert!(dfs.applies.is_empty());
        assert_eq!(
            queue
                .operation("op-expired")
                .expect("operation")
                .conflict
                .as_ref()
                .expect("conflict")
                .code,
            "operation-receipt-expired"
        );
    }
}
