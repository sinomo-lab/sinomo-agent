mod coding;
mod compaction;
mod config;
mod context;
mod model;
mod session;
mod tools;

use super::*;
use crate::{
    AgentCommandOutput, AgentTool, AgentToolArtifactRef, AgentToolKind, AgentToolRequest,
    AgentToolResult, AgentToolSpec, InteractionSurface, InteractionSurfaceKind,
    LocalWorkspaceAgentConfig, SkillCatalogStore, default_agent_tool_descriptors,
    default_background_agent_tools, default_coding_agent_tools, default_main_agent_tools,
    user_interaction_input_event,
};
use sinomo_lm::{LanguageModelError, LanguageModelResponseFormat};
use sinomo_memory::MemoryError;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::node::{LocalNodePermission, NodeId};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
enum ControlHistoryFailureMode {
    Always,
    TerminalTask,
}

struct FailingControlHistoryStore {
    mode: ControlHistoryFailureMode,
}

impl FailingControlHistoryStore {
    fn always() -> Self {
        Self {
            mode: ControlHistoryFailureMode::Always,
        }
    }

    fn terminal_task() -> Self {
        Self {
            mode: ControlHistoryFailureMode::TerminalTask,
        }
    }

    fn rejects(&self, record: &AgentSessionStoreRecord) -> bool {
        match self.mode {
            ControlHistoryFailureMode::Always => true,
            ControlHistoryFailureMode::TerminalTask => match &record.record {
                AgentSessionStoreRecordKind::TaskStatusChanged { snapshot } => {
                    is_terminal_task_status(&snapshot.status)
                }
                AgentSessionStoreRecordKind::ControlEventPublished { event } => matches!(
                    &event.payload,
                    AgentEventPayload::TaskStatusChanged { snapshot }
                        if is_terminal_task_status(&snapshot.status)
                ),
                AgentSessionStoreRecordKind::ControlSnapshotSaved { snapshot } => snapshot
                    .tasks
                    .iter()
                    .any(|task| is_terminal_task_status(&task.status)),
                _ => false,
            },
        }
    }
}

impl AgentSessionStore for FailingControlHistoryStore {
    fn load_session(
        &self,
        _session_id: &SessionId,
    ) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        Ok(None)
    }

    fn save_session(&self, _session: &AgentSession) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }

    fn append_items(
        &self,
        _session_id: &SessionId,
        _origin: &crate::AgentItemOrigin,
        _items: &[AgentItem],
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }

    fn append_projection(
        &self,
        _session_id: &SessionId,
        _projection: &AgentSessionProjection,
    ) -> Result<(), AgentSessionStoreError> {
        Ok(())
    }

    fn append_record(
        &self,
        record: &AgentSessionStoreRecord,
    ) -> Result<(), AgentSessionStoreError> {
        if self.rejects(record) {
            return Err(AgentSessionStoreError::Io {
                path: "control-history".to_string(),
                detail: "injected control-history persistence failure".to_string(),
            });
        }
        Ok(())
    }
}

struct SequencedModel {
    responses: Mutex<VecDeque<String>>,
    requests: Mutex<Vec<LanguageModelRequest>>,
}

impl SequencedModel {
    fn new(responses: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(Into::into).collect()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl InteractionModel for SequencedModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let response = self
            .responses
            .lock()
            .expect("responses")
            .pop_front()
            .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                detail: "no fake response queued".to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: "fake".to_string(),
            model: "fake".to_string(),
            content: response,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Clone)]
struct RecordingMemoryRetriever {
    result: Arc<Mutex<MemoryEvidenceBundle>>,
    calls: Arc<Mutex<Vec<(MemoryAccessContext, MemoryQuery)>>>,
}

impl RecordingMemoryRetriever {
    fn new(result: MemoryEvidenceBundle) -> Self {
        Self {
            result: Arc::new(Mutex::new(result)),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl MemoryRetriever for RecordingMemoryRetriever {
    fn retrieve_evidence(
        &self,
        access: &MemoryAccessContext,
        query: &MemoryQuery,
    ) -> Result<MemoryEvidenceBundle, MemoryError> {
        self.calls
            .lock()
            .expect("memory calls")
            .push((access.clone(), query.clone()));
        Ok(self.result.lock().expect("memory result").clone())
    }
}

fn evidence_bundle(summary: &str) -> MemoryEvidenceBundle {
    let file_id = sinomo_runtime::core::Id::derived("agent-test-memory-file", summary.as_bytes());
    let version = sinomo_memory::MemoryFileVersionRef {
        file_ref: sinomo_memory::MemoryFileRef {
            connector_id: "agent-test-memory/v1".to_string(),
            root_id: sinomo_runtime::core::Id::derived("agent-test-root", b"root"),
            object_id: file_id,
            path_hint: "fixture.md".to_string(),
        },
        version_id: sinomo_runtime::core::Id::derived(
            "agent-test-memory-version",
            summary.as_bytes(),
        ),
        content_hash: blake3::hash(summary.as_bytes()).to_hex().to_string(),
        byte_length: summary.len() as u64,
    };
    let anchor = sinomo_memory::MemoryAnchor::canonical(
        version.clone(),
        sinomo_memory::MemoryAnchorSelector::WholeFile,
        "text/markdown",
    )
    .expect("valid fixture anchor");
    let source = sinomo_memory::SourceDescriptor {
        version,
        media_type: "text/markdown".to_string(),
        policy: sinomo_memory::IndexRootPolicy {
            scope: MemoryScope::Agent {
                agent_id: "main-agent".to_string(),
            },
            visibility: sinomo_runtime::core::TrustDomain::new("trust.local").expect("trust"),
            allow_model_context: true,
            allow_relation_traversal: false,
        },
        retention: sinomo_memory::SourceRetentionCapability::NativeVersioned,
        freshness: sinomo_memory::SourceFreshness::Current,
        platform_identity: "agent-test-file".to_string(),
        source_version_hint: "fixture-v1".to_string(),
        observed_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    MemoryEvidenceBundle {
        snapshot: None,
        files: vec![sinomo_memory::MemoryEvidenceFile {
            source,
            evidence_anchor_ids: vec![anchor.anchor_id.clone()],
        }],
        evidence: vec![MemoryEvidence {
            anchor,
            content: summary.to_string(),
            score: 100,
            reason: "indexed test fixture".to_string(),
        }],
        ..MemoryEvidenceBundle::default()
    }
}

struct BlockingModel {
    started: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl InteractionModel for BlockingModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.started
            .send(())
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: source.to_string(),
            })?;
        self.release
            .lock()
            .expect("release lock")
            .recv()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: source.to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: r#"{
                "response_mode": "answer",
                "text": "completed after steering",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#
            .to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

struct PanickingModel;

impl InteractionModel for PanickingModel {
    fn complete(
        &self,
        _request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        panic!("intentional agent worker panic")
    }
}

struct SequencedUsageModel {
    responses: Mutex<VecDeque<(String, Option<sinomo_lm::LanguageModelUsage>)>>,
    requests: Mutex<Vec<LanguageModelRequest>>,
}

impl SequencedUsageModel {
    fn new<S>(
        responses: impl IntoIterator<Item = (S, Option<sinomo_lm::LanguageModelUsage>)>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(|(response, usage)| (response.into(), usage))
                    .collect(),
            ),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl InteractionModel for SequencedUsageModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let (response, usage) = self
            .responses
            .lock()
            .expect("responses")
            .pop_front()
            .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                detail: "no fake response queued".to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: "fake".to_string(),
            model: "fake".to_string(),
            content: response,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage,
        })
    }
}

struct StreamingModel {
    response: String,
    deltas: Vec<String>,
    requests: Mutex<Vec<LanguageModelRequest>>,
}

impl StreamingModel {
    fn new(
        response: impl Into<String>,
        deltas: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            response: response.into(),
            deltas: deltas.into_iter().map(Into::into).collect(),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl InteractionModel for StreamingModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming(request, &mut |_| {})
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        for delta in &self.deltas {
            on_delta(delta);
        }
        Ok(LanguageModelResponse {
            provider: "fake".to_string(),
            model: "fake".to_string(),
            content: self.response.clone(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

struct EventfulModel {
    turns: Mutex<VecDeque<(String, Vec<LanguageModelStreamEvent>)>>,
    requests: Mutex<Vec<LanguageModelRequest>>,
}

impl EventfulModel {
    fn new(
        turns: impl IntoIterator<Item = (impl Into<String>, Vec<LanguageModelStreamEvent>)>,
    ) -> Self {
        Self {
            turns: Mutex::new(
                turns
                    .into_iter()
                    .map(|(response, events)| (response.into(), events))
                    .collect(),
            ),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl InteractionModel for EventfulModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming_events(request, &mut |_| {})
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let (response, events) =
            self.turns
                .lock()
                .expect("turns")
                .pop_front()
                .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                    provider: "fake".to_string(),
                    model: "fake".to_string(),
                    detail: "no fake response queued".to_string(),
                })?;
        for event in events {
            on_event(event);
        }
        Ok(LanguageModelResponse {
            provider: "fake".to_string(),
            model: "fake".to_string(),
            content: response,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Clone, Default)]
struct CancellationAwareModel {
    started: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl CancellationAwareModel {
    fn started(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.started.clone()
    }
}

impl InteractionModel for CancellationAwareModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: r#"{
                "response_mode": "answer",
                "text": "not cancelled",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#
            .to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.started
            .store(true, std::sync::atomic::Ordering::SeqCst);
        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        while !cancellation_token.is_cancelled() {
            thread::sleep(std::time::Duration::from_millis(10));
        }
        Err(LanguageModelError::ProviderInvocationFailed {
            provider: request.provider.clone(),
            model: request.model.clone(),
            detail: "cancelled by test token".to_string(),
        })
    }
}

struct EchoTool;

impl AgentTool for EchoTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "read a fake file",
            false,
        ))
        .with_capabilities([AgentToolCapability::ReadWorkspace])
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        AgentToolResult::succeeded(
            "fake file read",
            serde_json::json!({
                "call_id": request.call_id,
                "input": request.call.input,
                "content": "workspace evidence"
            }),
        )
    }
}

struct ReplacementReadTool;

impl AgentTool for ReplacementReadTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "read a replacement fake file",
            false,
        ))
        .with_capabilities([AgentToolCapability::ReadWorkspace])
    }

    fn execute(&self, _request: AgentToolRequest<'_>) -> AgentToolResult {
        AgentToolResult::succeeded(
            "replacement file read",
            serde_json::json!({ "content": "replacement evidence" }),
        )
    }
}

struct CancellationProbeTool {
    executed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl AgentTool for CancellationProbeTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "cancel_probe",
            "probe cancellation before custom tool execution",
            false,
        ))
    }

    fn execute(&self, _request: AgentToolRequest<'_>) -> AgentToolResult {
        self.executed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        AgentToolResult::succeeded("probe executed", serde_json::json!({ "ok": true }))
    }
}

fn agent_input(prompt: &str) -> AgentLoopInput {
    let session = InteractionSession::new(
        SessionId::from_legacy_named("sess_agent").expect("session"),
        NodeId::from_legacy_named("node_agent").expect("node"),
        InteractionSurface::new("surface.agent", InteractionSurfaceKind::CommandLine),
    );
    let latest_event = user_interaction_input_event(&session, prompt);
    AgentLoopInput {
        session,
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: default_coding_agent_tools(),
    }
}

fn with_remote_management_for_test(registry: AgentToolRegistry) -> AgentToolRegistry {
    registry.with_local_permission_checker(|permission: LocalNodePermission| {
        Ok(permission == LocalNodePermission::RemoteManagement)
    })
}

fn coding_evidence(evidence: &AgentToolEvidence) -> AgentCodingToolEvidence {
    evidence
        .record(AGENT_CODING_EVIDENCE_KIND)
        .unwrap_or_default()
}

fn run_git_for_test<const N: usize>(root: &Path, args: [&str; N]) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("git command");
    assert!(status.success(), "git command failed");
}

mod usage;
