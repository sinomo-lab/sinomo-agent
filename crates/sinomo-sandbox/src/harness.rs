use crate::internal::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWorkspaceSnapshot {
    pub workspace_id: Id,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub object_ids: Vec<DfsObjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCommand {
    pub name: String,
    pub command: String,
    pub expected_exit_code: i32,
    pub observed_exit_code: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCommandResult {
    pub name: String,
    pub command: String,
    pub passed: bool,
    pub observed_exit_code: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRunRequest {
    pub job: EvolutionJob,
    pub candidate_artifact_id: ArtifactId,
    pub workspace_snapshot: SandboxWorkspaceSnapshot,
    pub limits: ModuleResourceLimits,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<SandboxCommand>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRunResult {
    pub job_id: Id,
    pub candidate_artifact_id: ArtifactId,
    pub status: SandboxRunStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub violations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_results: Vec<SandboxCommandResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxRunStatus {
    Passed,
    Failed,
    Blocked,
}

#[derive(Clone, Debug)]
pub struct SandboxHarness {
    node_id: NodeId,
}

impl SandboxHarness {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn run(&self, request: SandboxRunRequest) -> SandboxRunResult {
        let mut violations = Vec::new();
        if request.job.scope.workspace_id.as_ref() != Some(&request.workspace_snapshot.workspace_id)
        {
            violations.push("job scope workspace does not match sandbox snapshot".to_string());
        }
        if request.limits.cpu_ms == 0 || request.limits.memory_mb == 0 {
            violations.push("resource limits must be non-zero".to_string());
        }

        let command_results: Vec<_> = request
            .commands
            .iter()
            .map(|command| SandboxCommandResult {
                name: command.name.clone(),
                command: command.command.clone(),
                passed: command.observed_exit_code == command.expected_exit_code,
                observed_exit_code: command.observed_exit_code,
            })
            .collect();
        let command_failed = command_results.iter().any(|result| !result.passed);
        let status = if !violations.is_empty() {
            SandboxRunStatus::Blocked
        } else if command_failed {
            SandboxRunStatus::Failed
        } else {
            SandboxRunStatus::Passed
        };

        SandboxRunResult {
            job_id: request.job.job_id,
            candidate_artifact_id: request.candidate_artifact_id,
            status,
            violations,
            command_results,
        }
    }

    pub fn result_event(&self, result: &SandboxRunResult) -> EventEnvelope {
        let kind = if result.status == SandboxRunStatus::Blocked {
            "sandbox.run.blocked"
        } else {
            "sandbox.run.completed"
        };
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("sandbox.local").expect("valid source"),
            EventKind::new(kind).expect("valid event kind"),
            Salience::new(if result.status == SandboxRunStatus::Passed {
                0.72
            } else {
                0.88
            })
            .expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "job_id": result.job_id,
            "candidate_artifact_id": result.candidate_artifact_id,
            "status": result.status,
            "violations": result.violations,
            "command_results": result.command_results,
        }))
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxError {
    #[error("sandbox run blocked: {reason}")]
    Blocked { reason: String },
}
