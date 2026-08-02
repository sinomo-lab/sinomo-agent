//! Runtime test registry contracts for Sinomo.
//!
//! This crate owns the generic test surface only. Concrete crates and modules
//! register explicit test cases; the CLI decides which runtime capabilities
//! are available for a given invocation.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

pub type SinomoTestResult = Result<SinomoTestOutcome, SinomoTestError>;
pub type SinomoTestFn = fn(&mut SinomoTestContext) -> SinomoTestResult;

#[derive(Debug, Error)]
pub enum SinomoTestError {
    #[error("{detail}")]
    Failed { detail: String },
    #[error("missing required parameter `{name}`")]
    MissingParameter { name: String },
    #[error("invalid parameter `{name}`: {detail}")]
    InvalidParameter { name: String, detail: String },
    #[error("cluster runtime is required: {detail}")]
    ClusterRequired { detail: String },
    #[error("live network/model access is required: {detail}")]
    LiveRequired { detail: String },
}

impl SinomoTestError {
    pub fn failed(detail: impl Into<String>) -> Self {
        Self::Failed {
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SinomoTestRequirement {
    ClusterRuntime,
    LiveNetwork,
    ProviderAccount { provider: String },
    RuntimeCapability { capability: String },
}

impl SinomoTestRequirement {
    pub fn provider_account(provider: impl Into<String>) -> Self {
        Self::ProviderAccount {
            provider: provider.into(),
        }
    }

    pub fn runtime_capability(capability: impl Into<String>) -> Self {
        Self::RuntimeCapability {
            capability: capability.into(),
        }
    }

    pub fn label(&self) -> String {
        match self {
            Self::ClusterRuntime => "cluster-runtime".to_string(),
            Self::LiveNetwork => "live-network".to_string(),
            Self::ProviderAccount { provider } => format!("provider-account:{provider}"),
            Self::RuntimeCapability { capability } => {
                format!("runtime-capability:{capability}")
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SinomoTestCaseMetadata {
    pub id: String,
    pub summary: String,
    pub tags: Vec<String>,
    pub requirements: Vec<SinomoTestRequirement>,
}

#[derive(Clone)]
pub struct SinomoTestCase {
    metadata: SinomoTestCaseMetadata,
    run: SinomoTestFn,
}

impl SinomoTestCase {
    pub fn new(
        id: impl Into<String>,
        summary: impl Into<String>,
        tags: impl IntoIterator<Item = impl Into<String>>,
        requirements: impl IntoIterator<Item = SinomoTestRequirement>,
        run: SinomoTestFn,
    ) -> Self {
        Self {
            metadata: SinomoTestCaseMetadata {
                id: id.into(),
                summary: summary.into(),
                tags: tags.into_iter().map(Into::into).collect(),
                requirements: requirements.into_iter().collect(),
            },
            run,
        }
    }

    pub fn metadata(&self) -> &SinomoTestCaseMetadata {
        &self.metadata
    }

    pub fn run(&self, context: &mut SinomoTestContext) -> SinomoTestReport {
        let started = Instant::now();
        if let Some(skip) = self.preflight(context) {
            return SinomoTestReport {
                case: self.metadata.clone(),
                status: skip.status,
                summary: skip.summary,
                diagnostics: skip.diagnostics,
                duration_ms: started.elapsed().as_millis() as u64,
                errors: Vec::new(),
            };
        }
        match (self.run)(context) {
            Ok(outcome) => SinomoTestReport {
                case: self.metadata.clone(),
                status: outcome.status,
                summary: outcome.summary,
                diagnostics: merge_diagnostics(context.take_diagnostics(), outcome.diagnostics),
                duration_ms: started.elapsed().as_millis() as u64,
                errors: Vec::new(),
            },
            Err(error) => SinomoTestReport {
                case: self.metadata.clone(),
                status: SinomoTestStatus::Failed,
                summary: error.to_string(),
                diagnostics: context.take_diagnostics(),
                duration_ms: started.elapsed().as_millis() as u64,
                errors: vec![error.to_string()],
            },
        }
    }

    fn preflight(&self, context: &SinomoTestContext) -> Option<SinomoTestOutcome> {
        for requirement in &self.metadata.requirements {
            match requirement {
                SinomoTestRequirement::LiveNetwork if !context.live => {
                    return Some(SinomoTestOutcome::skipped(
                        "requires live network/model access; pass --live",
                    ));
                }
                SinomoTestRequirement::ClusterRuntime if context.cluster_snapshot.is_none() => {
                    let detail = "requires cluster runtime and cluster KV snapshot";
                    if context.require_cluster {
                        return Some(SinomoTestOutcome::failed(detail));
                    }
                    return Some(SinomoTestOutcome::skipped(detail));
                }
                SinomoTestRequirement::RuntimeCapability { capability }
                    if !context.runtime_capabilities.contains(capability) =>
                {
                    return Some(SinomoTestOutcome::skipped(format!(
                        "requires runtime capability `{capability}`"
                    )));
                }
                _ => {}
            }
        }
        None
    }
}

fn merge_diagnostics(
    mut context: serde_json::Value,
    outcome: serde_json::Value,
) -> serde_json::Value {
    let Some(context_object) = context.as_object_mut() else {
        return outcome;
    };
    let Some(outcome_object) = outcome.as_object() else {
        return context;
    };
    for (key, value) in outcome_object {
        context_object.insert(key.clone(), value.clone());
    }
    context
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SinomoTestStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinomoTestOutcome {
    pub status: SinomoTestStatus,
    pub summary: String,
    #[serde(default)]
    pub diagnostics: serde_json::Value,
}

impl SinomoTestOutcome {
    pub fn passed(summary: impl Into<String>) -> Self {
        Self {
            status: SinomoTestStatus::Passed,
            summary: summary.into(),
            diagnostics: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn failed(summary: impl Into<String>) -> Self {
        Self {
            status: SinomoTestStatus::Failed,
            summary: summary.into(),
            diagnostics: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn skipped(summary: impl Into<String>) -> Self {
        Self {
            status: SinomoTestStatus::Skipped,
            summary: summary.into(),
            diagnostics: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn with_diagnostics(mut self, diagnostics: serde_json::Value) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinomoTestReport {
    pub case: SinomoTestCaseMetadata,
    pub status: SinomoTestStatus,
    pub summary: String,
    pub duration_ms: u64,
    #[serde(default)]
    pub diagnostics: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Clone, Debug)]
enum SinomoTestClusterSnapshotEvidence {
    Unverified {
        snapshot: Arc<sinomo_core::kv::KvSnapshot>,
        source: PathBuf,
    },
    Authoritative(SinomoTestAuthoritativeClusterSnapshot),
}

#[derive(Clone, Debug)]
pub struct SinomoTestAuthoritativeClusterSnapshot {
    snapshot: Arc<sinomo_core::kv::KvSnapshot>,
    authority: sinomo_core::kv::KvAuthorityProof,
    source: PathBuf,
}

impl SinomoTestAuthoritativeClusterSnapshot {
    pub fn snapshot(&self) -> Arc<sinomo_core::kv::KvSnapshot> {
        Arc::clone(&self.snapshot)
    }

    pub fn authority(&self) -> sinomo_core::kv::KvAuthorityProof {
        self.authority
    }

    pub fn source(&self) -> &Path {
        &self.source
    }
}

#[derive(Clone, Debug)]
pub struct SinomoTestContext {
    data_dir: PathBuf,
    parameters: BTreeMap<String, String>,
    live: bool,
    require_cluster: bool,
    cluster_snapshot: Option<SinomoTestClusterSnapshotEvidence>,
    runtime_capabilities: BTreeSet<String>,
    diagnostics: serde_json::Map<String, serde_json::Value>,
}

impl SinomoTestContext {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            parameters: BTreeMap::new(),
            live: false,
            require_cluster: false,
            cluster_snapshot: None,
            runtime_capabilities: BTreeSet::new(),
            diagnostics: Default::default(),
        }
    }

    pub fn with_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(key.into(), value.into());
        self
    }

    pub fn with_parameters(
        mut self,
        parameters: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (key, value) in parameters {
            self.parameters.insert(key.into(), value.into());
        }
        self
    }

    pub fn with_live(mut self, live: bool) -> Self {
        self.live = live;
        self
    }

    pub fn with_require_cluster(mut self, require_cluster: bool) -> Self {
        self.require_cluster = require_cluster;
        self
    }

    pub fn with_cluster_snapshot(
        mut self,
        snapshot: sinomo_core::kv::KvSnapshot,
        source: impl Into<PathBuf>,
    ) -> Self {
        self.cluster_snapshot = Some(SinomoTestClusterSnapshotEvidence::Unverified {
            snapshot: Arc::new(snapshot),
            source: source.into(),
        });
        self
    }

    /// Installs a cluster snapshot together with the fresh authority proof
    /// obtained for this invocation. Consumers that require current routing
    /// can reuse this exact immutable generation instead of paying for another
    /// serialized Raft barrier during the same test case.
    pub fn try_with_authoritative_cluster_snapshot(
        mut self,
        read: sinomo_core::kv::KvAuthoritativeRead<sinomo_core::kv::KvSnapshot>,
        source: impl Into<PathBuf>,
    ) -> Result<Self, sinomo_core::kv::KvError> {
        sinomo_core::kv::validate_cluster_kv_authoritative_snapshot_read(&read)?;
        self.cluster_snapshot = Some(SinomoTestClusterSnapshotEvidence::Authoritative(
            SinomoTestAuthoritativeClusterSnapshot {
                snapshot: Arc::new(read.value),
                authority: read.authority,
                source: source.into(),
            },
        ));
        Ok(self)
    }

    pub fn with_runtime_capability(mut self, capability: impl Into<String>) -> Self {
        self.runtime_capabilities.insert(capability.into());
        self
    }

    pub fn with_runtime_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.runtime_capabilities
            .extend(capabilities.into_iter().map(Into::into));
        self
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn parameter(&self, key: &str) -> Option<&str> {
        self.parameters.get(key).map(String::as_str)
    }

    pub fn required_parameter(&self, key: &str) -> Result<&str, SinomoTestError> {
        self.parameter(key)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| SinomoTestError::MissingParameter {
                name: key.to_string(),
            })
    }

    pub fn live(&self) -> bool {
        self.live
    }

    pub fn require_cluster(&self) -> bool {
        self.require_cluster
    }

    pub fn cluster_snapshot(&self) -> Option<&sinomo_core::kv::KvSnapshot> {
        match self.cluster_snapshot.as_ref()? {
            SinomoTestClusterSnapshotEvidence::Unverified { snapshot, .. } => Some(snapshot),
            SinomoTestClusterSnapshotEvidence::Authoritative(snapshot) => Some(&snapshot.snapshot),
        }
    }

    pub fn authoritative_cluster_snapshot(&self) -> Option<SinomoTestAuthoritativeClusterSnapshot> {
        match self.cluster_snapshot.as_ref()? {
            SinomoTestClusterSnapshotEvidence::Unverified { .. } => None,
            SinomoTestClusterSnapshotEvidence::Authoritative(snapshot) => Some(snapshot.clone()),
        }
    }

    pub fn cluster_config_source(&self) -> Option<&Path> {
        match self.cluster_snapshot.as_ref()? {
            SinomoTestClusterSnapshotEvidence::Unverified { source, .. } => Some(source),
            SinomoTestClusterSnapshotEvidence::Authoritative(snapshot) => Some(&snapshot.source),
        }
    }

    pub fn runtime_capabilities(&self) -> &BTreeSet<String> {
        &self.runtime_capabilities
    }

    pub fn record(&mut self, key: impl Into<String>, value: impl Serialize) {
        let value = serde_json::to_value(value).unwrap_or_else(|source| {
            serde_json::json!({
                "serialization_error": source.to_string()
            })
        });
        self.diagnostics.insert(key.into(), value);
    }

    pub fn take_diagnostics(&mut self) -> serde_json::Value {
        serde_json::Value::Object(std::mem::take(&mut self.diagnostics))
    }
}

#[derive(Default)]
pub struct SinomoTestRegistryBuilder {
    cases: Vec<SinomoTestCase>,
}

impl SinomoTestRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, case: SinomoTestCase) -> Self {
        self.cases.push(case);
        self
    }

    pub fn try_build(self) -> Result<SinomoTestRegistry, SinomoTestRegistryError> {
        let mut cases = BTreeMap::new();
        for case in self.cases {
            let id = case.metadata.id.clone();
            if cases.insert(id.clone(), case).is_some() {
                return Err(SinomoTestRegistryError::DuplicateCase { id });
            }
        }
        Ok(SinomoTestRegistry { cases })
    }
}

#[derive(Debug, Error)]
pub enum SinomoTestRegistryError {
    #[error("duplicate test case `{id}`")]
    DuplicateCase { id: String },
    #[error("unknown test case `{id}`")]
    UnknownCase { id: String },
}

pub struct SinomoTestRegistry {
    cases: BTreeMap<String, SinomoTestCase>,
}

impl SinomoTestRegistry {
    pub fn builder() -> SinomoTestRegistryBuilder {
        SinomoTestRegistryBuilder::new()
    }

    pub fn cases(&self) -> impl Iterator<Item = &SinomoTestCase> {
        self.cases.values()
    }

    pub fn metadata(&self) -> Vec<SinomoTestCaseMetadata> {
        self.cases
            .values()
            .map(|case| case.metadata.clone())
            .collect()
    }

    pub fn tags(&self) -> Vec<String> {
        let mut tags = BTreeSet::new();
        for case in self.cases.values() {
            tags.extend(case.metadata.tags.iter().cloned());
        }
        tags.into_iter().collect()
    }

    pub fn get(&self, id: &str) -> Option<&SinomoTestCase> {
        self.cases.get(id)
    }

    pub fn run(
        &self,
        id: &str,
        context: &mut SinomoTestContext,
    ) -> Result<SinomoTestReport, SinomoTestRegistryError> {
        let case = self
            .get(id)
            .ok_or_else(|| SinomoTestRegistryError::UnknownCase { id: id.to_string() })?;
        Ok(case.run(context))
    }
}

#[macro_export]
macro_rules! sinomo_test_case {
    (
        pub fn $name:ident($ctx:ident) {
            id: $id:expr,
            summary: $summary:expr,
            tags: [$($tag:expr),* $(,)?],
            requirements: [$($requirement:expr),* $(,)?],
            run: $body:block
        }
    ) => {
        pub fn $name() -> $crate::SinomoTestCase {
            fn run($ctx: &mut $crate::SinomoTestContext) -> $crate::SinomoTestResult $body
            $crate::SinomoTestCase::new(
                $id,
                $summary,
                [$($tag),*],
                [$($requirement),*],
                run,
            )
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    sinomo_test_case! {
        pub fn sample_case(ctx) {
            id: "sample.pass",
            summary: "sample test",
            tags: ["sample"],
            requirements: [],
            run: {
                ctx.record("seen", true);
                Ok(SinomoTestOutcome::passed("ok"))
            }
        }
    }

    #[test]
    fn registry_runs_case_and_merges_context_diagnostics() {
        let registry = SinomoTestRegistry::builder()
            .register(sample_case())
            .try_build()
            .expect("registry");
        let mut context = SinomoTestContext::new("/tmp/sinomo-test");

        let report = registry.run("sample.pass", &mut context).expect("report");

        assert_eq!(report.status, SinomoTestStatus::Passed);
        assert_eq!(report.diagnostics["seen"], true);
    }

    #[test]
    fn authoritative_cluster_snapshot_reuses_one_immutable_generation() {
        use sinomo_core::kv::{KvAuthorityBasis, KvAuthorityProof, KvRevision, KvSnapshot};

        let snapshot = KvSnapshot {
            revision: KvRevision(7),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        };
        let authority = KvAuthorityProof {
            term: 3,
            commit_index: 11,
            revision: snapshot.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2,
            },
        };
        let context = SinomoTestContext::new("/tmp/sinomo-test")
            .try_with_authoritative_cluster_snapshot(
                sinomo_core::kv::KvAuthoritativeRead {
                    value: snapshot,
                    authority,
                },
                "cluster/kv-snapshot.sobj",
            )
            .expect("valid authoritative snapshot");

        let first = context
            .authoritative_cluster_snapshot()
            .expect("authoritative snapshot");
        let second = context
            .authoritative_cluster_snapshot()
            .expect("same authoritative snapshot");
        assert!(Arc::ptr_eq(&first.snapshot(), &second.snapshot()));
        assert_eq!(first.authority(), authority);
        assert_eq!(first.source(), Path::new("cluster/kv-snapshot.sobj"));
        assert_eq!(
            context.cluster_snapshot().expect("snapshot").revision,
            KvRevision(7)
        );
    }

    #[test]
    fn authoritative_cluster_snapshot_rejects_mismatched_or_incomplete_proof() {
        use sinomo_core::kv::{
            KvAuthoritativeRead, KvAuthorityBasis, KvAuthorityProof, KvRevision, KvSnapshot,
        };
        let snapshot = || KvSnapshot {
            revision: KvRevision(7),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        };
        let proof = |revision, voter_acks| KvAuthorityProof {
            term: 3,
            commit_index: 11,
            revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks,
                required_quorum: 2,
            },
        };

        assert!(
            SinomoTestContext::new("/tmp/sinomo-test")
                .try_with_authoritative_cluster_snapshot(
                    KvAuthoritativeRead {
                        value: snapshot(),
                        authority: proof(KvRevision(6), 2),
                    },
                    "cluster/kv-snapshot.sobj",
                )
                .is_err(),
            "proof and snapshot revisions must be inseparable"
        );
        assert!(
            SinomoTestContext::new("/tmp/sinomo-test")
                .try_with_authoritative_cluster_snapshot(
                    KvAuthoritativeRead {
                        value: snapshot(),
                        authority: proof(KvRevision(7), 1),
                    },
                    "cluster/kv-snapshot.sobj",
                )
                .is_err(),
            "an incomplete quorum must not create reusable authority evidence"
        );
    }

    #[test]
    fn live_requirement_skips_without_live_flag() {
        fn run(_: &mut SinomoTestContext) -> SinomoTestResult {
            Ok(SinomoTestOutcome::passed("should not run"))
        }
        let case = SinomoTestCase::new(
            "sample.live",
            "live",
            ["live"],
            [SinomoTestRequirement::LiveNetwork],
            run,
        );
        let mut context = SinomoTestContext::new("/tmp/sinomo-test");

        let report = case.run(&mut context);

        assert_eq!(report.status, SinomoTestStatus::Skipped);
    }

    #[test]
    fn runtime_capability_requirement_uses_explicit_context_inventory() {
        fn run(_: &mut SinomoTestContext) -> SinomoTestResult {
            Ok(SinomoTestOutcome::passed("capability available"))
        }
        let capability = "module.native-dylib:module.example";
        let case = SinomoTestCase::new(
            "sample.capability",
            "runtime capability",
            ["capability"],
            [SinomoTestRequirement::runtime_capability(capability)],
            run,
        );
        let mut missing = SinomoTestContext::new("/tmp/sinomo-test");
        let mut available =
            SinomoTestContext::new("/tmp/sinomo-test").with_runtime_capability(capability);

        let skipped = case.run(&mut missing);
        let passed = case.run(&mut available);

        assert_eq!(skipped.status, SinomoTestStatus::Skipped);
        assert!(skipped.summary.contains(capability));
        assert_eq!(passed.status, SinomoTestStatus::Passed);
        assert!(available.runtime_capabilities().contains(capability));
    }
}
