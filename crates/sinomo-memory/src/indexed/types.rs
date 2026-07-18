use super::super::{MemoryError, MemoryQuery};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{Id, MemoryScope, TrustDomain};
use std::collections::BTreeMap;
use std::path::PathBuf;
use time::OffsetDateTime;

pub type IndexRootId = Id;
pub type MemoryFileId = Id;
pub type MemoryFileVersionId = Id;
pub type MemoryAnchorId = Id;
pub type IndexPartitionId = Id;
pub type IndexGenerationId = Id;
pub type RouterManifestId = Id;
pub type OverlayOperationId = Id;
pub type OverlaySnapshotId = Id;
pub type MemoryRelationId = Id;
pub type AnchorLineageId = Id;
pub type PolicyDerivationId = Id;
pub type MemoryRetrievalPlanId = Id;

pub const MAX_ANCHOR_LINEAGE_ENDPOINTS: usize = 64;
pub const MAX_POLICY_DERIVATION_INPUTS: usize = 64;
pub const MAX_MEMORY_RETRIEVAL_PARTITIONS: usize = 4_096;
pub const MAX_MEMORY_RETRIEVAL_EVIDENCE: usize = 4_096;
pub const MAX_MEMORY_RETRIEVAL_EVIDENCE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_MEMORY_RETRIEVAL_TERMS: usize = 256;
pub const MAX_MEMORY_RETRIEVAL_TERM_CHARS: usize = 128;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexedMemoryContractError {
    pub contract: String,
    pub detail: String,
}

impl IndexedMemoryContractError {
    fn new(contract: &str, detail: impl Into<String>) -> Self {
        Self {
            contract: contract.to_string(),
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for IndexedMemoryContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid {}: {}", self.contract, self.detail)
    }
}

impl std::error::Error for IndexedMemoryContractError {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SourceRetentionCapability {
    NativeVersioned,
    SnapshotCapable,
    CurrentOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexRootPolicy {
    pub scope: MemoryScope,
    pub visibility: TrustDomain,
    #[serde(default = "default_true")]
    pub allow_model_context: bool,
    #[serde(default)]
    pub allow_relation_traversal: bool,
}

impl IndexRootPolicy {
    pub fn cluster_local() -> Self {
        Self {
            scope: MemoryScope::Cluster,
            visibility: TrustDomain::new("trust.local").expect("valid trust domain"),
            allow_model_context: true,
            allow_relation_traversal: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LocalIndexRootConfig {
    pub path: PathBuf,
    pub policy: IndexRootPolicy,
    #[serde(default = "default_text_extensions")]
    pub include_extensions: Vec<String>,
    #[serde(default = "default_max_source_bytes")]
    pub max_source_bytes: u64,
}

impl LocalIndexRootConfig {
    pub fn new(path: impl Into<PathBuf>, policy: IndexRootPolicy) -> Self {
        Self {
            path: path.into(),
            policy,
            include_extensions: default_text_extensions(),
            max_source_bytes: default_max_source_bytes(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexRoot {
    pub root_id: IndexRootId,
    pub connector_id: String,
    pub config: LocalIndexRootConfig,
    /// Changes whenever locally configured authorization or resource exposure
    /// changes. Pinned postings must match the current epoch before counting.
    pub authorization_epoch: Id,
    pub retention: SourceRetentionCapability,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct MemoryFileRef {
    pub connector_id: String,
    pub root_id: IndexRootId,
    pub object_id: MemoryFileId,
    pub path_hint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct MemoryFileVersionRef {
    pub file_ref: MemoryFileRef,
    pub version_id: MemoryFileVersionId,
    pub content_hash: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SourceFreshness {
    Current,
    Stale,
    Deleted,
    Unavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceDescriptor {
    pub version: MemoryFileVersionRef,
    pub media_type: String,
    pub policy: IndexRootPolicy,
    pub retention: SourceRetentionCapability,
    pub freshness: SourceFreshness,
    pub platform_identity: String,
    pub source_version_hint: String,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PolicyDerivationInput {
    pub source_version: MemoryFileVersionRef,
    pub policy: IndexRootPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyDerivationRule {
    InheritSource,
    Intersection,
}

/// Auditable proof that a derived fact's effective policy is no broader than
/// every exact source version on which it depends.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PolicyDerivation {
    pub derivation_id: PolicyDerivationId,
    pub rule: PolicyDerivationRule,
    /// Unique, canonically ordered source-policy inputs.
    pub inputs: Vec<PolicyDerivationInput>,
    pub effective_policy: IndexRootPolicy,
}

impl PolicyDerivation {
    pub fn from_source(source: &SourceDescriptor) -> Self {
        Self::intersect(vec![PolicyDerivationInput {
            source_version: source.version.clone(),
            policy: source.policy.clone(),
        }])
        .expect("one source policy is always representable")
    }

    /// Intersects source policies without inventing a broader scope. The
    /// current policy vocabulary has one scope and one trust domain, so
    /// heterogeneous scope/domain conjunctions are rejected until they have a
    /// first-class representation rather than being approximated unsafely.
    pub fn intersect(
        mut inputs: Vec<PolicyDerivationInput>,
    ) -> Result<Self, IndexedMemoryContractError> {
        if inputs.is_empty() || inputs.len() > MAX_POLICY_DERIVATION_INPUTS {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                format!(
                    "input count {} is outside 1..={MAX_POLICY_DERIVATION_INPUTS}",
                    inputs.len()
                ),
            ));
        }
        inputs.sort_by(|left, right| left.source_version.cmp(&right.source_version));
        if inputs
            .windows(2)
            .any(|pair| pair[0].source_version == pair[1].source_version)
        {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                "each exact source version may appear only once",
            ));
        }

        let first = &inputs[0].policy;
        if inputs.iter().any(|input| {
            input.policy.scope != first.scope || input.policy.visibility != first.visibility
        }) {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                "source policies with different scopes or trust domains cannot be represented by one effective policy",
            ));
        }
        let effective_policy = IndexRootPolicy {
            scope: first.scope.clone(),
            visibility: first.visibility.clone(),
            allow_model_context: inputs.iter().all(|input| input.policy.allow_model_context),
            allow_relation_traversal: inputs
                .iter()
                .all(|input| input.policy.allow_relation_traversal),
        };
        let rule = if inputs.len() == 1 {
            PolicyDerivationRule::InheritSource
        } else {
            PolicyDerivationRule::Intersection
        };
        let derivation_id = Self::canonical_id(&rule, &inputs, &effective_policy);
        Ok(Self {
            derivation_id,
            rule,
            inputs,
            effective_policy,
        })
    }

    pub fn validate(&self) -> Result<(), IndexedMemoryContractError> {
        let expected = Self::intersect(self.inputs.clone())?;
        if self.rule != expected.rule {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                "derivation rule does not match the number of source policies",
            ));
        }
        if self.effective_policy != expected.effective_policy {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                "effective policy is not the fail-closed intersection of its sources",
            ));
        }
        if self.inputs != expected.inputs {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                "source policies are not unique and canonically ordered",
            ));
        }
        if self.derivation_id != expected.derivation_id {
            return Err(IndexedMemoryContractError::new(
                "policy derivation",
                format!(
                    "derivation id {} is not canonical; expected {}",
                    self.derivation_id, expected.derivation_id
                ),
            ));
        }
        Ok(())
    }

    fn canonical_id(
        rule: &PolicyDerivationRule,
        inputs: &[PolicyDerivationInput],
        effective_policy: &IndexRootPolicy,
    ) -> PolicyDerivationId {
        Id::derived(
            "memory-policy-derivation",
            serde_json::to_vec(&(rule, inputs, effective_policy))
                .expect("policy derivation fingerprint is serializable"),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum MemoryAnchorSelector {
    WholeFile,
    TextRange {
        start_byte: u64,
        end_byte: u64,
        start_line: u64,
        end_line: u64,
    },
    ByteRange {
        start_byte: u64,
        end_byte: u64,
    },
    Structured {
        pointer: String,
    },
    Code {
        language: String,
        symbol: Option<String>,
        start_byte: u64,
        end_byte: u64,
    },
    Document {
        page: u32,
        region: Option<String>,
    },
    Image {
        region: String,
    },
    MediaTime {
        start_millis: u64,
        end_millis: u64,
    },
    Scene3d {
        selector: String,
    },
    EventSession {
        session_id: String,
        event_id: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryAnchor {
    pub anchor_id: MemoryAnchorId,
    pub source_version: MemoryFileVersionRef,
    pub selector: MemoryAnchorSelector,
    pub media_type: String,
}

impl MemoryAnchor {
    pub fn canonical(
        source_version: MemoryFileVersionRef,
        selector: MemoryAnchorSelector,
        media_type: impl Into<String>,
    ) -> Result<Self, MemoryError> {
        let media_type = media_type.into();
        validate_anchor_selector(&source_version, &selector, &media_type)?;
        let fingerprint = serde_json::to_vec(&(
            &source_version.file_ref.object_id,
            &source_version.version_id,
            &selector,
            &media_type,
        ))
        .expect("memory anchor fingerprint is serializable");
        Ok(Self {
            anchor_id: Id::derived("memory-anchor", fingerprint),
            source_version,
            selector,
            media_type,
        })
    }

    /// Revalidates both the selector bounds and the content-derived anchor id.
    /// This must be called for anchors received from extension or persisted
    /// data boundaries; serde deserialization alone is not validation.
    pub fn validate(&self) -> Result<(), MemoryError> {
        validate_anchor_selector(&self.source_version, &self.selector, &self.media_type)?;
        let expected = Self::canonical(
            self.source_version.clone(),
            self.selector.clone(),
            self.media_type.clone(),
        )?;
        if self.anchor_id != expected.anchor_id {
            return Err(MemoryError::InvalidMemoryAnchor {
                detail: format!(
                    "anchor id {} is not canonical; expected {}",
                    self.anchor_id, expected.anchor_id
                ),
            });
        }
        Ok(())
    }
}

fn validate_anchor_selector(
    source_version: &MemoryFileVersionRef,
    selector: &MemoryAnchorSelector,
    media_type: &str,
) -> Result<(), MemoryError> {
    let invalid = |detail: String| MemoryError::InvalidMemoryAnchor { detail };
    if media_type.trim().is_empty() {
        return Err(invalid("media type must not be empty".to_string()));
    }
    let validate_range = |kind: &str, start: u64, end: u64| {
        if start > end {
            Err(invalid(format!(
                "{kind} start {start} is greater than end {end}"
            )))
        } else if end > source_version.byte_length {
            Err(invalid(format!(
                "{kind} end {end} exceeds source length {}",
                source_version.byte_length
            )))
        } else {
            Ok(())
        }
    };
    match selector {
        MemoryAnchorSelector::WholeFile
        | MemoryAnchorSelector::Structured { .. }
        | MemoryAnchorSelector::Image { .. }
        | MemoryAnchorSelector::Scene3d { .. }
        | MemoryAnchorSelector::EventSession { .. } => {}
        MemoryAnchorSelector::TextRange {
            start_byte,
            end_byte,
            start_line,
            end_line,
        } => {
            validate_range("text range", *start_byte, *end_byte)?;
            if *start_line == 0 || start_line > end_line {
                return Err(invalid(format!(
                    "invalid one-based line range {start_line}..{end_line}"
                )));
            }
        }
        MemoryAnchorSelector::ByteRange {
            start_byte,
            end_byte,
        }
        | MemoryAnchorSelector::Code {
            start_byte,
            end_byte,
            ..
        } => validate_range("byte range", *start_byte, *end_byte)?,
        MemoryAnchorSelector::Document { page, .. } if *page == 0 => {
            return Err(invalid("document pages are one-based".to_string()));
        }
        MemoryAnchorSelector::MediaTime {
            start_millis,
            end_millis,
        } if start_millis > end_millis => {
            return Err(invalid(format!(
                "media start {start_millis} is greater than end {end_millis}"
            )));
        }
        MemoryAnchorSelector::Document { .. } | MemoryAnchorSelector::MediaTime { .. } => {}
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PipelineFingerprint {
    pub detector: String,
    pub extractor: String,
    pub extractor_version: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
}

impl PipelineFingerprint {
    pub fn builtin_text() -> Self {
        Self {
            detector: "sinomo.memory.text-detector".to_string(),
            extractor: "sinomo.memory.text-extractor".to_string(),
            extractor_version: "1".to_string(),
            options: BTreeMap::new(),
        }
    }

    pub fn id(&self) -> Id {
        Id::derived(
            "memory-pipeline",
            serde_json::to_vec(self).expect("pipeline fingerprint is serializable"),
        )
    }
}

/// The structural relationship between exact anchors in two distinct source
/// versions. Lineage never replaces either exact-version anchor.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AnchorLineageKind {
    /// One logical anchor continues in one anchor in the target version.
    Continues,
    /// One logical anchor was divided into multiple target anchors.
    Split,
    /// Multiple source anchors were combined into one target anchor.
    Merge,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AnchorLineageMapping {
    pub lineage_id: AnchorLineageId,
    /// Canonically ordered exact anchors from one source version.
    pub source_anchors: Vec<MemoryAnchor>,
    /// Canonically ordered exact anchors from one different target version.
    pub target_anchors: Vec<MemoryAnchor>,
    pub kind: AnchorLineageKind,
    pub producer: PipelineFingerprint,
    /// Confidence in thousandths, in the inclusive range 0..=1000.
    pub confidence_millis: u16,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

impl AnchorLineageMapping {
    pub fn canonical(
        mut source_anchors: Vec<MemoryAnchor>,
        mut target_anchors: Vec<MemoryAnchor>,
        kind: AnchorLineageKind,
        producer: PipelineFingerprint,
        confidence_millis: u16,
        observed_at: OffsetDateTime,
    ) -> Result<Self, IndexedMemoryContractError> {
        source_anchors.sort_by(|left, right| left.anchor_id.cmp(&right.anchor_id));
        target_anchors.sort_by(|left, right| left.anchor_id.cmp(&right.anchor_id));
        let lineage_id = Self::canonical_id(
            &source_anchors,
            &target_anchors,
            &kind,
            &producer,
            confidence_millis,
            observed_at,
        );
        let mapping = Self {
            lineage_id,
            source_anchors,
            target_anchors,
            kind,
            producer,
            confidence_millis,
            observed_at,
        };
        mapping.validate()?;
        Ok(mapping)
    }

    pub fn validate(&self) -> Result<(), IndexedMemoryContractError> {
        validate_lineage_endpoints("source", &self.source_anchors)?;
        validate_lineage_endpoints("target", &self.target_anchors)?;
        if self.confidence_millis > 1_000 {
            return Err(IndexedMemoryContractError::new(
                "anchor lineage",
                format!(
                    "confidence {} exceeds the 1000-thousandths maximum",
                    self.confidence_millis
                ),
            ));
        }
        if self.producer.detector.trim().is_empty()
            || self.producer.extractor.trim().is_empty()
            || self.producer.extractor_version.trim().is_empty()
        {
            return Err(IndexedMemoryContractError::new(
                "anchor lineage",
                "producer detector, extractor, and version must not be empty",
            ));
        }

        let source_version = &self.source_anchors[0].source_version.version_id;
        let target_version = &self.target_anchors[0].source_version.version_id;
        if source_version == target_version {
            return Err(IndexedMemoryContractError::new(
                "anchor lineage",
                "source and target anchors must refer to distinct source versions",
            ));
        }
        match self.kind {
            AnchorLineageKind::Continues
                if self.source_anchors.len() != 1 || self.target_anchors.len() != 1 =>
            {
                return Err(IndexedMemoryContractError::new(
                    "anchor lineage",
                    "continues mappings require exactly one source and one target anchor",
                ));
            }
            AnchorLineageKind::Split
                if self.source_anchors.len() != 1 || self.target_anchors.len() < 2 =>
            {
                return Err(IndexedMemoryContractError::new(
                    "anchor lineage",
                    "split mappings require one source and at least two target anchors",
                ));
            }
            AnchorLineageKind::Merge
                if self.source_anchors.len() < 2 || self.target_anchors.len() != 1 =>
            {
                return Err(IndexedMemoryContractError::new(
                    "anchor lineage",
                    "merge mappings require at least two source and one target anchor",
                ));
            }
            AnchorLineageKind::Continues | AnchorLineageKind::Split | AnchorLineageKind::Merge => {}
        }

        let expected = Self::canonical_id(
            &self.source_anchors,
            &self.target_anchors,
            &self.kind,
            &self.producer,
            self.confidence_millis,
            self.observed_at,
        );
        if self.lineage_id != expected {
            return Err(IndexedMemoryContractError::new(
                "anchor lineage",
                format!(
                    "lineage id {} is not canonical; expected {}",
                    self.lineage_id, expected
                ),
            ));
        }
        Ok(())
    }

    fn canonical_id(
        source_anchors: &[MemoryAnchor],
        target_anchors: &[MemoryAnchor],
        kind: &AnchorLineageKind,
        producer: &PipelineFingerprint,
        confidence_millis: u16,
        observed_at: OffsetDateTime,
    ) -> AnchorLineageId {
        #[derive(Serialize)]
        struct Fingerprint<'a> {
            source_anchors: &'a [MemoryAnchor],
            target_anchors: &'a [MemoryAnchor],
            kind: &'a AnchorLineageKind,
            producer: &'a PipelineFingerprint,
            confidence_millis: u16,
            #[serde(with = "time::serde::rfc3339")]
            observed_at: OffsetDateTime,
        }

        Id::derived(
            "memory-anchor-lineage",
            serde_json::to_vec(&Fingerprint {
                source_anchors,
                target_anchors,
                kind,
                producer,
                confidence_millis,
                observed_at,
            })
            .expect("anchor lineage fingerprint is serializable"),
        )
    }
}

fn validate_lineage_endpoints(
    side: &str,
    anchors: &[MemoryAnchor],
) -> Result<(), IndexedMemoryContractError> {
    if anchors.is_empty() || anchors.len() > MAX_ANCHOR_LINEAGE_ENDPOINTS {
        return Err(IndexedMemoryContractError::new(
            "anchor lineage",
            format!(
                "{side} endpoint count {} is outside 1..={MAX_ANCHOR_LINEAGE_ENDPOINTS}",
                anchors.len()
            ),
        ));
    }
    let version_id = &anchors[0].source_version.version_id;
    let mut previous: Option<&MemoryAnchorId> = None;
    for anchor in anchors {
        anchor.validate().map_err(|error| {
            IndexedMemoryContractError::new(
                "anchor lineage",
                format!("invalid {side} anchor: {error}"),
            )
        })?;
        if &anchor.source_version.version_id != version_id {
            return Err(IndexedMemoryContractError::new(
                "anchor lineage",
                format!("all {side} anchors must refer to one exact source version"),
            ));
        }
        if previous.is_some_and(|previous| previous >= &anchor.anchor_id) {
            return Err(IndexedMemoryContractError::new(
                "anchor lineage",
                format!("{side} anchors must be unique and canonically ordered by anchor id"),
            ));
        }
        previous = Some(&anchor.anchor_id);
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtractionProvenance {
    pub pipeline: PipelineFingerprint,
    pub source_version_id: MemoryFileVersionId,
    #[serde(with = "time::serde::rfc3339")]
    pub extracted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexFact {
    pub fact_id: Id,
    pub anchor_id: MemoryAnchorId,
    pub predicate: String,
    pub value: serde_json::Value,
    pub provenance: ExtractionProvenance,
    pub policy_derivation: PolicyDerivation,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LexicalAnchor {
    pub anchor: MemoryAnchor,
    pub terms: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexGenerationManifest {
    pub generation_id: IndexGenerationId,
    pub partition_id: IndexPartitionId,
    pub source_version_id: MemoryFileVersionId,
    pub authorization_epoch: Id,
    pub pipeline_id: Id,
    pub anchor_count: usize,
    pub fact_count: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub published_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LexicalPartitionGeneration {
    pub manifest: IndexGenerationManifest,
    pub source: SourceDescriptor,
    pub anchors: Vec<LexicalAnchor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facts: Vec<IndexFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_content: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PartitionManifest {
    pub partition_id: IndexPartitionId,
    pub current_generation_id: IndexGenerationId,
    #[serde(with = "time::serde::rfc3339")]
    pub published_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(tag = "kind", content = "id", rename_all = "kebab-case")]
pub enum MemoryNodeRef {
    File(MemoryFileId),
    Version(MemoryFileVersionId),
    Anchor(MemoryAnchorId),
    Entity(Id),
    External(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRelation {
    pub relation_id: MemoryRelationId,
    pub subject: MemoryNodeRef,
    pub predicate: String,
    pub object: MemoryNodeRef,
    pub evidence: Vec<MemoryAnchorId>,
    pub confidence_millis: u16,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub valid_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disputes: Option<MemoryRelationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<MemoryRelationId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum OverlayValidationState {
    Unverified,
    Validated,
    Rejected,
    Superseded,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum OverlayOperationKind {
    AssertRelation {
        relation: MemoryRelation,
    },
    DisputeRelation {
        relation_id: MemoryRelationId,
        reason: String,
    },
    Supersede {
        operation_id: OverlayOperationId,
    },
    Annotate {
        node: MemoryNodeRef,
        key: String,
        value: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OverlayOperation {
    pub operation_id: OverlayOperationId,
    pub scope: MemoryScope,
    pub state: OverlayValidationState,
    pub kind: OverlayOperationKind,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl OverlayOperation {
    /// Builds an overlay operation whose identity is derived from its complete,
    /// immutable payload. Repeating this call with the same payload yields the
    /// same operation id, which makes independently replayed overlay streams
    /// idempotent.
    pub fn canonical(
        scope: MemoryScope,
        state: OverlayValidationState,
        kind: OverlayOperationKind,
        created_at: OffsetDateTime,
    ) -> Self {
        let operation_id = Self::canonical_id(&scope, &state, &kind, created_at);
        Self {
            operation_id,
            scope,
            state,
            kind,
            created_at,
        }
    }

    pub fn canonical_id(
        scope: &MemoryScope,
        state: &OverlayValidationState,
        kind: &OverlayOperationKind,
        created_at: OffsetDateTime,
    ) -> OverlayOperationId {
        #[derive(Serialize)]
        struct OverlayOperationFingerprint<'a> {
            scope: &'a MemoryScope,
            state: &'a OverlayValidationState,
            kind: &'a OverlayOperationKind,
            #[serde(with = "time::serde::rfc3339")]
            created_at: OffsetDateTime,
        }

        let fingerprint = OverlayOperationFingerprint {
            scope,
            state,
            kind,
            created_at,
        };
        Id::derived(
            "memory-overlay-operation",
            serde_json::to_vec(&fingerprint)
                .expect("memory overlay operation fingerprint is serializable"),
        )
    }

    pub fn expected_operation_id(&self) -> OverlayOperationId {
        Self::canonical_id(&self.scope, &self.state, &self.kind, self.created_at)
    }
}

/// Snapshot-pinned continuation for append-ordered overlay replay.
/// `next_sequence` is the first operation not returned by the prior page.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OverlayPageCursor {
    pub snapshot_id: OverlaySnapshotId,
    pub next_sequence: u64,
    /// Store-local keyed authenticator over the snapshot and resume position.
    pub authenticator: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryAccessContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_agent_id: Option<String>,
    #[serde(default)]
    pub allowed_scopes: Vec<MemoryScope>,
    #[serde(default)]
    pub allowed_visibility: Vec<TrustDomain>,
}

impl MemoryAccessContext {
    pub fn cluster_local() -> Self {
        Self {
            subject_agent_id: None,
            allowed_scopes: vec![MemoryScope::Cluster],
            allowed_visibility: vec![TrustDomain::new("trust.local").expect("valid trust domain")],
        }
    }

    pub fn permits(&self, policy: &IndexRootPolicy) -> bool {
        policy.allow_model_context
            && self.allowed_scopes.contains(&policy.scope)
            && self.allowed_visibility.contains(&policy.visibility)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct QueryFeatureTrace {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<MemoryRetrievalPlanId>,
    pub terms: Vec<String>,
    pub routed_partitions: usize,
    pub searched_partitions: usize,
    pub stale_partitions: usize,
    /// Whether the independent audit log accepted this retrieval trace.
    #[serde(default)]
    pub trace_persisted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalTrace {
    pub query_fingerprint: Id,
    pub access_fingerprint: Id,
    pub router_manifest_id: RouterManifestId,
    pub coverage: EvidenceCoverage,
    pub selected_anchor_ids: Vec<MemoryAnchorId>,
    pub feature_trace: QueryFeatureTrace,
    #[serde(with = "time::serde::rfc3339")]
    pub retrieved_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RetrievalTraceCursor {
    pub byte_offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryQuerySnapshot {
    pub router_manifest_id: RouterManifestId,
    pub partition_generations: BTreeMap<IndexPartitionId, IndexGenerationId>,
    #[serde(with = "time::serde::rfc3339")]
    pub pinned_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operator", rename_all = "kebab-case")]
pub enum MemoryRetrievalOperator {
    /// Enumerate authorized source partitions when the query has no lexical
    /// terms. This remains bounded by the same partition/evidence limits.
    AuthorizedSourceEnumeration,
    /// Exact lexical routing for canonical, sorted, unique terms.
    LexicalExact { terms: Vec<String> },
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalPlanLimits {
    pub max_partitions: usize,
    pub max_evidence: usize,
    pub max_evidence_bytes: usize,
}

/// A reproducible, hard-bounded plan derived from a query and its pinned
/// router snapshot. The current first slice emits one lexical or authorized
/// enumeration operator; later providers can extend the operator vocabulary.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalPlan {
    pub schema_version: u32,
    pub plan_id: MemoryRetrievalPlanId,
    pub query_fingerprint: Id,
    pub router_manifest_id: RouterManifestId,
    pub operators: Vec<MemoryRetrievalOperator>,
    pub limits: MemoryRetrievalPlanLimits,
}

impl MemoryRetrievalPlan {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn from_query(
        query_fingerprint: Id,
        snapshot: &MemoryQuerySnapshot,
        query: &MemoryQuery,
        terms: impl IntoIterator<Item = String>,
    ) -> Self {
        let terms = canonical_plan_terms(terms);
        let operators = if terms.is_empty() {
            vec![MemoryRetrievalOperator::AuthorizedSourceEnumeration]
        } else {
            vec![MemoryRetrievalOperator::LexicalExact { terms }]
        };
        let limits = MemoryRetrievalPlanLimits {
            max_partitions: query.max_partitions.min(MAX_MEMORY_RETRIEVAL_PARTITIONS),
            max_evidence: query.max_records.min(MAX_MEMORY_RETRIEVAL_EVIDENCE),
            max_evidence_bytes: query.max_bytes.min(MAX_MEMORY_RETRIEVAL_EVIDENCE_BYTES),
        };
        let schema_version = Self::SCHEMA_VERSION;
        let router_manifest_id = snapshot.router_manifest_id.clone();
        let plan_id = Self::canonical_id(
            schema_version,
            &query_fingerprint,
            &router_manifest_id,
            &operators,
            limits,
        );
        Self {
            schema_version,
            plan_id,
            query_fingerprint,
            router_manifest_id,
            operators,
            limits,
        }
    }

    pub fn validate(&self) -> Result<(), IndexedMemoryContractError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(IndexedMemoryContractError::new(
                "memory retrieval plan",
                format!(
                    "unsupported schema version {}; expected {}",
                    self.schema_version,
                    Self::SCHEMA_VERSION
                ),
            ));
        }
        if self.operators.len() != 1 {
            return Err(IndexedMemoryContractError::new(
                "memory retrieval plan",
                "the first-slice planner requires exactly one bounded operator",
            ));
        }
        if self.limits.max_partitions > MAX_MEMORY_RETRIEVAL_PARTITIONS
            || self.limits.max_evidence > MAX_MEMORY_RETRIEVAL_EVIDENCE
            || self.limits.max_evidence_bytes > MAX_MEMORY_RETRIEVAL_EVIDENCE_BYTES
        {
            return Err(IndexedMemoryContractError::new(
                "memory retrieval plan",
                "one or more effective limits exceed the hard retrieval bounds",
            ));
        }
        if let MemoryRetrievalOperator::LexicalExact { terms } = &self.operators[0] {
            if terms.is_empty()
                || terms.len() > MAX_MEMORY_RETRIEVAL_TERMS
                || canonical_plan_terms(terms.clone()) != *terms
            {
                return Err(IndexedMemoryContractError::new(
                    "memory retrieval plan",
                    format!(
                        "lexical terms must be non-empty, unique, sorted, and bounded to {MAX_MEMORY_RETRIEVAL_TERMS} terms of {MAX_MEMORY_RETRIEVAL_TERM_CHARS} characters"
                    ),
                ));
            }
        }
        let expected = Self::canonical_id(
            self.schema_version,
            &self.query_fingerprint,
            &self.router_manifest_id,
            &self.operators,
            self.limits,
        );
        if self.plan_id != expected {
            return Err(IndexedMemoryContractError::new(
                "memory retrieval plan",
                format!(
                    "plan id {} is not canonical; expected {}",
                    self.plan_id, expected
                ),
            ));
        }
        Ok(())
    }

    fn canonical_id(
        schema_version: u32,
        query_fingerprint: &Id,
        router_manifest_id: &RouterManifestId,
        operators: &[MemoryRetrievalOperator],
        limits: MemoryRetrievalPlanLimits,
    ) -> MemoryRetrievalPlanId {
        Id::derived(
            "memory-retrieval-plan",
            serde_json::to_vec(&(
                schema_version,
                query_fingerprint,
                router_manifest_id,
                operators,
                limits,
            ))
            .expect("memory retrieval plan fingerprint is serializable"),
        )
    }
}

fn canonical_plan_terms(terms: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut canonical = std::collections::BTreeSet::new();
    for term in terms {
        let normalized = term
            .trim()
            .chars()
            .flat_map(char::to_lowercase)
            .take(MAX_MEMORY_RETRIEVAL_TERM_CHARS)
            .collect::<String>();
        if !normalized.is_empty() {
            canonical.insert(normalized);
            if canonical.len() > MAX_MEMORY_RETRIEVAL_TERMS {
                canonical.pop_last();
            }
        }
    }
    canonical.into_iter().collect()
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryQueryContinuation {
    pub router_manifest_id: RouterManifestId,
    pub query_fingerprint: Id,
    /// The first partition that has not been completely consumed. New
    /// continuations use this stable key instead of requiring the caller to
    /// retain an ever-growing prefix of routed partitions.
    pub next_partition_id: IndexPartitionId,
    pub next_generation_id: IndexGenerationId,
    /// `false` resumes at this partition (for an unfinished evidence anchor);
    /// `true` resumes strictly after a router posting already inspected under
    /// the prior page's bounded work budget.
    pub resume_after_partition: bool,
    pub next_anchor: usize,
    /// Store-local keyed authenticator over every continuation field. Callers
    /// may carry this value but cannot forge a different resume position.
    pub authenticator: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceCoverage {
    Complete,
    Approximate { reason: String },
    BudgetTruncated { reason: String },
}

impl Default for EvidenceCoverage {
    fn default() -> Self {
        Self::Complete
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvidence {
    pub anchor: MemoryAnchor,
    pub content: String,
    pub score: i32,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvidenceFile {
    pub source: SourceDescriptor,
    pub evidence_anchor_ids: Vec<MemoryAnchorId>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvidenceBundle {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<MemoryRetrievalPlan>,
    pub snapshot: Option<MemoryQuerySnapshot>,
    pub files: Vec<MemoryEvidenceFile>,
    pub evidence: Vec<MemoryEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<MemoryRelation>,
    pub coverage: EvidenceCoverage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<MemoryQueryContinuation>,
    pub trace: QueryFeatureTrace,
}

impl Default for QueryFeatureTrace {
    fn default() -> Self {
        Self {
            plan_id: None,
            terms: Vec::new(),
            routed_partitions: 0,
            searched_partitions: 0,
            stale_partitions: 0,
            trace_persisted: false,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct LocalScanCursor {
    pub frames: Vec<LocalScanFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LocalScanFrame {
    pub relative_dir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceCandidate {
    pub file_ref: MemoryFileRef,
    pub absolute_path: PathBuf,
    pub platform_identity: String,
    pub source_version_hint: String,
    pub byte_length: u64,
    pub media_type: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourcePage {
    pub sources: Vec<SourceCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<LocalScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct VerifiedSourceSnapshot {
    pub candidate: SourceCandidate,
    pub version: MemoryFileVersionRef,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexMaintenanceReport {
    pub discovered_files: usize,
    pub unchanged_files: usize,
    pub published_files: usize,
    pub tombstoned_files: usize,
    /// Total number of immutable generations published by this maintenance
    /// pass. The ID sample below is deliberately bounded.
    #[serde(default)]
    pub published_generation_count: usize,
    #[serde(default)]
    pub published_generations_truncated: bool,
    pub published_generations: Vec<IndexGenerationId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Page<T, C> {
    pub items: Vec<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<C>,
}

pub trait MemorySourceConnector: Send + Sync {
    fn scan_page(
        &self,
        root: &IndexRoot,
        cursor: Option<&LocalScanCursor>,
        limit: usize,
    ) -> Result<SourcePage, MemoryError>;

    fn acquire_snapshot(
        &self,
        root: &IndexRoot,
        source: &SourceCandidate,
    ) -> Result<VerifiedSourceSnapshot, MemoryError>;
}

pub trait MemorySourceCatalog: Send + Sync {
    fn register_root(&self, config: LocalIndexRootConfig) -> Result<IndexRoot, MemoryError>;
    fn root_page(
        &self,
        after: Option<&IndexRootId>,
        limit: usize,
    ) -> Result<Page<IndexRoot, IndexRootId>, MemoryError>;
    fn source_page(
        &self,
        root_id: &IndexRootId,
        after: Option<&MemoryFileId>,
        limit: usize,
    ) -> Result<Page<SourceDescriptor, MemoryFileId>, MemoryError>;
}

pub trait MemoryIndexStore: Send + Sync {
    fn publish_source(&self, source: &SourceCandidate) -> Result<IndexGenerationId, MemoryError>;
    fn pin_query_snapshot(&self) -> Result<MemoryQuerySnapshot, MemoryError>;
}

pub trait MemoryOverlayStore: Send + Sync {
    fn append_overlay(&self, operation: OverlayOperation) -> Result<(), MemoryError>;
    fn overlay_page(
        &self,
        cursor: Option<&OverlayPageCursor>,
        limit: usize,
    ) -> Result<Page<OverlayOperation, OverlayPageCursor>, MemoryError>;
}

pub trait MemoryTraceStore: Send + Sync {
    fn trace_page(
        &self,
        cursor: Option<RetrievalTraceCursor>,
        limit: usize,
    ) -> Result<Page<MemoryRetrievalTrace, RetrievalTraceCursor>, MemoryError>;
}

pub trait MemoryRetriever: Send + Sync {
    fn retrieve_evidence(
        &self,
        access: &MemoryAccessContext,
        query: &MemoryQuery,
    ) -> Result<MemoryEvidenceBundle, MemoryError>;
}

const fn default_true() -> bool {
    true
}

const fn default_max_source_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_text_extensions() -> Vec<String> {
    vec!["md".to_string(), "markdown".to_string(), "txt".to_string()]
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    fn file_version(label: &str, version: &str, byte_length: u64) -> MemoryFileVersionRef {
        MemoryFileVersionRef {
            file_ref: MemoryFileRef {
                connector_id: "test/v1".to_string(),
                root_id: Id::derived("test-memory-root", b"root"),
                object_id: Id::derived("test-memory-file", label.as_bytes()),
                path_hint: format!("{label}.md"),
            },
            version_id: Id::derived("test-memory-version", version.as_bytes()),
            content_hash: format!("hash-{version}"),
            byte_length,
        }
    }

    fn text_anchor(
        version: MemoryFileVersionRef,
        start_byte: u64,
        end_byte: u64,
        line: u64,
    ) -> MemoryAnchor {
        MemoryAnchor::canonical(
            version,
            MemoryAnchorSelector::TextRange {
                start_byte,
                end_byte,
                start_line: line,
                end_line: line,
            },
            "text/markdown",
        )
        .expect("valid test anchor")
    }

    #[test]
    fn anchor_lineage_is_exact_cross_version_and_canonical() {
        let source = text_anchor(file_version("notes", "v1", 16), 0, 5, 1);
        let target_a = text_anchor(file_version("notes", "v2", 24), 0, 5, 1);
        let target_b = text_anchor(file_version("notes", "v2", 24), 6, 12, 2);
        let observed_at = OffsetDateTime::UNIX_EPOCH;
        let mapping = AnchorLineageMapping::canonical(
            vec![source.clone()],
            vec![target_b.clone(), target_a.clone()],
            AnchorLineageKind::Split,
            PipelineFingerprint::builtin_text(),
            900,
            observed_at,
        )
        .expect("canonical lineage");
        assert!(mapping.target_anchors[0].anchor_id < mapping.target_anchors[1].anchor_id);
        mapping.validate().expect("valid mapping");
        assert_eq!(
            mapping,
            AnchorLineageMapping::canonical(
                vec![source.clone()],
                vec![target_a, target_b],
                AnchorLineageKind::Split,
                PipelineFingerprint::builtin_text(),
                900,
                observed_at,
            )
            .expect("same canonical lineage")
        );

        let same_version = AnchorLineageMapping::canonical(
            vec![source.clone()],
            vec![source],
            AnchorLineageKind::Continues,
            PipelineFingerprint::builtin_text(),
            1_000,
            observed_at,
        )
        .expect_err("lineage must cross versions");
        assert!(same_version.detail.contains("distinct source versions"));
    }

    #[test]
    fn policy_derivation_intersects_flags_and_rejects_broadening() {
        let visibility = TrustDomain::new("trust.local").expect("trust domain");
        let inputs = vec![
            PolicyDerivationInput {
                source_version: file_version("a", "v1", 1),
                policy: IndexRootPolicy {
                    scope: MemoryScope::Cluster,
                    visibility: visibility.clone(),
                    allow_model_context: true,
                    allow_relation_traversal: false,
                },
            },
            PolicyDerivationInput {
                source_version: file_version("b", "v1", 1),
                policy: IndexRootPolicy {
                    scope: MemoryScope::Cluster,
                    visibility,
                    allow_model_context: false,
                    allow_relation_traversal: true,
                },
            },
        ];
        let derivation = PolicyDerivation::intersect(inputs).expect("representable intersection");
        assert_eq!(derivation.rule, PolicyDerivationRule::Intersection);
        assert!(!derivation.effective_policy.allow_model_context);
        assert!(!derivation.effective_policy.allow_relation_traversal);
        derivation.validate().expect("valid derivation");

        let mut broadened = derivation;
        broadened.effective_policy.allow_model_context = true;
        assert!(broadened.validate().is_err());
    }

    #[test]
    fn retrieval_plan_is_query_derived_hard_bounded_and_tamper_evident() {
        let snapshot = MemoryQuerySnapshot {
            router_manifest_id: Id::derived("test-router", b"manifest"),
            partition_generations: BTreeMap::new(),
            pinned_at: OffsetDateTime::UNIX_EPOCH,
        };
        let query = MemoryQuery {
            max_records: usize::MAX,
            max_bytes: usize::MAX,
            max_partitions: usize::MAX,
            ..MemoryQuery::default()
        };
        let fingerprint = Id::derived("test-query", b"query");
        let plan = MemoryRetrievalPlan::from_query(
            fingerprint,
            &snapshot,
            &query,
            vec!["Beta".to_string(), "alpha".to_string(), "ALPHA".to_string()],
        );
        assert_eq!(
            plan.operators,
            vec![MemoryRetrievalOperator::LexicalExact {
                terms: vec!["alpha".to_string(), "beta".to_string()]
            }]
        );
        assert_eq!(
            plan.limits,
            MemoryRetrievalPlanLimits {
                max_partitions: MAX_MEMORY_RETRIEVAL_PARTITIONS,
                max_evidence: MAX_MEMORY_RETRIEVAL_EVIDENCE,
                max_evidence_bytes: MAX_MEMORY_RETRIEVAL_EVIDENCE_BYTES,
            }
        );
        plan.validate().expect("valid bounded plan");

        let encoded = serde_json::to_vec(&plan).expect("serialize plan");
        let decoded: MemoryRetrievalPlan =
            serde_json::from_slice(&encoded).expect("deserialize plan");
        assert_eq!(decoded, plan);
        decoded.validate().expect("round-trip remains valid");

        let mut tampered = plan;
        tampered.limits.max_evidence_bytes = MAX_MEMORY_RETRIEVAL_EVIDENCE_BYTES + 1;
        assert!(tampered.validate().is_err());
    }
}
