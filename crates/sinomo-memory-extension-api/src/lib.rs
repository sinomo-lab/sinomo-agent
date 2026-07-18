//! Stable, data-only ABI contracts for indexed-file memory extensions.
//!
//! This crate intentionally owns no source watchers, scheduler, storage,
//! retrieval, or policy implementation. Extensions receive verified content
//! identities and bounded requests and return normalized, source-anchored
//! facts. Invocation and SOBJ framing are supplied by `sinomo-module-api`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MEMORY_EXTENSION_SCHEMA_VERSION_V1: u16 = 1;
pub const MEMORY_EXTRACTION_REQUEST_SCHEMA_ID_V1: &str = "sinomo.memory.extraction-request/v1";
pub const MEMORY_EXTRACTION_CONTINUATION_SCHEMA_ID_V1: &str =
    "sinomo.memory.extraction-continuation/v1";
pub const MEMORY_DERIVED_FACT_BATCH_SCHEMA_ID_V1: &str = "sinomo.memory.derived-fact-batch/v1";

pub const MEMORY_SOURCE_CONNECTOR_INTERFACE_ID_V1: &str = "sinomo.memory.source-connector/v1";
pub const MEMORY_WORKSPACE_DETECTOR_INTERFACE_ID_V1: &str = "sinomo.memory.workspace-detector/v1";
pub const MEMORY_FORMAT_DETECTOR_INTERFACE_ID_V1: &str = "sinomo.memory.format-detector/v1";
pub const MEMORY_CONTENT_EXTRACTOR_INTERFACE_ID_V1: &str = "sinomo.memory.content-extractor/v1";
pub const MEMORY_RELATION_EXTRACTOR_INTERFACE_ID_V1: &str = "sinomo.memory.relation-extractor/v1";
pub const MEMORY_EMBEDDING_PROVIDER_INTERFACE_ID_V1: &str = "sinomo.memory.embedding-provider/v1";
pub const MEMORY_ANCHOR_LINEAGE_INTERFACE_ID_V1: &str = "sinomo.memory.anchor-lineage/v1";
pub const MEMORY_QUERY_ADAPTER_INTERFACE_ID_V1: &str = "sinomo.memory.query-adapter/v1";
pub const MEMORY_RERANKER_INTERFACE_ID_V1: &str = "sinomo.memory.reranker/v1";
pub const MEMORY_PREVIEW_RENDERER_INTERFACE_ID_V1: &str = "sinomo.memory.preview-renderer/v1";

/// Continuation state is returned through SOBJ, so it stays metadata-sized.
/// Large derived output belongs in bounded batches or artifact references.
pub const MAX_MEMORY_EXTENSION_CONTINUATION_BYTES: usize = 1024 * 1024;

macro_rules! wire_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd, Hash)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }
    };
}

wire_id!(MemoryConnectorId);
wire_id!(MemoryIndexRootId);
wire_id!(MemoryContentObjectId);
wire_id!(MemoryContentVersionId);
wire_id!(MemoryAnchorId);
wire_id!(MemoryFactId);
wire_id!(MemoryRelationId);
wire_id!(MemoryArtifactId);
wire_id!(MemoryVectorId);

/// Versioned capabilities published in a module interface descriptor.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryExtensionDescriptorV1 {
    pub schema_version: u16,
    pub descriptor_id: String,
    pub descriptor_version: String,
    pub interface_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_fact_kinds: Vec<String>,
    pub max_content_bytes: u64,
    pub max_facts_per_batch: u64,
    pub max_deadline_ms: u64,
    #[serde(default)]
    pub supports_byte_ranges: bool,
    #[serde(default)]
    pub supports_continuations: bool,
    #[serde(default)]
    pub supports_cancellation: bool,
}

impl MemoryExtensionDescriptorV1 {
    pub fn new(
        descriptor_id: impl Into<String>,
        descriptor_version: impl Into<String>,
        interface_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: MEMORY_EXTENSION_SCHEMA_VERSION_V1,
            descriptor_id: descriptor_id.into(),
            descriptor_version: descriptor_version.into(),
            interface_id: interface_id.into(),
            media_types: Vec::new(),
            output_fact_kinds: Vec::new(),
            max_content_bytes: u64::MAX,
            max_facts_per_batch: 1,
            max_deadline_ms: 1,
            supports_byte_ranges: false,
            supports_continuations: false,
            supports_cancellation: false,
        }
    }

    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        validate_schema_version(self.schema_version)?;
        validate_text(&self.descriptor_id, "descriptor_id")?;
        validate_text(&self.descriptor_version, "descriptor_version")?;
        validate_text(&self.interface_id, "interface_id")?;
        validate_unique_texts(&self.media_types, "media_types")?;
        validate_unique_texts(&self.output_fact_kinds, "output_fact_kinds")?;
        validate_nonzero(self.max_content_bytes, "max_content_bytes")?;
        validate_nonzero(self.max_facts_per_batch, "max_facts_per_batch")?;
        validate_nonzero(self.max_deadline_ms, "max_deadline_ms")?;
        Ok(())
    }
}

/// Complete identity of the active extension implementation. Every field is
/// included by the memory service when deriving its pipeline fingerprint.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryExtensionActiveIdentityV1 {
    pub module_id: String,
    pub module_version: String,
    pub export_id: String,
    pub interface_id: String,
    pub interface_version: String,
    pub artifact_id: String,
    pub artifact_content_hash: String,
    pub descriptor_hash: String,
    pub invocation_lane: MemoryExtensionInvocationLane,
}

impl MemoryExtensionActiveIdentityV1 {
    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        for (value, field) in [
            (&self.module_id, "module_id"),
            (&self.module_version, "module_version"),
            (&self.export_id, "export_id"),
            (&self.interface_id, "interface_id"),
            (&self.interface_version, "interface_version"),
            (&self.artifact_id, "artifact_id"),
            (&self.artifact_content_hash, "artifact_content_hash"),
            (&self.descriptor_hash, "descriptor_hash"),
        ] {
            validate_text(value, field)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryExtensionInvocationLane {
    Active,
    Candidate,
    Shadow,
}

/// Immutable source identity supplied to an extension. Paths are display
/// aliases only; object and version ids are the durable identity.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexedContentRefV1 {
    pub connector_id: MemoryConnectorId,
    pub root_id: MemoryIndexRootId,
    pub object_id: MemoryContentObjectId,
    pub version_id: MemoryContentVersionId,
    pub content_hash: String,
    pub media_type: String,
    pub byte_length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_hint: Option<String>,
}

impl IndexedContentRefV1 {
    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        validate_text(self.connector_id.as_str(), "connector_id")?;
        validate_text(self.root_id.as_str(), "root_id")?;
        validate_text(self.object_id.as_str(), "object_id")?;
        validate_text(self.version_id.as_str(), "version_id")?;
        validate_text(&self.content_hash, "content_hash")?;
        validate_text(&self.media_type, "media_type")?;
        if let Some(path_hint) = &self.path_hint {
            if path_hint.contains('\0') {
                return Err(MemoryExtensionContractError::InvalidField {
                    field: "path_hint",
                    detail: "must not contain NUL".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryByteRangeV1 {
    pub start: u64,
    pub end: u64,
}

impl MemoryByteRangeV1 {
    pub fn validate_within(&self, byte_length: u64) -> Result<(), MemoryExtensionContractError> {
        if self.start >= self.end || self.end > byte_length {
            return Err(MemoryExtensionContractError::InvalidField {
                field: "byte_range",
                detail: format!(
                    "range {}..{} must be non-empty and within {byte_length} bytes",
                    self.start, self.end
                ),
            });
        }
        Ok(())
    }
}

/// Opaque, schema-identified continuation returned by an extension.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtractionContinuationV1 {
    pub schema_id: String,
    pub token: Vec<u8>,
}

impl ExtractionContinuationV1 {
    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        validate_text(&self.schema_id, "continuation.schema_id")?;
        if self.token.is_empty() {
            return Err(MemoryExtensionContractError::InvalidField {
                field: "continuation.token",
                detail: "must be non-empty".to_string(),
            });
        }
        if self.token.len() > MAX_MEMORY_EXTENSION_CONTINUATION_BYTES {
            return Err(MemoryExtensionContractError::InvalidField {
                field: "continuation.token",
                detail: format!(
                    "is {} bytes; the limit is {MAX_MEMORY_EXTENSION_CONTINUATION_BYTES}",
                    self.token.len()
                ),
            });
        }
        Ok(())
    }
}

/// Bounded extraction request. Content bytes are transported separately by
/// the selected native attachment, object-RPC stream, or sandbox preopen.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtractionRequestV1 {
    pub schema_version: u16,
    pub request_id: String,
    pub content: IndexedContentRefV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_range: Option<MemoryByteRangeV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_fact_kinds: Vec<String>,
    pub max_facts: u64,
    pub deadline_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ExtractionContinuationV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options_sobj: Vec<u8>,
}

impl ExtractionRequestV1 {
    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        validate_schema_version(self.schema_version)?;
        validate_text(&self.request_id, "request_id")?;
        self.content.validate()?;
        if let Some(range) = self.byte_range {
            range.validate_within(self.content.byte_length)?;
        }
        validate_unique_texts(&self.requested_fact_kinds, "requested_fact_kinds")?;
        validate_nonzero(self.max_facts, "max_facts")?;
        validate_nonzero(self.deadline_ms, "deadline_ms")?;
        if let Some(token) = &self.cancellation_token {
            validate_text(token, "cancellation_token")?;
        }
        if let Some(continuation) = &self.continuation {
            continuation.validate()?;
        }
        Ok(())
    }
}

/// Exact selector for a normalized source anchor.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum NormalizedAnchorSelectorV1 {
    WholeFile,
    ByteRange {
        range: MemoryByteRangeV1,
    },
    TextRange {
        range: MemoryByteRangeV1,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_line: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_line: Option<u64>,
    },
    Structured {
        path: String,
    },
    Code {
        language: String,
        symbol: String,
        range: MemoryByteRangeV1,
    },
    Document {
        page: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<MemoryRegion2dV1>,
    },
    Image {
        region: MemoryRegion2dV1,
    },
    MediaTime {
        start_ms: u64,
        end_ms: u64,
    },
    Scene3d {
        node_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bounds: Option<MemoryBounds3dV1>,
    },
    EventSession {
        session_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRegion2dV1 {
    /// Coordinates in millionths of the source width or height.
    pub x_millionths: u32,
    pub y_millionths: u32,
    pub width_millionths: u32,
    pub height_millionths: u32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryBounds3dV1 {
    /// Provider-defined fixed-point coordinates whose unit is declared by the
    /// source format descriptor.
    pub min: [i64; 3],
    pub max: [i64; 3],
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NormalizedMemoryAnchorV1 {
    pub anchor_id: MemoryAnchorId,
    pub content: IndexedContentRefV1,
    pub selector: NormalizedAnchorSelectorV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_content_hash: Option<String>,
}

impl NormalizedMemoryAnchorV1 {
    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        validate_text(self.anchor_id.as_str(), "anchor_id")?;
        self.content.validate()?;
        match &self.selector {
            NormalizedAnchorSelectorV1::WholeFile => {}
            NormalizedAnchorSelectorV1::ByteRange { range }
            | NormalizedAnchorSelectorV1::TextRange { range, .. } => {
                range.validate_within(self.content.byte_length)?;
            }
            NormalizedAnchorSelectorV1::Structured { path } => {
                validate_text(path, "structured.path")?;
            }
            NormalizedAnchorSelectorV1::Code {
                language,
                symbol,
                range,
            } => {
                range.validate_within(self.content.byte_length)?;
                validate_text(language, "code.language")?;
                validate_text(symbol, "code.symbol")?;
            }
            NormalizedAnchorSelectorV1::Document { region, .. } => {
                if let Some(region) = region {
                    validate_region(*region)?;
                }
            }
            NormalizedAnchorSelectorV1::Image { region } => validate_region(*region)?,
            NormalizedAnchorSelectorV1::MediaTime { start_ms, end_ms } => {
                if start_ms >= end_ms {
                    return Err(MemoryExtensionContractError::InvalidField {
                        field: "media_time",
                        detail: "start_ms must be less than end_ms".to_string(),
                    });
                }
            }
            NormalizedAnchorSelectorV1::Scene3d { node_path, bounds } => {
                validate_text(node_path, "scene3d.node_path")?;
                if let Some(bounds) = bounds
                    && bounds
                        .min
                        .iter()
                        .zip(bounds.max.iter())
                        .any(|(min, max)| min > max)
                {
                    return Err(MemoryExtensionContractError::InvalidField {
                        field: "scene3d.bounds",
                        detail: "minimum coordinates must not exceed maximum coordinates"
                            .to_string(),
                    });
                }
            }
            NormalizedAnchorSelectorV1::EventSession {
                session_id,
                event_id,
            } => {
                validate_text(session_id, "event_session.session_id")?;
                if let Some(event_id) = event_id {
                    validate_text(event_id, "event_session.event_id")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExtractionProvenanceV1 {
    pub extension: MemoryExtensionActiveIdentityV1,
    pub logical_operation: String,
    pub extracted_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_fact_id: Option<MemoryFactId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryNodeRefV1 {
    pub namespace: String,
    pub kind: String,
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NormalizedIndexFactV1 {
    pub fact_id: MemoryFactId,
    pub kind: String,
    pub subject: MemoryNodeRefV1,
    /// Type-specific value encoded as a compact SOBJ record.
    pub value_sobj: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_anchor_ids: Vec<MemoryAnchorId>,
    pub provenance: ExtractionProvenanceV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NormalizedMemoryRelationV1 {
    pub relation_id: MemoryRelationId,
    pub relation_type: String,
    pub from: MemoryNodeRefV1,
    pub to: MemoryNodeRefV1,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_anchor_ids: Vec<MemoryAnchorId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence_basis_points: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_until: Option<String>,
    pub observed_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disputed_by: Vec<MemoryRelationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<MemoryRelationId>,
    pub provenance: ExtractionProvenanceV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DerivedVectorRefV1 {
    pub vector_id: MemoryVectorId,
    pub vector_space_id: String,
    pub dimensions: u32,
    pub artifact: DerivedArtifactRefV1,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_anchor_ids: Vec<MemoryAnchorId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DerivedArtifactRefV1 {
    pub artifact_id: MemoryArtifactId,
    pub media_type: String,
    pub content_hash: String,
    pub byte_length: u64,
    /// Opaque attachment name resolved only by the host invocation adapter.
    pub attachment_name: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryExtensionDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryExtensionDiagnosticV1 {
    pub severity: MemoryExtensionDiagnosticSeverity,
    pub code: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_id: Option<MemoryAnchorId>,
}

/// One normalized, bounded response batch. Incomplete output must carry a
/// continuation so callers never mistake partial extraction for full
/// coverage.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DerivedFactBatchV1 {
    pub schema_version: u16,
    pub request_id: String,
    pub content: IndexedContentRefV1,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub anchors: Vec<NormalizedMemoryAnchorV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facts: Vec<NormalizedIndexFactV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<NormalizedMemoryRelationV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vectors: Vec<DerivedVectorRefV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<DerivedArtifactRefV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<MemoryExtensionDiagnosticV1>,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ExtractionContinuationV1>,
}

/// Current protocol aliases. Versioned names remain available for code that
/// needs to state an exact wire revision at the type boundary.
pub type MemoryExtensionDescriptor = MemoryExtensionDescriptorV1;
pub type MemoryExtensionActiveIdentity = MemoryExtensionActiveIdentityV1;
pub type IndexedContentRef = IndexedContentRefV1;
pub type ExtractionRequest = ExtractionRequestV1;
pub type ExtractionContinuation = ExtractionContinuationV1;
pub type NormalizedAnchor = NormalizedMemoryAnchorV1;
pub type NormalizedAnchorSelector = NormalizedAnchorSelectorV1;
pub type ExtractionProvenance = ExtractionProvenanceV1;
pub type IndexFact = NormalizedIndexFactV1;
pub type MemoryNodeRef = MemoryNodeRefV1;
pub type MemoryRelation = NormalizedMemoryRelationV1;
pub type VectorRef = DerivedVectorRefV1;
pub type ArtifactRef = DerivedArtifactRefV1;
pub type ExtensionDiagnostic = MemoryExtensionDiagnosticV1;
pub type DerivedFactBatch = DerivedFactBatchV1;

impl DerivedFactBatchV1 {
    pub fn validate(&self) -> Result<(), MemoryExtensionContractError> {
        validate_schema_version(self.schema_version)?;
        validate_text(&self.request_id, "request_id")?;
        self.content.validate()?;
        if !self.complete && self.continuation.is_none() {
            return Err(MemoryExtensionContractError::InvalidField {
                field: "continuation",
                detail: "an incomplete batch must provide a continuation".to_string(),
            });
        }
        if self.complete && self.continuation.is_some() {
            return Err(MemoryExtensionContractError::InvalidField {
                field: "continuation",
                detail: "a complete batch must not provide a continuation".to_string(),
            });
        }
        if let Some(continuation) = &self.continuation {
            continuation.validate()?;
        }

        let mut anchor_ids = BTreeSet::new();
        for anchor in &self.anchors {
            anchor.validate()?;
            if anchor.content != self.content {
                return Err(MemoryExtensionContractError::InvalidField {
                    field: "anchors",
                    detail: format!(
                        "anchor `{}` refers to a different content version",
                        anchor.anchor_id.as_str()
                    ),
                });
            }
            if !anchor_ids.insert(anchor.anchor_id.as_str()) {
                return Err(MemoryExtensionContractError::DuplicateId {
                    field: "anchors",
                    id: anchor.anchor_id.as_str().to_string(),
                });
            }
        }

        let mut fact_ids = BTreeSet::new();
        for fact in &self.facts {
            validate_text(fact.fact_id.as_str(), "fact_id")?;
            validate_text(&fact.kind, "fact.kind")?;
            validate_node(&fact.subject)?;
            validate_provenance(&fact.provenance)?;
            validate_evidence(&fact.evidence_anchor_ids, &anchor_ids)?;
            if !fact_ids.insert(fact.fact_id.as_str()) {
                return Err(MemoryExtensionContractError::DuplicateId {
                    field: "facts",
                    id: fact.fact_id.as_str().to_string(),
                });
            }
        }

        let mut relation_ids = BTreeSet::new();
        for relation in &self.relations {
            validate_text(relation.relation_id.as_str(), "relation_id")?;
            validate_text(&relation.relation_type, "relation.relation_type")?;
            validate_node(&relation.from)?;
            validate_node(&relation.to)?;
            validate_text(&relation.observed_at, "relation.observed_at")?;
            validate_valid_time(
                relation.valid_from.as_deref(),
                relation.valid_until.as_deref(),
            )?;
            validate_relation_references(
                &relation.relation_id,
                &relation.disputed_by,
                "relation.disputed_by",
            )?;
            validate_relation_references(
                &relation.relation_id,
                &relation.supersedes,
                "relation.supersedes",
            )?;
            validate_provenance(&relation.provenance)?;
            validate_evidence(&relation.evidence_anchor_ids, &anchor_ids)?;
            if relation
                .confidence_basis_points
                .is_some_and(|confidence| confidence > 10_000)
            {
                return Err(MemoryExtensionContractError::InvalidField {
                    field: "relation.confidence_basis_points",
                    detail: "must be at most 10000".to_string(),
                });
            }
            if !relation_ids.insert(relation.relation_id.as_str()) {
                return Err(MemoryExtensionContractError::DuplicateId {
                    field: "relations",
                    id: relation.relation_id.as_str().to_string(),
                });
            }
        }

        let mut artifact_ids = BTreeSet::new();
        for artifact in &self.artifacts {
            validate_artifact(artifact)?;
            if !artifact_ids.insert(artifact.artifact_id.as_str()) {
                return Err(MemoryExtensionContractError::DuplicateId {
                    field: "artifacts",
                    id: artifact.artifact_id.as_str().to_string(),
                });
            }
        }
        let mut vector_ids = BTreeSet::new();
        for vector in &self.vectors {
            validate_text(vector.vector_id.as_str(), "vector_id")?;
            validate_text(&vector.vector_space_id, "vector_space_id")?;
            validate_nonzero(u64::from(vector.dimensions), "vector.dimensions")?;
            validate_artifact(&vector.artifact)?;
            validate_evidence(&vector.evidence_anchor_ids, &anchor_ids)?;
            if !vector_ids.insert(vector.vector_id.as_str()) {
                return Err(MemoryExtensionContractError::DuplicateId {
                    field: "vectors",
                    id: vector.vector_id.as_str().to_string(),
                });
            }
        }
        for diagnostic in &self.diagnostics {
            validate_text(&diagnostic.code, "diagnostic.code")?;
            if diagnostic.detail.contains('\0') {
                return Err(MemoryExtensionContractError::InvalidField {
                    field: "diagnostic.detail",
                    detail: "must not contain NUL".to_string(),
                });
            }
            if let Some(anchor_id) = &diagnostic.anchor_id {
                validate_anchor_reference(anchor_id, &anchor_ids)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MemoryExtensionContractError {
    #[error("unsupported memory extension schema version {actual}; expected {expected}")]
    UnsupportedSchemaVersion { expected: u16, actual: u16 },
    #[error("invalid `{field}`: {detail}")]
    InvalidField { field: &'static str, detail: String },
    #[error("duplicate id `{id}` in `{field}`")]
    DuplicateId { field: &'static str, id: String },
    #[error("evidence anchor `{anchor_id}` is not present in this batch")]
    MissingEvidenceAnchor { anchor_id: String },
}

fn validate_schema_version(actual: u16) -> Result<(), MemoryExtensionContractError> {
    if actual != MEMORY_EXTENSION_SCHEMA_VERSION_V1 {
        return Err(MemoryExtensionContractError::UnsupportedSchemaVersion {
            expected: MEMORY_EXTENSION_SCHEMA_VERSION_V1,
            actual,
        });
    }
    Ok(())
}

fn validate_text(value: &str, field: &'static str) -> Result<(), MemoryExtensionContractError> {
    if value.is_empty() || value != value.trim() || value.chars().any(char::is_control) {
        return Err(MemoryExtensionContractError::InvalidField {
            field,
            detail: "must be non-empty, trimmed, and contain no control characters".to_string(),
        });
    }
    Ok(())
}

fn validate_nonzero(value: u64, field: &'static str) -> Result<(), MemoryExtensionContractError> {
    if value == 0 {
        return Err(MemoryExtensionContractError::InvalidField {
            field,
            detail: "must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn validate_unique_texts(
    values: &[String],
    field: &'static str,
) -> Result<(), MemoryExtensionContractError> {
    let mut unique = BTreeSet::new();
    for value in values {
        validate_text(value, field)?;
        if !unique.insert(value.as_str()) {
            return Err(MemoryExtensionContractError::DuplicateId {
                field,
                id: value.clone(),
            });
        }
    }
    Ok(())
}

fn validate_region(region: MemoryRegion2dV1) -> Result<(), MemoryExtensionContractError> {
    const WHOLE: u32 = 1_000_000;
    if region.width_millionths == 0
        || region.height_millionths == 0
        || region.x_millionths.saturating_add(region.width_millionths) > WHOLE
        || region.y_millionths.saturating_add(region.height_millionths) > WHOLE
    {
        return Err(MemoryExtensionContractError::InvalidField {
            field: "region",
            detail: "must be non-empty and fit within normalized source bounds".to_string(),
        });
    }
    Ok(())
}

fn validate_node(node: &MemoryNodeRefV1) -> Result<(), MemoryExtensionContractError> {
    validate_text(&node.namespace, "node.namespace")?;
    validate_text(&node.kind, "node.kind")?;
    validate_text(&node.id, "node.id")
}

fn validate_provenance(
    provenance: &ExtractionProvenanceV1,
) -> Result<(), MemoryExtensionContractError> {
    provenance.extension.validate()?;
    validate_text(
        &provenance.logical_operation,
        "provenance.logical_operation",
    )?;
    validate_text(&provenance.extracted_at, "provenance.extracted_at")
}

fn validate_evidence(
    evidence: &[MemoryAnchorId],
    anchors: &BTreeSet<&str>,
) -> Result<(), MemoryExtensionContractError> {
    let mut unique = BTreeSet::new();
    for anchor_id in evidence {
        validate_text(anchor_id.as_str(), "evidence_anchor_id")?;
        if !anchors.contains(anchor_id.as_str()) {
            return Err(MemoryExtensionContractError::MissingEvidenceAnchor {
                anchor_id: anchor_id.as_str().to_string(),
            });
        }
        if !unique.insert(anchor_id.as_str()) {
            return Err(MemoryExtensionContractError::DuplicateId {
                field: "evidence_anchor_ids",
                id: anchor_id.as_str().to_string(),
            });
        }
    }
    Ok(())
}

fn validate_anchor_reference(
    anchor_id: &MemoryAnchorId,
    anchors: &BTreeSet<&str>,
) -> Result<(), MemoryExtensionContractError> {
    validate_text(anchor_id.as_str(), "anchor_id")?;
    if !anchors.contains(anchor_id.as_str()) {
        return Err(MemoryExtensionContractError::MissingEvidenceAnchor {
            anchor_id: anchor_id.as_str().to_string(),
        });
    }
    Ok(())
}

fn validate_valid_time(
    valid_from: Option<&str>,
    valid_until: Option<&str>,
) -> Result<(), MemoryExtensionContractError> {
    if let Some(valid_from) = valid_from {
        validate_text(valid_from, "relation.valid_from")?;
    }
    if let Some(valid_until) = valid_until {
        validate_text(valid_until, "relation.valid_until")?;
    }
    if let (Some(valid_from), Some(valid_until)) = (valid_from, valid_until)
        && comparable_canonical_utc_timestamps(valid_from, valid_until)
        && valid_from > valid_until
    {
        return Err(MemoryExtensionContractError::InvalidField {
            field: "relation.valid_time",
            detail: "valid_from must not be later than valid_until".to_string(),
        });
    }
    Ok(())
}

/// Equal-width canonical UTC timestamps sort chronologically as UTF-8. Offset
/// timestamps and mixed fractional precision still receive text validation,
/// but are left to the memory service's typed temporal index to order.
fn comparable_canonical_utc_timestamps(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && has_canonical_utc_timestamp_shape(left)
        && has_canonical_utc_timestamp_shape(right)
}

fn has_canonical_utc_timestamp_shape(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes.last() != Some(&b'Z')
    {
        return false;
    }
    if bytes.len() > 20 && bytes[19] != b'.' {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| {
        matches!(index, 4 | 7 | 10 | 13 | 16)
            || (index == 19 && bytes.len() > 20)
            || index + 1 == bytes.len()
            || byte.is_ascii_digit()
    })
}

fn validate_relation_references(
    relation_id: &MemoryRelationId,
    references: &[MemoryRelationId],
    field: &'static str,
) -> Result<(), MemoryExtensionContractError> {
    let mut unique = BTreeSet::new();
    for reference in references {
        validate_text(reference.as_str(), field)?;
        if reference == relation_id {
            return Err(MemoryExtensionContractError::InvalidField {
                field,
                detail: "must not reference the relation itself".to_string(),
            });
        }
        if !unique.insert(reference.as_str()) {
            return Err(MemoryExtensionContractError::DuplicateId {
                field,
                id: reference.as_str().to_string(),
            });
        }
    }
    Ok(())
}

fn validate_artifact(artifact: &DerivedArtifactRefV1) -> Result<(), MemoryExtensionContractError> {
    validate_text(artifact.artifact_id.as_str(), "artifact_id")?;
    validate_text(&artifact.media_type, "artifact.media_type")?;
    validate_text(&artifact.content_hash, "artifact.content_hash")?;
    validate_text(&artifact.attachment_name, "artifact.attachment_name")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content() -> IndexedContentRefV1 {
        IndexedContentRefV1 {
            connector_id: "connector.local".into(),
            root_id: "root.docs".into(),
            object_id: "object.readme".into(),
            version_id: "version.1".into(),
            content_hash: "blake3:abc".to_string(),
            media_type: "text/markdown".to_string(),
            byte_length: 128,
            path_hint: Some("README.md".to_string()),
        }
    }

    fn extension() -> MemoryExtensionActiveIdentityV1 {
        MemoryExtensionActiveIdentityV1 {
            module_id: "module.markdown".to_string(),
            module_version: "1.0.0".to_string(),
            export_id: "markdown.extractor".to_string(),
            interface_id: MEMORY_CONTENT_EXTRACTOR_INTERFACE_ID_V1.to_string(),
            interface_version: "1.0.0".to_string(),
            artifact_id: "artifact.markdown".to_string(),
            artifact_content_hash: "blake3:def".to_string(),
            descriptor_hash: "blake3:123".to_string(),
            invocation_lane: MemoryExtensionInvocationLane::Active,
        }
    }

    fn provenance() -> ExtractionProvenanceV1 {
        ExtractionProvenanceV1 {
            extension: extension(),
            logical_operation: "extract".to_string(),
            extracted_at: "2026-07-18T00:00:00Z".to_string(),
            parent_fact_id: None,
        }
    }

    fn anchor(anchor_id: &str) -> NormalizedMemoryAnchorV1 {
        NormalizedMemoryAnchorV1 {
            anchor_id: anchor_id.into(),
            content: content(),
            selector: NormalizedAnchorSelectorV1::WholeFile,
            label: None,
            anchor_content_hash: None,
        }
    }

    fn node(id: &str) -> MemoryNodeRefV1 {
        MemoryNodeRefV1 {
            namespace: "document".to_string(),
            kind: "section".to_string(),
            id: id.to_string(),
        }
    }

    fn relation(relation_id: &str) -> NormalizedMemoryRelationV1 {
        NormalizedMemoryRelationV1 {
            relation_id: relation_id.into(),
            relation_type: "document.references".to_string(),
            from: node("intro"),
            to: node("details"),
            evidence_anchor_ids: Vec::new(),
            confidence_basis_points: Some(9_000),
            valid_from: None,
            valid_until: None,
            observed_at: "2026-07-18T00:00:00Z".to_string(),
            disputed_by: Vec::new(),
            supersedes: Vec::new(),
            provenance: provenance(),
        }
    }

    fn artifact(artifact_id: &str, attachment_name: &str) -> DerivedArtifactRefV1 {
        DerivedArtifactRefV1 {
            artifact_id: artifact_id.into(),
            media_type: "application/octet-stream".to_string(),
            content_hash: format!("blake3:{artifact_id}"),
            byte_length: 16,
            attachment_name: attachment_name.to_string(),
        }
    }

    fn vector(vector_id: &str, artifact_id: &str) -> DerivedVectorRefV1 {
        DerivedVectorRefV1 {
            vector_id: vector_id.into(),
            vector_space_id: "text.default".to_string(),
            dimensions: 4,
            artifact: artifact(artifact_id, artifact_id),
            evidence_anchor_ids: Vec::new(),
        }
    }

    fn batch() -> DerivedFactBatchV1 {
        DerivedFactBatchV1 {
            schema_version: MEMORY_EXTENSION_SCHEMA_VERSION_V1,
            request_id: "request.1".to_string(),
            content: content(),
            anchors: vec![anchor("anchor.readme")],
            facts: Vec::new(),
            relations: Vec::new(),
            vectors: Vec::new(),
            artifacts: Vec::new(),
            diagnostics: Vec::new(),
            complete: true,
            continuation: None,
        }
    }

    #[test]
    fn extraction_request_roundtrips_through_module_sobj() {
        let request = ExtractionRequestV1 {
            schema_version: MEMORY_EXTENSION_SCHEMA_VERSION_V1,
            request_id: "request.1".to_string(),
            content: content(),
            byte_range: Some(MemoryByteRangeV1 { start: 0, end: 64 }),
            requested_fact_kinds: vec!["text.section".to_string()],
            max_facts: 32,
            deadline_ms: 5_000,
            cancellation_token: Some("cancel.1".to_string()),
            continuation: None,
            options_sobj: Vec::new(),
        };
        request.validate().expect("valid request");

        let encoded = sinomo_module_api::encode_sobj(&request).expect("encode request");
        let decoded: ExtractionRequestV1 =
            sinomo_module_api::decode_sobj(&encoded).expect("decode request");
        assert_eq!(decoded, request);
    }

    #[test]
    fn incomplete_fact_batch_requires_honest_continuation() {
        let batch = DerivedFactBatchV1 {
            schema_version: MEMORY_EXTENSION_SCHEMA_VERSION_V1,
            request_id: "request.1".to_string(),
            content: content(),
            anchors: Vec::new(),
            facts: Vec::new(),
            relations: Vec::new(),
            vectors: Vec::new(),
            artifacts: Vec::new(),
            diagnostics: Vec::new(),
            complete: false,
            continuation: None,
        };

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::InvalidField {
                field: "continuation",
                ..
            })
        ));
    }

    #[test]
    fn complete_fact_batch_rejects_continuation() {
        let mut batch = batch();
        batch.continuation = Some(ExtractionContinuationV1 {
            schema_id: MEMORY_EXTRACTION_CONTINUATION_SCHEMA_ID_V1.to_string(),
            token: vec![1],
        });

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::InvalidField {
                field: "continuation",
                ..
            })
        ));
    }

    #[test]
    fn fact_batch_rejects_duplicate_artifact_ids() {
        let mut batch = batch();
        batch.artifacts = vec![
            artifact("artifact.same", "first"),
            artifact("artifact.same", "second"),
        ];

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::DuplicateId {
                field: "artifacts",
                id,
            }) if id == "artifact.same"
        ));
    }

    #[test]
    fn fact_batch_rejects_duplicate_vector_ids() {
        let mut batch = batch();
        batch.vectors = vec![
            vector("vector.same", "artifact.vector.1"),
            vector("vector.same", "artifact.vector.2"),
        ];

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::DuplicateId {
                field: "vectors",
                id,
            }) if id == "vector.same"
        ));
    }

    #[test]
    fn diagnostic_anchor_must_name_an_anchor_in_the_same_batch() {
        let mut batch = batch();
        batch.diagnostics.push(MemoryExtensionDiagnosticV1 {
            severity: MemoryExtensionDiagnosticSeverity::Warning,
            code: "extractor.warning".to_string(),
            detail: "could not decode one section".to_string(),
            anchor_id: Some("anchor.missing".into()),
        });

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::MissingEvidenceAnchor { anchor_id })
                if anchor_id == "anchor.missing"
        ));
    }

    #[test]
    fn relation_valid_time_bounds_require_valid_text() {
        let mut batch = batch();
        let mut relation = relation("relation.1");
        relation.valid_from = Some(" 2026-07-18T00:00:00Z".to_string());
        batch.relations.push(relation);

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::InvalidField {
                field: "relation.valid_from",
                ..
            })
        ));
    }

    #[test]
    fn canonical_utc_relation_valid_time_must_be_ordered() {
        let mut batch = batch();
        let mut relation = relation("relation.1");
        relation.valid_from = Some("2026-07-19T00:00:00Z".to_string());
        relation.valid_until = Some("2026-07-18T00:00:00Z".to_string());
        batch.relations.push(relation);

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::InvalidField {
                field: "relation.valid_time",
                ..
            })
        ));
    }

    #[test]
    fn relation_reference_lists_require_unique_valid_ids() {
        let mut duplicate_batch = batch();
        let mut duplicate = relation("relation.1");
        duplicate.disputed_by = vec!["relation.2".into(), "relation.2".into()];
        duplicate_batch.relations.push(duplicate);
        assert!(matches!(
            duplicate_batch.validate(),
            Err(MemoryExtensionContractError::DuplicateId {
                field: "relation.disputed_by",
                id,
            }) if id == "relation.2"
        ));

        let mut invalid_batch = batch();
        let mut invalid = relation("relation.1");
        invalid.supersedes = vec![" relation.0".into()];
        invalid_batch.relations.push(invalid);
        assert!(matches!(
            invalid_batch.validate(),
            Err(MemoryExtensionContractError::InvalidField {
                field: "relation.supersedes",
                ..
            })
        ));
    }

    #[test]
    fn relation_cannot_dispute_or_supersede_itself() {
        for (disputed_by, supersedes, field) in [
            (
                vec!["relation.1".into()],
                Vec::new(),
                "relation.disputed_by",
            ),
            (Vec::new(), vec!["relation.1".into()], "relation.supersedes"),
        ] {
            let mut batch = batch();
            let mut relation = relation("relation.1");
            relation.disputed_by = disputed_by;
            relation.supersedes = supersedes;
            batch.relations.push(relation);

            assert!(matches!(
                batch.validate(),
                Err(MemoryExtensionContractError::InvalidField {
                    field: actual_field,
                    ..
                }) if actual_field == field
            ));
        }
    }

    #[test]
    fn fact_evidence_must_name_an_anchor_in_the_same_batch() {
        let fact = NormalizedIndexFactV1 {
            fact_id: "fact.1".into(),
            kind: "text.section".to_string(),
            subject: MemoryNodeRefV1 {
                namespace: "document".to_string(),
                kind: "section".to_string(),
                id: "intro".to_string(),
            },
            value_sobj: vec![1],
            evidence_anchor_ids: vec![MemoryAnchorId::new("anchor.missing")],
            provenance: provenance(),
        };
        let batch = DerivedFactBatchV1 {
            schema_version: MEMORY_EXTENSION_SCHEMA_VERSION_V1,
            request_id: "request.1".to_string(),
            content: content(),
            anchors: Vec::new(),
            facts: vec![fact],
            relations: Vec::new(),
            vectors: Vec::new(),
            artifacts: Vec::new(),
            diagnostics: Vec::new(),
            complete: true,
            continuation: None,
        };

        assert!(matches!(
            batch.validate(),
            Err(MemoryExtensionContractError::MissingEvidenceAnchor { .. })
        ));
    }
}
