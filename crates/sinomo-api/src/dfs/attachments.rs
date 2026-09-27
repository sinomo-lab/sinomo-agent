//! Versioned file attachments. Payloads are DFS content, never cluster KV.
//!
//! The kind and schema are open, namespaced identifiers. Unknown kinds remain
//! readable without loading their producer. Installing a module grants no write
//! authority: callers use the same authenticated SIFS capability as file writes.

use super::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize};

pub const DFS_ATTACHMENT_ROUTE: &str = "/daemon/dfs/browser/attachments-v2";
pub const DFS_ATTACHMENT_COMPONENT_ROUTE: &str = "/dfs/attachments/component-v2";
pub const DFS_ATTACHMENT_COMPONENT_CAPABILITY: &str = "dfs.attachments.manage";
pub const DFS_ATTACHMENT_UPLOAD_ROUTE: &str = "/daemon/dfs/browser/attachment-upload-v2";

/// The payload travels as a semantic RPC body, not a buffered SOBJ byte array.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentUploadRequest {
    pub source: DfsAttachmentSource,
    pub metadata: DfsAttachmentMetadata,
    pub expected: Option<DfsAttachmentRevision>,
    pub operation_id: String,
    pub payload_bytes: u64,
    /// Publication requires this actor's unexpired ClaimGeneration operation.
    pub generation: bool,
}

impl DfsAttachmentUploadRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.metadata.validate()?;
        validate_attachment_operation_id(&self.operation_id)?;
        if self.source.path.len() > 4096
            || self.payload_bytes > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64
        {
            return Err("attachment upload exceeds its path or payload bound".into());
        }
        Ok(())
    }
}

pub fn validate_attachment_operation_id(operation_id: &str) -> Result<(), String> {
    if !identifier(operation_id) {
        return Err("invalid attachment operation id".into());
    }
    Ok(())
}
pub const DFS_ATTACHMENT_HEADER_BYTES: usize = 4096;
pub const DFS_ATTACHMENT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const DFS_ATTACHMENT_MAX_RANGE_BYTES: usize = 1024 * 1024;
pub const DFS_ATTACHMENT_MAX_PAGE: usize = 64;
pub const DFS_TAGS_KIND: &str = "sinomo.tags";
pub const DFS_TRANSCRIPT_KIND: &str = "sinomo.transcript";
pub const DFS_THUMBNAIL_KIND: &str = "sinomo.thumbnail";
pub const DFS_ATTACHMENT_ROOT: &str = "/.attachments/v1";
pub const DFS_ATTACHMENT_CATALOG_MAX_BYTES: usize = 1024 * 1024;
pub const DFS_ATTACHMENT_INLINE_BYTES: usize = 4 * 1024;
/// Spill additional small values to blobs once a source reaches this budget.
pub const DFS_ATTACHMENT_INLINE_BUDGET_BYTES: usize = 128 * 1024;
pub const DFS_ATTACHMENT_MAX_PER_SOURCE: usize = 1024;
pub const DFS_ATTACHMENT_MAX_PER_OWNER_PER_SOURCE: usize = 128;
pub const DFS_ATTACHMENT_MAX_OWNER_SOURCE_BYTES: u64 = 256 * 1024 * 1024;
pub const DFS_ATTACHMENT_POLICY_KEY: &str = "dfs.attachments";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct DfsAttachmentProducerPolicy {
    pub bytes_per_minute: u64,
    pub operations_per_minute: u64,
    pub max_attachments_per_source: usize,
    pub max_bytes_per_source: u64,
}
impl Default for DfsAttachmentProducerPolicy {
    fn default() -> Self {
        Self {
            bytes_per_minute: 256 * 1024 * 1024,
            operations_per_minute: 1024,
            max_attachments_per_source: DFS_ATTACHMENT_MAX_PER_OWNER_PER_SOURCE,
            max_bytes_per_source: DFS_ATTACHMENT_MAX_OWNER_SOURCE_BYTES,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct DfsAttachmentPolicy {
    pub max_queued: usize,
    pub max_queued_per_producer: usize,
    pub max_inflight_bytes: u64,
    pub default_producer: DfsAttachmentProducerPolicy,
    /// Keys are host-assigned `component:<id>` or authenticated `node:<id>`.
    pub producers: std::collections::BTreeMap<String, DfsAttachmentProducerPolicy>,
}
impl Default for DfsAttachmentPolicy {
    fn default() -> Self {
        Self {
            max_queued: 64,
            max_queued_per_producer: 4,
            max_inflight_bytes: 128 * 1024 * 1024,
            default_producer: Default::default(),
            producers: Default::default(),
        }
    }
}
impl DfsAttachmentPolicy {
    pub fn producer(&self, actor: &str) -> &DfsAttachmentProducerPolicy {
        self.producers.get(actor).unwrap_or(&self.default_producer)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.max_queued == 0
            || self.max_queued > 256
            || self.max_queued_per_producer == 0
            || self.max_queued_per_producer > self.max_queued
            || self.max_inflight_bytes == 0
            || self.max_inflight_bytes > 1024 * 1024 * 1024
            || self.producers.len() > 256
        {
            return Err("invalid attachment admission limits".into());
        }
        for (name, policy) in self
            .producers
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .chain(std::iter::once(("default", &self.default_producer)))
        {
            if name.len() > 512
                || policy.bytes_per_minute == 0
                || policy.operations_per_minute == 0
                || policy.max_attachments_per_source == 0
                || policy.max_attachments_per_source > DFS_ATTACHMENT_MAX_PER_SOURCE
                || policy.max_bytes_per_source == 0
            {
                return Err("invalid attachment producer limits".into());
            }
        }
        Ok(())
    }
}

pub fn dfs_attachment_policy_definition() -> sinomo_config::ConfigDefinition {
    use sinomo_config::*;
    let producer_fields = || {
        vec![
            ConfigSchemaField::new(
                "bytes_per_minute",
                "Upload bytes per minute",
                "Per-producer admitted upload budget on each serving node.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "operations_per_minute",
                "Operations per minute",
                "Per-producer mutation admission budget on each serving node.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "max_attachments_per_source",
                "Attachments per source",
                "Maximum retained attachment slots (including deletion receipts) per producer on one source, enforced by catalog CAS.",
                ConfigSchemaValue::u64(None),
            ),
            ConfigSchemaField::new(
                "max_bytes_per_source",
                "Bytes per source",
                "Maximum live and reserved logical payload bytes per producer on one source.",
                ConfigSchemaValue::u64(None),
            ),
        ]
    };
    ConfigDefinition::new(DFS_ATTACHMENT_POLICY_KEY, super::SINOMO_DFS_MODULE_OWNER,
        ConfigValueType::Object { schema_name: "DfsAttachmentPolicy".into() },
        "Bound and fairly schedule file attachment producers. Storage quotas apply per producer and source; work budgets apply per serving node.", ConfigClusterMode::ClusterOnly).unwrap()
        .with_default(&DfsAttachmentPolicy::default()).unwrap()
        .with_schema(ConfigSchema::object("DfsAttachmentPolicy", "File attachment budgets", vec![
            ConfigSchemaField::new("max_queued", "Queued mutations", "Total admitted attachment mutations.", ConfigSchemaValue::u64(None)),
            ConfigSchemaField::new("max_queued_per_producer", "Queued per producer", "Prevent one producer from filling the queue.", ConfigSchemaValue::u64(None)),
            ConfigSchemaField::new("max_inflight_bytes", "In-flight bytes", "Total reserved upload bytes across queued and executing mutations.", ConfigSchemaValue::u64(None)),
            ConfigSchemaField::new("default_producer", "Default producer limits", "Limits for producers without an override.", ConfigSchemaValue::object(producer_fields()).without_unknown_fields()),
            ConfigSchemaField::new("producers", "Producer overrides", "Host-assigned component or node identities, never payload producer labels.", ConfigSchemaValue::map(ConfigSchemaValue::object(producer_fields()).without_unknown_fields())),
        ])).unwrap()
        .with_module_id(super::sifs_mount_settings_module_id()).unwrap()
}

/// Fixed versioned layout over the existing SIFS metadata lanes. Object IDs
/// determine the lane, so rename does not relocate the associated catalog.
pub fn dfs_attachment_catalog_directory(source: &DfsAttachmentSource) -> String {
    let identity = format!("{}-{}", source.file_system_id, source.object_id);
    let hash = blake3::hash(identity.as_bytes());
    let prefix = match hash.as_bytes()[0] % 3 {
        0 => "/.attachments/v2",
        1 => "/services/.attachments/v2",
        _ => "/users/.attachments/v2",
    };
    let digest = hash.to_hex().to_string();
    format!("{prefix}/{}/{}/{identity}", &digest[2..4], &digest[4..6])
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentSource {
    /// Locator only. Identity is checked on every operation, including reads.
    pub path: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentKey {
    /// Reverse-domain or otherwise namespaced kind, for example org.example.ocr.
    pub kind: String,
    /// Separates languages, thumbnail sizes, or independent module outputs.
    pub name: String,
}

impl DfsAttachmentKey {
    pub fn tags() -> Self {
        Self {
            kind: DFS_TAGS_KIND.into(),
            name: "default".into(),
        }
    }

    pub fn transcript(language: &str) -> Self {
        Self {
            kind: DFS_TRANSCRIPT_KIND.into(),
            name: language.into(),
        }
    }

    pub fn thumbnail(side: u32) -> Self {
        Self {
            kind: DFS_THUMBNAIL_KIND.into(),
            name: format!("raster-v1-{side}"),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.kind.contains('.')
            || !identifier(&self.kind)
            || !identifier(&self.name)
            || self.kind.len() + self.name.len() > 240
        {
            return Err(
                "attachment kind/name must be bounded ASCII identifiers; kind must be namespaced"
                    .into(),
            );
        }
        Ok(())
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        && value != "."
        && value != ".."
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentMetadata {
    pub key: DfsAttachmentKey,
    /// Versioned schema identifier, owned by the producer (not a fetched URL).
    pub schema: String,
    pub media_type: String,
    /// None for object annotations; Some for content-derived attachments.
    pub source_version: Option<DfsVersion>,
    /// Extractor/model/configuration fingerprint, or an authored-data label.
    pub producer: String,
}

impl DfsAttachmentMetadata {
    pub fn validate(&self) -> Result<(), String> {
        self.key.validate()?;
        for (label, value, max) in [
            ("schema", &self.schema, 256),
            ("media type", &self.media_type, 128),
            ("producer", &self.producer, 512),
        ] {
            if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
                return Err(format!("invalid attachment {label}"));
            }
        }
        match self.key.kind.as_str() {
            DFS_TAGS_KIND
                if self.schema != "sinomo.tags/v1"
                    || self.media_type != "application/json"
                    || self.source_version.is_some() =>
            {
                Err("tags require sinomo.tags/v1 JSON and object scope".into())
            }
            DFS_TRANSCRIPT_KIND
                if self.schema != "sinomo.transcript/v1"
                    || self.media_type != "application/x-ndjson"
                    || self.source_version.is_none() =>
            {
                Err(
                    "transcripts require sinomo.transcript/v1 NDJSON and an exact source version"
                        .into(),
                )
            }
            DFS_THUMBNAIL_KIND
                if self.schema != "sinomo.thumbnail/v1"
                    || self.media_type != "image/png"
                    || self.source_version.is_none() =>
            {
                Err("thumbnails require sinomo.thumbnail/v1 PNG and an exact source version".into())
            }
            _ => Ok(()),
        }
    }
}

/// Identity and revision of the attachment object, independent of source bytes.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentRevision {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentDescriptor {
    pub metadata: DfsAttachmentMetadata,
    pub revision: DfsAttachmentRevision,
    /// Last committed operation, usable to reconcile an uncertain Put reply.
    pub operation_id: String,
    pub payload_bytes: u64,
    /// Stale descriptors can be replaced with CAS, but their body is not served.
    pub current: bool,
}

/// On-content header. The body follows a fixed-size header and can be range-read
/// without deserializing the entire attachment. All bytes commit as one object.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAttachmentHeader {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub metadata: DfsAttachmentMetadata,
    pub payload_bytes: u64,
    pub operation_id: String,
}

impl DfsAttachmentHeader {
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        if encoded.len() > DFS_ATTACHMENT_HEADER_BYTES - 12 {
            return Err("attachment header is too large".into());
        }
        let mut result = vec![0; DFS_ATTACHMENT_HEADER_BYTES];
        result[..8].copy_from_slice(b"SATT0001");
        result[8..12].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
        result[12..12 + encoded.len()].copy_from_slice(&encoded);
        Ok(result)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != DFS_ATTACHMENT_HEADER_BYTES || &bytes[..8] != b"SATT0001" {
            return Err("invalid attachment header".into());
        }
        let len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        if len > DFS_ATTACHMENT_HEADER_BYTES - 12 || bytes[12 + len..].iter().any(|b| *b != 0) {
            return Err("invalid attachment header length or padding".into());
        }
        let header: Self =
            serde_json::from_slice(&bytes[12..12 + len]).map_err(|e| e.to_string())?;
        header.validate()?;
        Ok(header)
    }

    fn validate(&self) -> Result<(), String> {
        self.metadata.validate()?;
        if !identifier(&self.operation_id)
            || self.payload_bytes > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES as u64
        {
            return Err("invalid attachment operation id or payload size".into());
        }
        Ok(())
    }
}

/// Bucket by a hash, not the timestamp prefix of an object ULID. No directory
/// or manifest contains one entry per file in the entire filesystem.
pub fn dfs_attachment_directory(source: &DfsAttachmentSource) -> String {
    let identity = format!("{}-{}", source.file_system_id, source.object_id);
    let digest = blake3::hash(identity.as_bytes()).to_hex().to_string();
    format!(
        "{DFS_ATTACHMENT_ROOT}/{}/{}/{identity}",
        &digest[..2],
        &digest[2..4]
    )
}

pub fn dfs_attachment_path(
    source: &DfsAttachmentSource,
    key: &DfsAttachmentKey,
) -> Result<String, String> {
    key.validate()?;
    Ok(format!(
        "{}/{}~{}.satt",
        dfs_attachment_directory(source),
        key.kind,
        key.name
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DfsAttachmentRequest {
    /// Reserve before expensive extraction; payload_bytes is the output budget.
    ClaimGeneration { upload: DfsAttachmentUploadRequest },
    Maintain {
        source: DfsAttachmentSource,
        cursor: Option<String>,
        limit: usize,
    },
    GetMany {
        source: DfsAttachmentSource,
        keys: Vec<DfsAttachmentKey>,
    },
    Get {
        source: DfsAttachmentSource,
        key: DfsAttachmentKey,
    },
    List {
        source: DfsAttachmentSource,
        cursor: Option<String>,
        limit: usize,
    },
    Read {
        source: DfsAttachmentSource,
        key: DfsAttachmentKey,
        revision: DfsAttachmentRevision,
        offset: u64,
        max_bytes: usize,
    },
    Put {
        source: DfsAttachmentSource,
        metadata: DfsAttachmentMetadata,
        expected: Option<DfsAttachmentRevision>,
        operation_id: String,
        bytes: Vec<u8>,
    },
    Delete {
        source: DfsAttachmentSource,
        key: DfsAttachmentKey,
        expected: DfsAttachmentRevision,
        operation_id: String,
    },
}

impl DfsAttachmentRequest {
    pub fn source(&self) -> &DfsAttachmentSource {
        match self {
            Self::ClaimGeneration { upload } => &upload.source,
            Self::Maintain { source, .. }
            | Self::GetMany { source, .. }
            | Self::Get { source, .. }
            | Self::List { source, .. }
            | Self::Read { source, .. }
            | Self::Put { source, .. }
            | Self::Delete { source, .. } => source,
        }
    }

    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::ClaimGeneration { .. }
                | Self::Put { .. }
                | Self::Delete { .. }
                | Self::Maintain { .. }
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
pub enum DfsAttachmentResponse {
    Generation {
        granted: bool,
        expires_at_unix_nanos: u64,
    },
    Maintenance {
        reclaimed: usize,
        next_cursor: Option<String>,
        next_due_unix_nanos: Option<u64>,
    },
    Descriptors {
        attachments: Vec<Option<DfsAttachmentDescriptor>>,
    },
    Descriptor {
        attachment: Option<DfsAttachmentDescriptor>,
    },
    Page {
        attachments: Vec<DfsAttachmentDescriptor>,
        next_cursor: Option<String>,
    },
    Data {
        bytes: Vec<u8>,
        eof: bool,
    },
    Stored {
        attachment: DfsAttachmentDescriptor,
    },
    Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileTags {
    pub tags: Vec<String>,
}

impl DfsFileTags {
    pub fn canonicalize(&mut self) -> Result<(), String> {
        if self.tags.len() > 256
            || self.tags.iter().any(|tag| {
                tag.trim().is_empty() || tag.len() > 128 || tag.chars().any(char::is_control)
            })
        {
            return Err("tags require at most 256 nonempty labels of at most 128 bytes".into());
        }
        for tag in &mut self.tags {
            *tag = tag.trim().to_string();
        }
        self.tags.sort();
        self.tags.dedup();
        Ok(())
    }

    pub fn encode(mut self) -> Result<Vec<u8>, String> {
        self.canonicalize()?;
        serde_json::to_vec(&self).map_err(|e| e.to_string())
    }
}

/// One NDJSON line. Bounded segments support incremental full-text extraction
/// and timestamp evidence without decoding a complete long transcript.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsTranscriptSegment {
    pub start_millis: u64,
    pub end_millis: u64,
    pub speaker: Option<String>,
    pub text: String,
}

impl DfsTranscriptSegment {
    pub fn validate(&self) -> Result<(), String> {
        if self.end_millis < self.start_millis
            || self.text.len() > 16 * 1024
            || self.text.trim().is_empty()
            || self
                .speaker
                .as_ref()
                .is_some_and(|s| s.len() > 128 || s.chars().any(char::is_control))
        {
            return Err("invalid transcript segment (time, text, or speaker bounds)".into());
        }
        Ok(())
    }
}

pub fn validate_dfs_attachment_payload(
    metadata: &DfsAttachmentMetadata,
    bytes: &[u8],
) -> Result<(), String> {
    metadata.validate()?;
    if bytes.len() > DFS_ATTACHMENT_MAX_PAYLOAD_BYTES {
        return Err("attachment payload exceeds 64 MiB".into());
    }
    match metadata.key.kind.as_str() {
        DFS_TAGS_KIND => {
            if bytes.len() > 64 * 1024 {
                return Err("tag payload exceeds 64 KiB".into());
            }
            let mut tags: DfsFileTags = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
            tags.canonicalize()
        }
        DFS_TRANSCRIPT_KIND => {
            let mut previous = 0;
            for line in bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
                if line.len() > 128 * 1024 {
                    return Err("transcript line exceeds 128 KiB".into());
                }
                let segment: DfsTranscriptSegment =
                    serde_json::from_slice(line).map_err(|e| e.to_string())?;
                segment.validate()?;
                if segment.start_millis < previous {
                    return Err("transcript segments must be ordered by start time".into());
                }
                previous = segment.start_millis;
            }
            Ok(())
        }
        DFS_THUMBNAIL_KIND => {
            if bytes.len() > 1024 * 1024 || !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
                return Err("thumbnail must be PNG, at most 1 MiB".into());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
