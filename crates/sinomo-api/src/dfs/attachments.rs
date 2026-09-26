//! Versioned file attachments. Payloads are DFS content, never cluster KV.
//!
//! The kind and schema are open, namespaced identifiers. Unknown kinds remain
//! readable without loading their producer. Installing a module grants no write
//! authority: callers use the same authenticated SIFS capability as file writes.

use super::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize};

pub const DFS_ATTACHMENT_ROUTE: &str = "/daemon/dfs/browser/attachments-v1";
pub const DFS_ATTACHMENT_HEADER_BYTES: usize = 4096;
pub const DFS_ATTACHMENT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const DFS_ATTACHMENT_MAX_RANGE_BYTES: usize = 1024 * 1024;
pub const DFS_ATTACHMENT_MAX_PAGE: usize = 64;
pub const DFS_TAGS_KIND: &str = "sinomo.tags";
pub const DFS_TRANSCRIPT_KIND: &str = "sinomo.transcript";
pub const DFS_THUMBNAIL_KIND: &str = "sinomo.thumbnail";
pub const DFS_ATTACHMENT_ROOT: &str = "/.attachments/v1";

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
            Self::Get { source, .. }
            | Self::List { source, .. }
            | Self::Read { source, .. }
            | Self::Put { source, .. }
            | Self::Delete { source, .. } => source,
        }
    }

    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Put { .. } | Self::Delete { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
pub enum DfsAttachmentResponse {
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
