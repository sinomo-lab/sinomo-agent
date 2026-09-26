//! Bounded attachment-to-index extraction. The caller owns authorized DFS range
//! reads and checkpoints jobs against both the source and attachment revision.
//! Unknown module schemas are deliberately not interpreted as text.

use serde::{Deserialize, Serialize};
use sinomo_runtime::dfs::{
    DFS_TAGS_KIND, DFS_TRANSCRIPT_KIND, DfsAttachmentDescriptor, DfsFileTags, DfsTranscriptSegment,
};

const TRANSCRIPT_LINE_LIMIT: usize = 128 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum AttachmentIndexValue {
    Tag(String),
    Transcript {
        start_millis: u64,
        end_millis: u64,
        speaker: Option<String>,
        text: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AttachmentIndexEntry {
    /// Byte evidence within the attachment payload, not the original media.
    pub start_byte: u64,
    pub end_byte: u64,
    pub value: AttachmentIndexValue,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AttachmentIndexPage {
    pub entries: Vec<AttachmentIndexEntry>,
    pub next_offset: Option<u64>,
}

/// Extract at most 32 transcript segments or 256 bounded tags. `offset` is zero
/// or a previously returned offset for the *same attachment revision*. Every
/// read must use that revision and recheck source policy through the DFS API.
/// A completed page may be checkpointed with its index writes; failed pages
/// must not advance the durable indexing cursor.
pub fn extract_attachment_index_page(
    attachment: &DfsAttachmentDescriptor,
    offset: u64,
    mut read_range: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<AttachmentIndexPage, String> {
    attachment.metadata.validate()?;
    if !attachment.current || offset > attachment.payload_bytes {
        return Err("attachment is stale or extraction offset is invalid".into());
    }
    let mut page = AttachmentIndexPage {
        entries: Vec::new(),
        next_offset: None,
    };
    match attachment.metadata.key.kind.as_str() {
        DFS_TAGS_KIND => {
            if offset != 0 || attachment.payload_bytes > 64 * 1024 {
                return Err("invalid tag extraction bounds".into());
            }
            let bytes = read_range(0, attachment.payload_bytes as usize)?;
            if bytes.len() as u64 != attachment.payload_bytes {
                return Err("short tag read".into());
            }
            let mut tags: DfsFileTags =
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            tags.canonicalize()?;
            page.entries = tags
                .tags
                .into_iter()
                .map(|tag| AttachmentIndexEntry {
                    start_byte: 0,
                    end_byte: attachment.payload_bytes,
                    value: AttachmentIndexValue::Tag(tag),
                })
                .collect();
        }
        DFS_TRANSCRIPT_KIND => {
            let remaining = attachment.payload_bytes - offset;
            if remaining == 0 {
                return Ok(page);
            }
            let size = remaining.min(TRANSCRIPT_LINE_LIMIT as u64 + 1) as usize;
            let bytes = read_range(offset, size)?;
            if bytes.len() != size {
                return Err("short transcript read".into());
            }
            let mut consumed = 0;
            for line in bytes.split_inclusive(|b| *b == b'\n') {
                let complete = line.ends_with(b"\n")
                    || offset + consumed as u64 + line.len() as u64 == attachment.payload_bytes;
                if !complete {
                    break;
                }
                let content = line.strip_suffix(b"\n").unwrap_or(line);
                if !content.is_empty() {
                    if content.len() > TRANSCRIPT_LINE_LIMIT {
                        return Err("transcript segment exceeds extraction limit".into());
                    }
                    let segment: DfsTranscriptSegment =
                        serde_json::from_slice(content).map_err(|e| e.to_string())?;
                    segment.validate()?;
                    page.entries.push(AttachmentIndexEntry {
                        start_byte: offset + consumed as u64,
                        end_byte: offset + consumed as u64 + content.len() as u64,
                        value: AttachmentIndexValue::Transcript {
                            start_millis: segment.start_millis,
                            end_millis: segment.end_millis,
                            speaker: segment.speaker,
                            text: segment.text,
                        },
                    });
                }
                consumed += line.len();
                if page.entries.len() == 32 {
                    break;
                }
            }
            if consumed == 0 {
                return Err("transcript segment exceeds extraction limit".into());
            }
            if offset + (consumed as u64) < attachment.payload_bytes {
                page.next_offset = Some(offset + consumed as u64);
            }
        }
        _ => {}
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::dfs::*;

    #[test]
    fn attachment_transcripts_extract_bounded_exact_evidence_pages() {
        let version = DfsVersion::from_legacy_named("attachment-index-version").unwrap();
        let object_id = DfsObjectId::from_legacy_named("attachment-index-object").unwrap();
        let mut bytes = Vec::new();
        for i in 0..100 {
            serde_json::to_writer(
                &mut bytes,
                &DfsTranscriptSegment {
                    start_millis: i,
                    end_millis: i + 1,
                    speaker: None,
                    text: format!("searchable segment {i}"),
                },
            )
            .unwrap();
            bytes.push(b'\n');
        }
        let descriptor = DfsAttachmentDescriptor {
            metadata: DfsAttachmentMetadata {
                key: DfsAttachmentKey::transcript("en"),
                schema: "sinomo.transcript/v1".into(),
                media_type: "application/x-ndjson".into(),
                source_version: Some(version.clone()),
                producer: "test".into(),
            },
            revision: DfsAttachmentRevision { object_id, version },
            operation_id: "index-fixture".into(),
            payload_bytes: bytes.len() as u64,
            current: true,
        };
        let mut offset = 0;
        let mut count = 0;
        loop {
            let page = extract_attachment_index_page(&descriptor, offset, |start, len| {
                assert!(len <= TRANSCRIPT_LINE_LIMIT + 1);
                Ok(bytes[start as usize..start as usize + len].to_vec())
            })
            .unwrap();
            assert!(page.entries.len() <= 32);
            for entry in page.entries {
                let segment: DfsTranscriptSegment = serde_json::from_slice(
                    &bytes[entry.start_byte as usize..entry.end_byte as usize],
                )
                .unwrap();
                assert_eq!(segment.start_millis, count);
                count += 1;
            }
            match page.next_offset {
                Some(next) => {
                    assert!(next > offset);
                    offset = next;
                }
                None => break,
            }
        }
        assert_eq!(count, 100);
        let stale = DfsAttachmentDescriptor {
            current: false,
            ..descriptor
        };
        assert!(
            extract_attachment_index_page(&stale, 0, |_, _| panic!("must not read stale content"))
                .is_err()
        );
    }
}
