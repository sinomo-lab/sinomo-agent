//! Bind a publication read to one physical segment while the source shard is
//! admitted. Indexing, reading and decoding the bounded page run after admission.

use super::*;
use crate::{DfsMetadataJournalFrameBatch, DfsMetadataJournalFrameRecord};

#[derive(Debug)]
pub(crate) struct PinnedPublicationPage {
    root: PathBuf,
    path: PathBuf,
    file: Mutex<fs::File>,
    snapshot: JournalSegmentFileSnapshot,
    content_generation: OnceLock<u64>,
    shard: DfsMetadataShardId,
    first: u64,
    head: u64,
    max_entries: usize,
    max_bytes: usize,
    batch: OnceLock<DfsMetadataJournalFrameBatch>,
}

impl PinnedPublicationPage {
    pub(crate) fn capture(
        root: &Path,
        shard: &DfsMetadataShardId,
        after: u64,
        head: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Option<Self>> {
        if after >= head || max_entries == 0 {
            return Ok(None);
        }
        let first = after + 1;
        let path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", journal_segment_id(first)));
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        };
        let snapshot = JournalSegmentFileSnapshot::from_file(&file).map_err(|source| {
            MetadataBackendError::Io {
                path: path.clone(),
                source,
            }
        })?;
        let content_generation = OnceLock::new();
        if let Some(index) = cached_journal_segment_index(&path)
            && index.matches_file_snapshot(&snapshot)
        {
            // Holes and conflicting duplicates keep their checked recovery path.
            if Self::first_offset(&index, first).is_none() {
                return Ok(None);
            }
            content_generation.get_or_init(|| index.content_generation);
        }
        Ok(Some(Self {
            root: root.to_owned(),
            path,
            file: Mutex::new(file),
            snapshot,
            content_generation,
            shard: shard.clone(),
            first,
            head,
            max_entries,
            max_bytes,
            batch: OnceLock::new(),
        }))
    }

    fn first_offset(index: &JournalSegmentIndex, first: u64) -> Option<usize> {
        let position = index
            .entries
            .partition_point(|entry| entry.sequence < first);
        (index.sequences_monotonic && index.entries.get(position)?.sequence == first)
            .then_some(position)
    }

    /// A writer-certified append preserves earlier offsets. Truncation,
    /// replacement or loss of that evidence requires a fresh plan. The open
    /// file prevents inode reuse while the request or its ACK is outstanding.
    pub(crate) fn is_current(&self) -> Result<bool> {
        let file = match fs::File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        let snapshot = JournalSegmentFileSnapshot::from_file(&file).map_err(|source| {
            MetadataBackendError::Io {
                path: self.path.clone(),
                source,
            }
        })?;
        Ok(self.snapshot == snapshot
            || (self.snapshot.identity == snapshot.identity
                && self.snapshot.identity.supports_append_extension()
                && snapshot.file_len > self.snapshot.file_len
                && cached_journal_segment_index(&self.path).is_some_and(|index| {
                    self.content_generation.get() == Some(&index.content_generation)
                        && index.matches_file_snapshot(&snapshot)
                })))
    }

    fn selected_range(&self) -> Result<(u64, std::ops::RangeInclusive<u64>)> {
        let cached = cached_journal_segment_index(&self.path).filter(|index| {
            index.matches_file_snapshot(&self.snapshot)
                || self.content_generation.get() == Some(&index.content_generation)
        });
        let index = match cached {
            Some(index) => index,
            None => {
                // The shared offset index is built under its existing physical
                // fence. This cold reconstruction holds no foreground admission.
                let _fence = crate::metadata_lock::metadata_inspection_lock(
                    self.root.clone(),
                    crate::metadata_cursor::journal_write_lock_path(&self.root),
                )?
                .ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "publication lost its physical journal owner".into(),
                    )
                })?;
                open_indexed_journal_segment(&self.path)?.index
            }
        };
        if !index.matches_file_snapshot(&self.snapshot)
            && self.content_generation.get() != Some(&index.content_generation)
        {
            return Err(MetadataBackendError::Serde(
                "journal changed before publication range selection".into(),
            ));
        }
        self.content_generation
            .get_or_init(|| index.content_generation);
        // A cold rebuild which discovers a pruned or irregular prefix seeds
        // the offset index for the next turn's checked recovery path.
        let start = Self::first_offset(&index, self.first).ok_or_else(|| {
            MetadataBackendError::Serde("journal prefix requires checked catch-up recovery".into())
        })?;
        let mut last = self.first;
        let mut bytes = 0u64;
        for (i, offset) in index.entries[start..]
            .iter()
            .take(self.max_entries)
            .enumerate()
        {
            if offset.sequence > self.head {
                break;
            }
            if Some(offset.sequence) != self.first.checked_add(i as u64) {
                break;
            }
            let end = index
                .entries
                .get(start + i + 1)
                .map_or(index.indexed_bytes, |next| next.record_start);
            let size = end.checked_sub(offset.record_start).ok_or_else(|| {
                MetadataBackendError::Serde("invalid journal record extent".into())
            })?;
            // Preserve the single oversized entry exception so the next ACK can advance.
            if i > 0 && bytes.saturating_add(size) > self.max_bytes.max(1) as u64 {
                break;
            }
            bytes += size;
            last = offset.sequence;
        }
        Ok((index.entries[start].record_start, self.first..=last))
    }

    pub(crate) fn materialize(&self) -> Result<DfsMetadataJournalFrameBatch> {
        // Targets at the same cursor share the same one-time read.
        let mut file = self.file.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_current()? {
            return Err(MetadataBackendError::Serde(
                "journal changed before publication page read".into(),
            ));
        }
        if let Some(batch) = self.batch.get() {
            return Ok(batch.clone());
        }
        let (offset, sequences) = self.selected_range()?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|source| MetadataBackendError::Io {
                path: self.path.clone(),
                source,
            })?;
        let mut journal_frames = Vec::new();
        for sequence in sequences {
            let frame =
                read_next_segmented_journal_frame(&mut file, &self.path)?.ok_or_else(|| {
                    MetadataBackendError::Serde("publication page was truncated".into())
                })?;
            if frame.entry().sequence != sequence || frame.entry().shard_id != self.shard {
                return Err(MetadataBackendError::Serde(
                    "publication page differs from its captured range".into(),
                ));
            }
            journal_frames.push(DfsMetadataJournalFrameRecord {
                sequence,
                exact_bytes: bytes::Bytes::from_owner(frame.exact_bytes_arc()),
            });
        }
        if !self.is_current()? {
            return Err(MetadataBackendError::Serde(
                "journal changed during publication page read".into(),
            ));
        }
        let batch = DfsMetadataJournalFrameBatch {
            shard_id: self.shard.clone(),
            next_after_sequence: journal_frames
                .last()
                .expect("captured first frame")
                .sequence,
            journal_frames,
        };
        Ok(self.batch.get_or_init(|| batch).clone())
    }
}
