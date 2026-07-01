//! Metadata scan and journal batching limits.

/// Default maximum entries carried by one metadata journal batch.
pub const DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT: usize = 512;
/// Default maximum encoded bytes carried by one metadata journal batch.
pub const DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT: usize = 4 * 1024 * 1024;

pub(crate) const OBJECT_LOCAL_METADATA_PAGE_SIZE: usize = 4096;
pub(crate) const SHARD_MIGRATION_PAGE_SIZE: usize = 1024;
