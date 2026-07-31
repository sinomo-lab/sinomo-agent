//! Authoritative, paginated metadata roots consumed by local content GC.
//!
//! Catalog pages are produced by current metadata-shard primaries at one exact
//! durable cut. A content node may merge pages only while the shard identity,
//! topology digest, journal sequence, and state fingerprint remain stable.
//! Incomplete pages deliberately carry no roots: their structured coverage is
//! a fail-closed reason to skip destructive content collection.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::metadata_error::MetadataBackendError;
use crate::metadata_io::{create_dir_all, encode_sobj, read_sobj_optional, write_sobj_atomic};
use crate::{
    DfsFileSystemConfig, DfsFileSystemId, DfsMetadataContentProtection, DfsMetadataShardConfig,
    DfsMetadataShardHeadWatermark, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
    DfsMetadataVersionIdentity,
};

pub const DFS_CONTENT_ROOT_CATALOG_FORMAT_VERSION: u32 = 2;
pub const DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION: u32 = 1;
/// Historical conservative page size retained for callers that deliberately
/// prefer the old, smaller transfer unit.
pub const DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES: usize = 256;
/// Preferred encoded size for ordinary catalog pages.
///
/// Current version/content-hash roots fit roughly 2,048 entries inside this
/// target. The hard encoded ceiling below remains authoritative when unusually
/// long roots make the entry-count estimate inaccurate.
pub const DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES: usize = 224 * 1024;
pub const DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES: usize = 256 * 1024;
pub const DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT: usize = 2048;
pub const DEFAULT_DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRIES: usize =
    DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT;
const DFS_METADATA_PREFIX_PROOF_DIGEST_PREFIX: &str = "dfs-metadata-prefix-v1:blake3:";
const DFS_CONTENT_ROOT_CATALOG_JSON_TOPOLOGY_PREFIX: &[u8] =
    b"sinomo-dfs-content-root-catalog-topology-json-v1\0";

fn catalog_error(detail: impl Into<String>) -> MetadataBackendError {
    MetadataBackendError::Serde(detail.into())
}

fn valid_fingerprint(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_state_fingerprint(value: &str) -> bool {
    value
        .strip_prefix(DFS_METADATA_PREFIX_PROOF_DIGEST_PREFIX)
        .is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct DfsContentRootCatalogShardIdentity {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub primary_node_id: NodeId,
    pub epoch: u64,
}

impl DfsContentRootCatalogShardIdentity {
    pub fn from_config(config: &DfsMetadataShardConfig) -> Self {
        Self {
            file_system_id: config.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            primary_node_id: config.primary_node_id.clone(),
            epoch: config.epoch,
        }
    }

    fn validate(&self) -> Result<(), MetadataBackendError> {
        if self.shard_id.0.trim().is_empty() {
            return Err(catalog_error(
                "DFS content-root catalog shard identity is empty",
            ));
        }
        Ok(())
    }
}

fn same_logical_shard(
    left: &DfsContentRootCatalogShardIdentity,
    right: &DfsContentRootCatalogShardIdentity,
) -> bool {
    left.file_system_id == right.file_system_id && left.shard_id == right.shard_id
}

/// Stable ordering key for one exact content-GC root.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsContentRootCatalogEntry {
    Version(DfsMetadataVersionIdentity),
    ContentHash(String),
}

impl DfsContentRootCatalogEntry {
    fn validate(&self) -> Result<(), MetadataBackendError> {
        if let Self::ContentHash(content_hash) = self
            && (content_hash.trim().is_empty() || content_hash.len() > 256)
        {
            return Err(catalog_error(
                "DFS content-root catalog content hash must contain 1..=256 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentRootCatalogCursor {
    pub journal_sequence: u64,
    pub state_fingerprint: String,
    /// Stable observation time for every page in one catalog fetch.
    ///
    /// A new first-page request receives a new value even when the durable
    /// metadata cut is unchanged; continuations must echo it exactly.
    #[serde(default)]
    pub generated_at_unix_nanos: u64,
    pub last_entry: DfsContentRootCatalogEntry,
}

impl DfsContentRootCatalogCursor {
    fn validate(&self) -> Result<(), MetadataBackendError> {
        if !valid_state_fingerprint(&self.state_fingerprint) {
            return Err(catalog_error(
                "DFS content-root catalog cursor has an invalid state fingerprint",
            ));
        }
        self.last_entry.validate()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentRootCatalogPageRequest {
    pub identity: DfsContentRootCatalogShardIdentity,
    pub expected_topology_digest: String,
    pub minimum_journal_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_state_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<DfsContentRootCatalogCursor>,
    pub max_entries: usize,
}

impl DfsContentRootCatalogPageRequest {
    pub fn validate(&self) -> Result<(), MetadataBackendError> {
        self.identity.validate()?;
        if !valid_fingerprint(&self.expected_topology_digest) {
            return Err(catalog_error(
                "DFS content-root catalog request has an invalid topology digest",
            ));
        }
        if self.max_entries == 0 || self.max_entries > DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT {
            return Err(catalog_error(format!(
                "DFS content-root catalog page limit must be 1..={DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT}"
            )));
        }
        if let Some(fingerprint) = self.minimum_state_fingerprint.as_deref()
            && !valid_state_fingerprint(fingerprint)
        {
            return Err(catalog_error(
                "DFS content-root catalog request has an invalid minimum state fingerprint",
            ));
        }
        if let Some(after) = self.after.as_ref() {
            after.validate()?;
            if after.journal_sequence != self.minimum_journal_sequence
                || self.minimum_state_fingerprint.as_deref()
                    != Some(after.state_fingerprint.as_str())
            {
                return Err(catalog_error(
                    "DFS content-root catalog cursor is not bound to the requested durable cut",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsContentRootCatalogIncompleteReason {
    NotAuthoritativePrimary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        local_node_id: Option<NodeId>,
        primary_node_id: NodeId,
    },
    UnknownShard,
    AuthorityChanged {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observed: Option<DfsContentRootCatalogShardIdentity>,
    },
    TopologyChanged {
        expected_topology_digest: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observed_topology_digest: Option<String>,
    },
    MembershipHandoffFrozen {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        handoff_id: Option<String>,
    },
    DurableCutUnavailable {
        minimum_journal_sequence: u64,
        observed_journal_sequence: u64,
    },
    StateFingerprintChanged,
    MetadataBackendUnavailable,
    TransportUnavailable {
        primary_node_id: NodeId,
        reason: String,
    },
    PageBudgetExceeded {
        max_pages: usize,
    },
    UnsupportedCatalogFormat {
        observed_format_version: u32,
    },
    NoFileSystemsConfigured,
    NoMetadataShardsConfigured,
    FileSystemWithoutMetadataShards {
        file_system_id: DfsFileSystemId,
    },
    MissingFileSystem {
        file_system_id: DfsFileSystemId,
    },
    UnexpectedFileSystem {
        file_system_id: DfsFileSystemId,
    },
    MembershipHandoffInProgress {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        handoff_id: String,
    },
    CatalogRefreshFailed {
        reason: String,
    },
    CatalogNotDurablyInstalled,
    MissingShard {
        identity: DfsContentRootCatalogShardIdentity,
    },
    UnexpectedShard {
        identity: DfsContentRootCatalogShardIdentity,
    },
    DuplicateShard {
        identity: DfsContentRootCatalogShardIdentity,
    },
    ShardIncomplete {
        identity: DfsContentRootCatalogShardIdentity,
        reasons: Vec<DfsContentRootCatalogIncompleteReason>,
    },
    CatalogExpired {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<DfsContentRootCatalogShardIdentity>,
        generated_at_unix_nanos: u64,
        now_unix_nanos: u64,
        max_age_nanos: u64,
    },
    CatalogTimestampInFuture {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<DfsContentRootCatalogShardIdentity>,
        observed_unix_nanos: u64,
        now_unix_nanos: u64,
    },
    HeadWatermarkUnavailable {
        identity: DfsContentRootCatalogShardIdentity,
    },
    HeadWatermarkIdentityMismatch {
        expected: DfsContentRootCatalogShardIdentity,
        observed: DfsContentRootCatalogShardIdentity,
    },
    CatalogBehindHeadWatermark {
        identity: DfsContentRootCatalogShardIdentity,
        catalog_journal_sequence: u64,
        watermark_journal_sequence: u64,
    },
    HeadWatermarkFingerprintChanged {
        identity: DfsContentRootCatalogShardIdentity,
        catalog_state_fingerprint: String,
        watermark_state_fingerprint: String,
    },
    MalformedCatalog {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsContentRootCatalogCoverage {
    Complete {
        prefix_proof_version: u32,
        journal_sequence: u64,
        state_fingerprint: String,
    },
    Incomplete {
        reasons: Vec<DfsContentRootCatalogIncompleteReason>,
    },
}

impl DfsContentRootCatalogCoverage {
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete { .. })
    }

    pub fn durable_cut(&self) -> Option<(u64, &str)> {
        match self {
            Self::Complete {
                journal_sequence,
                state_fingerprint,
                ..
            } => Some((*journal_sequence, state_fingerprint)),
            Self::Incomplete { .. } => None,
        }
    }

    fn validate(&self) -> Result<(), MetadataBackendError> {
        match self {
            Self::Complete {
                prefix_proof_version,
                state_fingerprint,
                ..
            } => {
                if *prefix_proof_version != DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION {
                    return Err(catalog_error(
                        "DFS content-root catalog uses an unsupported prefix proof",
                    ));
                }
                if !valid_state_fingerprint(state_fingerprint) {
                    return Err(catalog_error(
                        "DFS content-root catalog has an invalid state fingerprint",
                    ));
                }
            }
            Self::Incomplete { reasons } if reasons.is_empty() => {
                return Err(catalog_error(
                    "incomplete DFS content-root catalog coverage has no reason",
                ));
            }
            Self::Incomplete { .. } => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentRootCatalogPage {
    pub identity: DfsContentRootCatalogShardIdentity,
    pub topology_digest: String,
    pub generated_at_unix_nanos: u64,
    pub coverage: DfsContentRootCatalogCoverage,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<DfsContentRootCatalogEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsContentRootCatalogCursor>,
}

impl DfsContentRootCatalogPage {
    pub fn incomplete_for_request(
        request: &DfsContentRootCatalogPageRequest,
        reason: DfsContentRootCatalogIncompleteReason,
        generated_at_unix_nanos: u64,
    ) -> Self {
        Self {
            identity: request.identity.clone(),
            topology_digest: request.expected_topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: DfsContentRootCatalogCoverage::Incomplete {
                reasons: vec![reason],
            },
            entries: Vec::new(),
            next_cursor: None,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.coverage.is_complete()
    }

    pub fn encoded_size_bytes(&self) -> Result<usize, MetadataBackendError> {
        encode_sobj(self).map(|bytes| bytes.len())
    }

    pub fn validate_for_request(
        &self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<(), MetadataBackendError> {
        self.validate_shape_for_request(request)?;
        let encoded_size_bytes = self.encoded_size_bytes()?;
        if encoded_size_bytes > DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES {
            return Err(catalog_error(format!(
                "DFS content-root catalog page encodes to {encoded_size_bytes} bytes, exceeding the {}-byte transport budget",
                DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES
            )));
        }
        Ok(())
    }

    /// Validate the page's authority, durable cut, ordering, and cursor
    /// contract without serializing the full page again.
    ///
    /// Producers enforce the encoded byte ceiling while constructing a page,
    /// and untrusted network responses use `validate_for_request`. Internal
    /// fragment assembly can use this shape-only path after either boundary.
    pub(crate) fn validate_shape_for_request(
        &self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<(), MetadataBackendError> {
        request.validate()?;
        self.identity.validate()?;
        if self.identity != request.identity {
            return Err(catalog_error(
                "DFS content-root catalog page authority identity does not match its request",
            ));
        }
        if self.topology_digest != request.expected_topology_digest
            || !valid_fingerprint(&self.topology_digest)
        {
            return Err(catalog_error(
                "DFS content-root catalog page topology digest does not match its request",
            ));
        }
        if self.entries.len() > request.max_entries {
            return Err(catalog_error(
                "DFS content-root catalog page exceeds its requested entry bound",
            ));
        }
        for entry in &self.entries {
            entry.validate()?;
        }
        if self.entries.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(catalog_error(
                "DFS content-root catalog page entries are not strictly ordered",
            ));
        }
        if let Some(after) = request.after.as_ref()
            && self
                .entries
                .first()
                .is_some_and(|first| first <= &after.last_entry)
        {
            return Err(catalog_error(
                "DFS content-root catalog page did not advance beyond its cursor",
            ));
        }
        self.coverage.validate()?;

        match &self.coverage {
            DfsContentRootCatalogCoverage::Incomplete { reasons } => {
                if reasons.is_empty() || !self.entries.is_empty() || self.next_cursor.is_some() {
                    return Err(catalog_error(
                        "incomplete DFS content-root catalog pages must contain reasons and no roots",
                    ));
                }
            }
            DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version,
                journal_sequence,
                state_fingerprint,
            } => {
                if *prefix_proof_version != DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION
                    || *journal_sequence < request.minimum_journal_sequence
                    || !valid_state_fingerprint(state_fingerprint)
                    || (*journal_sequence == request.minimum_journal_sequence
                        && request
                            .minimum_state_fingerprint
                            .as_ref()
                            .is_some_and(|minimum| minimum != state_fingerprint))
                {
                    return Err(catalog_error(
                        "DFS content-root catalog page does not prove the requested durable cut",
                    ));
                }
                if let Some(after) = request.after.as_ref()
                    && (after.journal_sequence != *journal_sequence
                        || after.state_fingerprint != *state_fingerprint
                        || after.generated_at_unix_nanos != self.generated_at_unix_nanos)
                {
                    return Err(catalog_error(
                        "DFS content-root catalog page changed durable cut during pagination",
                    ));
                }
                if let Some(next) = self.next_cursor.as_ref() {
                    next.validate()?;
                    if next.journal_sequence != *journal_sequence
                        || next.state_fingerprint != *state_fingerprint
                        || next.generated_at_unix_nanos != self.generated_at_unix_nanos
                        || self.entries.last() != Some(&next.last_entry)
                    {
                        return Err(catalog_error(
                            "DFS content-root catalog continuation is not bound to the page proof",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentRootCatalogFragment {
    pub identity: DfsContentRootCatalogShardIdentity,
    pub topology_digest: String,
    pub generated_at_unix_nanos: u64,
    pub coverage: DfsContentRootCatalogCoverage,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<DfsContentRootCatalogEntry>,
}

/// Incrementally validates and merges one shard's catalog pages.
///
/// The builder consumes each page as it arrives, so refresh callers retain
/// only the final root vector plus the current transport page. It also owns the
/// expected continuation request, preventing callers from accidentally
/// weakening the durable-cut or pagination-cursor checks between pages.
pub(crate) struct DfsContentRootCatalogFragmentBuilder {
    initial_request: DfsContentRootCatalogPageRequest,
    request: DfsContentRootCatalogPageRequest,
    generated_at_unix_nanos: u64,
    coverage: Option<DfsContentRootCatalogCoverage>,
    entries: Vec<DfsContentRootCatalogEntry>,
    page_count: usize,
    terminated: bool,
}

impl DfsContentRootCatalogFragmentBuilder {
    pub(crate) fn new(
        initial_request: &DfsContentRootCatalogPageRequest,
    ) -> Result<Self, MetadataBackendError> {
        initial_request.validate()?;
        if initial_request.after.is_some() {
            return Err(catalog_error(
                "DFS content-root catalog fragment must start at the first page",
            ));
        }
        Ok(Self {
            initial_request: initial_request.clone(),
            request: initial_request.clone(),
            generated_at_unix_nanos: 0,
            coverage: None,
            entries: Vec::new(),
            page_count: 0,
            terminated: false,
        })
    }

    /// Consume one page and return its exact continuation request, if any.
    pub(crate) fn push_page(
        &mut self,
        page: DfsContentRootCatalogPage,
    ) -> Result<Option<DfsContentRootCatalogPageRequest>, MetadataBackendError> {
        if self.terminated {
            return Err(catalog_error(
                "DFS content-root catalog fragment contains pages after completion",
            ));
        }
        page.validate_shape_for_request(&self.request)?;
        self.page_count = self.page_count.saturating_add(1);
        self.generated_at_unix_nanos = self
            .generated_at_unix_nanos
            .max(page.generated_at_unix_nanos);

        let DfsContentRootCatalogPage {
            coverage,
            entries,
            next_cursor,
            ..
        } = page;
        let page_complete = coverage.is_complete();
        self.coverage = Some(coverage);
        if !page_complete {
            self.entries.clear();
            self.terminated = true;
            return Ok(None);
        }
        if self
            .entries
            .last()
            .zip(entries.first())
            .is_some_and(|(previous, next)| previous >= next)
        {
            return Err(catalog_error(
                "DFS content-root catalog pages are not globally ordered",
            ));
        }
        self.entries.extend(entries);

        let Some(next) = next_cursor else {
            self.terminated = true;
            return Ok(None);
        };
        self.request.minimum_journal_sequence = next.journal_sequence;
        self.request.minimum_state_fingerprint = Some(next.state_fingerprint.clone());
        self.request.after = Some(next);
        Ok(Some(self.request.clone()))
    }

    pub(crate) fn finish(self) -> Result<DfsContentRootCatalogFragment, MetadataBackendError> {
        if self.page_count == 0 {
            return Err(catalog_error(
                "DFS content-root catalog fragment must contain at least one page",
            ));
        }
        if !self.terminated {
            return Err(catalog_error(
                "DFS content-root catalog fragment ended before its continuation",
            ));
        }
        Ok(DfsContentRootCatalogFragment {
            identity: self.initial_request.identity,
            topology_digest: self.initial_request.expected_topology_digest,
            generated_at_unix_nanos: self.generated_at_unix_nanos,
            coverage: self.coverage.expect("a consumed page has coverage"),
            entries: self.entries,
        })
    }
}

impl DfsContentRootCatalogFragment {
    pub fn from_pages(
        initial_request: &DfsContentRootCatalogPageRequest,
        pages: &[DfsContentRootCatalogPage],
    ) -> Result<Self, MetadataBackendError> {
        let mut builder = DfsContentRootCatalogFragmentBuilder::new(initial_request)?;
        for page in pages {
            builder.push_page(page.clone())?;
        }
        builder.finish()
    }

    pub fn is_complete(&self) -> bool {
        self.coverage.is_complete()
    }

    pub fn validate(&self) -> Result<(), MetadataBackendError> {
        self.identity.validate()?;
        if !valid_fingerprint(&self.topology_digest) {
            return Err(catalog_error(
                "DFS content-root catalog fragment has an invalid topology digest",
            ));
        }
        self.coverage.validate()?;
        for entry in &self.entries {
            entry.validate()?;
        }
        if self.entries.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(catalog_error(
                "DFS content-root catalog fragment entries are not strictly ordered",
            ));
        }
        if !self.is_complete() && !self.entries.is_empty() {
            return Err(catalog_error(
                "incomplete DFS content-root catalog fragments must contain no roots",
            ));
        }
        Ok(())
    }
}

/// Fail-closed authorization result consumed by local destructive content GC.
///
/// `collection_authorized` is redundant with `protection.is_some()` on
/// purpose: it keeps serialized maintenance reports explicit and makes an
/// accidentally default-constructed value deny collection.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsContentRootCatalogAuthorizationSeal;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentRootCatalogCollectionValidation {
    #[serde(default)]
    collection_authorized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protection: Option<DfsMetadataContentProtection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    reasons: Vec<DfsContentRootCatalogIncompleteReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    catalog_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    catalog_generated_at_unix_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    catalog_installed_at_unix_nanos: Option<u64>,
    #[serde(default)]
    covered_shard_count: usize,
    #[serde(default)]
    expected_shard_count: usize,
    #[serde(default)]
    catalog_age_nanos: u64,
    #[serde(default)]
    max_journal_lag_entries: u64,
    /// In-process capability proving that the fields above came from a
    /// successful validation of one complete, current durable catalog.
    ///
    /// The seal is deliberately absent from the wire representation. A
    /// serialized maintenance report remains useful for diagnostics, but
    /// deserializing it can never recreate permission to delete content.
    #[serde(skip)]
    authorization_seal: Option<DfsContentRootCatalogAuthorizationSeal>,
}

impl DfsContentRootCatalogCollectionValidation {
    pub fn incomplete(reasons: Vec<DfsContentRootCatalogIncompleteReason>) -> Self {
        Self::denied(reasons, 0)
    }

    pub fn no_metadata_shards_configured() -> Self {
        Self::incomplete(vec![
            DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured,
        ])
    }

    pub fn incomplete_for_expected_shards(
        reasons: Vec<DfsContentRootCatalogIncompleteReason>,
        expected_shard_count: usize,
    ) -> Self {
        let mut validation = Self::denied(reasons, 0);
        validation.expected_shard_count = expected_shard_count;
        validation
    }

    /// Whether this value carries the non-serialized capability created by a
    /// successful in-process catalog validation.
    pub fn is_collection_authorized(&self) -> bool {
        self.collection_authorized
            && self.authorization_seal.is_some()
            && self.protection.is_some()
            && self.catalog_generation.is_some()
            && self.catalog_generated_at_unix_nanos.is_some()
            && self.catalog_installed_at_unix_nanos.is_some()
    }

    /// Protected metadata roots, available only while the authorization seal
    /// is present.
    pub fn protection(&self) -> Option<&DfsMetadataContentProtection> {
        if self.is_collection_authorized() {
            self.protection.as_ref()
        } else {
            None
        }
    }

    /// Validated catalog generation, available only while the authorization
    /// seal is present.
    pub fn catalog_generation(&self) -> Option<&str> {
        if self.is_collection_authorized() {
            self.catalog_generation.as_deref()
        } else {
            None
        }
    }

    /// Validated remote catalog generation time, available only while the
    /// authorization seal is present.
    ///
    /// This time remains freshness and age evidence. Local causal ordering
    /// against content-store state uses [`Self::catalog_installed_at_unix_nanos`].
    pub fn catalog_generated_at_unix_nanos(&self) -> Option<u64> {
        if self.is_collection_authorized() {
            self.catalog_generated_at_unix_nanos
        } else {
            None
        }
    }

    /// Durable local catalog-installation time, available only while the
    /// authorization seal is present.
    ///
    /// Unlike the remote generation time, this is local causal evidence that
    /// the installed catalog observation happened after an earlier local
    /// content reservation.
    pub fn catalog_installed_at_unix_nanos(&self) -> Option<u64> {
        if self.is_collection_authorized() {
            self.catalog_installed_at_unix_nanos
        } else {
            None
        }
    }

    pub fn reasons(&self) -> &[DfsContentRootCatalogIncompleteReason] {
        &self.reasons
    }

    pub fn covered_shard_count(&self) -> usize {
        self.covered_shard_count
    }

    pub fn expected_shard_count(&self) -> usize {
        self.expected_shard_count
    }

    pub fn catalog_age_nanos(&self) -> u64 {
        self.catalog_age_nanos
    }

    pub fn max_journal_lag_entries(&self) -> u64 {
        self.max_journal_lag_entries
    }

    fn denied(
        reasons: Vec<DfsContentRootCatalogIncompleteReason>,
        max_journal_lag_entries: u64,
    ) -> Self {
        Self {
            collection_authorized: false,
            protection: None,
            reasons,
            catalog_generation: None,
            catalog_generated_at_unix_nanos: None,
            catalog_installed_at_unix_nanos: None,
            covered_shard_count: 0,
            expected_shard_count: 0,
            catalog_age_nanos: 0,
            max_journal_lag_entries,
            authorization_seal: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsContentRootCatalogDurableInstallSeal;

const DFS_CONTENT_ROOT_CATALOG_STORAGE_FORMAT_VERSION: u32 = 1;

/// Catalogs can carry several differently shaped structured incomplete
/// reasons. Persist the full value as one JSON payload inside a stable SOBJ
/// envelope so the SOBJ type lattice never has to flatten those variants (or
/// infer element types for an intentionally empty topology).
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsContentRootCatalogDurableEnvelope {
    storage_format_version: u32,
    catalog_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentRootCatalog {
    format_version: u32,
    topology_digest: String,
    /// Positive filesystem coverage proof for every namespace sharing the
    /// local content store. A filesystem with no shard is represented here
    /// and in `incomplete_reasons`; it is never silently omitted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    expected_file_system_ids: Vec<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    expected_identities: Vec<DfsContentRootCatalogShardIdentity>,
    generated_at_unix_nanos: u64,
    /// Local durable-publication time. It is intentionally excluded from the
    /// generation fingerprint, which identifies the authoritative observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_at_unix_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    incomplete_reasons: Vec<DfsContentRootCatalogIncompleteReason>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fragments: Vec<DfsContentRootCatalogFragment>,
    /// Local capability proving that this exact catalog image was atomically
    /// installed in or loaded from the durable catalog store.
    ///
    /// The seal is never serialized. Constructing or deserializing a catalog
    /// therefore yields diagnostic data, not permission to delete content.
    #[serde(skip)]
    durable_install_seal: Option<DfsContentRootCatalogDurableInstallSeal>,
}

impl DfsContentRootCatalog {
    pub fn from_fragments(
        expected_topology_digest: String,
        expected_identities: Vec<DfsContentRootCatalogShardIdentity>,
        generated_at_unix_nanos: u64,
        fragments: Vec<DfsContentRootCatalogFragment>,
    ) -> Result<Self, MetadataBackendError> {
        let mut expected_file_system_ids = expected_identities
            .iter()
            .map(|identity| identity.file_system_id.clone())
            .collect::<Vec<_>>();
        expected_file_system_ids.sort();
        expected_file_system_ids.dedup();
        Self::from_topology_fragments(
            expected_topology_digest,
            expected_file_system_ids,
            expected_identities,
            generated_at_unix_nanos,
            Vec::new(),
            fragments,
        )
    }

    pub fn from_topology_fragments(
        expected_topology_digest: String,
        mut expected_file_system_ids: Vec<DfsFileSystemId>,
        mut expected_identities: Vec<DfsContentRootCatalogShardIdentity>,
        generated_at_unix_nanos: u64,
        mut incomplete_reasons: Vec<DfsContentRootCatalogIncompleteReason>,
        mut fragments: Vec<DfsContentRootCatalogFragment>,
    ) -> Result<Self, MetadataBackendError> {
        if !valid_fingerprint(&expected_topology_digest) {
            return Err(catalog_error(
                "DFS content-root catalog has an invalid topology digest",
            ));
        }
        expected_file_system_ids.sort();
        if expected_file_system_ids
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(catalog_error(
                "DFS content-root catalog expected filesystem identities must be unique",
            ));
        }
        expected_identities.sort();
        if expected_identities
            .windows(2)
            .any(|pair| same_logical_shard(&pair[0], &pair[1]))
        {
            return Err(catalog_error(
                "DFS content-root catalog expected logical shard identities must be unique",
            ));
        }
        for identity in &expected_identities {
            identity.validate()?;
            if !expected_file_system_ids.contains(&identity.file_system_id) {
                return Err(catalog_error(
                    "DFS content-root catalog shard belongs to an unexpected filesystem",
                ));
            }
        }
        if expected_file_system_ids.is_empty()
            && !incomplete_reasons.iter().any(|reason| {
                matches!(
                    reason,
                    DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured
                )
            })
        {
            incomplete_reasons.push(DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured);
        }
        for file_system_id in &expected_file_system_ids {
            if !expected_identities
                .iter()
                .any(|identity| &identity.file_system_id == file_system_id)
                && !incomplete_reasons.iter().any(|reason| {
                    matches!(
                        reason,
                        DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                            file_system_id: observed,
                        } if observed == file_system_id
                    )
                })
            {
                incomplete_reasons.push(
                    DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                        file_system_id: file_system_id.clone(),
                    },
                );
            }
        }
        fragments.sort_by(|left, right| left.identity.cmp(&right.identity));
        if fragments
            .windows(2)
            .any(|pair| same_logical_shard(&pair[0].identity, &pair[1].identity))
        {
            return Err(catalog_error(
                "DFS content-root catalog contains duplicate logical shard fragments",
            ));
        }
        for fragment in &fragments {
            fragment.validate()?;
            if fragment.topology_digest != expected_topology_digest
                || !expected_identities.contains(&fragment.identity)
            {
                return Err(catalog_error(
                    "DFS content-root catalog fragment does not match expected coverage",
                ));
            }
        }
        let catalog = Self {
            format_version: DFS_CONTENT_ROOT_CATALOG_FORMAT_VERSION,
            topology_digest: expected_topology_digest,
            expected_file_system_ids,
            expected_identities,
            generated_at_unix_nanos,
            installed_at_unix_nanos: None,
            incomplete_reasons,
            fragments,
            durable_install_seal: None,
        };
        catalog.validate_structure()?;
        Ok(catalog)
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn topology_digest(&self) -> &str {
        &self.topology_digest
    }

    pub fn expected_file_system_ids(&self) -> &[DfsFileSystemId] {
        &self.expected_file_system_ids
    }

    pub fn expected_identities(&self) -> &[DfsContentRootCatalogShardIdentity] {
        &self.expected_identities
    }

    pub fn generated_at_unix_nanos(&self) -> u64 {
        self.generated_at_unix_nanos
    }

    pub fn installed_at_unix_nanos(&self) -> Option<u64> {
        self.installed_at_unix_nanos
    }

    pub fn incomplete_reasons(&self) -> &[DfsContentRootCatalogIncompleteReason] {
        &self.incomplete_reasons
    }

    pub fn fragments(&self) -> &[DfsContentRootCatalogFragment] {
        &self.fragments
    }

    pub fn is_durably_installed(&self) -> bool {
        self.installed_at_unix_nanos.is_some() && self.durable_install_seal.is_some()
    }

    pub fn is_complete(&self) -> bool {
        self.validate_structure().is_ok()
            && !self.expected_file_system_ids.is_empty()
            && self.incomplete_reasons.is_empty()
            && self.expected_file_system_ids.iter().all(|file_system_id| {
                self.expected_identities
                    .iter()
                    .any(|identity| &identity.file_system_id == file_system_id)
            })
            && self.fragments.len() == self.expected_identities.len()
            && self
                .expected_identities
                .iter()
                .zip(&self.fragments)
                .all(|(expected, fragment)| {
                    expected == &fragment.identity && fragment.is_complete()
                })
    }

    /// Stable identity for one complete authoritative observation.
    ///
    /// A newly fetched observation has a different generation even when its
    /// shard heads have not advanced, which lets the content store require two
    /// independent complete observations before deleting a first-absent item.
    pub fn generation_fingerprint(&self) -> Result<String, MetadataBackendError> {
        if !self.is_complete() {
            return Err(catalog_error(
                "incomplete DFS content-root catalog has no generation fingerprint",
            ));
        }
        self.generation_fingerprint_assuming_complete()
    }

    fn generation_fingerprint_assuming_complete(&self) -> Result<String, MetadataBackendError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"sinomo-dfs-content-root-catalog-generation\0v1\0");
        hasher.update(&DFS_CONTENT_ROOT_CATALOG_FORMAT_VERSION.to_be_bytes());
        update_generation_hash(&mut hasher, self.topology_digest.as_bytes());
        hasher.update(&self.generated_at_unix_nanos.to_be_bytes());
        hasher.update(&(self.expected_file_system_ids.len() as u64).to_be_bytes());
        for file_system_id in &self.expected_file_system_ids {
            update_generation_hash(&mut hasher, &encode_sobj(file_system_id)?);
        }
        hasher.update(&(self.fragments.len() as u64).to_be_bytes());
        for fragment in &self.fragments {
            update_generation_hash(&mut hasher, &encode_sobj(&fragment.identity)?);
            let (journal_sequence, state_fingerprint) = fragment
                .coverage
                .durable_cut()
                .expect("complete fragments have a durable cut");
            hasher.update(&journal_sequence.to_be_bytes());
            update_generation_hash(&mut hasher, state_fingerprint.as_bytes());
        }
        Ok(hasher.finalize().to_hex().to_string())
    }

    fn protection_assuming_complete(&self) -> DfsMetadataContentProtection {
        let mut versions = BTreeSet::new();
        let mut content_hashes = BTreeSet::new();
        for entry in self.fragments.iter().flat_map(|fragment| &fragment.entries) {
            match entry {
                DfsContentRootCatalogEntry::Version(identity) => {
                    versions.insert(identity.clone());
                }
                DfsContentRootCatalogEntry::ContentHash(content_hash) => {
                    content_hashes.insert(content_hash.clone());
                }
            }
        }
        DfsMetadataContentProtection {
            versions: versions.into_iter().collect(),
            content_hashes: content_hashes.into_iter().collect(),
        }
    }

    pub fn protected_versions(&self) -> Option<BTreeSet<DfsMetadataVersionIdentity>> {
        self.is_complete().then(|| {
            self.fragments
                .iter()
                .flat_map(|fragment| &fragment.entries)
                .filter_map(|entry| match entry {
                    DfsContentRootCatalogEntry::Version(identity) => Some(identity.clone()),
                    DfsContentRootCatalogEntry::ContentHash(_) => None,
                })
                .collect()
        })
    }

    pub fn protected_version_pairs(&self) -> Option<BTreeSet<(String, String)>> {
        self.is_complete().then(|| {
            self.fragments
                .iter()
                .flat_map(|fragment| &fragment.entries)
                .filter_map(|entry| match entry {
                    DfsContentRootCatalogEntry::Version(identity) => Some((
                        identity.object_id.as_str().to_string(),
                        identity.version.as_str().to_string(),
                    )),
                    DfsContentRootCatalogEntry::ContentHash(_) => None,
                })
                .collect()
        })
    }

    pub fn protected_content_hashes(&self) -> Option<BTreeSet<String>> {
        self.is_complete().then(|| {
            self.fragments
                .iter()
                .flat_map(|fragment| &fragment.entries)
                .filter_map(|entry| match entry {
                    DfsContentRootCatalogEntry::ContentHash(content_hash) => {
                        Some(content_hash.clone())
                    }
                    DfsContentRootCatalogEntry::Version(_) => None,
                })
                .collect()
        })
    }

    /// Validate this durable catalog against the current configured topology
    /// and the cluster's durable shard-head fences. Missing head watermarks are
    /// fail-closed on this production entry point.
    pub fn validate_for_collection(
        &self,
        expected_file_systems: &[DfsFileSystemConfig],
        expected_shards: &[DfsMetadataShardConfig],
        pending_handoffs: &[DfsMetadataShardMembershipHandoff],
        head_watermarks: &[DfsMetadataShardHeadWatermark],
        now_unix_nanos: u64,
        max_age: Duration,
    ) -> Result<DfsContentRootCatalogCollectionValidation, MetadataBackendError> {
        self.validate_for_collection_with_head_watermark_policy(
            expected_file_systems,
            expected_shards,
            pending_handoffs,
            head_watermarks,
            true,
            now_unix_nanos,
            max_age,
        )
    }

    /// Validation variant used by explicit diagnostic/dry-run callers that may
    /// choose to observe missing head watermarks without making them mandatory.
    /// Any watermark that is present is still checked exactly.
    pub fn validate_for_collection_with_head_watermark_policy(
        &self,
        expected_file_systems: &[DfsFileSystemConfig],
        expected_shards: &[DfsMetadataShardConfig],
        pending_handoffs: &[DfsMetadataShardMembershipHandoff],
        head_watermarks: &[DfsMetadataShardHeadWatermark],
        require_head_watermarks: bool,
        now_unix_nanos: u64,
        max_age: Duration,
    ) -> Result<DfsContentRootCatalogCollectionValidation, MetadataBackendError> {
        let expected_topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
            expected_file_systems,
            expected_shards,
        )?;
        let expected_file_system_ids = expected_file_systems
            .iter()
            .map(|config| config.file_system_id.clone())
            .collect::<Vec<_>>();
        let expected_identities = expected_shards
            .iter()
            .map(DfsContentRootCatalogShardIdentity::from_config)
            .collect::<Vec<_>>();
        self.validate_for_collection_identities(
            &expected_file_system_ids,
            &expected_identities,
            &expected_topology_digest,
            pending_handoffs,
            head_watermarks,
            require_head_watermarks,
            now_unix_nanos,
            max_age,
        )
    }

    pub fn validate_for_collection_identities(
        &self,
        expected_file_system_ids: &[DfsFileSystemId],
        expected_identities: &[DfsContentRootCatalogShardIdentity],
        expected_topology_digest: &str,
        pending_handoffs: &[DfsMetadataShardMembershipHandoff],
        head_watermarks: &[DfsMetadataShardHeadWatermark],
        require_head_watermarks: bool,
        now_unix_nanos: u64,
        max_age: Duration,
    ) -> Result<DfsContentRootCatalogCollectionValidation, MetadataBackendError> {
        let mut expected_file_system_ids = expected_file_system_ids.to_vec();
        expected_file_system_ids.sort();
        let mut expected_identities = expected_identities.to_vec();
        expected_identities.sort();
        if expected_file_system_ids.is_empty() || expected_identities.is_empty() {
            let mut reasons = Vec::new();
            if expected_file_system_ids.is_empty() {
                reasons.push(DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured);
            }
            if expected_identities.is_empty() {
                reasons.push(DfsContentRootCatalogIncompleteReason::NoMetadataShardsConfigured);
            }
            for file_system_id in &expected_file_system_ids {
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                        file_system_id: file_system_id.clone(),
                    },
                );
            }
            let mut validation = DfsContentRootCatalogCollectionValidation::denied(reasons, 0);
            validation.catalog_age_nanos =
                now_unix_nanos.saturating_sub(self.generated_at_unix_nanos);
            return Ok(validation);
        }
        if expected_file_system_ids
            .windows(2)
            .any(|pair| pair[0] == pair[1])
            || expected_identities
                .windows(2)
                .any(|pair| same_logical_shard(&pair[0], &pair[1]))
            || !valid_fingerprint(expected_topology_digest)
        {
            return Err(catalog_error(
                "DFS content-root catalog collection expectation is invalid",
            ));
        }
        for identity in &expected_identities {
            identity.validate()?;
            if !expected_file_system_ids.contains(&identity.file_system_id) {
                return Err(catalog_error(
                    "DFS content-root catalog collection shard belongs to an unexpected filesystem",
                ));
            }
        }
        let mut seen_watermark_shards = BTreeSet::new();
        if head_watermarks.iter().any(|watermark| {
            !seen_watermark_shards
                .insert((watermark.file_system_id.clone(), watermark.shard_id.clone()))
        }) {
            return Err(catalog_error(
                "DFS content-root catalog collection head watermarks contain duplicate logical shards",
            ));
        }

        let mut reasons = Vec::new();
        if let Err(error) = self.validate_structure() {
            reasons.push(DfsContentRootCatalogIncompleteReason::MalformedCatalog {
                reason: error.to_string(),
            });
            let mut validation = DfsContentRootCatalogCollectionValidation::denied(reasons, 0);
            validation.expected_shard_count = expected_identities.len();
            validation.catalog_age_nanos =
                now_unix_nanos.saturating_sub(self.generated_at_unix_nanos);
            return Ok(validation);
        }
        reasons.extend(self.incomplete_reasons.iter().cloned());
        for file_system_id in &expected_file_system_ids {
            if !self.expected_file_system_ids.contains(file_system_id) {
                reasons.push(DfsContentRootCatalogIncompleteReason::MissingFileSystem {
                    file_system_id: file_system_id.clone(),
                });
            }
            if !expected_identities
                .iter()
                .any(|identity| &identity.file_system_id == file_system_id)
            {
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                        file_system_id: file_system_id.clone(),
                    },
                );
            }
        }
        for file_system_id in &self.expected_file_system_ids {
            if !expected_file_system_ids.contains(file_system_id) {
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::UnexpectedFileSystem {
                        file_system_id: file_system_id.clone(),
                    },
                );
            }
        }
        for handoff in pending_handoffs {
            reasons.push(
                DfsContentRootCatalogIncompleteReason::MembershipHandoffInProgress {
                    file_system_id: handoff.retiring_shard.file_system_id.clone(),
                    shard_id: DfsMetadataShardId::new(handoff.retiring_shard.shard_id.clone()),
                    handoff_id: handoff.handoff_id.clone(),
                },
            );
        }
        let expected_shard_count = expected_identities.len();
        let covered_shard_count = expected_identities
            .iter()
            .filter(|identity| {
                self.fragments
                    .iter()
                    .any(|fragment| &fragment.identity == *identity && fragment.is_complete())
            })
            .count();
        let oldest_generated_at_unix_nanos = self
            .fragments
            .iter()
            .map(|fragment| fragment.generated_at_unix_nanos)
            .chain(std::iter::once(self.generated_at_unix_nanos))
            .min()
            .unwrap_or(self.generated_at_unix_nanos);
        let catalog_age_nanos = now_unix_nanos.saturating_sub(oldest_generated_at_unix_nanos);
        if self.topology_digest != expected_topology_digest {
            reasons.push(DfsContentRootCatalogIncompleteReason::TopologyChanged {
                expected_topology_digest: expected_topology_digest.to_string(),
                observed_topology_digest: Some(self.topology_digest.clone()),
            });
        }
        for identity in &expected_identities {
            let fragment = self
                .fragments
                .iter()
                .find(|fragment| &fragment.identity == identity);
            match fragment {
                None => reasons.push(DfsContentRootCatalogIncompleteReason::MissingShard {
                    identity: identity.clone(),
                }),
                Some(fragment) => {
                    if let DfsContentRootCatalogCoverage::Incomplete {
                        reasons: fragment_reasons,
                    } = &fragment.coverage
                    {
                        reasons.push(DfsContentRootCatalogIncompleteReason::ShardIncomplete {
                            identity: identity.clone(),
                            reasons: fragment_reasons.clone(),
                        });
                    }
                }
            }
        }
        for identity in &self.expected_identities {
            if !expected_identities.contains(identity) {
                reasons.push(DfsContentRootCatalogIncompleteReason::UnexpectedShard {
                    identity: identity.clone(),
                });
            }
        }

        let max_age_nanos = max_age.as_nanos().min(u64::MAX as u128) as u64;
        append_catalog_age_reason(
            &mut reasons,
            None,
            self.generated_at_unix_nanos,
            now_unix_nanos,
            max_age_nanos,
        );
        match (
            self.installed_at_unix_nanos,
            self.durable_install_seal.as_ref(),
        ) {
            (Some(installed_at_unix_nanos), Some(_)) => append_catalog_age_reason(
                &mut reasons,
                None,
                installed_at_unix_nanos,
                now_unix_nanos,
                max_age_nanos,
            ),
            (Some(_), None) | (None, _) => {
                reasons.push(DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled)
            }
        }
        for fragment in &self.fragments {
            append_catalog_age_reason(
                &mut reasons,
                Some(fragment.identity.clone()),
                fragment.generated_at_unix_nanos,
                now_unix_nanos,
                max_age_nanos,
            );
        }

        let mut max_journal_lag_entries = 0_u64;
        for identity in &expected_identities {
            let matching = head_watermarks
                .iter()
                .filter(|watermark| {
                    watermark.file_system_id == identity.file_system_id
                        && watermark.shard_id == identity.shard_id.0
                })
                .collect::<Vec<_>>();
            let conflicting = matching.iter().copied().find(|watermark| {
                watermark.epoch > identity.epoch
                    || (watermark.epoch == identity.epoch
                        && watermark.primary_node_id != identity.primary_node_id)
            });
            if let Some(conflicting) = conflicting {
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::HeadWatermarkIdentityMismatch {
                        expected: identity.clone(),
                        observed: watermark_identity(conflicting),
                    },
                );
            }
            let exact = matching
                .iter()
                .copied()
                .find(|watermark| watermark_identity(watermark) == *identity);
            let Some(watermark) = exact else {
                if require_head_watermarks && conflicting.is_none() {
                    if let Some(observed) = matching.first() {
                        reasons.push(
                            DfsContentRootCatalogIncompleteReason::HeadWatermarkIdentityMismatch {
                                expected: identity.clone(),
                                observed: watermark_identity(observed),
                            },
                        );
                    } else {
                        reasons.push(
                            DfsContentRootCatalogIncompleteReason::HeadWatermarkUnavailable {
                                identity: identity.clone(),
                            },
                        );
                    }
                }
                continue;
            };
            let Some(fragment) = self
                .fragments
                .iter()
                .find(|fragment| fragment.identity == *identity)
            else {
                continue;
            };
            let Some((catalog_sequence, catalog_fingerprint)) = fragment.coverage.durable_cut()
            else {
                continue;
            };
            if catalog_sequence < watermark.journal_sequence {
                max_journal_lag_entries = max_journal_lag_entries
                    .max(watermark.journal_sequence.saturating_sub(catalog_sequence));
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::CatalogBehindHeadWatermark {
                        identity: identity.clone(),
                        catalog_journal_sequence: catalog_sequence,
                        watermark_journal_sequence: watermark.journal_sequence,
                    },
                );
                continue;
            }
            if catalog_sequence == watermark.journal_sequence
                && catalog_fingerprint != watermark.state_fingerprint
            {
                reasons.push(
                    DfsContentRootCatalogIncompleteReason::HeadWatermarkFingerprintChanged {
                        identity: identity.clone(),
                        catalog_state_fingerprint: catalog_fingerprint.to_string(),
                        watermark_state_fingerprint: watermark.state_fingerprint.clone(),
                    },
                );
            }
        }

        if !reasons.is_empty() {
            let mut validation =
                DfsContentRootCatalogCollectionValidation::denied(reasons, max_journal_lag_entries);
            validation.covered_shard_count = covered_shard_count;
            validation.expected_shard_count = expected_shard_count;
            validation.catalog_age_nanos = catalog_age_nanos;
            return Ok(validation);
        }
        let protection = self.protection_assuming_complete();
        Ok(DfsContentRootCatalogCollectionValidation {
            collection_authorized: true,
            protection: Some(protection),
            reasons: Vec::new(),
            catalog_generation: Some(self.generation_fingerprint_assuming_complete()?),
            catalog_generated_at_unix_nanos: Some(self.generated_at_unix_nanos),
            catalog_installed_at_unix_nanos: self.installed_at_unix_nanos,
            covered_shard_count,
            expected_shard_count,
            catalog_age_nanos,
            max_journal_lag_entries,
            authorization_seal: Some(DfsContentRootCatalogAuthorizationSeal),
        })
    }

    fn validate_structure(&self) -> Result<(), MetadataBackendError> {
        if self.format_version != DFS_CONTENT_ROOT_CATALOG_FORMAT_VERSION {
            return Err(catalog_error(format!(
                "unsupported DFS content-root catalog format {}",
                self.format_version
            )));
        }
        if !valid_fingerprint(&self.topology_digest) {
            return Err(catalog_error(
                "DFS content-root catalog has an invalid topology digest",
            ));
        }
        if self
            .expected_file_system_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(catalog_error(
                "DFS content-root catalog expected filesystem identities are not canonical",
            ));
        }
        if self.expected_file_system_ids.is_empty()
            && !self.incomplete_reasons.iter().any(|reason| {
                matches!(
                    reason,
                    DfsContentRootCatalogIncompleteReason::NoFileSystemsConfigured
                )
            })
        {
            return Err(catalog_error(
                "DFS content-root catalog has no positive filesystem coverage or incomplete proof",
            ));
        }
        if self
            .expected_identities
            .windows(2)
            .any(|pair| pair[0] >= pair[1] || same_logical_shard(&pair[0], &pair[1]))
        {
            return Err(catalog_error(
                "DFS content-root catalog expected shard identities are not canonical",
            ));
        }
        if self.expected_identities.is_empty() && self.incomplete_reasons.is_empty() {
            return Err(catalog_error(
                "DFS content-root catalog has no shard coverage or incomplete proof",
            ));
        }
        for identity in &self.expected_identities {
            identity.validate()?;
            if !self
                .expected_file_system_ids
                .contains(&identity.file_system_id)
            {
                return Err(catalog_error(
                    "DFS content-root catalog shard belongs to an unexpected filesystem",
                ));
            }
        }
        if self.fragments.windows(2).any(|pair| {
            pair[0].identity >= pair[1].identity
                || same_logical_shard(&pair[0].identity, &pair[1].identity)
        }) {
            return Err(catalog_error(
                "DFS content-root catalog shard fragments are not canonical",
            ));
        }
        for fragment in &self.fragments {
            fragment.validate()?;
            if fragment.topology_digest != self.topology_digest
                || !self.expected_identities.contains(&fragment.identity)
            {
                return Err(catalog_error(
                    "DFS content-root catalog fragment does not match its catalog",
                ));
            }
        }
        if let Some(oldest_fragment_timestamp) = self
            .fragments
            .iter()
            .map(|fragment| fragment.generated_at_unix_nanos)
            .min()
            && self.generated_at_unix_nanos != oldest_fragment_timestamp
        {
            return Err(catalog_error(
                "DFS content-root catalog timestamp is not its oldest shard observation",
            ));
        }
        Ok(())
    }
}

fn update_generation_hash(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn append_catalog_age_reason(
    reasons: &mut Vec<DfsContentRootCatalogIncompleteReason>,
    identity: Option<DfsContentRootCatalogShardIdentity>,
    observed_unix_nanos: u64,
    now_unix_nanos: u64,
    max_age_nanos: u64,
) {
    if observed_unix_nanos > now_unix_nanos {
        reasons.push(
            DfsContentRootCatalogIncompleteReason::CatalogTimestampInFuture {
                identity,
                observed_unix_nanos,
                now_unix_nanos,
            },
        );
    } else if now_unix_nanos.saturating_sub(observed_unix_nanos) > max_age_nanos {
        reasons.push(DfsContentRootCatalogIncompleteReason::CatalogExpired {
            identity,
            generated_at_unix_nanos: observed_unix_nanos,
            now_unix_nanos,
            max_age_nanos,
        });
    }
}

fn watermark_identity(
    watermark: &DfsMetadataShardHeadWatermark,
) -> DfsContentRootCatalogShardIdentity {
    DfsContentRootCatalogShardIdentity {
        file_system_id: watermark.file_system_id.clone(),
        shard_id: DfsMetadataShardId::new(watermark.shard_id.clone()),
        primary_node_id: watermark.primary_node_id.clone(),
        epoch: watermark.epoch,
    }
}

pub fn dfs_content_root_catalog_entries(
    protection: &DfsMetadataContentProtection,
) -> Vec<DfsContentRootCatalogEntry> {
    dfs_content_root_catalog_entries_owned(protection.clone())
}

pub(crate) fn dfs_content_root_catalog_entries_owned(
    protection: DfsMetadataContentProtection,
) -> Vec<DfsContentRootCatalogEntry> {
    let mut entries = protection
        .versions
        .into_iter()
        .map(DfsContentRootCatalogEntry::Version)
        .chain(
            protection
                .content_hashes
                .into_iter()
                .map(DfsContentRootCatalogEntry::ContentHash),
        )
        .collect::<Vec<_>>();
    entries.sort();
    entries.dedup();
    entries
}

/// Converts protection roots that are already strictly ordered and unique.
///
/// Authoritative metadata-table protection is assembled through `BTreeSet`s,
/// so sorting the combined catalog roots again adds an avoidable O(n log n)
/// pass to every accumulated-state refresh. Keep the general public converter
/// defensive while allowing that canonical producer to take the linear path.
pub(crate) fn dfs_content_root_catalog_entries_from_canonical_protection(
    protection: DfsMetadataContentProtection,
) -> Vec<DfsContentRootCatalogEntry> {
    debug_assert!(
        protection.versions.windows(2).all(|pair| pair[0] < pair[1]),
        "canonical content-root versions must be strictly ordered"
    );
    debug_assert!(
        protection
            .content_hashes
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "canonical content-root hashes must be strictly ordered"
    );
    protection
        .versions
        .into_iter()
        .map(DfsContentRootCatalogEntry::Version)
        .chain(
            protection
                .content_hashes
                .into_iter()
                .map(DfsContentRootCatalogEntry::ContentHash),
        )
        .collect()
}

pub fn dfs_content_root_catalog_topology_digest(
    shards: &[DfsMetadataShardConfig],
) -> Result<String, MetadataBackendError> {
    let canonical = canonical_content_root_catalog_shards(shards);
    Ok(
        blake3::hash(&encode_content_root_catalog_topology(&canonical)?)
            .to_hex()
            .to_string(),
    )
}

/// Digest the complete namespace topology which shares one content store.
///
/// Filesystem configs are included even before their first metadata shard is
/// published, so an empty shard set can never silently erase a filesystem
/// from the collection proof.
pub fn dfs_content_root_catalog_topology_digest_with_file_systems(
    file_systems: &[DfsFileSystemConfig],
    shards: &[DfsMetadataShardConfig],
) -> Result<String, MetadataBackendError> {
    let mut canonical_file_systems = file_systems.to_vec();
    canonical_file_systems.sort_by(|left, right| left.file_system_id.cmp(&right.file_system_id));
    let canonical_shards = canonical_content_root_catalog_shards(shards);
    Ok(blake3::hash(&encode_content_root_catalog_topology(&(
        canonical_file_systems,
        canonical_shards,
    ))?)
    .to_hex()
    .to_string())
}

fn encode_content_root_catalog_topology(
    topology: &impl Serialize,
) -> Result<Vec<u8>, MetadataBackendError> {
    match encode_sobj(topology) {
        Ok(encoded) => Ok(encoded),
        Err(sobj_error) => {
            // SOBJ collections require every element to share one inferred
            // shape. Valid DFS topology can contain mixed enum variants and
            // policy records with different omitted optional fields, so retain
            // the legacy digest whenever SOBJ can represent the value and use
            // a domain-separated canonical JSON fallback otherwise.
            let json = serde_json::to_vec(topology).map_err(|json_error| {
                catalog_error(format!(
                    "DFS content-root catalog topology cannot be encoded as SOBJ ({sobj_error}) or JSON ({json_error})"
                ))
            })?;
            let mut encoded = Vec::with_capacity(
                DFS_CONTENT_ROOT_CATALOG_JSON_TOPOLOGY_PREFIX.len() + json.len(),
            );
            encoded.extend_from_slice(DFS_CONTENT_ROOT_CATALOG_JSON_TOPOLOGY_PREFIX);
            encoded.extend_from_slice(&json);
            Ok(encoded)
        }
    }
}

fn canonical_content_root_catalog_shards(
    shards: &[DfsMetadataShardConfig],
) -> Vec<DfsMetadataShardConfig> {
    let mut canonical = shards.to_vec();
    for shard in &mut canonical {
        shard.replica_node_ids.sort();
        shard.replica_node_ids.dedup();
    }
    canonical.sort_by(|left, right| {
        left.file_system_id
            .cmp(&right.file_system_id)
            .then_with(|| left.shard_id.cmp(&right.shard_id))
            .then_with(|| left.epoch.cmp(&right.epoch))
            .then_with(|| left.primary_node_id.cmp(&right.primary_node_id))
    });
    canonical
}

#[derive(Clone, Debug)]
pub struct LocalDfsContentRootCatalogStore {
    path: PathBuf,
}

pub fn default_dfs_content_root_catalog_store(
    content_root: impl AsRef<Path>,
) -> LocalDfsContentRootCatalogStore {
    LocalDfsContentRootCatalogStore::new(content_root)
}

impl LocalDfsContentRootCatalogStore {
    pub fn new(content_root: impl AsRef<Path>) -> Self {
        Self {
            path: content_root
                .as_ref()
                .join("catalog")
                .join("metadata-content-roots.sobj"),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<DfsContentRootCatalog>, MetadataBackendError> {
        let mut catalog =
            match read_sobj_optional::<DfsContentRootCatalogDurableEnvelope>(&self.path) {
                Ok(Some(envelope)) => {
                    if envelope.storage_format_version
                        != DFS_CONTENT_ROOT_CATALOG_STORAGE_FORMAT_VERSION
                    {
                        return Err(catalog_error(format!(
                            "unsupported DFS content-root catalog storage format {}",
                            envelope.storage_format_version
                        )));
                    }
                    Some(
                        serde_json::from_str(&envelope.catalog_json).map_err(|error| {
                            catalog_error(format!(
                                "failed to decode durable DFS content-root catalog payload: {error}"
                            ))
                        })?,
                    )
                }
                Ok(None) => None,
                // Accept direct pre-envelope catalogs written by development
                // builds. The next successful publication migrates them.
                Err(envelope_error) => {
                    match read_sobj_optional::<DfsContentRootCatalog>(&self.path) {
                        Ok(catalog) => catalog,
                        Err(_) => return Err(envelope_error),
                    }
                }
            };
        if let Some(catalog) = catalog.as_mut() {
            catalog.validate_structure()?;
            if catalog.installed_at_unix_nanos.is_none() {
                return Err(catalog_error(
                    "durable DFS content-root catalog has no installation timestamp",
                ));
            }
            catalog.durable_install_seal = Some(DfsContentRootCatalogDurableInstallSeal);
        }
        Ok(catalog)
    }

    pub(crate) fn install(
        &self,
        catalog: DfsContentRootCatalog,
    ) -> Result<DfsContentRootCatalog, MetadataBackendError> {
        let now_unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        self.install_at(catalog, now_unix_nanos)
    }

    fn install_at(
        &self,
        mut catalog: DfsContentRootCatalog,
        installed_at_unix_nanos: u64,
    ) -> Result<DfsContentRootCatalog, MetadataBackendError> {
        catalog.validate_structure()?;
        catalog.installed_at_unix_nanos = Some(installed_at_unix_nanos);
        catalog.durable_install_seal = None;
        let envelope = DfsContentRootCatalogDurableEnvelope {
            storage_format_version: DFS_CONTENT_ROOT_CATALOG_STORAGE_FORMAT_VERSION,
            catalog_json: serde_json::to_string(&catalog).map_err(|error| {
                catalog_error(format!(
                    "failed to encode durable DFS content-root catalog payload: {error}"
                ))
            })?,
        };
        let parent = self
            .path
            .parent()
            .ok_or_else(|| catalog_error("DFS content-root catalog path has no parent"))?;
        create_dir_all(parent.to_path_buf())?;
        write_sobj_atomic(&self.path, &envelope)?;
        catalog.durable_install_seal = Some(DfsContentRootCatalogDurableInstallSeal);
        Ok(catalog)
    }

    /// Test-support hook for installing a deterministic-time catalog fixture.
    ///
    /// Production callers cannot submit arbitrary root bodies to the
    /// authoritative store; they must use the crate-internal primary paging
    /// and merge path.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn install_test_catalog_at(
        &self,
        catalog: DfsContentRootCatalog,
        installed_at_unix_nanos: u64,
    ) -> Result<DfsContentRootCatalog, MetadataBackendError> {
        self.install_at(catalog, installed_at_unix_nanos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsMetadataShardAssignment, DfsVersion};
    use sinomo_core::DfsObjectPolicy;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
    }

    fn fs(value: &str) -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named(value).expect("filesystem")
    }

    fn shard_config() -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: fs("catalog-fs"),
            shard_id: "catalog-shard".to_string(),
            primary_node_id: node("catalog-primary"),
            replica_node_ids: vec![node("catalog-replica")],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedConfirmed,
        }
    }

    fn file_system_config() -> DfsFileSystemConfig {
        let shard = shard_config();
        DfsFileSystemConfig {
            file_system_id: shard.file_system_id,
            display_name: "Catalog filesystem".to_string(),
            owner_node_id: shard.primary_node_id,
            subtree_policies: Vec::new(),
        }
    }

    fn request() -> DfsContentRootCatalogPageRequest {
        let shard = shard_config();
        let file_system = file_system_config();
        DfsContentRootCatalogPageRequest {
            identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
            expected_topology_digest: dfs_content_root_catalog_topology_digest_with_file_systems(
                &[file_system],
                &[shard],
            )
            .expect("topology"),
            minimum_journal_sequence: 3,
            minimum_state_fingerprint: None,
            after: None,
            max_entries: 2,
        }
    }

    fn entry(value: &str) -> DfsContentRootCatalogEntry {
        DfsContentRootCatalogEntry::Version(DfsMetadataVersionIdentity::new(
            crate::DfsObjectId::from_legacy_named(format!("catalog-object-{value}"))
                .expect("object"),
            DfsVersion::from_legacy_named(format!("catalog-version-{value}")).expect("version"),
        ))
    }

    fn content_hash_entry(index: usize, length: usize) -> DfsContentRootCatalogEntry {
        let prefix = format!("{index:08x}");
        assert!(length >= prefix.len());
        DfsContentRootCatalogEntry::ContentHash(format!(
            "{prefix}{}",
            "a".repeat(length - prefix.len())
        ))
    }

    fn state_fingerprint(value: &str) -> String {
        format!(
            "{DFS_METADATA_PREFIX_PROOF_DIGEST_PREFIX}{}",
            blake3::hash(value.as_bytes()).to_hex()
        )
    }

    fn complete_page(
        request: &DfsContentRootCatalogPageRequest,
        generated_at_unix_nanos: u64,
        entries: Vec<DfsContentRootCatalogEntry>,
    ) -> DfsContentRootCatalogPage {
        DfsContentRootCatalogPage {
            identity: request.identity.clone(),
            topology_digest: request.expected_topology_digest.clone(),
            generated_at_unix_nanos,
            coverage: DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: request.minimum_journal_sequence,
                state_fingerprint: request
                    .minimum_state_fingerprint
                    .clone()
                    .unwrap_or_else(|| state_fingerprint("catalog-state")),
            },
            entries,
            next_cursor: None,
        }
    }

    fn complete_fragment(
        request: &DfsContentRootCatalogPageRequest,
        generated_at_unix_nanos: u64,
        entries: Vec<DfsContentRootCatalogEntry>,
    ) -> DfsContentRootCatalogFragment {
        DfsContentRootCatalogFragment::from_pages(
            request,
            &[complete_page(request, generated_at_unix_nanos, entries)],
        )
        .expect("complete fragment")
    }

    fn head_watermark(
        request: &DfsContentRootCatalogPageRequest,
        journal_sequence: u64,
        state_fingerprint: String,
    ) -> DfsMetadataShardHeadWatermark {
        DfsMetadataShardHeadWatermark {
            file_system_id: request.identity.file_system_id.clone(),
            shard_id: request.identity.shard_id.0.clone(),
            primary_node_id: request.identity.primary_node_id.clone(),
            epoch: request.identity.epoch,
            journal_sequence,
            state_fingerprint,
            acknowledged_node_ids: vec![request.identity.primary_node_id.clone()],
        }
    }

    #[test]
    fn empty_incomplete_catalog_is_durably_installable() {
        let topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(&[], &[])
            .expect("empty topology");
        let catalog = DfsContentRootCatalog::from_topology_fragments(
            topology_digest,
            Vec::new(),
            Vec::new(),
            100,
            vec![
                DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed {
                    reason: "malformed topology".to_string(),
                },
            ],
            Vec::new(),
        )
        .expect("empty incomplete catalog");
        assert!(!catalog.is_complete());
        assert!(catalog.expected_file_system_ids().is_empty());

        let root = sinomo_testdir::TestDir::new("dfs-empty-incomplete-content-root-catalog");
        let store = LocalDfsContentRootCatalogStore::new(&root);
        store
            .install_test_catalog_at(catalog, 101)
            .expect("install empty incomplete catalog");
        let loaded = store.load().expect("load").expect("catalog");
        assert!(loaded.expected_file_system_ids().is_empty());
        assert!(!loaded.is_complete());
    }

    #[test]
    fn page_validation_binds_continuations_to_one_durable_cut() {
        let request = request();
        let fingerprint = state_fingerprint("catalog-state");
        let first_entry = entry("a");
        let page = DfsContentRootCatalogPage {
            identity: request.identity.clone(),
            topology_digest: request.expected_topology_digest.clone(),
            generated_at_unix_nanos: 9,
            coverage: DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: 3,
                state_fingerprint: fingerprint.clone(),
            },
            entries: vec![first_entry.clone()],
            next_cursor: Some(DfsContentRootCatalogCursor {
                journal_sequence: 3,
                state_fingerprint: fingerprint,
                generated_at_unix_nanos: 9,
                last_entry: first_entry,
            }),
        };
        page.validate_for_request(&request).expect("valid page");

        let mut drifted = page.clone();
        drifted
            .next_cursor
            .as_mut()
            .expect("cursor")
            .state_fingerprint = state_fingerprint("other-state");
        assert!(drifted.validate_for_request(&request).is_err());
        let mut timestamp_drifted = page;
        timestamp_drifted
            .next_cursor
            .as_mut()
            .expect("cursor")
            .generated_at_unix_nanos = 10;
        assert!(timestamp_drifted.validate_for_request(&request).is_err());
    }

    #[test]
    fn page_contract_accepts_legacy_and_byte_bounded_target_batches() {
        let mut legacy_request = request();
        legacy_request.max_entries = DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES;
        let legacy_page = complete_page(
            &legacy_request,
            9,
            (0..DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES)
                .map(|index| content_hash_entry(index, 71))
                .collect(),
        );
        legacy_page
            .validate_for_request(&legacy_request)
            .expect("legacy 256-entry page remains valid");

        let mut target_request = request();
        target_request.max_entries = DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT;
        let target_page = complete_page(
            &target_request,
            10,
            (0..DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT)
                .map(|index| content_hash_entry(index, 71))
                .collect(),
        );
        let encoded_size_bytes = target_page.encoded_size_bytes().expect("encoded page size");
        assert!(
            encoded_size_bytes <= DFS_CONTENT_ROOT_CATALOG_TARGET_PAGE_BYTES,
            "current blake3-sized roots should fit the preferred page target, observed {encoded_size_bytes} bytes"
        );
        target_page
            .validate_for_request(&target_request)
            .expect("target page remains inside the hard transport budget");
        assert_eq!(
            DEFAULT_DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRIES,
            DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT
        );
        let mut over_limit_request = target_request;
        over_limit_request.max_entries = DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT + 1;
        assert!(
            over_limit_request.validate().is_err(),
            "the count limit bounds work even before encoded-size validation"
        );
    }

    #[test]
    fn page_contract_rejects_oversized_long_root_batches() {
        let mut request = request();
        request.max_entries = DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT;
        let page = complete_page(
            &request,
            10,
            (0..DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRY_LIMIT)
                .map(|index| content_hash_entry(index, 256))
                .collect(),
        );
        assert!(
            page.encoded_size_bytes().expect("encoded page size")
                > DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_BYTES,
            "the fixture must cross the hard encoded page budget"
        );
        assert!(
            page.validate_for_request(&request).is_err(),
            "oversized pages fail closed even when they satisfy the entry-count limit"
        );
    }

    #[test]
    fn fragment_builder_merges_pages_without_retaining_page_vectors() {
        let request = request();
        let fingerprint = state_fingerprint("catalog-state");
        let first_entries = vec![content_hash_entry(0, 71), content_hash_entry(1, 71)];
        let mut first = complete_page(&request, 100, first_entries.clone());
        first.next_cursor = Some(DfsContentRootCatalogCursor {
            journal_sequence: request.minimum_journal_sequence,
            state_fingerprint: fingerprint,
            generated_at_unix_nanos: 100,
            last_entry: first_entries.last().cloned().expect("last entry"),
        });

        let mut builder =
            DfsContentRootCatalogFragmentBuilder::new(&request).expect("fragment builder");
        let continuation = builder
            .push_page(first)
            .expect("first page")
            .expect("continuation");
        let second_entries = vec![content_hash_entry(2, 71), content_hash_entry(3, 71)];
        builder
            .push_page(complete_page(&continuation, 100, second_entries.clone()))
            .expect("second page");
        let fragment = builder.finish().expect("complete fragment");

        assert!(fragment.is_complete());
        assert_eq!(
            fragment.entries,
            first_entries
                .into_iter()
                .chain(second_entries)
                .collect::<Vec<_>>()
        );
        assert_eq!(fragment.generated_at_unix_nanos, 100);
    }

    #[test]
    fn fragment_builder_discards_accumulated_roots_on_incomplete_continuation() {
        let request = request();
        let first_entry = entry("a");
        let mut first = complete_page(&request, 100, vec![first_entry.clone()]);
        first.next_cursor = Some(DfsContentRootCatalogCursor {
            journal_sequence: request.minimum_journal_sequence,
            state_fingerprint: state_fingerprint("catalog-state"),
            generated_at_unix_nanos: 100,
            last_entry: first_entry,
        });
        let mut builder =
            DfsContentRootCatalogFragmentBuilder::new(&request).expect("fragment builder");
        let continuation = builder
            .push_page(first)
            .expect("first page")
            .expect("continuation");
        builder
            .push_page(DfsContentRootCatalogPage::incomplete_for_request(
                &continuation,
                DfsContentRootCatalogIncompleteReason::StateFingerprintChanged,
                101,
            ))
            .expect("incomplete continuation");
        let fragment = builder.finish().expect("incomplete fragment");

        assert!(!fragment.is_complete());
        assert!(
            fragment.entries.is_empty(),
            "an incomplete shard can never retain partially observed roots"
        );
    }

    #[test]
    fn incomplete_pages_are_rootless_and_make_catalog_incomplete() {
        let request = request();
        let page = DfsContentRootCatalogPage::incomplete_for_request(
            &request,
            DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                handoff_id: Some("handoff".to_string()),
            },
            11,
        );
        page.validate_for_request(&request)
            .expect("valid incomplete page");
        let fragment = DfsContentRootCatalogFragment::from_pages(&request, &[page])
            .expect("incomplete fragment");
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            11,
            vec![fragment],
        )
        .expect("catalog");
        assert!(!catalog.is_complete());
        assert!(catalog.protected_versions().is_none());
        assert!(catalog.protected_content_hashes().is_none());
    }

    #[test]
    fn empty_complete_fragment_is_positive_authoritative_proof() {
        let request = request();
        let fragment = complete_fragment(&request, 100, Vec::new());
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-empty-content-root-catalog");
        let store = LocalDfsContentRootCatalogStore::new(&root);
        let installed = store.install_at(catalog, 101).expect("install");
        let validation = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[head_watermark(&request, 3, fingerprint.clone())],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate");

        assert!(validation.is_collection_authorized());
        assert_eq!(validation.covered_shard_count(), 1);
        assert_eq!(validation.expected_shard_count(), 1);
        assert_eq!(validation.catalog_age_nanos(), 5);
        assert_eq!(
            validation.protection().cloned(),
            Some(DfsMetadataContentProtection::default())
        );
        assert!(validation.catalog_generation().is_some());
        assert_eq!(validation.catalog_generated_at_unix_nanos(), Some(100));
        assert_eq!(validation.catalog_installed_at_unix_nanos(), Some(101));

        let serialized = serde_json::to_value(&validation).expect("serialize validation report");
        assert_eq!(
            serialized
                .get("collection_authorized")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "serialized reports retain the diagnostic authorization outcome"
        );
        assert_eq!(
            serialized
                .get("catalog_installed_at_unix_nanos")
                .and_then(serde_json::Value::as_u64),
            Some(101),
            "serialized reports retain the diagnostic local installation time"
        );
        let deserialized: DfsContentRootCatalogCollectionValidation =
            serde_json::from_value(serialized).expect("deserialize validation report");
        assert!(
            !deserialized.is_collection_authorized(),
            "deserializing a report must not recreate destructive-GC authority"
        );
        assert!(deserialized.protection().is_none());
        assert_eq!(deserialized.catalog_installed_at_unix_nanos(), None);
        assert!(deserialized.authorization_seal.is_none());

        let serialized_catalog =
            serde_json::to_value(&installed).expect("serialize installed catalog");
        let deserialized_catalog: DfsContentRootCatalog =
            serde_json::from_value(serialized_catalog).expect("deserialize installed catalog");
        assert!(!deserialized_catalog.is_durably_installed());
        let unsealed_validation = deserialized_catalog
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[head_watermark(&request, 3, fingerprint.clone())],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate deserialized catalog");
        assert!(!unsealed_validation.is_collection_authorized());
        assert!(unsealed_validation.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled
        )));

        let loaded = store
            .load()
            .expect("load durable catalog")
            .expect("catalog");
        assert!(loaded.is_durably_installed());
        assert!(
            loaded
                .validate_for_collection(
                    &[file_system_config()],
                    &[shard_config()],
                    &[],
                    &[head_watermark(&request, 3, fingerprint)],
                    105,
                    Duration::from_nanos(10),
                )
                .expect("validate loaded catalog")
                .is_collection_authorized()
        );
    }

    #[test]
    fn independent_observations_of_unchanged_head_have_distinct_generations() {
        let request = request();
        let first = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![complete_fragment(&request, 100, Vec::new())],
        )
        .expect("first catalog");
        let second = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            101,
            vec![complete_fragment(&request, 101, Vec::new())],
        )
        .expect("second catalog");

        assert_ne!(
            first.generation_fingerprint().expect("first generation"),
            second.generation_fingerprint().expect("second generation"),
            "a later complete observation must advance durable absence proof even when the metadata head is unchanged"
        );
    }

    #[test]
    fn topology_digest_supports_mixed_sifs_policies_and_shard_assignments() {
        let mut file_system = file_system_config();
        file_system.subtree_policies = vec![
            crate::DfsSubtreePolicy::legacy("/files/local", DfsObjectPolicy::LocalOwned),
            crate::DfsSubtreePolicy::legacy("/services", DfsObjectPolicy::ReplicatedEventual),
        ];
        let root_shard = shard_config();
        let mut services_shard = root_shard.clone();
        services_shard.shard_id = "services".to_string();
        services_shard.assignment = DfsMetadataShardAssignment::PathPrefix {
            prefix: "/services".to_string(),
        };
        let shards = vec![root_shard, services_shard];

        let first = dfs_content_root_catalog_topology_digest_with_file_systems(
            &[file_system.clone()],
            &shards,
        )
        .expect("mixed SIFS topology digest");
        let second =
            dfs_content_root_catalog_topology_digest_with_file_systems(&[file_system], &shards)
                .expect("stable mixed SIFS topology digest");

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(dfs_content_root_catalog_topology_digest(&shards).is_ok());
    }

    #[test]
    fn incomplete_fragment_propagates_across_complete_shards() {
        let first_shard = shard_config();
        let mut second_shard = first_shard.clone();
        second_shard.shard_id = "catalog-shard-b".to_string();
        second_shard.primary_node_id = node("catalog-primary-b");
        let topology =
            dfs_content_root_catalog_topology_digest(&[first_shard.clone(), second_shard.clone()])
                .expect("topology");
        let mut first_request = request();
        first_request.expected_topology_digest = topology.clone();
        let second_request = DfsContentRootCatalogPageRequest {
            identity: DfsContentRootCatalogShardIdentity::from_config(&second_shard),
            expected_topology_digest: topology.clone(),
            ..first_request.clone()
        };
        let complete = complete_fragment(&first_request, 100, vec![entry("a")]);
        let incomplete = DfsContentRootCatalogFragment::from_pages(
            &second_request,
            &[DfsContentRootCatalogPage::incomplete_for_request(
                &second_request,
                DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
                100,
            )],
        )
        .expect("incomplete fragment");
        let catalog = DfsContentRootCatalog::from_fragments(
            topology,
            vec![
                first_request.identity.clone(),
                second_request.identity.clone(),
            ],
            100,
            vec![complete, incomplete],
        )
        .expect("catalog");

        assert!(!catalog.is_complete());
        assert!(catalog.protected_versions().is_none());
        assert!(catalog.protected_content_hashes().is_none());
    }

    #[test]
    fn catalog_rejects_multiple_authorities_for_one_logical_shard() {
        let request = request();
        let mut replacement_identity = request.identity.clone();
        replacement_identity.primary_node_id = node("catalog-replacement-primary");
        replacement_identity.epoch += 1;

        let catalog = DfsContentRootCatalog::from_topology_fragments(
            request.expected_topology_digest,
            vec![request.identity.file_system_id.clone()],
            vec![request.identity, replacement_identity],
            100,
            Vec::new(),
            Vec::new(),
        );

        assert!(
            catalog.is_err(),
            "one logical shard must not contribute multiple authority generations"
        );
    }

    #[test]
    fn collection_validation_rejects_missing_stale_and_expired_proofs() {
        let request = request();
        let fragment = complete_fragment(&request, 100, vec![entry("a")]);
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let mut second_shard = shard_config();
        second_shard.shard_id = "catalog-shard-b".to_string();
        second_shard.primary_node_id = node("catalog-primary-b");
        let expected_shards = vec![shard_config(), second_shard.clone()];
        let expected_file_systems = vec![file_system_config()];
        let expected_topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
            &expected_file_systems,
            &expected_shards,
        )
        .expect("topology");
        let mut expected_request = request.clone();
        expected_request.expected_topology_digest = expected_topology_digest.clone();
        let missing_fragment = complete_fragment(&expected_request, 100, vec![entry("a")]);
        let missing_catalog = DfsContentRootCatalog::from_fragments(
            expected_topology_digest,
            vec![
                request.identity.clone(),
                DfsContentRootCatalogShardIdentity::from_config(&second_shard),
            ],
            100,
            vec![missing_fragment],
        )
        .expect("incomplete catalog");
        let missing_root = sinomo_testdir::TestDir::new("dfs-missing-content-root-catalog");
        let missing_catalog = LocalDfsContentRootCatalogStore::new(&missing_root)
            .install_at(missing_catalog, 101)
            .expect("install incomplete catalog");
        let missing = missing_catalog
            .validate_for_collection(
                &expected_file_systems,
                &expected_shards,
                &[],
                &[
                    head_watermark(&expected_request, 3, fingerprint.clone()),
                    DfsMetadataShardHeadWatermark {
                        file_system_id: second_shard.file_system_id.clone(),
                        shard_id: second_shard.shard_id.clone(),
                        primary_node_id: second_shard.primary_node_id.clone(),
                        epoch: second_shard.epoch,
                        journal_sequence: 3,
                        state_fingerprint: state_fingerprint("second-shard"),
                        acknowledged_node_ids: vec![second_shard.primary_node_id.clone()],
                    },
                ],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate missing shard");
        assert!(!missing.is_collection_authorized());
        assert!(missing.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::MissingShard { .. }
        )));

        let root = sinomo_testdir::TestDir::new("dfs-stale-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_at(
                DfsContentRootCatalog::from_fragments(
                    request.expected_topology_digest.clone(),
                    vec![request.identity.clone()],
                    100,
                    vec![fragment.clone()],
                )
                .expect("catalog"),
                101,
            )
            .expect("install");
        let stale_topology = installed
            .validate_for_collection(
                &expected_file_systems,
                &expected_shards,
                &[],
                &[head_watermark(&request, 3, fingerprint.clone())],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate stale topology");
        assert!(!stale_topology.is_collection_authorized());
        assert!(stale_topology.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::TopologyChanged { .. }
        )));

        let expired = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[head_watermark(&request, 3, fingerprint)],
                200,
                Duration::from_nanos(50),
            )
            .expect("validate expired catalog");
        assert!(!expired.is_collection_authorized());
        assert!(expired.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::CatalogExpired { .. }
        )));

        let duplicate = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment.clone(), fragment],
        );
        assert!(duplicate.is_err());
    }

    #[test]
    fn collection_validation_rejects_missing_or_changed_head_watermark() {
        let request = request();
        let fragment = complete_fragment(&request, 100, vec![entry("a")]);
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-head-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_at(catalog, 101)
            .expect("install");

        let missing = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate missing watermark");
        assert!(!missing.is_collection_authorized());
        assert!(missing.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::HeadWatermarkUnavailable { .. }
        )));
        let authenticated_primary_only = installed
            .validate_for_collection_with_head_watermark_policy(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[],
                false,
                105,
                Duration::from_nanos(10),
            )
            .expect("validate authenticated primary proof without optional watermark");
        assert!(authenticated_primary_only.is_collection_authorized());

        let changed = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[head_watermark(
                    &request,
                    request.minimum_journal_sequence,
                    state_fingerprint("changed-head"),
                )],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate changed watermark");
        assert!(!changed.is_collection_authorized());
        assert!(changed.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::HeadWatermarkFingerprintChanged { .. }
        )));
    }

    #[test]
    fn collection_validation_rejects_duplicate_logical_head_watermarks() {
        let request = request();
        let fragment = complete_fragment(&request, 100, Vec::new());
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-duplicate-head-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_at(catalog, 101)
            .expect("install");
        let first = head_watermark(&request, 3, fingerprint.clone());
        let second = head_watermark(&request, 4, fingerprint);

        assert!(
            installed
                .validate_for_collection(
                    &[file_system_config()],
                    &[shard_config()],
                    &[],
                    &[first, second],
                    105,
                    Duration::from_nanos(10),
                )
                .is_err(),
            "ambiguous head records must never authorize destructive collection"
        );
    }

    #[test]
    fn configured_file_system_without_shards_cannot_authorize_collection() {
        let configured = file_system_config();
        let configured_without_shards = DfsFileSystemConfig {
            file_system_id: fs("catalog-empty-fs"),
            display_name: "Catalog filesystem without shards".to_string(),
            owner_node_id: node("catalog-empty-owner"),
            subtree_policies: Vec::new(),
        };
        let expected_file_systems = vec![configured, configured_without_shards.clone()];
        let expected_shards = vec![shard_config()];
        let topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
            &expected_file_systems,
            &expected_shards,
        )
        .expect("topology");
        let mut request = request();
        request.expected_topology_digest = topology_digest.clone();
        let fragment = complete_fragment(&request, 100, vec![entry("a")]);
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let catalog = DfsContentRootCatalog::from_topology_fragments(
            topology_digest,
            expected_file_systems
                .iter()
                .map(|config| config.file_system_id.clone())
                .collect(),
            vec![request.identity.clone()],
            100,
            Vec::new(),
            vec![fragment],
        )
        .expect("catalog");

        assert!(!catalog.is_complete());
        assert!(catalog.incomplete_reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                file_system_id,
            } if file_system_id == &configured_without_shards.file_system_id
        )));
        let root = sinomo_testdir::TestDir::new("dfs-unsharded-fs-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_test_catalog_at(catalog, 101)
            .expect("install");
        let validation = installed
            .validate_for_collection(
                &expected_file_systems,
                &expected_shards,
                &[],
                &[head_watermark(&request, 3, fingerprint)],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate");

        assert_eq!(validation.covered_shard_count(), 1);
        assert_eq!(validation.expected_shard_count(), 1);
        assert!(!validation.is_collection_authorized());
        assert!(validation.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::FileSystemWithoutMetadataShards {
                file_system_id,
            } if file_system_id == &configured_without_shards.file_system_id
        )));
    }

    #[test]
    fn pending_membership_handoff_blocks_an_installed_complete_catalog() {
        let request = request();
        let fragment = complete_fragment(&request, 100, vec![entry("a")]);
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment],
        )
        .expect("catalog");
        assert!(catalog.is_complete());
        let root = sinomo_testdir::TestDir::new("dfs-handoff-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_test_catalog_at(catalog, 101)
            .expect("install");
        let retiring_shard = shard_config();
        let mut desired_shard = retiring_shard.clone();
        desired_shard.primary_node_id = node("catalog-desired-primary");
        desired_shard.replica_node_ids = vec![retiring_shard.primary_node_id.clone()];
        desired_shard.epoch += 1;
        let handoff = DfsMetadataShardMembershipHandoff {
            handoff_id: "catalog-handoff".to_string(),
            retiring_shard,
            desired_shard,
            required_prepared_sequence: 3,
            required_head_watermark: None,
            retiring_primary_fence: None,
        };
        let validation = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                std::slice::from_ref(&handoff),
                &[head_watermark(&request, 3, fingerprint)],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate");

        assert!(!validation.is_collection_authorized());
        assert!(validation.reasons().iter().any(|reason| matches!(
            reason,
            DfsContentRootCatalogIncompleteReason::MembershipHandoffInProgress {
                file_system_id,
                shard_id,
                handoff_id,
            } if file_system_id == &request.identity.file_system_id
                && shard_id == &request.identity.shard_id
                && handoff_id == &handoff.handoff_id
        )));
    }

    #[test]
    fn optional_watermark_policy_rejects_newer_or_different_primary_authority() {
        let request = request();
        let fragment = complete_fragment(&request, 100, vec![entry("a")]);
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-optional-watermark-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_test_catalog_at(catalog, 101)
            .expect("install");
        let mut newer_epoch = head_watermark(&request, 3, fingerprint.clone());
        newer_epoch.epoch += 1;
        let mut different_primary = head_watermark(&request, 3, fingerprint);
        different_primary.primary_node_id = node("catalog-other-primary");

        for watermark in [newer_epoch, different_primary] {
            let observed = DfsContentRootCatalogShardIdentity {
                file_system_id: watermark.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(watermark.shard_id.clone()),
                primary_node_id: watermark.primary_node_id.clone(),
                epoch: watermark.epoch,
            };
            let validation = installed
                .validate_for_collection_with_head_watermark_policy(
                    &[file_system_config()],
                    &[shard_config()],
                    &[],
                    std::slice::from_ref(&watermark),
                    false,
                    105,
                    Duration::from_nanos(10),
                )
                .expect("validate");

            assert!(!validation.is_collection_authorized());
            assert!(validation.reasons().iter().any(|reason| matches!(
                reason,
                DfsContentRootCatalogIncompleteReason::HeadWatermarkIdentityMismatch {
                    expected,
                    observed: actual,
                } if expected == &request.identity && actual == &observed
            )));
        }
    }

    #[test]
    fn collection_validation_reports_only_positive_journal_lag() {
        let request = request();
        let fragment = complete_fragment(&request, 100, vec![entry("a")]);
        let fingerprint = fragment
            .coverage
            .durable_cut()
            .expect("durable cut")
            .1
            .to_string();
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![fragment],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-journal-lag-content-root-catalog");
        let installed = LocalDfsContentRootCatalogStore::new(&root)
            .install_test_catalog_at(catalog, 101)
            .expect("install");

        let behind = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[head_watermark(&request, 8, fingerprint.clone())],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate catalog behind watermark");
        assert!(!behind.is_collection_authorized());
        assert_eq!(behind.max_journal_lag_entries(), 5);

        let ahead = installed
            .validate_for_collection(
                &[file_system_config()],
                &[shard_config()],
                &[],
                &[head_watermark(&request, 2, fingerprint)],
                105,
                Duration::from_nanos(10),
            )
            .expect("validate catalog ahead of watermark");
        assert!(ahead.is_collection_authorized());
        assert_eq!(ahead.max_journal_lag_entries(), 0);
    }

    #[test]
    fn pagination_rejects_a_state_change_between_pages() {
        let request = request();
        let first_entry = entry("a");
        let first_fingerprint = state_fingerprint("first-cut");
        let first = DfsContentRootCatalogPage {
            identity: request.identity.clone(),
            topology_digest: request.expected_topology_digest.clone(),
            generated_at_unix_nanos: 100,
            coverage: DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: 3,
                state_fingerprint: first_fingerprint.clone(),
            },
            entries: vec![first_entry.clone()],
            next_cursor: Some(DfsContentRootCatalogCursor {
                journal_sequence: 3,
                state_fingerprint: first_fingerprint,
                generated_at_unix_nanos: 100,
                last_entry: first_entry,
            }),
        };
        let second = DfsContentRootCatalogPage {
            identity: request.identity.clone(),
            topology_digest: request.expected_topology_digest.clone(),
            generated_at_unix_nanos: 101,
            coverage: DfsContentRootCatalogCoverage::Complete {
                prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                journal_sequence: 3,
                state_fingerprint: state_fingerprint("second-cut"),
            },
            entries: vec![entry("b")],
            next_cursor: None,
        };

        assert!(DfsContentRootCatalogFragment::from_pages(&request, &[first, second]).is_err());
    }

    #[test]
    fn durable_catalog_store_roundtrips_and_torn_input_fails_closed() {
        let request = request();
        let catalog = DfsContentRootCatalog::from_fragments(
            request.expected_topology_digest.clone(),
            vec![request.identity.clone()],
            100,
            vec![complete_fragment(&request, 100, vec![entry("a")])],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-content-root-catalog-store");
        let store = LocalDfsContentRootCatalogStore::new(&root);
        let installed = store.install_at(catalog, 101).expect("install");
        assert_eq!(store.load().expect("load"), Some(installed.clone()));

        let mut malformed = installed.clone();
        malformed.format_version = DFS_CONTENT_ROOT_CATALOG_FORMAT_VERSION + 1;
        assert!(store.install_at(malformed, 102).is_err());
        assert_eq!(
            store.load().expect("load after rejected install"),
            Some(installed)
        );

        std::fs::write(store.path(), b"torn catalog").expect("write torn catalog");
        assert!(store.load().is_err());
    }
}
