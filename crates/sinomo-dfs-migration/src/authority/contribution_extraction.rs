//! Old-image contribution extraction: a closed shard S answers, on the node
//! holding the copy it converts from, what that copy contributes to another
//! shard T's namespace counts.
//!
//! The node answers from the capture S's own conversion records of that copy
//! (the converter's bounded, resumable source replay, whose result is the
//! stage's `captured-unit.json`). The route never captures or converts. Until
//! that capture exists and its original preparations are resolved it answers
//! `SourcePending`; the requester then waits on S's keys, and S's conversion
//! writes S's storage record after its capture.
//!
//! The contributions follow the census rules for one source
//! ([`crate::namespace_counts::contributions::contributions_beside_unsettled_work`]). S's
//! non-Completed namespace jobs, Pending requests and staging intents that
//! touch T are reported beside them with their recorded phase, in the format
//! the current-table answer uses, including the fences and parent
//! participants a job names; S's other work is ignored. A job S's own
//! conversion settles from its rows is marked so, and its Pending outer
//! request is left to it (`shard_conversion::mark_settled_by_own_conversion`).
//!
//! The answer is paged and bound to a basis: S's serving epoch, its storage
//! record, the captured source identity and fingerprints and the catalog it
//! is attributed with. A continuation whose basis moved is answered
//! `Changed`. Every page also answers the request's question about what the
//! capture holds: live canonical rows and live directories S owns under the
//! catalog, by the census rules, and S's Running jobs. A continuation past
//! the last entry carries only a question. Refusals are typed and retryable: the node is not the one
//! that converts S at the requested epoch, S is not old, or the requester's
//! catalog assigns paths differently.
use super::contribution_source::{SourceHoldings, SourceJobTouchingTarget, SourceQuestion};
use super::contribution_sources::{
    AnswerBasis, TargetScope, catalog_ownership_digest, storage_record_digest,
};
use super::*;
use crate::namespace_counts::{
    NamespaceCountAliasContribution, NamespaceCountChildDirectoryContribution,
    NamespaceCountMaintenanceLimits,
};
use sinomo_core::node::NodeId;
use sinomo_dfs::{DfsFileSystemId, DfsMetadataShardId, DfsMetadataShardStorage};

pub(super) const ROUTE: &str = "/dfs-migration/count-contributions-v1";
/// Largest number of entries (aliases, child directories and reported work
/// together) one page carries.
const PAGE_ENTRIES: usize = 512;
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the per-shard conversion driver")
)]
pub(super) const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_CAPTURE_BYTES: usize = 64 * 1024 * 1024;
const MAX_CAPTURE_RECORDS: usize = 1_000_000;
/// Extractions retained between pages and rounds, per serving node.
const RETAINED_EXTRACTIONS: usize = 64;
/// Captures whose holdings are retained between question parts and rounds.
/// Each indexes every live canonical row of one shard.
const RETAINED_HOLDINGS: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub file_system_id: DfsFileSystemId,
    pub source_shard: String,
    pub target_shard: String,
    /// S's serving epoch as the requester read it.
    pub source_epoch: u64,
    /// [`catalog_ownership_digest`] of the catalog the requester attributes
    /// contributions with.
    pub catalog_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<Continuation>,
    /// What the requester asks about the capture's holdings.
    #[serde(default, skip_serializing_if = "SourceQuestion::is_empty")]
    pub question: SourceQuestion,
}

/// Resume after `after` entries of the read whose first page had `basis`.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Continuation {
    pub basis: String,
    pub after: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Response {
    Page {
        basis: String,
        aliases: Vec<NamespaceCountAliasContribution>,
        child_directories: Vec<NamespaceCountChildDirectoryContribution>,
        running_jobs_touching_target: Vec<SourceJobTouchingTarget>,
        /// Entries read so far; absent exactly when `complete`.
        after: Option<u64>,
        /// Explicit on the wire; omission is not a final page.
        complete: bool,
        /// The answer to the request's question.
        #[serde(default)]
        held: SourceHoldings,
    },
    /// S's conversion has not recorded a usable capture of this copy yet.
    SourcePending {
        reason: String,
    },
    /// The continuation's basis is no longer S's; the read restarts.
    Changed {
        basis: String,
    },
    Refused {
        refusal: Refusal,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "reason", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Refusal {
    /// This node does not hold the copy S converts from at this epoch.
    NotPrimary { epoch: u64, serving_node_id: NodeId },
    /// S serves current tables; ask `count-contributions-v1` instead.
    NotOld,
    /// The requester's catalog, or the one S's capture was taken under,
    /// assigns paths differently from S's current catalog.
    CatalogMismatch,
}

/// One target's complete answer from one capture, kept for its pages.
#[derive(Clone)]
struct Extraction {
    basis_facts: CaptureFacts,
    entries: Arc<Vec<Entry>>,
}
#[derive(Clone)]
struct CaptureFacts {
    source_identity: String,
    source_snapshot_fingerprint: String,
    source_journal_sequence: u64,
    source_state_fingerprint: String,
    capture: String,
}
#[derive(Clone)]
enum Entry {
    Alias(NamespaceCountAliasContribution),
    Child(NamespaceCountChildDirectoryContribution),
    Work(SourceJobTouchingTarget),
}

/// The capture file as it was when an extraction was computed. The file is
/// written once; a different identity is a different capture.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CaptureFile {
    path: PathBuf,
    len: u64,
    modified_unix_nanos: Option<u128>,
    identity: Option<(u64, u64)>,
}
impl CaptureFile {
    fn observe(path: &Path) -> Result<Option<Self>> {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                });
            }
        };
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            Some((metadata.dev(), metadata.ino()))
        };
        #[cfg(not(unix))]
        let identity = None;
        Ok(Some(Self {
            path: path.to_owned(),
            len: metadata.len(),
            modified_unix_nanos: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|time| time.as_nanos()),
            identity,
        }))
    }
}

type ExtractionKey = (CaptureFile, String, String);

/// Extractions this node computed, bounded by [`RETAINED_EXTRACTIONS`], and
/// the holdings of the captures they were read from, bounded by
/// [`RETAINED_HOLDINGS`].
#[derive(Default)]
pub(super) struct Extractions {
    entries: BTreeMap<ExtractionKey, Extraction>,
    order: std::collections::VecDeque<ExtractionKey>,
    holdings: BTreeMap<HoldingsKey, Arc<CaptureHoldings>>,
    holdings_order: std::collections::VecDeque<HoldingsKey>,
}

/// A capture and the catalog ownership its holdings are attributed with.
type HoldingsKey = (CaptureFile, String);

/// What one capture holds under the catalog it is attributed with.
type CaptureHoldings = crate::namespace_counts::contributions::ShardHoldings;

impl Extractions {
    fn holdings(&self, key: &HoldingsKey) -> Option<Arc<CaptureHoldings>> {
        self.holdings.get(key).cloned()
    }
    fn insert_holdings(&mut self, file: HoldingsKey, holdings: Arc<CaptureHoldings>) {
        if self.holdings.insert(file.clone(), holdings).is_none() {
            self.holdings_order.push_back(file);
        }
        while self.holdings_order.len() > RETAINED_HOLDINGS {
            if let Some(oldest) = self.holdings_order.pop_front() {
                self.holdings.remove(&oldest);
            }
        }
    }
    fn get(&self, key: &ExtractionKey) -> Option<Extraction> {
        self.entries.get(key).cloned()
    }
    fn insert(&mut self, key: ExtractionKey, extraction: Extraction) {
        if self.entries.insert(key.clone(), extraction).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > RETAINED_EXTRACTIONS {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

impl NativeImportAuthority {
    pub(super) fn receive_contribution_extraction(
        &self,
        _requester: &NodeId,
        request: Request,
    ) -> Result<Response> {
        if request.source_shard == request.target_shard
            || request.source_shard.is_empty()
            || request.target_shard.is_empty()
        {
            return Err(error("contribution extraction needs two distinct shards"));
        }
        if request.question.len() > sinomo_dfs::DFS_COUNT_CONTRIBUTION_QUESTION_LIMIT {
            return Err(error("contribution extraction question asks too many entries"));
        }
        let fs = &request.file_system_id;
        let catalog = self.assignments(std::slice::from_ref(fs))?;
        let find = |shard_id: &str| {
            catalog
                .iter()
                .find(|shard| shard.file_system_id == *fs && shard.shard_id == shard_id)
        };
        let (Some(source), Some(_)) = (find(&request.source_shard), find(&request.target_shard))
        else {
            return Ok(Response::Refused {
                refusal: Refusal::CatalogMismatch,
            });
        };
        let source = source.clone();
        let (entries, _) =
            self.read_exact(&[sinomo_dfs::metadata_shard_storage_key(fs, &source.shard_id)])?;
        let storage_entry = entries.into_iter().next().and_then(|row| row.entry);
        let storage = storage_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardStorage>().map_err(error))
            .transpose()?;
        if let Some(record) = &storage {
            record.validate_against(&source)?;
        }
        // The copy S converts from: its primary's, or the converting member's
        // in a converting takeover at the current epoch.
        let serving = storage
            .as_ref()
            .and_then(DfsMetadataShardStorage::converting_attempt)
            .filter(|attempt| attempt.takeover.is_some() && attempt.epoch == source.epoch)
            .map_or_else(
                || source.primary_node_id.clone(),
                |attempt| attempt.primary_node_id.clone(),
            );
        if serving != self.local_node()? || source.epoch != request.source_epoch {
            return Ok(Response::Refused {
                refusal: Refusal::NotPrimary {
                    epoch: source.epoch,
                    serving_node_id: serving,
                },
            });
        }
        let catalog_digest = catalog_ownership_digest(&catalog)?;
        if catalog_digest != request.catalog_digest {
            return Ok(Response::Refused {
                refusal: Refusal::CatalogMismatch,
            });
        }
        let shard = DfsMetadataShardId::new(&source.shard_id);
        let destination = self.metadata_root.join(&source.shard_id);
        let old = match &storage {
            Some(record) => !record.is_active(),
            // No record: the filesystem-level rule admits an activated
            // current-format copy, which serves its own tables.
            None => {
                !sinomo_dfs::inspect_metadata_storage_generation(&destination).is_ok_and(|header| {
                    header.identity.shard_id == shard
                        && matches!(
                            header.state,
                            sinomo_dfs::DfsMetadataGenerationState::Active { .. }
                        )
                })
            }
        };
        if !old {
            return Ok(Response::Refused {
                refusal: Refusal::NotOld,
            });
        }
        let path = crate::metadata::shard_capture_path(&destination, &shard)?;
        let Some(file) = CaptureFile::observe(&path)? else {
            return Ok(Response::SourcePending {
                reason: "the shard's conversion has not recorded its capture".into(),
            });
        };
        let key = (
            file.clone(),
            request.target_shard.clone(),
            catalog_digest.clone(),
        );
        let retained = self
            .contribution_extractions
            .lock()
            .map_err(error)?
            .get(&key);
        let retained_holdings = self
            .contribution_extractions
            .lock()
            .map_err(error)?
            .holdings(&(file.clone(), catalog_digest.clone()));
        let (extraction, holdings) = match (retained, retained_holdings) {
            (Some(extraction), Some(holdings)) => (extraction, Some(holdings)),
            (Some(extraction), None) if request.question.is_empty() => (extraction, None),
            _ => {
                match self.extract_contributions(&path, &shard, &catalog, &request.target_shard)? {
                    CaptureExtraction::Ready(extraction, holdings) => {
                        let mut retained = self.contribution_extractions.lock().map_err(error)?;
                        retained.insert(key, extraction.clone());
                        retained.insert_holdings(
                            (file.clone(), catalog_digest.clone()),
                            Arc::clone(&holdings),
                        );
                        (extraction, Some(holdings))
                    }
                    CaptureExtraction::Unresolved => {
                        return Ok(Response::SourcePending {
                            reason:
                                "the shard's capture still has unresolved original preparations"
                                    .into(),
                        });
                    }
                    CaptureExtraction::OtherCatalog => {
                        return Ok(Response::Refused {
                            refusal: Refusal::CatalogMismatch,
                        });
                    }
                }
            }
        };
        let storage_digest = storage_record_digest(storage_entry.as_ref());
        let facts = &extraction.basis_facts;
        let basis = AnswerBasis::OldImage {
            epoch: source.epoch,
            storage: &storage_digest,
            catalog: &catalog_digest,
            source_identity: &facts.source_identity,
            source_snapshot_fingerprint: &facts.source_snapshot_fingerprint,
            source_journal_sequence: facts.source_journal_sequence,
            source_state_fingerprint: &facts.source_state_fingerprint,
            capture: &facts.capture,
        }
        .render()?;
        let start = match &request.continuation {
            None => 0,
            Some(continuation) if continuation.basis != basis => {
                return Ok(Response::Changed { basis });
            }
            Some(continuation) => usize::try_from(continuation.after).map_err(error)?,
        };
        let total = extraction.entries.len();
        if start > total {
            return Err(error(
                "contribution continuation is past the end of its read",
            ));
        }
        let end = start.saturating_add(PAGE_ENTRIES).min(total);
        let (mut aliases, mut child_directories, mut running_jobs_touching_target) =
            (Vec::new(), Vec::new(), Vec::new());
        for entry in &extraction.entries[start..end] {
            match entry {
                Entry::Alias(alias) => aliases.push(alias.clone()),
                Entry::Child(child) => child_directories.push(child.clone()),
                Entry::Work(work) => running_jobs_touching_target.push(work.clone()),
            }
        }
        let complete = end == total;
        let held = match &holdings {
            Some(holdings) => holdings.answer(fs, &request.question)?,
            None => SourceHoldings::default(),
        };
        Ok(Response::Page {
            basis,
            aliases,
            child_directories,
            running_jobs_touching_target,
            after: (!complete)
                .then(|| u64::try_from(end))
                .transpose()
                .map_err(error)?,
            complete,
            held,
        })
    }

    /// Decode and verify the capture at `path`, then extract one target's
    /// answer.
    fn extract_contributions(
        &self,
        path: &Path,
        shard: &DfsMetadataShardId,
        catalog: &[DfsMetadataShardConfig],
        target: &str,
    ) -> Result<CaptureExtraction> {
        let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(
            path,
            MAX_CAPTURE_BYTES as u64,
        )
        .map_err(error)?;
        let captured = crate::metadata::canonical_generation::decode_captured_unit(
            &bytes,
            MAX_CAPTURE_BYTES,
            MAX_CAPTURE_RECORDS,
        )?;
        drop(bytes);
        if captured.source.physical_shard != *shard {
            return Err(error("retained capture names another physical shard"));
        }
        // The capture names the exact source files it was read from; a copy
        // that no longer has them is not the image S converts from.
        captured.verify_retained_source()?;
        if captured.has_unresolved_work_beside_recorded_rows()
            || crate::namespace_counts::has_unrecovered_preparation(&captured)
        {
            return Ok(CaptureExtraction::Unresolved);
        }
        if catalog_ownership_digest(&captured.source.projection_assignments)?
            != catalog_ownership_digest(catalog)?
        {
            return Ok(CaptureExtraction::OtherCatalog);
        }
        let limits = NamespaceCountMaintenanceLimits {
            max_objects: MAX_CAPTURE_RECORDS,
            max_encoded_bytes: 2 * MAX_CAPTURE_BYTES,
            deadline: None,
        };
        let contributions =
            crate::namespace_counts::contributions::contributions_beside_unsettled_work(
                &captured, catalog, target, limits,
            )?;
        let fs = catalog
            .iter()
            .find(|candidate| candidate.shard_id == shard.0)
            .map(|candidate| candidate.file_system_id.clone())
            .ok_or_else(|| error("contribution source left the catalog"))?;
        let scope = TargetScope {
            file_system_id: &fs,
            catalog,
            target,
        };
        let work = scope.unsettled_work(&captured.projected_rows)?;
        let work = super::owner::shard_conversion::mark_settled_by_own_conversion(
            &captured.projected_rows,
            catalog,
            &shard.0,
            work,
        )?;
        let answer = super::contribution_sources::finish_answer(contributions, work, String::new());
        let mut entries = Vec::with_capacity(
            answer.contributions.aliases.len()
                + answer.contributions.child_directories.len()
                + answer.running_jobs_touching_target.len(),
        );
        entries.extend(answer.contributions.aliases.into_iter().map(Entry::Alias));
        entries.extend(
            answer
                .contributions
                .child_directories
                .into_iter()
                .map(Entry::Child),
        );
        entries.extend(
            answer
                .running_jobs_touching_target
                .into_iter()
                .map(Entry::Work),
        );
        let holdings = crate::namespace_counts::contributions::shard_holdings(&captured, catalog, limits)?;
        let source = &captured.source;
        Ok(CaptureExtraction::Ready(
            Extraction {
                basis_facts: CaptureFacts {
                    source_identity: source.source_identity.clone(),
                    source_snapshot_fingerprint: source.source_snapshot_fingerprint.clone(),
                    source_journal_sequence: source.source_journal_sequence,
                    source_state_fingerprint: source.source_state_fingerprint.clone(),
                    capture: captured.projection_fingerprint().to_owned(),
                },
                entries: Arc::new(entries),
            },
            Arc::new(holdings),
        ))
    }
}

enum CaptureExtraction {
    Ready(Extraction, Arc<CaptureHoldings>),
    /// Original preparations still have no proven outcome; the shard's own
    /// conversion resolves them through its fenced recovery.
    Unresolved,
    /// The capture was taken under a catalog that assigns paths differently.
    OtherCatalog,
}
