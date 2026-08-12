//! Durable, platform-neutral namespace-feed reconciliation for Cloud Files.
//!
//! Concrete DFS polling and Windows placeholder effects are injected traits.
//! The state store persists an effect intent before invoking the local adapter,
//! records the manifest/no-echo ledger after the effect, and advances a shard
//! cursor only after every effect in the batch is durable.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;

use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use sinomo_dfs_cloud_files_api::{
    CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
};
use sinomo_module_api::{decode_sobj, encode_sobj};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::storage::durable_file::{
    self, DurableAtomicFileOptions, DurableFileSync, DurableLengthEndian,
    DurableRecordAppendOptions, DurableRecordReadOptions,
};

const RECONCILIATION_SCHEMA_VERSION: u16 = 1;
const RECONCILIATION_DIR: &str = "namespace-reconciliation";
const RECONCILIATION_STATE_FILE: &str = "state.sobj";
const RECONCILIATION_LOCK_FILE: &str = "state.lock";
const RECONCILIATION_TEMP_LABEL: &str = "cloud-files-state";
const MATERIALIZATION_AUDIT_INTENTS_FILE: &str = "materialization-audit-intents.sobj";
const MATERIALIZATION_AUDIT_INTENTS_TEMP_LABEL: &str = "cloud-files-materialization-audits";
const MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION: u16 = 1;
const MATERIALIZATION_AUDIT_INTENTS_MAX_BYTES: u64 = 4 * 1024 * 1024;
const CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES: u64 = 2 * 1024 * 1024;
const CURSOR_RECOVERY_REWRITE_MAX_BYTES: u64 = 256 * 1024;
const CURSOR_RECOVERY_PAGE_INTENT_FILE: &str = "cursor-recovery-page.sobj";
const CURSOR_RECOVERY_PAGE_INTENT_TEMP_LABEL: &str = "cloud-files-cursor-recovery-page";
const CURSOR_RECOVERY_PAGE_INTENT_SCHEMA_VERSION: u16 = 1;
#[cfg(not(test))]
const RECONCILIATION_STATE_MAX_BYTES: u64 = 32 * 1024 * 1024;
#[cfg(test)]
const RECONCILIATION_STATE_MAX_BYTES: u64 = 256 * 1024;
const MOUNT_SCOPE_DOMAIN: &[u8] = b"sinomo.dfs.cloud-files.mount-state.v1\0";
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const MANIFEST_DIR: &str = "manifest";
const MANIFEST_INDEX_FILE: &str = "index.sobj";
const MANIFEST_INDEX_TEMP_LABEL: &str = "cloud-files-manifest-index";
const MANIFEST_JOURNAL_FILE: &str = "active.slog";
const MANIFEST_RECORD_MAX_BYTES: usize = 1024 * 1024;
const MANIFEST_INDEX_MAX_BYTES: u64 = 1024 * 1024;
#[cfg(not(test))]
const MANIFEST_JOURNAL_COMPACT_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const MANIFEST_JOURNAL_COMPACT_BYTES: u64 = 64 * 1024;
#[cfg(not(test))]
const MANIFEST_JOURNAL_MAX_BYTES: u64 = 32 * 1024 * 1024;
#[cfg(test)]
const MANIFEST_JOURNAL_MAX_BYTES: u64 = 256 * 1024;
#[cfg(not(test))]
const MANIFEST_CHUNK_TARGET_BYTES: usize = 4 * 1024 * 1024;
#[cfg(test)]
const MANIFEST_CHUNK_TARGET_BYTES: usize = 32 * 1024;
const MANIFEST_CHUNK_MAX_BYTES: usize = 8 * 1024 * 1024;
const MANIFEST_CHUNK_TEMP_LABEL: &str = "cloud-files-manifest-chunk";
const MANIFEST_RECORD_MUTATIONS_MAX: usize = 512;
const CURSOR_RECOVERY_PAGE_INTENT_MAX_BYTES: u64 =
    RECONCILIATION_STATE_MAX_BYTES * 2 + 8 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesNamespaceCursor {
    pub file_system_id: String,
    pub shard_id: String,
    pub authority_epoch: u64,
    pub after_sequence: u64,
}

impl CloudFilesNamespaceCursor {
    pub fn shard_key(&self) -> Result<String, CloudFilesNamespaceReconcileError> {
        if self.file_system_id.trim().is_empty()
            || self.shard_id.trim().is_empty()
            || self.file_system_id.contains('/')
            || self.shard_id.contains('/')
        {
            return Err(reconcile_error(
                "invalid-cursor",
                "filesystem and shard IDs must be nonempty and slash-free",
            ));
        }
        Ok(format!("{}/{}", self.file_system_id, self.shard_id))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesNamespaceProvenance {
    pub event_id: String,
    pub journal_sequence: u64,
    pub journal_ordinal: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesNamespaceFeedEvent {
    Upsert {
        path: String,
        object_id: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_size_bytes: Option<u64>,
        kind: String,
        provenance: CloudFilesNamespaceProvenance,
    },
    Remove {
        path: String,
        object_id: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        provenance: CloudFilesNamespaceProvenance,
    },
    Rename {
        from_path: String,
        to_path: String,
        object_id: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_size_bytes: Option<u64>,
        kind: String,
        provenance: CloudFilesNamespaceProvenance,
    },
    RescanRequired {
        reason: String,
        detail: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesNamespaceFeedBatch {
    pub input_cursor: CloudFilesNamespaceCursor,
    pub next_cursor: CloudFilesNamespaceCursor,
    pub events: Vec<CloudFilesNamespaceFeedEvent>,
    pub journal_head_sequence: u64,
    pub has_more: bool,
}

pub trait CloudFilesNamespaceFeedAdapter {
    fn poll(
        &mut self,
        cursor: &CloudFilesNamespaceCursor,
    ) -> Result<CloudFilesNamespaceFeedBatch, String>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesNamespaceAction {
    Upsert {
        path: String,
        object_id: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_size_bytes: Option<u64>,
        kind: String,
    },
    Remove {
        path: String,
        object_id: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
    },
    Rename {
        from_path: String,
        to_path: String,
        object_id: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_size_bytes: Option<u64>,
        kind: String,
    },
}

impl CloudFilesNamespaceAction {
    fn paths(&self) -> Vec<&str> {
        match self {
            Self::Upsert { path, .. } | Self::Remove { path, .. } => vec![path],
            Self::Rename {
                from_path, to_path, ..
            } => vec![from_path, to_path],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesLocalPathState {
    Clean,
    Dirty { detail: String },
    Pending { operation_id: String },
}

pub trait CloudFilesNamespaceEffectAdapter {
    fn local_path_state(&mut self, path: &str) -> Result<CloudFilesLocalPathState, String>;

    /// Must be idempotent for the same action; a crash can occur after the
    /// effect but before its durable manifest record.
    fn apply(&mut self, action: &CloudFilesNamespaceAction) -> Result<(), String>;

    fn record_conflict(&mut self, _conflict: &CloudFilesNamespaceConflict) -> Result<(), String> {
        Ok(())
    }

    fn request_full_scan(&mut self, _request: &CloudFilesFullScanRequest) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesReconciledManifestEntry {
    pub path: String,
    pub object_id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesNamespaceSuppression {
    pub operation_id: String,
    pub expected_action: CloudFilesNamespaceAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesNamespaceConflict {
    pub event_id: String,
    pub action: CloudFilesNamespaceAction,
    pub local_states: BTreeMap<String, CloudFilesLocalPathState>,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesNamespaceStartupConflict {
    pub path: String,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesNamespaceConflictResolution {
    ApplyRemote,
    KeepLocal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesFullScanRequest {
    pub shard_key: String,
    pub cursor: CloudFilesNamespaceCursor,
    pub reason: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesPendingNamespaceEffect {
    event_id: String,
    shard_key: String,
    journal_sequence: u64,
    action: CloudFilesNamespaceAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolving_conflict_event_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesAppliedNamespaceEvent {
    shard_key: String,
    journal_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesNamespaceReconciliationState {
    schema_version: u16,
    mount: String,
    root: String,
    #[serde(default)]
    cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
    // Schema-v1 snapshots embedded the whole manifest. New writes omit it and
    // use the independently journaled/chunked store, while deserialization
    // still accepts that legacy field for one-time migration.
    #[serde(default, rename = "manifest", skip_serializing)]
    legacy_manifest: BTreeMap<String, CloudFilesReconciledManifestEntry>,
    #[serde(default)]
    suppressions: BTreeMap<String, CloudFilesNamespaceSuppression>,
    #[serde(default)]
    conflicts: BTreeMap<String, CloudFilesNamespaceConflict>,
    #[serde(default)]
    startup_conflicts: BTreeMap<String, CloudFilesNamespaceStartupConflict>,
    /// Directories whose children have actually been projected into the local
    /// Cloud Files namespace. Absence from the sparse manifest is not evidence
    /// of remote absence unless the owning directory is present here.
    #[serde(default)]
    materialized_directories: BTreeSet<String>,
    #[serde(default)]
    unsupported_projection_count: u64,
    #[serde(default)]
    unsupported_projection_examples: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    full_scan_request: Option<CloudFilesFullScanRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inflight_effect: Option<CloudFilesPendingNamespaceEffect>,
    #[serde(default)]
    applied_events: BTreeMap<String, CloudFilesAppliedNamespaceEvent>,
}

/// Raw owner-validated snapshot accepted by the explicit recovery. The bytes
/// are retained because the operator token binds the exact pre-migration disk
/// image, not merely the decoded cursor values.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct CloudFilesCursorOnlyReconciliationInspection {
    pub scope_path: PathBuf,
    pub raw_files: BTreeMap<String, Vec<u8>>,
    pub cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
}

pub(crate) struct CloudFilesCursorOnlyReconciliationInspectionGuard {
    inspection: CloudFilesCursorOnlyReconciliationInspection,
    _lock: File,
}

/// Lock-held, plan-bound proof that the namespace owner has already crossed
/// its Terminal durability boundary. The module keeps this guard alive while
/// it fsyncs the ReconcileArmed/TerminalPendingFinalize successor, closing the
/// otherwise unavoidable crash window between the two owner markers.
pub(crate) struct CloudFilesCursorRecoveryTerminalSuccessorGuard {
    reconciler: CloudFilesNamespaceReconciler,
}

impl CloudFilesCursorRecoveryTerminalSuccessorGuard {
    pub(crate) fn into_reconciler(self) -> CloudFilesNamespaceReconciler {
        self.reconciler
    }
}

impl CloudFilesCursorOnlyReconciliationInspectionGuard {
    pub(crate) fn inspection(&self) -> &CloudFilesCursorOnlyReconciliationInspection {
        &self.inspection
    }

    /// Builds both byte images while the owner lock and original snapshot are
    /// still held. The caller persists this plan in the module authorization
    /// marker before it establishes StateOwned registration ownership.
    pub(crate) fn authoritative_reconciliation_plan(
        &self,
        recovery_id: &str,
        recovery_token: &str,
        nonce: &str,
    ) -> Result<CloudFilesCursorRecoveryReconciliationPlan, CloudFilesNamespaceReconcileError> {
        let original_state = self
            .inspection
            .raw_files
            .get(RECONCILIATION_STATE_FILE)
            .cloned()
            .ok_or_else(|| {
                reconcile_error(
                    "cursor-recovery-state-drift",
                    "inspected state bytes are unavailable",
                )
            })?;
        let mut state: CloudFilesNamespaceReconciliationState = decode_sobj(&original_state)
            .map_err(|source| reconcile_error("decode-reconciliation-state", source.to_string()))?;
        if state.cursors != self.inspection.cursors || state.cursors.is_empty() {
            return Err(reconcile_error(
                "cursor-recovery-state-drift",
                "reconciliation cursor set changed after token inspection",
            ));
        }
        let cursor_count = u64::try_from(state.cursors.len()).map_err(|_| {
            reconcile_error(
                "cursor-recovery-cursor-count",
                "cursor count overflowed u64",
            )
        })?;
        let boundary = state
            .cursors
            .values()
            .next()
            .cloned()
            .expect("nonempty cursor-only inspection");
        state.cursors.clear();
        state.materialized_directories.insert(state.root.clone());
        state.full_scan_request = Some(CloudFilesFullScanRequest {
            shard_key: boundary.shard_key()?,
            cursor: boundary,
            reason: "cursor-only-state-recovery".to_string(),
            detail: format!(
                "explicit recovery {recovery_id} cleared stale cursors and requires a terminal authoritative root reconciliation"
            ),
        });
        let expected_state = encode_reconciliation_state(&state)?;
        let original_audit = self
            .inspection
            .raw_files
            .get(MATERIALIZATION_AUDIT_INTENTS_FILE)
            .cloned()
            .ok_or_else(|| {
                reconcile_error(
                    "cursor-recovery-state-drift",
                    "inspected audit bytes are unavailable",
                )
            })?;
        let mut audit: CloudFilesMaterializationAuditIntentState = decode_sobj(&original_audit)
            .map_err(|source| {
                reconcile_error("decode-materialization-audit-intents", source.to_string())
            })?;
        audit.directories.insert(state.root.clone());
        let expected_audit = encode_materialization_audit_intents(&audit)?;
        Ok(CloudFilesCursorRecoveryReconciliationPlan {
            schema_version: CURSOR_RECOVERY_REWRITE_SCHEMA_VERSION,
            recovery_id: recovery_id.to_string(),
            recovery_token: recovery_token.to_string(),
            nonce: nonce.to_string(),
            root: state.root,
            cursor_count_cleared: cursor_count,
            original_state,
            expected_state,
            original_audit,
            expected_audit,
        })
    }

    /// Replays the only admissible deterministic rewrite cuts. The
    /// ReconcileArmed marker is fsynced before either owner state file can
    /// change, and it remains installed until the authoritative worker has
    /// durably reached Terminal.
    pub(crate) fn apply_authoritative_reconciliation_plan(
        &self,
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<u64, CloudFilesNamespaceReconcileError> {
        let original: CloudFilesNamespaceReconciliationState = decode_sobj(&plan.original_state)
            .map_err(|source| {
                reconcile_error("decode-recovery-original-state", source.to_string())
            })?;
        let validated = validate_cursor_recovery_plan(plan, &original.mount, &original.root)?;
        if validated.original.cursors != self.inspection.cursors {
            return Err(reconcile_error(
                "cursor-recovery-state-drift",
                "authorized recovery cursor set differs from the locked inspection",
            ));
        }
        let cut = inspect_authorized_cursor_recovery_cut(&self.inspection.scope_path, plan)?;
        for path in &cut.temp_paths {
            durable_file::remove_file_if_exists_and_sync_parent(path)
                .map_err(storage_error("remove authorized cursor recovery stage"))?;
        }
        let state_path = self.inspection.scope_path.join(RECONCILIATION_STATE_FILE);
        let audit_path = self
            .inspection
            .scope_path
            .join(MATERIALIZATION_AUDIT_INTENTS_FILE);
        let rewrite_path = self
            .inspection
            .scope_path
            .join(CURSOR_RECOVERY_REWRITE_FILE);
        let armed = CloudFilesCursorRecoveryRewrite::from_plan(
            plan,
            CloudFilesCursorRecoveryRewritePhase::ReconcileArmed,
        );
        persist_cursor_recovery_rewrite(&rewrite_path, &armed)?;
        if cut.state_is_original {
            durable_file::write_file_atomic(
                &state_path,
                &plan.expected_state,
                DurableAtomicFileOptions::new(
                    RECONCILIATION_STATE_FILE,
                    CURSOR_RECOVERY_STATE_TEMP_LABEL,
                )
                .sync_all(),
            )
            .map_err(storage_error(
                "persist cursor recovery reconciliation fence",
            ))?;
        }
        if cut.audit_is_original {
            durable_file::write_file_atomic(
                &audit_path,
                &plan.expected_audit,
                DurableAtomicFileOptions::new(
                    MATERIALIZATION_AUDIT_INTENTS_FILE,
                    CURSOR_RECOVERY_AUDIT_TEMP_LABEL,
                )
                .sync_all(),
            )
            .map_err(storage_error(
                "persist cursor recovery materialization audit fence",
            ))?;
        }
        let final_state = read_bounded_plain_file(
            &state_path,
            CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES,
            "cursor recovery reconciliation state",
        )?;
        let final_audit = read_bounded_plain_file(
            &audit_path,
            CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES,
            "cursor recovery materialization audit state",
        )?;
        if final_state != plan.expected_state || final_audit != plan.expected_audit {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-torn",
                "cursor recovery did not reach the exact authorized target bytes",
            ));
        }
        Ok(plan.cursor_count_cleared)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct CloudFilesCursorRecoveryReconciliationPlan {
    pub(crate) schema_version: u16,
    pub(crate) recovery_id: String,
    pub(crate) recovery_token: String,
    pub(crate) nonce: String,
    pub(crate) root: String,
    pub(crate) cursor_count_cleared: u64,
    pub(crate) original_state: Vec<u8>,
    pub(crate) expected_state: Vec<u8>,
    pub(crate) original_audit: Vec<u8>,
    pub(crate) expected_audit: Vec<u8>,
}

impl CloudFilesCursorRecoveryReconciliationPlan {
    pub(crate) fn cursor_count_cleared(&self) -> u64 {
        self.cursor_count_cleared
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesCursorRecoveryRewrite {
    schema_version: u16,
    recovery_id: String,
    recovery_token: String,
    nonce: String,
    root: String,
    cursor_count_cleared: u64,
    original_state_fingerprint: String,
    expected_state_fingerprint: String,
    original_audit_fingerprint: String,
    expected_audit_fingerprint: String,
    #[serde(rename = "phase")]
    phase: CloudFilesCursorRecoveryRewritePhase,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum CloudFilesCursorRecoveryRewritePhase {
    ReconcileArmed,
    Terminal,
}

const CURSOR_RECOVERY_REWRITE_FILE: &str = "cursor-recovery.sobj";
const CURSOR_RECOVERY_REWRITE_SCHEMA_VERSION: u16 = 1;
const CURSOR_RECOVERY_REWRITE_TEMP_LABEL: &str = "cloud-files-cursor-recovery-rewrite";
const CURSOR_RECOVERY_STATE_TEMP_LABEL: &str = "cloud-files-cursor-recovery-state";
const CURSOR_RECOVERY_AUDIT_TEMP_LABEL: &str = "cloud-files-cursor-recovery-audit";

impl CloudFilesCursorRecoveryRewrite {
    fn from_plan(
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
        phase: CloudFilesCursorRecoveryRewritePhase,
    ) -> Self {
        Self {
            schema_version: plan.schema_version,
            recovery_id: plan.recovery_id.clone(),
            recovery_token: plan.recovery_token.clone(),
            nonce: plan.nonce.clone(),
            root: plan.root.clone(),
            cursor_count_cleared: plan.cursor_count_cleared,
            original_state_fingerprint: cursor_recovery_bytes_fingerprint(&plan.original_state),
            expected_state_fingerprint: cursor_recovery_bytes_fingerprint(&plan.expected_state),
            original_audit_fingerprint: cursor_recovery_bytes_fingerprint(&plan.original_audit),
            expected_audit_fingerprint: cursor_recovery_bytes_fingerprint(&plan.expected_audit),
            phase,
        }
    }

    fn matches_plan(&self, plan: &CloudFilesCursorRecoveryReconciliationPlan) -> bool {
        let expected = Self::from_plan(plan, self.phase);
        self == &expected
    }
}

fn cursor_recovery_bytes_fingerprint(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn persist_cursor_recovery_rewrite(
    path: &Path,
    rewrite: &CloudFilesCursorRecoveryRewrite,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    let bytes = encode_sobj(rewrite)
        .map_err(|source| reconcile_error("encode-cursor-recovery-rewrite", source.to_string()))?;
    if bytes.len() as u64 > CURSOR_RECOVERY_REWRITE_MAX_BYTES {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-too-large",
            "cursor recovery rewrite marker exceeds its durable owner bound",
        ));
    }
    durable_file::write_file_atomic(
        path,
        &bytes,
        DurableAtomicFileOptions::new(
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
        )
        .sync_all(),
    )
    .map_err(storage_error("persist cursor recovery rewrite phase"))
}

struct ValidatedCursorRecoveryPlan {
    original: CloudFilesNamespaceReconciliationState,
    expected: CloudFilesNamespaceReconciliationState,
}

fn validate_cursor_recovery_plan(
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    mount: &str,
    root: &str,
) -> Result<ValidatedCursorRecoveryPlan, CloudFilesNamespaceReconcileError> {
    if plan.schema_version != CURSOR_RECOVERY_REWRITE_SCHEMA_VERSION
        || plan.recovery_id.trim().is_empty()
        || plan.recovery_token.trim().is_empty()
        || plan.nonce.trim().is_empty()
        || plan.original_state.is_empty()
        || plan.expected_state.is_empty()
        || plan.original_audit.is_empty()
        || plan.expected_audit.is_empty()
        || plan.original_state.len() as u64 > CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES
        || plan.expected_state.len() as u64 > CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES
        || plan.original_audit.len() as u64 > CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES
        || plan.expected_audit.len() as u64 > CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-plan",
            "cursor recovery rewrite plan is malformed or exceeds its durable image bound",
        ));
    }
    let original: CloudFilesNamespaceReconciliationState = decode_sobj(&plan.original_state)
        .map_err(|source| reconcile_error("decode-recovery-original-state", source.to_string()))?;
    let expected: CloudFilesNamespaceReconciliationState = decode_sobj(&plan.expected_state)
        .map_err(|source| reconcile_error("decode-recovery-expected-state", source.to_string()))?;
    let original_audit: CloudFilesMaterializationAuditIntentState =
        decode_sobj(&plan.original_audit).map_err(|source| {
            reconcile_error("decode-recovery-original-audit", source.to_string())
        })?;
    let expected_audit: CloudFilesMaterializationAuditIntentState =
        decode_sobj(&plan.expected_audit).map_err(|source| {
            reconcile_error("decode-recovery-expected-audit", source.to_string())
        })?;
    if encode_sobj(&original)
        .map_err(|source| reconcile_error("encode-recovery-original-state", source.to_string()))?
        != plan.original_state
        || encode_sobj(&expected).map_err(|source| {
            reconcile_error("encode-recovery-expected-state", source.to_string())
        })? != plan.expected_state
        || encode_sobj(&original_audit).map_err(|source| {
            reconcile_error("encode-recovery-original-audit", source.to_string())
        })? != plan.original_audit
        || encode_sobj(&expected_audit).map_err(|source| {
            reconcile_error("encode-recovery-expected-audit", source.to_string())
        })? != plan.expected_audit
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-plan",
            "cursor recovery rewrite plan contains noncanonical state bytes",
        ));
    }

    let mut recomputed = original.clone();
    let boundary = recomputed.cursors.values().next().cloned().ok_or_else(|| {
        reconcile_error(
            "cursor-recovery-rewrite-plan",
            "authorized original state has no cursor",
        )
    })?;
    let cursor_count = u64::try_from(recomputed.cursors.len()).map_err(|_| {
        reconcile_error(
            "cursor-recovery-cursor-count",
            "cursor count overflowed u64",
        )
    })?;
    recomputed.cursors.clear();
    recomputed.materialized_directories.insert(root.to_string());
    recomputed.full_scan_request = Some(CloudFilesFullScanRequest {
        shard_key: boundary.shard_key()?,
        cursor: boundary,
        reason: "cursor-only-state-recovery".to_string(),
        detail: format!(
            "explicit recovery {} cleared stale cursors and requires a terminal authoritative root reconciliation",
            plan.recovery_id
        ),
    });
    let mut recomputed_audit = original_audit.clone();
    recomputed_audit.directories.insert(root.to_string());
    if plan.root != root
        || original.schema_version != RECONCILIATION_SCHEMA_VERSION
        || original.mount != mount
        || original.root != root
        || original.cursors.is_empty()
        || !original.legacy_manifest.is_empty()
        || !original.suppressions.is_empty()
        || !original.conflicts.is_empty()
        || !original.startup_conflicts.is_empty()
        || !original.materialized_directories.is_empty()
        || original.unsupported_projection_count != 0
        || !original.unsupported_projection_examples.is_empty()
        || original.full_scan_request.is_some()
        || original.inflight_effect.is_some()
        || !original.applied_events.is_empty()
        || expected.schema_version != RECONCILIATION_SCHEMA_VERSION
        || expected.mount != mount
        || expected.root != root
        || recomputed != expected
        || cursor_count != plan.cursor_count_cleared
        || original_audit.schema_version != MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION
        || original_audit.mount != mount
        || original_audit.root != root
        || !original_audit.directories.is_empty()
        || expected_audit.schema_version != MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION
        || expected_audit.mount != mount
        || expected_audit.root != root
        || recomputed_audit != expected_audit
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-plan",
            "cursor recovery plan does not bind the exact cursor-only transition",
        ));
    }
    for (key, cursor) in &original.cursors {
        if key != &cursor.shard_key()? || cursor.authority_epoch == 0 {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-plan",
                "cursor recovery original image contains an invalid cursor",
            ));
        }
    }
    Ok(ValidatedCursorRecoveryPlan { original, expected })
}

pub(crate) fn validate_authorized_cursor_recovery_plan(
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    mount: &str,
    root: &str,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    validate_cursor_recovery_plan(plan, mount, root).map(|_| ())
}

#[cfg(test)]
pub(crate) fn cursor_recovery_plan_fixture(
    mount: &str,
    root: &str,
    recovery_id: &str,
    recovery_token: &str,
    nonce: &str,
) -> CloudFilesCursorRecoveryReconciliationPlan {
    let cursor = CloudFilesNamespaceCursor {
        file_system_id: "sifs".to_string(),
        shard_id: "services".to_string(),
        authority_epoch: 7,
        after_sequence: 41,
    };
    let mut cursors = BTreeMap::new();
    cursors.insert(
        cursor.shard_key().expect("fixture cursor key"),
        cursor.clone(),
    );
    let original = CloudFilesNamespaceReconciliationState {
        schema_version: RECONCILIATION_SCHEMA_VERSION,
        mount: mount.to_string(),
        root: root.to_string(),
        cursors,
        legacy_manifest: BTreeMap::new(),
        suppressions: BTreeMap::new(),
        conflicts: BTreeMap::new(),
        startup_conflicts: BTreeMap::new(),
        materialized_directories: BTreeSet::new(),
        unsupported_projection_count: 0,
        unsupported_projection_examples: BTreeMap::new(),
        full_scan_request: None,
        inflight_effect: None,
        applied_events: BTreeMap::new(),
    };
    let mut expected = original.clone();
    expected.cursors.clear();
    expected.materialized_directories.insert(root.to_string());
    expected.full_scan_request = Some(CloudFilesFullScanRequest {
        shard_key: cursor.shard_key().expect("fixture cursor key"),
        cursor,
        reason: "cursor-only-state-recovery".to_string(),
        detail: format!(
            "explicit recovery {recovery_id} cleared stale cursors and requires a terminal authoritative root reconciliation"
        ),
    });
    let original_audit = CloudFilesMaterializationAuditIntentState {
        schema_version: MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION,
        mount: mount.to_string(),
        root: root.to_string(),
        directories: BTreeSet::new(),
    };
    let mut expected_audit = original_audit.clone();
    expected_audit.directories.insert(root.to_string());
    CloudFilesCursorRecoveryReconciliationPlan {
        schema_version: CURSOR_RECOVERY_REWRITE_SCHEMA_VERSION,
        recovery_id: recovery_id.to_string(),
        recovery_token: recovery_token.to_string(),
        nonce: nonce.to_string(),
        root: root.to_string(),
        cursor_count_cleared: 1,
        original_state: encode_sobj(&original).expect("fixture original state"),
        expected_state: encode_sobj(&expected).expect("fixture expected state"),
        original_audit: encode_sobj(&original_audit).expect("fixture original audit"),
        expected_audit: encode_sobj(&expected_audit).expect("fixture expected audit"),
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CursorRecoveryTempKind {
    Rewrite,
    State,
    Audit,
}

fn cursor_recovery_temp_kind(name: &str) -> Option<CursorRecoveryTempKind> {
    [
        (
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
            CursorRecoveryTempKind::Rewrite,
        ),
        (
            RECONCILIATION_STATE_FILE,
            CURSOR_RECOVERY_STATE_TEMP_LABEL,
            CursorRecoveryTempKind::State,
        ),
        (
            MATERIALIZATION_AUDIT_INTENTS_FILE,
            CURSOR_RECOVERY_AUDIT_TEMP_LABEL,
            CursorRecoveryTempKind::Audit,
        ),
    ]
    .into_iter()
    .find_map(|(file_name, label, kind)| {
        let prefix = format!(".{file_name}.{label}-");
        let suffix = name.strip_prefix(&prefix)?;
        let (pid_text, sequence_text) = suffix.split_once('-')?;
        if sequence_text.contains('-') {
            return None;
        }
        let pid = pid_text.parse::<u32>().ok()?;
        let sequence = sequence_text.parse::<u64>().ok()?;
        (pid.to_string() == pid_text && sequence.to_string() == sequence_text).then_some(kind)
    })
}

fn looks_like_cursor_recovery_temp(name: &str) -> bool {
    [
        (
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
        ),
        (RECONCILIATION_STATE_FILE, CURSOR_RECOVERY_STATE_TEMP_LABEL),
        (
            MATERIALIZATION_AUDIT_INTENTS_FILE,
            CURSOR_RECOVERY_AUDIT_TEMP_LABEL,
        ),
    ]
    .into_iter()
    .any(|(file_name, label)| name.starts_with(&format!(".{file_name}.{label}-")))
}

fn read_bounded_plain_file(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<Vec<u8>, CloudFilesNamespaceReconcileError> {
    let metadata =
        fs::symlink_metadata(path).map_err(io_error("inspect bounded cursor recovery file"))?;
    if !metadata.file_type().is_file() || metadata_is_reparse(&metadata) {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            format!("{label} is not a plain file"),
        ));
    }
    if metadata.len() > max_bytes {
        return Err(reconcile_error(
            "cursor-recovery-state-too-large",
            format!("{label} exceeds its {max_bytes}-byte recovery bound"),
        ));
    }
    let file = File::open(path).map_err(io_error("open bounded cursor recovery file"))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(io_error("read bounded cursor recovery file"))?;
    if bytes.len() as u64 > max_bytes {
        return Err(reconcile_error(
            "cursor-recovery-state-too-large",
            format!("{label} grew beyond its recovery bound while being read"),
        ));
    }
    Ok(bytes)
}

struct AuthorizedCursorRecoveryCut {
    raw_files: BTreeMap<String, Vec<u8>>,
    state_is_original: bool,
    audit_is_original: bool,
    temp_paths: Vec<PathBuf>,
}

fn inspect_authorized_cursor_recovery_cut(
    scope: &Path,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<AuthorizedCursorRecoveryCut, CloudFilesNamespaceReconcileError> {
    let mut raw_files = BTreeMap::new();
    let mut state_bytes = None;
    let mut audit_bytes = None;
    let mut rewrite = None;
    let mut temps = BTreeMap::<CursorRecoveryTempKind, (PathBuf, Vec<u8>)>::new();
    let mut saw_lock = false;
    let mut saw_manifest = false;
    for entry in fs::read_dir(scope).map_err(io_error("list authorized reconciliation scope"))? {
        let entry = entry.map_err(io_error("read authorized reconciliation scope entry"))?;
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "authorized reconciliation scope contains a non-UTF-8 entry",
            )
        })?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(io_error("inspect authorized reconciliation scope entry"))?;
        if metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("authorized reconciliation contains reparse evidence `{name}`"),
            ));
        }
        match name.as_str() {
            RECONCILIATION_STATE_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(
                    &path,
                    CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES,
                    "authorized reconciliation state",
                )?;
                raw_files.insert(name, bytes.clone());
                state_bytes = Some(bytes);
            }
            MATERIALIZATION_AUDIT_INTENTS_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(
                    &path,
                    CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES,
                    "authorized materialization audit state",
                )?;
                raw_files.insert(name, bytes.clone());
                audit_bytes = Some(bytes);
            }
            RECONCILIATION_LOCK_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(&path, 0, "authorized reconciliation lock")?;
                raw_files.insert(name, bytes);
                saw_lock = true;
            }
            MANIFEST_DIR if metadata.file_type().is_dir() => {
                inspect_empty_cursor_recovery_manifest(&path, &mut raw_files)?;
                saw_manifest = true;
            }
            CURSOR_RECOVERY_REWRITE_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(
                    &path,
                    CURSOR_RECOVERY_REWRITE_MAX_BYTES,
                    "authorized cursor recovery rewrite marker",
                )?;
                let decoded: CloudFilesCursorRecoveryRewrite =
                    decode_sobj(&bytes).map_err(|source| {
                        reconcile_error("decode-cursor-recovery-rewrite", source.to_string())
                    })?;
                if encode_sobj(&decoded).map_err(|source| {
                    reconcile_error("encode-cursor-recovery-rewrite", source.to_string())
                })? != bytes
                    || !decoded.matches_plan(plan)
                    || decoded.phase != CloudFilesCursorRecoveryRewritePhase::ReconcileArmed
                {
                    return Err(reconcile_error(
                        "cursor-recovery-rewrite-mismatch",
                        "authorized rewrite marker is noncanonical, non-armed, or mismatched",
                    ));
                }
                raw_files.insert(name, bytes);
                rewrite = Some(decoded);
            }
            _ => {
                let Some(kind) = cursor_recovery_temp_kind(&name) else {
                    return Err(reconcile_error(
                        "cursor-recovery-reconciliation-topology",
                        format!("unexpected authorized reconciliation entry `{name}`"),
                    ));
                };
                let max_bytes = match kind {
                    CursorRecoveryTempKind::Rewrite => CURSOR_RECOVERY_REWRITE_MAX_BYTES,
                    CursorRecoveryTempKind::State | CursorRecoveryTempKind::Audit => {
                        CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES
                    }
                };
                let bytes =
                    read_bounded_plain_file(&path, max_bytes, "authorized cursor recovery stage")?;
                if temps.insert(kind, (path, bytes.clone())).is_some() {
                    return Err(reconcile_error(
                        "cursor-recovery-rewrite-torn",
                        "multiple cursor recovery stages target one durable owner file",
                    ));
                }
                raw_files.insert(name, bytes);
            }
        }
    }
    if !saw_lock || !saw_manifest {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "authorized reconciliation scope is missing its lock or empty manifest",
        ));
    }
    let state_bytes = state_bytes.ok_or_else(|| {
        reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "authorized reconciliation scope is missing state.sobj",
        )
    })?;
    let audit_bytes = audit_bytes.ok_or_else(|| {
        reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "authorized reconciliation scope is missing audit state",
        )
    })?;
    let state_is_original = state_bytes == plan.original_state;
    let state_is_expected = state_bytes == plan.expected_state;
    let audit_is_original = audit_bytes == plan.original_audit;
    let audit_is_expected = audit_bytes == plan.expected_audit;
    let rewrite_present = rewrite.is_some();
    let cut_is_valid = if rewrite_present {
        (state_is_original && audit_is_original)
            || (state_is_expected && audit_is_original)
            || (state_is_expected && audit_is_expected)
    } else {
        state_is_original && audit_is_original
    };
    if !cut_is_valid {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-torn",
            "reconciliation state is outside the exact authorized rewrite-cut set",
        ));
    }

    let armed_bytes = encode_sobj(&CloudFilesCursorRecoveryRewrite::from_plan(
        plan,
        CloudFilesCursorRecoveryRewritePhase::ReconcileArmed,
    ))
    .map_err(|source| reconcile_error("encode-cursor-recovery-rewrite", source.to_string()))?;
    if let Some((_, bytes)) = temps.get(&CursorRecoveryTempKind::Rewrite)
        && bytes != &armed_bytes
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-mismatch",
            "cursor recovery rewrite stage does not encode the exact armed marker",
        ));
    }
    if let Some((_, bytes)) = temps.get(&CursorRecoveryTempKind::State)
        && bytes != &plan.expected_state
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-mismatch",
            "cursor recovery state stage does not encode the exact expected state",
        ));
    }
    if let Some((_, bytes)) = temps.get(&CursorRecoveryTempKind::Audit)
        && bytes != &plan.expected_audit
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-mismatch",
            "cursor recovery audit stage does not encode the exact expected audit state",
        ));
    }
    if temps.len() > 1 {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-torn",
            "cursor recovery permits at most one exact durable stage at a time",
        ));
    }
    let predecessor_matches = match temps.keys().next().copied() {
        None => true,
        Some(CursorRecoveryTempKind::Rewrite) => {
            !rewrite_present && state_is_original && audit_is_original
        }
        Some(CursorRecoveryTempKind::State) => {
            rewrite_present && state_is_original && audit_is_original
        }
        Some(CursorRecoveryTempKind::Audit) => {
            rewrite_present && state_is_expected && audit_is_original
        }
    };
    if !predecessor_matches {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-torn",
            "cursor recovery stage does not follow its exact durable predecessor cut",
        ));
    }
    Ok(AuthorizedCursorRecoveryCut {
        raw_files,
        state_is_original,
        audit_is_original,
        temp_paths: temps.into_values().map(|(path, _)| path).collect(),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesManifestIndex {
    schema_version: u16,
    generation: u64,
    applied_through_sequence: u64,
    chunks: Vec<CloudFilesManifestChunkReference>,
}

impl Default for CloudFilesManifestIndex {
    fn default() -> Self {
        Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation: 0,
            applied_through_sequence: 0,
            chunks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesManifestChunkReference {
    file_name: String,
    entry_count: u64,
    content_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesManifestChunk {
    schema_version: u16,
    generation: u64,
    ordinal: u64,
    entries: Vec<CloudFilesReconciledManifestEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum CloudFilesManifestMutation {
    Upsert {
        entry: CloudFilesReconciledManifestEntry,
    },
    Remove {
        path: String,
    },
}

/// Crash fence for one authoritative cursor-recovery page. The sidecar is
/// installed and fsynced before the manifest transition. It binds the exact
/// owner plan, state images, logical manifest images, and affected manifest
/// entries so an authorized reopen can distinguish only the three protocol
/// cuts: before/before, before/after, and after/after.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesCursorRecoveryPageIntent {
    schema_version: u16,
    recovery_id: String,
    recovery_token: String,
    nonce: String,
    plan_fingerprint: String,
    mount: String,
    root: String,
    before_state_fingerprint: String,
    before_state: Vec<u8>,
    after_state_fingerprint: String,
    after_state: Vec<u8>,
    before_manifest_fingerprint: String,
    after_manifest_fingerprint: String,
    mutations: Vec<CloudFilesManifestMutation>,
    previous_manifest_entries: BTreeMap<String, Option<CloudFilesReconciledManifestEntry>>,
    materialized_directories: Vec<String>,
    cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
    scan_complete: bool,
}

#[derive(Clone, Debug)]
struct CloudFilesCursorRecoveryPage {
    materialized_directories: Vec<String>,
    cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
    scan_complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesManifestJournalRecordBody {
    schema_version: u16,
    sequence: u64,
    mutations: Vec<CloudFilesManifestMutation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesManifestJournalRecord {
    body: CloudFilesManifestJournalRecordBody,
    checksum: String,
}

struct CloudFilesManifestStore {
    dir: PathBuf,
    index_path: PathBuf,
    journal_path: PathBuf,
    journal: Option<File>,
    index: CloudFilesManifestIndex,
    next_sequence: u64,
    journal_bytes: u64,
    usable: bool,
}

impl CloudFilesManifestStore {
    fn open(
        reconciliation_dir: &Path,
        legacy_manifest: BTreeMap<String, CloudFilesReconciledManifestEntry>,
    ) -> Result<
        (
            Self,
            BTreeMap<String, CloudFilesReconciledManifestEntry>,
            bool,
        ),
        CloudFilesNamespaceReconcileError,
    > {
        let dir = reconciliation_dir.join(MANIFEST_DIR);
        durable_file::ensure_private_directory(&dir)
            .map_err(storage_error("create namespace manifest directory"))?;
        let index_path = dir.join(MANIFEST_INDEX_FILE);
        let journal_path = dir.join(MANIFEST_JOURNAL_FILE);
        let index = load_manifest_index(&index_path)?.unwrap_or_default();
        let index_exists = index_path.exists();
        let mut manifest = if index_exists {
            load_manifest_chunks(&dir, &index)?
        } else {
            legacy_manifest.clone()
        };
        let journal = durable_file::open_file_for_append(&journal_path)
            .map_err(storage_error("open namespace manifest journal"))?
            .file;
        let mut store = Self {
            dir,
            index_path,
            journal_path,
            journal: Some(journal),
            next_sequence: index.applied_through_sequence.saturating_add(1),
            journal_bytes: 0,
            index,
            usable: true,
        };
        store.replay_and_repair_torn_tail(&mut manifest)?;
        let migrated_legacy = !legacy_manifest.is_empty();
        if !index_exists {
            store.install_compacted_manifest(&manifest)?;
        } else {
            store.remove_unreferenced_chunks()?;
        }
        Ok((store, manifest, migrated_legacy))
    }

    /// Opens an already-owned manifest without creating, compacting, pruning,
    /// or repairing any artifact. Cursor-recovery terminal admission uses this
    /// path because observing a Terminal successor must be a strict proof of
    /// the bytes already on disk, not a side effect of the opener.
    fn open_authorized_cursor_recovery_terminal(
        reconciliation_dir: &Path,
    ) -> Result<
        (Self, BTreeMap<String, CloudFilesReconciledManifestEntry>),
        CloudFilesNamespaceReconcileError,
    > {
        let dir = reconciliation_dir.join(MANIFEST_DIR);
        let dir_metadata = fs::symlink_metadata(&dir)
            .map_err(io_error("inspect terminal recovery manifest directory"))?;
        if !dir_metadata.file_type().is_dir() || metadata_is_reparse(&dir_metadata) {
            return Err(reconcile_error(
                "cursor-recovery-manifest-topology",
                "terminal recovery manifest is not a plain directory",
            ));
        }

        let index_path = dir.join(MANIFEST_INDEX_FILE);
        let index_bytes = read_bounded_plain_file(
            &index_path,
            MANIFEST_INDEX_MAX_BYTES,
            "terminal recovery manifest index",
        )?;
        let index: CloudFilesManifestIndex = decode_sobj(&index_bytes)
            .map_err(|source| reconcile_error("decode-manifest-index", source.to_string()))?;
        if index.schema_version != MANIFEST_SCHEMA_VERSION
            || index.generation == 0
            || encode_sobj(&index)
                .map_err(|source| reconcile_error("encode-manifest-index", source.to_string()))?
                != index_bytes
        {
            return Err(reconcile_error(
                "cursor-recovery-manifest-invalid",
                "terminal recovery manifest index is noncanonical or unsupported",
            ));
        }

        let mut expected_names = BTreeSet::from([
            MANIFEST_INDEX_FILE.to_string(),
            MANIFEST_JOURNAL_FILE.to_string(),
        ]);
        let mut manifest = BTreeMap::new();
        for (ordinal, reference) in index.chunks.iter().enumerate() {
            let ordinal = u64::try_from(ordinal).map_err(|_| {
                reconcile_error(
                    "manifest-chunk-overflow",
                    "terminal recovery manifest chunk ordinal overflowed",
                )
            })?;
            if reference.file_name != manifest_chunk_file_name(index.generation, ordinal)
                || reference.file_name.contains('/')
                || reference.file_name.contains('\\')
                || !expected_names.insert(reference.file_name.clone())
            {
                return Err(reconcile_error(
                    "cursor-recovery-manifest-invalid",
                    "terminal recovery manifest index contains a duplicate or invalid chunk path",
                ));
            }
            let bytes = read_bounded_plain_file(
                &dir.join(&reference.file_name),
                MANIFEST_CHUNK_MAX_BYTES as u64,
                "terminal recovery manifest chunk",
            )?;
            if bytes_hash(&bytes) != reference.content_hash {
                return Err(reconcile_error(
                    "manifest-chunk-checksum",
                    format!(
                        "terminal recovery manifest chunk `{}` failed checksum validation",
                        reference.file_name
                    ),
                ));
            }
            let chunk: CloudFilesManifestChunk = decode_sobj(&bytes)
                .map_err(|source| reconcile_error("decode-manifest-chunk", source.to_string()))?;
            if chunk.schema_version != MANIFEST_SCHEMA_VERSION
                || chunk.generation != index.generation
                || chunk.ordinal != ordinal
                || u64::try_from(chunk.entries.len()).unwrap_or(u64::MAX) != reference.entry_count
                || encode_sobj(&chunk).map_err(|source| {
                    reconcile_error("encode-manifest-chunk", source.to_string())
                })? != bytes
            {
                return Err(reconcile_error(
                    "cursor-recovery-manifest-invalid",
                    format!(
                        "terminal recovery manifest chunk `{}` is noncanonical or mismatched",
                        reference.file_name
                    ),
                ));
            }
            for entry in chunk.entries {
                validate_manifest_entry(&entry)?;
                if manifest.insert(entry.path.clone(), entry).is_some() {
                    return Err(reconcile_error(
                        "cursor-recovery-manifest-invalid",
                        "terminal recovery manifest contains a duplicate path",
                    ));
                }
            }
        }

        for entry in fs::read_dir(&dir).map_err(io_error("list terminal recovery manifest"))? {
            let entry = entry.map_err(io_error("read terminal recovery manifest entry"))?;
            let name = entry.file_name().into_string().map_err(|_| {
                reconcile_error(
                    "cursor-recovery-manifest-topology",
                    "terminal recovery manifest contains a non-UTF-8 entry",
                )
            })?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(io_error("inspect terminal recovery manifest entry"))?;
            if !metadata.file_type().is_file()
                || metadata_is_reparse(&metadata)
                || !expected_names.contains(&name)
            {
                return Err(reconcile_error(
                    "cursor-recovery-manifest-topology",
                    format!(
                        "terminal recovery manifest contains unexpected or non-plain entry `{name}`"
                    ),
                ));
            }
        }

        let journal_path = dir.join(MANIFEST_JOURNAL_FILE);
        let journal_metadata = fs::symlink_metadata(&journal_path)
            .map_err(io_error("inspect terminal recovery manifest journal"))?;
        if !journal_metadata.file_type().is_file()
            || metadata_is_reparse(&journal_metadata)
            || journal_metadata.len() > MANIFEST_JOURNAL_MAX_BYTES
        {
            return Err(reconcile_error(
                "cursor-recovery-manifest-topology",
                "terminal recovery manifest journal is not a bounded plain file",
            ));
        }
        let mut journal = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(false)
            .open(&journal_path)
            .map_err(io_error("open terminal recovery manifest journal"))?;
        journal
            .seek(SeekFrom::Start(0))
            .map_err(io_error("rewind terminal recovery manifest journal"))?;
        let mut last_sequence = None;
        let mut expected_live_sequence = index.applied_through_sequence.saturating_add(1);
        let mut last_valid_offset = 0_u64;
        loop {
            let record = durable_file::read_length_prefixed_record(
                &mut journal,
                &journal_path,
                DurableRecordReadOptions::new(DurableLengthEndian::Little)
                    .max_record_bytes(MANIFEST_RECORD_MAX_BYTES),
            )
            .map_err(storage_error(
                "strictly read terminal recovery manifest journal",
            ))?;
            let Some(record) = record else {
                break;
            };
            let decoded: CloudFilesManifestJournalRecord = decode_sobj(&record.bytes)
                .map_err(|source| reconcile_error("decode-manifest-record", source.to_string()))?;
            validate_manifest_record(&decoded)?;
            if encode_sobj(&decoded)
                .map_err(|source| reconcile_error("encode-manifest-record", source.to_string()))?
                != record.bytes
            {
                return Err(reconcile_error(
                    "cursor-recovery-manifest-invalid",
                    "terminal recovery manifest journal contains a noncanonical record",
                ));
            }
            let sequence = decoded.body.sequence;
            if last_sequence.is_some_and(|previous| sequence <= previous) {
                return Err(reconcile_error(
                    "manifest-sequence-order",
                    "terminal recovery manifest sequences are not strictly increasing",
                ));
            }
            if sequence > index.applied_through_sequence {
                if sequence != expected_live_sequence {
                    return Err(reconcile_error(
                        "manifest-sequence-gap",
                        format!(
                            "terminal recovery expected manifest sequence {expected_live_sequence}, observed {sequence}"
                        ),
                    ));
                }
                apply_manifest_mutations(&mut manifest, &decoded.body.mutations);
                expected_live_sequence = expected_live_sequence.saturating_add(1);
            }
            last_sequence = Some(sequence);
            last_valid_offset = record.record_end;
        }
        if last_valid_offset != journal_metadata.len() {
            return Err(reconcile_error(
                "cursor-recovery-manifest-torn",
                "terminal recovery manifest journal has an incomplete trailing record",
            ));
        }
        journal
            .seek(SeekFrom::End(0))
            .map_err(io_error("seek terminal recovery manifest journal"))?;
        Ok((
            Self {
                dir,
                index_path,
                journal_path,
                journal: Some(journal),
                index,
                next_sequence: expected_live_sequence,
                journal_bytes: journal_metadata.len(),
                usable: true,
            },
            manifest,
        ))
    }

    fn append_mutations(
        &mut self,
        mutations: Vec<CloudFilesManifestMutation>,
        current: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        if mutations.is_empty() {
            return Ok(());
        }
        let records = self.encode_mutation_records(mutations)?;
        let framed_bytes = records.iter().try_fold(0_u64, |total, bytes| {
            let bytes = u64::try_from(bytes.len()).map_err(|_| {
                reconcile_error(
                    "manifest-journal-too-large",
                    "namespace manifest record length overflowed",
                )
            })?;
            total
                .checked_add(4_u64.saturating_add(bytes))
                .ok_or_else(|| {
                    reconcile_error(
                        "manifest-journal-too-large",
                        "namespace manifest journal length overflowed",
                    )
                })
        })?;
        if self.journal_bytes.saturating_add(framed_bytes) > MANIFEST_JOURNAL_MAX_BYTES {
            // Installing a compact snapshot of the target state is itself a
            // durable manifest transition. The caller persists any cursor or
            // suppression state only after this returns.
            return self.install_compacted_manifest(current);
        }
        let record_refs = records.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let append_result = durable_file::append_length_prefixed_records(
            self.journal.as_mut().ok_or_else(|| {
                reconcile_error(
                    "reconciler-reopen-required",
                    "namespace manifest journal is not open",
                )
            })?,
            &self.journal_path,
            record_refs,
            DurableRecordAppendOptions::new(DurableLengthEndian::Little).sync(DurableFileSync::All),
        );
        if let Err(source) = append_result {
            self.usable = false;
            return Err(storage_error("append and fsync namespace manifest journal")(source));
        }
        self.next_sequence = self
            .next_sequence
            .checked_add(u64::try_from(records.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                reconcile_error(
                    "manifest-sequence-overflow",
                    "namespace manifest sequence overflowed",
                )
            })?;
        self.journal_bytes = self.journal_bytes.saturating_add(framed_bytes);
        Ok(())
    }

    fn compact_if_needed(
        &mut self,
        manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        if self.journal_bytes >= MANIFEST_JOURNAL_COMPACT_BYTES {
            self.install_compacted_manifest(manifest)?;
        }
        Ok(())
    }

    fn encode_mutation_records(
        &self,
        mutations: Vec<CloudFilesManifestMutation>,
    ) -> Result<Vec<Vec<u8>>, CloudFilesNamespaceReconcileError> {
        let mut records = Vec::new();
        let mut batch = Vec::new();
        let mut sequence = self.next_sequence;
        for mutation in mutations {
            batch.push(mutation);
            let at_limit = batch.len() >= MANIFEST_RECORD_MUTATIONS_MAX;
            let encoded = encode_manifest_record(sequence, &batch)?;
            if encoded.len() > MANIFEST_RECORD_MAX_BYTES {
                let last = batch.pop().expect("just pushed manifest mutation");
                if batch.is_empty() {
                    return Err(reconcile_error(
                        "manifest-record-too-large",
                        "one namespace manifest mutation exceeds its durable record bound",
                    ));
                }
                records.push(encode_manifest_record(sequence, &batch)?);
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    reconcile_error(
                        "manifest-sequence-overflow",
                        "namespace manifest sequence overflowed",
                    )
                })?;
                batch = vec![last];
                let single = encode_manifest_record(sequence, &batch)?;
                if single.len() > MANIFEST_RECORD_MAX_BYTES {
                    return Err(reconcile_error(
                        "manifest-record-too-large",
                        "one namespace manifest mutation exceeds its durable record bound",
                    ));
                }
            } else if at_limit {
                records.push(encoded);
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    reconcile_error(
                        "manifest-sequence-overflow",
                        "namespace manifest sequence overflowed",
                    )
                })?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            records.push(encode_manifest_record(sequence, &batch)?);
        }
        Ok(records)
    }

    fn install_compacted_manifest(
        &mut self,
        manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let generation = self.index.generation.checked_add(1).ok_or_else(|| {
            reconcile_error(
                "manifest-generation-overflow",
                "namespace manifest generation overflowed",
            )
        })?;
        let chunks = encode_manifest_chunks(generation, manifest)?;
        let mut references = Vec::with_capacity(chunks.len());
        for (ordinal, bytes) in chunks.into_iter().enumerate() {
            let ordinal = u64::try_from(ordinal).map_err(|_| {
                reconcile_error(
                    "manifest-chunk-overflow",
                    "namespace manifest chunk ordinal overflowed",
                )
            })?;
            let file_name = manifest_chunk_file_name(generation, ordinal);
            let path = self.dir.join(&file_name);
            if let Err(source) = durable_file::write_file_atomic(
                &path,
                &bytes,
                DurableAtomicFileOptions::new(&file_name, MANIFEST_CHUNK_TEMP_LABEL).sync_all(),
            ) {
                self.usable = false;
                return Err(storage_error("persist namespace manifest chunk")(source));
            }
            let chunk = decode_sobj::<CloudFilesManifestChunk>(&bytes)
                .map_err(|source| reconcile_error("decode-manifest-chunk", source.to_string()))?;
            references.push(CloudFilesManifestChunkReference {
                file_name,
                entry_count: u64::try_from(chunk.entries.len()).unwrap_or(u64::MAX),
                content_hash: bytes_hash(&bytes),
            });
        }
        let next_index = CloudFilesManifestIndex {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation,
            applied_through_sequence: self.next_sequence.saturating_sub(1),
            chunks: references,
        };
        let index_bytes = encode_sobj(&next_index)
            .map_err(|source| reconcile_error("encode-manifest-index", source.to_string()))?;
        if let Err(source) = durable_file::write_file_atomic(
            &self.index_path,
            &index_bytes,
            DurableAtomicFileOptions::new(MANIFEST_INDEX_FILE, MANIFEST_INDEX_TEMP_LABEL)
                .sync_all(),
        ) {
            self.usable = false;
            return Err(storage_error("persist namespace manifest index")(source));
        }
        let journal = self.journal.as_mut().ok_or_else(|| {
            reconcile_error(
                "reconciler-reopen-required",
                "namespace manifest journal is not open",
            )
        })?;
        if let Err(source) = durable_file::truncate_open_file_and_sync(
            journal,
            &self.journal_path,
            0,
            DurableFileSync::All,
        ) {
            self.usable = false;
            return Err(storage_error(
                "truncate compacted namespace manifest journal",
            )(source));
        }
        self.index = next_index;
        self.journal_bytes = 0;
        self.remove_unreferenced_chunks()?;
        Ok(())
    }

    fn replay_and_repair_torn_tail(
        &mut self,
        manifest: &mut BTreeMap<String, CloudFilesReconciledManifestEntry>,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        let mut journal = self.journal.take().ok_or_else(|| {
            reconcile_error(
                "reconciler-reopen-required",
                "namespace manifest journal is not open",
            )
        })?;
        journal
            .seek(SeekFrom::Start(0))
            .map_err(io_error("rewind namespace manifest journal"))?;
        let mut last_sequence = None;
        let mut expected_live_sequence = self.index.applied_through_sequence.saturating_add(1);
        let mut last_valid_offset = 0_u64;
        loop {
            let record = match durable_file::read_length_prefixed_record(
                &mut journal,
                &self.journal_path,
                DurableRecordReadOptions::new(DurableLengthEndian::Little)
                    .max_record_bytes(MANIFEST_RECORD_MAX_BYTES),
            ) {
                Ok(record) => record,
                Err(durable_file::DurableFileError::RecordTooLarge { record_bytes, .. }) => {
                    let file_len = journal
                        .metadata()
                        .map_err(io_error("inspect namespace manifest journal"))?
                        .len();
                    let declared_end = last_valid_offset
                        .saturating_add(4)
                        .saturating_add(u64::try_from(record_bytes).unwrap_or(u64::MAX));
                    if declared_end <= file_len {
                        return Err(reconcile_error(
                            "manifest-record-too-large",
                            format!(
                                "complete namespace manifest record declares {record_bytes} bytes; max is {MANIFEST_RECORD_MAX_BYTES}"
                            ),
                        ));
                    }
                    durable_file::truncate_open_file_and_sync(
                        &mut journal,
                        &self.journal_path,
                        last_valid_offset,
                        DurableFileSync::All,
                    )
                    .map_err(storage_error("truncate torn namespace manifest header"))?;
                    break;
                }
                Err(source) => {
                    return Err(storage_error("replay namespace manifest journal")(source));
                }
            };
            let Some(record) = record else {
                let file_len = journal
                    .metadata()
                    .map_err(io_error("inspect namespace manifest journal"))?
                    .len();
                if last_valid_offset < file_len {
                    durable_file::truncate_open_file_and_sync(
                        &mut journal,
                        &self.journal_path,
                        last_valid_offset,
                        DurableFileSync::All,
                    )
                    .map_err(storage_error("truncate torn namespace manifest tail"))?;
                }
                break;
            };
            let decoded = decode_sobj::<CloudFilesManifestJournalRecord>(&record.bytes)
                .map_err(|source| reconcile_error("decode-manifest-record", source.to_string()))?;
            validate_manifest_record(&decoded)?;
            let sequence = decoded.body.sequence;
            if last_sequence.is_some_and(|previous| sequence <= previous) {
                return Err(reconcile_error(
                    "manifest-sequence-order",
                    "namespace manifest journal sequences are not strictly increasing",
                ));
            }
            if sequence > self.index.applied_through_sequence {
                if sequence != expected_live_sequence {
                    return Err(reconcile_error(
                        "manifest-sequence-gap",
                        format!(
                            "expected namespace manifest sequence {expected_live_sequence}, observed {sequence}"
                        ),
                    ));
                }
                apply_manifest_mutations(manifest, &decoded.body.mutations);
                expected_live_sequence = expected_live_sequence.saturating_add(1);
            }
            last_sequence = Some(sequence);
            last_valid_offset = record.record_end;
        }
        self.next_sequence = expected_live_sequence;
        self.journal_bytes = journal
            .metadata()
            .map_err(io_error("inspect replayed namespace manifest journal"))?
            .len();
        journal
            .seek(SeekFrom::End(0))
            .map_err(io_error("seek namespace manifest journal for append"))?;
        self.journal = Some(journal);
        Ok(())
    }

    fn remove_unreferenced_chunks(&mut self) -> Result<(), CloudFilesNamespaceReconcileError> {
        let retained = self
            .index
            .chunks
            .iter()
            .map(|chunk| chunk.file_name.as_str())
            .collect::<BTreeSet<_>>();
        for entry in fs::read_dir(&self.dir).map_err(io_error("list namespace manifest files"))? {
            let entry = entry.map_err(io_error("read namespace manifest file"))?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.starts_with("chunk-")
                || !file_name.ends_with(".sobj")
                || retained.contains(file_name.as_ref())
            {
                continue;
            }
            durable_file::remove_file_if_exists_and_sync_parent(&entry.path())
                .map_err(storage_error("remove obsolete namespace manifest chunk"))?;
        }
        Ok(())
    }

    fn ensure_usable(&self) -> Result<(), CloudFilesNamespaceReconcileError> {
        if self.usable {
            Ok(())
        } else {
            Err(reconcile_error(
                "reconciler-reopen-required",
                "a prior namespace manifest write was uncertain; reopen before continuing",
            ))
        }
    }
}

fn manifest_transition(
    previous: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    current: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> Vec<CloudFilesManifestMutation> {
    let mut mutations = previous
        .keys()
        .filter(|path| !current.contains_key(*path))
        .map(|path| CloudFilesManifestMutation::Remove { path: path.clone() })
        .collect::<Vec<_>>();
    mutations.extend(
        current
            .iter()
            .filter(|(path, entry)| previous.get(*path) != Some(*entry))
            .map(|(_, entry)| CloudFilesManifestMutation::Upsert {
                entry: entry.clone(),
            }),
    );
    mutations
}

type CloudFilesManifestUndo = Vec<(String, Option<CloudFilesReconciledManifestEntry>)>;

fn manifest_mutations_for_action(
    action: &CloudFilesNamespaceAction,
) -> Vec<CloudFilesManifestMutation> {
    match action {
        CloudFilesNamespaceAction::Upsert {
            path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
        } => vec![CloudFilesManifestMutation::Upsert {
            entry: CloudFilesReconciledManifestEntry {
                path: path.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                content_size_bytes: *content_size_bytes,
                kind: kind.clone(),
            },
        }],
        CloudFilesNamespaceAction::Remove { path, .. } => {
            vec![CloudFilesManifestMutation::Remove { path: path.clone() }]
        }
        CloudFilesNamespaceAction::Rename {
            from_path,
            to_path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
        } => vec![
            CloudFilesManifestMutation::Remove {
                path: from_path.clone(),
            },
            CloudFilesManifestMutation::Upsert {
                entry: CloudFilesReconciledManifestEntry {
                    path: to_path.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: content_hash.clone(),
                    content_size_bytes: *content_size_bytes,
                    kind: kind.clone(),
                },
            },
        ],
    }
}

fn apply_manifest_mutations_with_undo(
    manifest: &mut BTreeMap<String, CloudFilesReconciledManifestEntry>,
    mutations: &[CloudFilesManifestMutation],
) -> CloudFilesManifestUndo {
    let mut seen = BTreeSet::new();
    let mut undo = Vec::new();
    for mutation in mutations {
        let path = match mutation {
            CloudFilesManifestMutation::Upsert { entry } => &entry.path,
            CloudFilesManifestMutation::Remove { path } => path,
        };
        if seen.insert(path.clone()) {
            undo.push((path.clone(), manifest.get(path).cloned()));
        }
    }
    apply_manifest_mutations(manifest, mutations);
    undo
}

fn restore_manifest(
    manifest: &mut BTreeMap<String, CloudFilesReconciledManifestEntry>,
    undo: CloudFilesManifestUndo,
) {
    for (path, previous) in undo {
        match previous {
            Some(entry) => {
                manifest.insert(path, entry);
            }
            None => {
                manifest.remove(&path);
            }
        }
    }
}

fn unsupported_projection_kind(entry: &CloudFilesReconciledManifestEntry) -> Option<&str> {
    matches!(entry.kind.as_str(), "symlink" | "hardlink" | "artifact")
        .then_some(entry.kind.as_str())
}

fn unsupported_projection_summary(
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> (u64, BTreeMap<String, String>) {
    let mut count = 0u64;
    let mut examples = BTreeMap::new();
    for (path, entry) in manifest {
        let Some(kind) = unsupported_projection_kind(entry) else {
            continue;
        };
        count = count.saturating_add(1);
        if examples.len() < CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT {
            examples.insert(path.clone(), kind.to_string());
        }
    }
    (count, examples)
}

fn update_unsupported_projection_summary(
    state: &mut CloudFilesNamespaceReconciliationState,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    undo: &CloudFilesManifestUndo,
) {
    for (path, previous) in undo {
        let was_unsupported = previous
            .as_ref()
            .and_then(unsupported_projection_kind)
            .is_some();
        let current_kind = manifest.get(path).and_then(unsupported_projection_kind);
        let is_unsupported = current_kind.is_some();
        match (was_unsupported, is_unsupported) {
            (true, false) => {
                state.unsupported_projection_count =
                    state.unsupported_projection_count.saturating_sub(1);
            }
            (false, true) => {
                state.unsupported_projection_count =
                    state.unsupported_projection_count.saturating_add(1);
            }
            _ => {}
        }
        state.unsupported_projection_examples.remove(path);
        if let Some(kind) = current_kind
            && state.unsupported_projection_examples.len()
                < CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT
        {
            state
                .unsupported_projection_examples
                .insert(path.clone(), kind.to_string());
        }
    }
}

fn apply_manifest_mutations(
    manifest: &mut BTreeMap<String, CloudFilesReconciledManifestEntry>,
    mutations: &[CloudFilesManifestMutation],
) {
    for mutation in mutations {
        match mutation {
            CloudFilesManifestMutation::Upsert { entry } => {
                manifest.insert(entry.path.clone(), entry.clone());
            }
            CloudFilesManifestMutation::Remove { path } => {
                manifest.remove(path);
            }
        }
    }
}

fn manifest_mutation_path(mutation: &CloudFilesManifestMutation) -> &str {
    match mutation {
        CloudFilesManifestMutation::Upsert { entry } => &entry.path,
        CloudFilesManifestMutation::Remove { path } => path,
    }
}

fn cursor_recovery_plan_fingerprint(
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<String, CloudFilesNamespaceReconcileError> {
    let bytes = encode_sobj(plan)
        .map_err(|source| reconcile_error("encode-cursor-recovery-plan", source.to_string()))?;
    Ok(cursor_recovery_bytes_fingerprint(&bytes))
}

fn cursor_recovery_manifest_fingerprint(
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> Result<String, CloudFilesNamespaceReconcileError> {
    const DOMAIN: &[u8] = b"sinomo.dfs.cloud-files.cursor-recovery-manifest.v1\0";
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(&(manifest.len() as u64).to_le_bytes());
    for (path, entry) in manifest {
        let entry_bytes = encode_sobj(entry).map_err(|source| {
            reconcile_error("encode-cursor-recovery-manifest-entry", source.to_string())
        })?;
        hasher.update(&(path.len() as u64).to_le_bytes());
        hasher.update(path.as_bytes());
        hasher.update(&(entry_bytes.len() as u64).to_le_bytes());
        hasher.update(&entry_bytes);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn encode_cursor_recovery_page_intent(
    intent: &CloudFilesCursorRecoveryPageIntent,
) -> Result<Vec<u8>, CloudFilesNamespaceReconcileError> {
    let bytes = encode_sobj(intent).map_err(|source| {
        reconcile_error("encode-cursor-recovery-page-intent", source.to_string())
    })?;
    if bytes.len() as u64 > CURSOR_RECOVERY_PAGE_INTENT_MAX_BYTES {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-too-large",
            format!(
                "cursor recovery page intent exceeds its {CURSOR_RECOVERY_PAGE_INTENT_MAX_BYTES}-byte bound"
            ),
        ));
    }
    Ok(bytes)
}

fn persist_cursor_recovery_page_intent(
    path: &Path,
    intent: &CloudFilesCursorRecoveryPageIntent,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    let bytes = encode_cursor_recovery_page_intent(intent)?;
    durable_file::write_file_atomic(
        path,
        &bytes,
        DurableAtomicFileOptions::new(
            CURSOR_RECOVERY_PAGE_INTENT_FILE,
            CURSOR_RECOVERY_PAGE_INTENT_TEMP_LABEL,
        )
        .sync_all(),
    )
    .map_err(storage_error("persist cursor recovery page intent"))
}

fn canonical_atomic_stage_name(name: &str, file_name: &str, temp_label: &str) -> bool {
    let prefix = format!(".{file_name}.{temp_label}-");
    let Some(suffix) = name.strip_prefix(&prefix) else {
        return false;
    };
    let Some((pid_text, sequence_text)) = suffix.split_once('-') else {
        return false;
    };
    if sequence_text.contains('-') {
        return false;
    }
    let Ok(pid) = pid_text.parse::<u32>() else {
        return false;
    };
    let Ok(sequence) = sequence_text.parse::<u64>() else {
        return false;
    };
    pid.to_string() == pid_text && sequence.to_string() == sequence_text
}

fn looks_like_cursor_recovery_page_intent_temp(name: &str) -> bool {
    name.starts_with(&format!(
        ".{CURSOR_RECOVERY_PAGE_INTENT_FILE}.{CURSOR_RECOVERY_PAGE_INTENT_TEMP_LABEL}-"
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorRecoveryPageIntentEvidenceKind {
    Final,
    Temp,
}

struct CursorRecoveryPageIntentEvidence {
    path: PathBuf,
    kind: CursorRecoveryPageIntentEvidenceKind,
    intent: CloudFilesCursorRecoveryPageIntent,
}

fn inspect_cursor_recovery_page_intent(
    scope: &Path,
) -> Result<Option<CursorRecoveryPageIntentEvidence>, CloudFilesNamespaceReconcileError> {
    let mut evidence = None;
    for entry in fs::read_dir(scope).map_err(io_error("list cursor recovery page-intent scope"))? {
        let entry = entry.map_err(io_error("read cursor recovery page-intent scope entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "cursor recovery scope contains a non-UTF-8 entry",
            )
        })?;
        let kind = if name == CURSOR_RECOVERY_PAGE_INTENT_FILE {
            Some(CursorRecoveryPageIntentEvidenceKind::Final)
        } else if looks_like_cursor_recovery_page_intent_temp(&name) {
            if !canonical_atomic_stage_name(
                &name,
                CURSOR_RECOVERY_PAGE_INTENT_FILE,
                CURSOR_RECOVERY_PAGE_INTENT_TEMP_LABEL,
            ) {
                return Err(reconcile_error(
                    "cursor-recovery-page-intent-topology",
                    "cursor recovery page intent has a malformed durable stage name",
                ));
            }
            Some(CursorRecoveryPageIntentEvidenceKind::Temp)
        } else {
            None
        };
        let Some(kind) = kind else {
            continue;
        };
        if evidence.is_some() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-topology",
                "cursor recovery permits at most one page intent or durable page-intent stage",
            ));
        }
        let path = entry.path();
        let bytes = read_bounded_plain_file(
            &path,
            CURSOR_RECOVERY_PAGE_INTENT_MAX_BYTES,
            "cursor recovery page intent",
        )?;
        let intent: CloudFilesCursorRecoveryPageIntent = decode_sobj(&bytes).map_err(|source| {
            reconcile_error("decode-cursor-recovery-page-intent", source.to_string())
        })?;
        if encode_cursor_recovery_page_intent(&intent)? != bytes {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-noncanonical",
                "cursor recovery page intent bytes are noncanonical",
            ));
        }
        evidence = Some(CursorRecoveryPageIntentEvidence { path, kind, intent });
    }
    Ok(evidence)
}

struct ValidatedCursorRecoveryPageIntent {
    before_state: CloudFilesNamespaceReconciliationState,
    after_state: CloudFilesNamespaceReconciliationState,
    actual_manifest_is_before: bool,
    actual_manifest_is_after: bool,
}

fn validate_cursor_recovery_page_intent(
    intent: &CloudFilesCursorRecoveryPageIntent,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    actual_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> Result<ValidatedCursorRecoveryPageIntent, CloudFilesNamespaceReconcileError> {
    let validated_plan = validate_cursor_recovery_plan(plan, &intent.mount, &intent.root)?;
    if intent.schema_version != CURSOR_RECOVERY_PAGE_INTENT_SCHEMA_VERSION
        || intent.recovery_id != plan.recovery_id
        || intent.recovery_token != plan.recovery_token
        || intent.nonce != plan.nonce
        || intent.root != plan.root
        || intent.mount != validated_plan.expected.mount
        || intent.plan_fingerprint != cursor_recovery_plan_fingerprint(plan)?
        || intent.before_state.is_empty()
        || intent.after_state.is_empty()
        || intent.before_state.len() as u64 > RECONCILIATION_STATE_MAX_BYTES
        || intent.after_state.len() as u64 > RECONCILIATION_STATE_MAX_BYTES
        || intent.before_state_fingerprint
            != cursor_recovery_bytes_fingerprint(&intent.before_state)
        || intent.after_state_fingerprint != cursor_recovery_bytes_fingerprint(&intent.after_state)
    {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-mismatch",
            "cursor recovery page intent is malformed or does not match its authorized plan",
        ));
    }

    let before_state: CloudFilesNamespaceReconciliationState = decode_sobj(&intent.before_state)
        .map_err(|source| {
            reconcile_error("decode-cursor-recovery-page-before", source.to_string())
        })?;
    let after_state: CloudFilesNamespaceReconciliationState = decode_sobj(&intent.after_state)
        .map_err(|source| {
            reconcile_error("decode-cursor-recovery-page-after", source.to_string())
        })?;
    if encode_reconciliation_state(&before_state)? != intent.before_state
        || encode_reconciliation_state(&after_state)? != intent.after_state
        || !cursor_recovery_progress_state_is_valid(&before_state, &validated_plan, false)?
        || !cursor_recovery_progress_state_is_valid(&after_state, &validated_plan, false)?
        || !before_state.cursors.is_empty()
        || before_state.full_scan_request != validated_plan.expected.full_scan_request
    {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-transition",
            "cursor recovery page intent does not start from valid armed progress",
        ));
    }

    let mut previous_directory = None::<&str>;
    for directory in &intent.materialized_directories {
        if canonical_sifs_path(directory)? != *directory
            || previous_directory.is_some_and(|previous| previous >= directory.as_str())
        {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "cursor recovery page contains noncanonical or duplicate materialized directories",
            ));
        }
        ensure_within_root(directory, &intent.root)?;
        previous_directory = Some(directory);
    }

    let mut previous_path = None::<&str>;
    for mutation in &intent.mutations {
        let path = manifest_mutation_path(mutation);
        if canonical_sifs_path(path)? != path
            || previous_path.is_some_and(|previous| previous >= path)
        {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "cursor recovery page contains noncanonical, duplicate, or unordered mutations",
            ));
        }
        ensure_within_root(path, &intent.root)?;
        if let CloudFilesManifestMutation::Upsert { entry } = mutation {
            if entry.path != path {
                return Err(reconcile_error(
                    "cursor-recovery-page-intent-transition",
                    "cursor recovery page mutation path does not match its entry",
                ));
            }
            validate_manifest_entry(entry)?;
        }
        previous_path = Some(path);
    }
    if intent.previous_manifest_entries.len() != intent.mutations.len() {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-transition",
            "cursor recovery page preimage set does not match its mutations",
        ));
    }
    for (path, previous) in &intent.previous_manifest_entries {
        if !intent
            .mutations
            .iter()
            .any(|mutation| manifest_mutation_path(mutation) == path)
        {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "cursor recovery page contains an unrelated manifest preimage",
            ));
        }
        if let Some(entry) = previous {
            if entry.path != *path || canonical_sifs_path(path)? != *path {
                return Err(reconcile_error(
                    "cursor-recovery-page-intent-transition",
                    "cursor recovery page contains a mismatched manifest preimage",
                ));
            }
            ensure_within_root(path, &intent.root)?;
            validate_manifest_entry(entry)?;
        }
    }
    if intent.scan_complete {
        if intent.cursors.is_empty() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "terminal cursor recovery page must install a cursor",
            ));
        }
    } else if !intent.cursors.is_empty() {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-transition",
            "partial cursor recovery page may not install cursors",
        ));
    }
    for (key, cursor) in &intent.cursors {
        if key != &cursor.shard_key()? || cursor.authority_epoch == 0 {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "cursor recovery page contains an invalid cursor",
            ));
        }
    }

    let actual_fingerprint = cursor_recovery_manifest_fingerprint(actual_manifest)?;
    let actual_manifest_is_before = actual_fingerprint == intent.before_manifest_fingerprint;
    let actual_manifest_is_after = actual_fingerprint == intent.after_manifest_fingerprint;
    if !actual_manifest_is_before && !actual_manifest_is_after {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-cut",
            "cursor recovery manifest is outside the page intent's exact before/after cut",
        ));
    }
    let mut before_manifest = actual_manifest.clone();
    if !actual_manifest_is_before {
        for (path, previous) in &intent.previous_manifest_entries {
            match previous {
                Some(entry) => {
                    before_manifest.insert(path.clone(), entry.clone());
                }
                None => {
                    before_manifest.remove(path);
                }
            }
        }
    }
    if cursor_recovery_manifest_fingerprint(&before_manifest)? != intent.before_manifest_fingerprint
    {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-transition",
            "cursor recovery page manifest preimages do not reconstruct its exact predecessor",
        ));
    }
    for mutation in &intent.mutations {
        let path = manifest_mutation_path(mutation);
        if before_manifest.get(path) != intent.previous_manifest_entries[path].as_ref() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "cursor recovery page manifest preimage differs from its predecessor",
            ));
        }
    }
    let mut after_manifest = before_manifest.clone();
    apply_manifest_mutations(&mut after_manifest, &intent.mutations);
    if cursor_recovery_manifest_fingerprint(&after_manifest)? != intent.after_manifest_fingerprint
        || (actual_manifest_is_after && actual_manifest != &after_manifest)
    {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-transition",
            "cursor recovery page mutations do not derive its exact successor manifest",
        ));
    }

    let mut expected_after = before_state.clone();
    expected_after
        .materialized_directories
        .extend(intent.materialized_directories.iter().cloned());
    apply_materialized_manifest_mutations(&mut expected_after, &intent.mutations);
    if intent.scan_complete {
        for (key, cursor) in &intent.cursors {
            let replace = expected_after
                .cursors
                .get(key)
                .is_none_or(|current| cursor.after_sequence >= current.after_sequence);
            if replace {
                expected_after.cursors.insert(key.clone(), cursor.clone());
            }
        }
        expected_after.full_scan_request = None;
        expected_after.applied_events.clear();
    }
    let (unsupported_count, unsupported_examples) = unsupported_projection_summary(&after_manifest);
    expected_after.unsupported_projection_count = unsupported_count;
    expected_after.unsupported_projection_examples = unsupported_examples;
    compact_reconciliation_state(&mut expected_after);
    if expected_after != after_state
        || (intent.scan_complete
            && (after_state.cursors.is_empty() || after_state.full_scan_request.is_some()))
        || (!intent.scan_complete
            && (!after_state.cursors.is_empty()
                || after_state.full_scan_request != validated_plan.expected.full_scan_request))
    {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-transition",
            "cursor recovery page state is not the deterministic authoritative successor",
        ));
    }

    Ok(ValidatedCursorRecoveryPageIntent {
        before_state,
        after_state,
        actual_manifest_is_before,
        actual_manifest_is_after,
    })
}

fn repair_authorized_cursor_recovery_page_intent(
    scope: &Path,
    state_path: &Path,
    state_bytes: &[u8],
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> Result<CloudFilesNamespaceReconciliationState, CloudFilesNamespaceReconcileError> {
    let decoded_state: CloudFilesNamespaceReconciliationState = decode_sobj(state_bytes)
        .map_err(|source| reconcile_error("decode-reconciliation-state", source.to_string()))?;
    let Some(evidence) = inspect_cursor_recovery_page_intent(scope)? else {
        return Ok(decoded_state);
    };
    let rewrite =
        load_required_cursor_recovery_rewrite(&scope.join(CURSOR_RECOVERY_REWRITE_FILE), plan)?;
    if rewrite.phase != CloudFilesCursorRecoveryRewritePhase::ReconcileArmed {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-phase",
            "cursor recovery page intent is admissible only while ReconcileArmed",
        ));
    }
    let validated = validate_cursor_recovery_page_intent(&evidence.intent, plan, manifest)?;
    let state_is_before = state_bytes == evidence.intent.before_state;
    let state_is_after = state_bytes == evidence.intent.after_state;
    if !state_is_before && !state_is_after {
        return Err(reconcile_error(
            "cursor-recovery-page-intent-cut",
            "cursor recovery state is outside the page intent's exact before/after cut",
        ));
    }

    let repaired_state = match evidence.kind {
        CursorRecoveryPageIntentEvidenceKind::Temp => {
            if !state_is_before || !validated.actual_manifest_is_before {
                return Err(reconcile_error(
                    "cursor-recovery-page-intent-cut",
                    "a page-intent stage is admissible only at the exact predecessor cut",
                ));
            }
            validated.before_state
        }
        CursorRecoveryPageIntentEvidenceKind::Final
            if state_is_before
                && validated.actual_manifest_is_before
                && !validated.actual_manifest_is_after =>
        {
            validated.before_state
        }
        CursorRecoveryPageIntentEvidenceKind::Final
            if state_is_before && validated.actual_manifest_is_after =>
        {
            durable_file::write_file_atomic(
                state_path,
                &evidence.intent.after_state,
                DurableAtomicFileOptions::new(RECONCILIATION_STATE_FILE, RECONCILIATION_TEMP_LABEL)
                    .sync_all(),
            )
            .map_err(storage_error("repair cursor recovery page state"))?;
            validated.after_state
        }
        CursorRecoveryPageIntentEvidenceKind::Final
            if state_is_after && validated.actual_manifest_is_after =>
        {
            validated.after_state
        }
        CursorRecoveryPageIntentEvidenceKind::Final => {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-cut",
                "cursor recovery page intent does not match an exact durable protocol cut",
            ));
        }
    };
    durable_file::remove_file_if_exists_and_sync_parent(&evidence.path).map_err(storage_error(
        "complete authorized cursor recovery page intent",
    ))?;
    Ok(repaired_state)
}

fn encode_manifest_record(
    sequence: u64,
    mutations: &[CloudFilesManifestMutation],
) -> Result<Vec<u8>, CloudFilesNamespaceReconcileError> {
    let body = CloudFilesManifestJournalRecordBody {
        schema_version: MANIFEST_SCHEMA_VERSION,
        sequence,
        mutations: mutations.to_vec(),
    };
    let checksum = manifest_record_checksum(&body)?;
    encode_sobj(&CloudFilesManifestJournalRecord { body, checksum })
        .map_err(|source| reconcile_error("encode-manifest-record", source.to_string()))
}

fn manifest_record_checksum(
    body: &CloudFilesManifestJournalRecordBody,
) -> Result<String, CloudFilesNamespaceReconcileError> {
    let bytes = encode_sobj(body)
        .map_err(|source| reconcile_error("encode-manifest-record", source.to_string()))?;
    Ok(bytes_hash(&bytes))
}

fn validate_manifest_record(
    record: &CloudFilesManifestJournalRecord,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    if record.body.schema_version != MANIFEST_SCHEMA_VERSION || record.body.sequence == 0 {
        return Err(reconcile_error(
            "unsupported-manifest-schema",
            "namespace manifest record has an unsupported schema or zero sequence",
        ));
    }
    if record.body.mutations.is_empty()
        || record.body.mutations.len() > MANIFEST_RECORD_MUTATIONS_MAX
    {
        return Err(reconcile_error(
            "invalid-manifest-record",
            "namespace manifest record has an empty or oversized mutation batch",
        ));
    }
    if record.checksum != manifest_record_checksum(&record.body)? {
        return Err(reconcile_error(
            "manifest-record-checksum",
            format!(
                "namespace manifest record {} failed checksum validation",
                record.body.sequence
            ),
        ));
    }
    for mutation in &record.body.mutations {
        match mutation {
            CloudFilesManifestMutation::Upsert { entry } => validate_manifest_entry(entry)?,
            CloudFilesManifestMutation::Remove { path } => {
                if canonical_sifs_path(path)? != *path {
                    return Err(reconcile_error(
                        "invalid-manifest-record",
                        format!("namespace manifest remove path `{path}` is not canonical"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn bytes_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn manifest_chunk_file_name(generation: u64, ordinal: u64) -> String {
    format!("chunk-{generation:020}-{ordinal:08}.sobj")
}

fn encode_manifest_chunks(
    generation: u64,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> Result<Vec<Vec<u8>>, CloudFilesNamespaceReconcileError> {
    let mut chunks = Vec::new();
    let mut entries = Vec::new();
    for entry in manifest.values() {
        entries.push(entry.clone());
        let ordinal = u64::try_from(chunks.len()).map_err(|_| {
            reconcile_error(
                "manifest-chunk-overflow",
                "namespace manifest chunk ordinal overflowed",
            )
        })?;
        let candidate = encode_sobj(&CloudFilesManifestChunk {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation,
            ordinal,
            entries: entries.clone(),
        })
        .map_err(|source| reconcile_error("encode-manifest-chunk", source.to_string()))?;
        if candidate.len() <= MANIFEST_CHUNK_TARGET_BYTES {
            continue;
        }
        let last = entries.pop().expect("just pushed manifest entry");
        if entries.is_empty() {
            if candidate.len() > MANIFEST_CHUNK_MAX_BYTES {
                return Err(reconcile_error(
                    "manifest-chunk-too-large",
                    "one namespace manifest entry exceeds the durable chunk bound",
                ));
            }
            chunks.push(candidate);
        } else {
            let bytes = encode_sobj(&CloudFilesManifestChunk {
                schema_version: MANIFEST_SCHEMA_VERSION,
                generation,
                ordinal,
                entries: std::mem::take(&mut entries),
            })
            .map_err(|source| reconcile_error("encode-manifest-chunk", source.to_string()))?;
            if bytes.len() > MANIFEST_CHUNK_MAX_BYTES {
                return Err(reconcile_error(
                    "manifest-chunk-too-large",
                    "namespace manifest chunk exceeds its hard bound",
                ));
            }
            chunks.push(bytes);
            entries.push(last);
        }
    }
    if !entries.is_empty() {
        let ordinal = u64::try_from(chunks.len()).map_err(|_| {
            reconcile_error(
                "manifest-chunk-overflow",
                "namespace manifest chunk ordinal overflowed",
            )
        })?;
        let bytes = encode_sobj(&CloudFilesManifestChunk {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation,
            ordinal,
            entries,
        })
        .map_err(|source| reconcile_error("encode-manifest-chunk", source.to_string()))?;
        if bytes.len() > MANIFEST_CHUNK_MAX_BYTES {
            return Err(reconcile_error(
                "manifest-chunk-too-large",
                "namespace manifest chunk exceeds its hard bound",
            ));
        }
        chunks.push(bytes);
    }
    Ok(chunks)
}

fn load_manifest_index(
    path: &Path,
) -> Result<Option<CloudFilesManifestIndex>, CloudFilesNamespaceReconcileError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("read namespace manifest index")(source)),
    };
    if bytes.len() > MANIFEST_CHUNK_MAX_BYTES {
        return Err(reconcile_error(
            "manifest-index-too-large",
            "namespace manifest index exceeds its hard bound",
        ));
    }
    let index = decode_sobj::<CloudFilesManifestIndex>(&bytes)
        .map_err(|source| reconcile_error("decode-manifest-index", source.to_string()))?;
    if index.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(reconcile_error(
            "unsupported-manifest-schema",
            format!(
                "namespace manifest index schema {} is unsupported",
                index.schema_version
            ),
        ));
    }
    Ok(Some(index))
}

fn load_manifest_chunks(
    dir: &Path,
    index: &CloudFilesManifestIndex,
) -> Result<BTreeMap<String, CloudFilesReconciledManifestEntry>, CloudFilesNamespaceReconcileError>
{
    let mut manifest = BTreeMap::new();
    for (ordinal, reference) in index.chunks.iter().enumerate() {
        let ordinal = u64::try_from(ordinal).map_err(|_| {
            reconcile_error(
                "manifest-chunk-overflow",
                "namespace manifest chunk ordinal overflowed",
            )
        })?;
        if reference.file_name != manifest_chunk_file_name(index.generation, ordinal)
            || reference.file_name.contains('/')
            || reference.file_name.contains('\\')
        {
            return Err(reconcile_error(
                "invalid-manifest-index",
                "namespace manifest index contains an invalid chunk path",
            ));
        }
        let bytes = fs::read(dir.join(&reference.file_name))
            .map_err(io_error("read namespace manifest chunk"))?;
        if bytes.len() > MANIFEST_CHUNK_MAX_BYTES || bytes_hash(&bytes) != reference.content_hash {
            return Err(reconcile_error(
                "manifest-chunk-checksum",
                format!(
                    "namespace manifest chunk `{}` exceeds its bound or failed checksum validation",
                    reference.file_name
                ),
            ));
        }
        let chunk = decode_sobj::<CloudFilesManifestChunk>(&bytes)
            .map_err(|source| reconcile_error("decode-manifest-chunk", source.to_string()))?;
        if chunk.schema_version != MANIFEST_SCHEMA_VERSION
            || chunk.generation != index.generation
            || chunk.ordinal != ordinal
            || u64::try_from(chunk.entries.len()).unwrap_or(u64::MAX) != reference.entry_count
        {
            return Err(reconcile_error(
                "invalid-manifest-chunk",
                format!(
                    "namespace manifest chunk `{}` identity/count does not match its index",
                    reference.file_name
                ),
            ));
        }
        for entry in chunk.entries {
            validate_manifest_entry(&entry)?;
            if manifest.insert(entry.path.clone(), entry).is_some() {
                return Err(reconcile_error(
                    "invalid-manifest-chunk",
                    "namespace manifest chunks contain a duplicate path",
                ));
            }
        }
    }
    Ok(manifest)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CloudFilesNamespaceReconcileProgress {
    pub effects_applied: u64,
    pub effects_suppressed: u64,
    pub conflicts_recorded: u64,
    pub cursor_advanced: bool,
    pub full_scan_requested: bool,
    /// Successful local operations whose exact authoritative echo is now
    /// covered by the durable cursor written for this batch.
    pub reconciled_local_operation_ids: Vec<String>,
}

#[derive(Debug)]
pub struct CloudFilesNamespaceReconcileError {
    pub code: &'static str,
    pub detail: String,
}

impl std::fmt::Display for CloudFilesNamespaceReconcileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for CloudFilesNamespaceReconcileError {}

pub struct CloudFilesNamespaceReconciler {
    root: String,
    state_path: PathBuf,
    _lock: File,
    cursor_recovery_plan: Option<CloudFilesCursorRecoveryReconciliationPlan>,
    manifest_store: CloudFilesManifestStore,
    manifest: BTreeMap<String, CloudFilesReconciledManifestEntry>,
    state: CloudFilesNamespaceReconciliationState,
    usable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct CloudFilesMaterializationAuditIntentState {
    schema_version: u16,
    mount: String,
    root: String,
    #[serde(default)]
    directories: BTreeSet<String>,
}

/// Durable, mount/root-scoped requests to authoritatively audit a directory
/// whose children are about to be exposed by an on-demand Cloud Files page.
///
/// The callback records an intent before the CFAPI side effect. The writable
/// worker keeps the intent until the audited directory and any terminal feed
/// cursor are durable in the reconciler. Recording too much after a failed
/// callback is safe: it can only cause an idempotent authoritative re-audit.
pub struct CloudFilesMaterializationAuditIntentStore {
    state_path: PathBuf,
    state: CloudFilesMaterializationAuditIntentState,
    usable: bool,
}

impl CloudFilesMaterializationAuditIntentStore {
    pub fn open(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesNamespaceReconcileError> {
        let mount = mount.into();
        if mount.trim().is_empty() {
            return Err(reconcile_error(
                "invalid-mount",
                "mount identity cannot be empty",
            ));
        }
        let root = canonical_sifs_path(&root.into())?;
        let reconciliation_dir = ensure_reconciliation_scope(data_dir, &mount, &root)?;
        remove_abandoned_atomic_stages(
            &reconciliation_dir,
            MATERIALIZATION_AUDIT_INTENTS_FILE,
            MATERIALIZATION_AUDIT_INTENTS_TEMP_LABEL,
        )?;
        let state_path = reconciliation_dir.join(MATERIALIZATION_AUDIT_INTENTS_FILE);
        let state = load_materialization_audit_intents(&state_path)?.unwrap_or_else(|| {
            CloudFilesMaterializationAuditIntentState {
                schema_version: MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION,
                mount: mount.clone(),
                root: root.clone(),
                directories: BTreeSet::new(),
            }
        });
        if state.schema_version != MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION
            || state.mount != mount
            || state.root != root
        {
            return Err(reconcile_error(
                "materialization-audit-scope-mismatch",
                "persisted materialization audit intents have a different schema, mount, or root",
            ));
        }
        for directory in &state.directories {
            if canonical_sifs_path(directory)? != *directory {
                return Err(reconcile_error(
                    "invalid-materialization-audit-intent",
                    format!("persisted audit directory `{directory}` is not canonical"),
                ));
            }
            ensure_within_root(directory, &root)?;
        }
        let mut store = Self {
            state_path,
            state,
            usable: true,
        };
        if !store.state_path.exists() {
            store.persist()?;
        }
        Ok(store)
    }

    pub fn pending_directories(&self) -> &BTreeSet<String> {
        &self.state.directories
    }

    /// Fsyncs the audit intent before the caller exposes any placeholders.
    pub fn record_directory(
        &mut self,
        directory: &str,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let directory = canonical_sifs_path(directory)?;
        ensure_within_root(&directory, &self.state.root)?;
        if self.state.directories.contains(&directory) {
            return Ok(());
        }
        let previous = self.state.clone();
        self.state.directories.insert(directory);
        self.persist_or_restore(previous)
    }

    /// Removes intents only after the reconciler has durably adopted the same
    /// directory ownership evidence. An uncertain acknowledgement is safe to
    /// replay because the reconciler write always precedes this write.
    pub fn acknowledge_directories<'a>(
        &mut self,
        directories: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let directories = directories
            .into_iter()
            .map(|directory| {
                let directory = canonical_sifs_path(directory)?;
                ensure_within_root(&directory, &self.state.root)?;
                Ok(directory)
            })
            .collect::<Result<Vec<_>, CloudFilesNamespaceReconcileError>>()?;
        let mut changed = false;
        let previous = self.state.clone();
        for directory in directories {
            changed |= self.state.directories.remove(&directory);
        }
        if !changed {
            return Ok(());
        }
        self.persist_or_restore(previous)
    }

    fn ensure_usable(&self) -> Result<(), CloudFilesNamespaceReconcileError> {
        if self.usable {
            Ok(())
        } else {
            Err(reconcile_error(
                "materialization-audit-reopen-required",
                "a prior durable audit-intent write was uncertain; reopen before continuing",
            ))
        }
    }

    fn persist(&mut self) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let bytes = encode_materialization_audit_intents(&self.state)?;
        if let Err(source) = durable_file::write_file_atomic(
            &self.state_path,
            &bytes,
            DurableAtomicFileOptions::new(
                MATERIALIZATION_AUDIT_INTENTS_FILE,
                MATERIALIZATION_AUDIT_INTENTS_TEMP_LABEL,
            )
            .sync_all(),
        ) {
            self.usable = false;
            return Err(storage_error("persist materialization audit intents")(
                source,
            ));
        }
        Ok(())
    }

    fn persist_or_restore(
        &mut self,
        previous: CloudFilesMaterializationAuditIntentState,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        if let Err(error) = encode_materialization_audit_intents(&self.state) {
            self.state = previous;
            return Err(error);
        }
        self.persist()
    }
}

impl CloudFilesNamespaceReconciler {
    pub fn open(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesNamespaceReconcileError> {
        let mount = mount.into();
        if mount.trim().is_empty() {
            return Err(reconcile_error(
                "invalid-mount",
                "mount identity cannot be empty",
            ));
        }
        let root = canonical_sifs_path(&root.into())?;
        let runtime_dir = data_dir.module_runtime_dir();
        fs::create_dir_all(&runtime_dir).map_err(io_error("create module runtime directory"))?;
        let module_dir = runtime_dir.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
        durable_file::ensure_private_directory(&module_dir)
            .map_err(storage_error("create Cloud Files module runtime directory"))?;
        let reconciliation_root = module_dir.join(RECONCILIATION_DIR);
        durable_file::ensure_private_directory(&reconciliation_root)
            .map_err(storage_error("create namespace reconciliation root"))?;
        let reconciliation_dir = reconciliation_root.join(mount_scope_directory(&mount, &root)?);
        durable_file::ensure_private_directory(&reconciliation_dir)
            .map_err(storage_error("create namespace reconciliation scope"))?;
        if cursor_recovery_evidence_exists(&reconciliation_dir)? {
            return Err(reconcile_error(
                "cursor-recovery-requires-authorized-open",
                "ordinary namespace reconciler open is forbidden while cursor recovery marker or stage evidence exists",
            ));
        }
        let lock_path = reconciliation_dir.join(RECONCILIATION_LOCK_FILE);
        let lock = durable_file::open_file_for_append(&lock_path)
            .map_err(storage_error("open namespace reconciliation lock"))?
            .file;
        lock.try_lock_exclusive().map_err(|source| {
            reconcile_error(
                "reconciler-busy",
                format!("another namespace reconciler owns the state: {source}"),
            )
        })?;
        remove_abandoned_state_stages(&reconciliation_dir)?;
        let state_path = reconciliation_dir.join(RECONCILIATION_STATE_FILE);
        let mut state = load_reconciliation_state(&state_path)?.unwrap_or_else(|| {
            CloudFilesNamespaceReconciliationState {
                schema_version: RECONCILIATION_SCHEMA_VERSION,
                mount: mount.clone(),
                root: root.clone(),
                cursors: BTreeMap::new(),
                legacy_manifest: BTreeMap::new(),
                suppressions: BTreeMap::new(),
                conflicts: BTreeMap::new(),
                startup_conflicts: BTreeMap::new(),
                materialized_directories: BTreeSet::new(),
                unsupported_projection_count: 0,
                unsupported_projection_examples: BTreeMap::new(),
                full_scan_request: None,
                inflight_effect: None,
                applied_events: BTreeMap::new(),
            }
        });
        if state.schema_version != RECONCILIATION_SCHEMA_VERSION
            || state.mount != mount
            || state.root != root
        {
            return Err(reconcile_error(
                "reconciliation-scope-mismatch",
                "persisted namespace state has a different schema, mount, or root",
            ));
        }
        let legacy_manifest = std::mem::take(&mut state.legacy_manifest);
        let (manifest_store, manifest, migrated_legacy_manifest) =
            CloudFilesManifestStore::open(&reconciliation_dir, legacy_manifest)?;
        for (path, entry) in &manifest {
            if path != &entry.path || canonical_sifs_path(path)? != *path {
                return Err(reconcile_error(
                    "invalid-manifest-chunk",
                    format!("persisted namespace manifest path `{path}` is not canonical"),
                ));
            }
            ensure_within_root(path, &root)?;
        }
        for directory in &state.materialized_directories {
            if canonical_sifs_path(directory)? != *directory {
                return Err(reconcile_error(
                    "invalid-materialized-directory",
                    format!("persisted materialized directory `{directory}` is not canonical"),
                ));
            }
            ensure_within_root(directory, &root)?;
        }
        let (unsupported_projection_count, unsupported_projection_examples) =
            unsupported_projection_summary(&manifest);
        let projection_summary_changed = state.unsupported_projection_count
            != unsupported_projection_count
            || state.unsupported_projection_examples != unsupported_projection_examples;
        state.unsupported_projection_count = unsupported_projection_count;
        state.unsupported_projection_examples = unsupported_projection_examples;
        let mut reconciler = Self {
            root,
            state_path,
            _lock: lock,
            cursor_recovery_plan: None,
            manifest_store,
            manifest,
            state,
            usable: true,
        };
        if !reconciler.state_path.exists() || migrated_legacy_manifest || projection_summary_changed
        {
            reconciler.persist()?;
        }
        Ok(reconciler)
    }

    pub(crate) fn open_authorized_cursor_recovery(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<Self, CloudFilesNamespaceReconcileError> {
        open_authorized_cursor_recovery_progress(data_dir, mount, root, plan)
    }
    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    pub fn cursors(&self) -> &BTreeMap<String, CloudFilesNamespaceCursor> {
        &self.state.cursors
    }

    pub fn manifest(&self) -> &BTreeMap<String, CloudFilesReconciledManifestEntry> {
        &self.manifest
    }

    pub fn materialized_directories(&self) -> &BTreeSet<String> {
        &self.state.materialized_directories
    }

    pub(crate) fn cursor_recovery_terminal(&self) -> bool {
        !self.state.cursors.is_empty()
            && self.state.full_scan_request.is_none()
            && self.state.inflight_effect.is_none()
            && self.state.materialized_directories.contains(&self.root)
    }

    /// Persists the plan-bound terminal phase while this reconciler still owns
    /// the namespace lock. The recovery worker must call this only after its
    /// local observer barrier and exact root audit have reached commit-ready.
    pub(crate) fn mark_authorized_cursor_recovery_terminal(
        &mut self,
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<CloudFilesCursorRecoveryTerminalProof, CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let validated = validate_cursor_recovery_plan(plan, &self.state.mount, &self.root)?;
        if !cursor_recovery_progress_state_is_valid(&self.state, &validated, true)?
            || !self.cursor_recovery_terminal()
        {
            return Err(reconcile_error(
                "cursor-recovery-terminal-incomplete",
                "cursor recovery cannot become Terminal before its authoritative terminal state is durable",
            ));
        }
        let scope = self.state_path.parent().ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "cursor recovery state has no reconciliation scope",
            )
        })?;
        if inspect_cursor_recovery_page_intent(scope)?.is_some() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-live",
                "cursor recovery cannot become Terminal while a page transaction is incomplete",
            ));
        }
        let audit = load_required_cursor_recovery_audit(scope, &self.state.mount, &self.root)?;
        if !audit.directories.is_empty() {
            return Err(reconcile_error(
                "cursor-recovery-terminal-audit-pending",
                "cursor recovery cannot become Terminal while materialization audit intents remain",
            ));
        }
        let rewrite_path = scope.join(CURSOR_RECOVERY_REWRITE_FILE);
        let rewrite = resume_terminal_rewrite_stage(
            scope,
            &rewrite_path,
            plan,
            load_required_cursor_recovery_rewrite(&rewrite_path, plan)?,
            true,
            true,
        )?;
        match rewrite.phase {
            CloudFilesCursorRecoveryRewritePhase::Terminal => {}
            CloudFilesCursorRecoveryRewritePhase::ReconcileArmed => {}
        }
        let terminal = CloudFilesCursorRecoveryRewrite::from_plan(
            plan,
            CloudFilesCursorRecoveryRewritePhase::Terminal,
        );
        if rewrite.phase != CloudFilesCursorRecoveryRewritePhase::Terminal {
            persist_cursor_recovery_rewrite(&rewrite_path, &terminal)?;
        }
        cursor_recovery_terminal_proof(plan)
    }

    /// Reconstructs the same narrow finalization capability after a crash,
    /// but only from an exact plan-bound Terminal marker and terminal owner
    /// state. The module owner must retain this value until its synchronous
    /// host Ready acknowledgement has completed.
    pub(crate) fn resume_authorized_cursor_recovery_terminal_proof(
        &self,
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<CloudFilesCursorRecoveryTerminalProof, CloudFilesNamespaceReconcileError> {
        let validated = validate_cursor_recovery_plan(plan, &self.state.mount, &self.root)?;
        if !cursor_recovery_progress_state_is_valid(&self.state, &validated, true)? {
            return Err(reconcile_error(
                "cursor-recovery-terminal-incomplete",
                "cannot resume finalization without matching terminal reconciliation state",
            ));
        }
        let scope = self.state_path.parent().ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "cursor recovery state has no reconciliation scope",
            )
        })?;
        if inspect_cursor_recovery_page_intent(scope)?.is_some() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-live",
                "cursor recovery cannot resume Terminal proof while a page transaction is incomplete",
            ));
        }
        if !load_required_cursor_recovery_audit(scope, &self.state.mount, &self.root)?
            .directories
            .is_empty()
        {
            return Err(reconcile_error(
                "cursor-recovery-terminal-audit-pending",
                "cannot resume finalization while materialization audit intents remain",
            ));
        }
        let rewrite =
            load_required_cursor_recovery_rewrite(&scope.join(CURSOR_RECOVERY_REWRITE_FILE), plan)?;
        if rewrite.phase != CloudFilesCursorRecoveryRewritePhase::Terminal {
            return Err(reconcile_error(
                "cursor-recovery-not-terminal",
                "cannot resume finalization from a nonterminal recovery marker",
            ));
        }
        cursor_recovery_terminal_proof(plan)
    }

    pub(crate) fn resume_authorized_cursor_recovery_namespace_finalized_proof(
        &self,
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
    ) -> Result<CloudFilesCursorRecoveryTerminalProof, CloudFilesNamespaceReconcileError> {
        let validated = validate_cursor_recovery_plan(plan, &self.state.mount, &self.root)?;
        if !cursor_recovery_progress_state_is_valid(&self.state, &validated, true)? {
            return Err(reconcile_error(
                "cursor-recovery-terminal-incomplete",
                "NamespaceFinalized recovery lost its exact terminal owner state",
            ));
        }
        let scope = self.state_path.parent().ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "NamespaceFinalized state has no reconciliation scope",
            )
        })?;
        if inspect_cursor_recovery_page_intent(scope)?.is_some()
            || !load_required_cursor_recovery_audit(scope, &self.state.mount, &self.root)?
                .directories
                .is_empty()
        {
            return Err(reconcile_error(
                "cursor-recovery-terminal-incomplete",
                "NamespaceFinalized recovery retains page or audit work",
            ));
        }
        ensure_cursor_recovery_rewrite_absent(scope, &scope.join(CURSOR_RECOVERY_REWRITE_FILE))?;
        cursor_recovery_terminal_proof(plan)
    }

    /// Consumes the capability only after the module owner has synchronously
    /// observed host Ready. The existing worker retains this reconciler lock,
    /// its watcher, and buffered local observations across marker removal.
    pub(crate) fn finalize_authorized_cursor_recovery_after_ready(
        &mut self,
        plan: &CloudFilesCursorRecoveryReconciliationPlan,
        ready: CloudFilesCursorRecoveryReadyToFinalize,
    ) -> Result<bool, CloudFilesNamespaceReconcileError> {
        validate_cursor_recovery_ready_to_finalize(plan, &self.root, &ready)?;
        let validated = validate_cursor_recovery_plan(plan, &self.state.mount, &self.root)?;
        if !cursor_recovery_progress_state_is_valid(&self.state, &validated, true)? {
            return Err(reconcile_error(
                "cursor-recovery-terminal-incomplete",
                "cursor recovery finalization lost its terminal reconciliation state",
            ));
        }
        let scope = self.state_path.parent().ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "cursor recovery state has no reconciliation scope",
            )
        })?;
        if inspect_cursor_recovery_page_intent(scope)?.is_some() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-live",
                "cursor recovery cannot finalize while a page transaction is incomplete",
            ));
        }
        if !load_required_cursor_recovery_audit(scope, &self.state.mount, &self.root)?
            .directories
            .is_empty()
        {
            return Err(reconcile_error(
                "cursor-recovery-terminal-audit-pending",
                "cursor recovery finalization still has pending audit intents",
            ));
        }
        let rewrite_path = scope.join(CURSOR_RECOVERY_REWRITE_FILE);
        let rewrite = match fs::symlink_metadata(&rewrite_path) {
            Ok(_) => load_required_cursor_recovery_rewrite(&rewrite_path, plan)?,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                self.cursor_recovery_plan = None;
                return Ok(false);
            }
            Err(source) => {
                return Err(io_error("inspect cursor recovery terminal marker")(source));
            }
        };
        if rewrite.phase != CloudFilesCursorRecoveryRewritePhase::Terminal {
            return Err(reconcile_error(
                "cursor-recovery-not-terminal",
                "host Ready may finalize only an exact Terminal recovery marker",
            ));
        }
        durable_file::remove_file_if_exists_and_sync_parent(&rewrite_path)
            .map_err(storage_error("finalize terminal cursor recovery marker"))?;
        self.cursor_recovery_plan = None;
        Ok(true)
    }

    /// Durably adopts one exact authoritative identity recovered from a local
    /// Cloud Files placeholder. The manifest upsert and directory ownership
    /// marker share one durability boundary, so a restart cannot reinterpret
    /// the placeholder as an unowned offline create.
    pub fn record_materialized_placeholder(
        &mut self,
        directory: &str,
        entry: CloudFilesReconciledManifestEntry,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let directory = canonical_sifs_path(directory)?;
        ensure_within_root(&directory, &self.root)?;
        if entry.path != canonical_sifs_path(&entry.path)? {
            return Err(reconcile_error(
                "invalid-materialized-placeholder",
                format!(
                    "materialized placeholder path `{}` is not canonical",
                    entry.path
                ),
            ));
        }
        ensure_within_root(&entry.path, &self.root)?;
        validate_manifest_entry(&entry)?;
        if sifs_parent_path(&entry.path).as_deref() != Some(directory.as_str()) {
            return Err(reconcile_error(
                "invalid-materialized-placeholder",
                format!(
                    "placeholder `{}` is not an immediate child of materialized directory `{directory}`",
                    entry.path
                ),
            ));
        }
        if self.state.materialized_directories.contains(&directory)
            && self.manifest.get(&entry.path) == Some(&entry)
        {
            return Ok(());
        }
        let previous = self.state.clone();
        self.state.materialized_directories.insert(directory);
        self.state.startup_conflicts.remove(&entry.path);
        let mutations = vec![CloudFilesManifestMutation::Upsert { entry }];
        let undo = apply_manifest_mutations_with_undo(&mut self.manifest, &mutations);
        self.persist_manifest_or_restore(previous, mutations, undo)
    }

    pub fn conflicts(&self) -> &BTreeMap<String, CloudFilesNamespaceConflict> {
        &self.state.conflicts
    }

    pub fn startup_conflicts(&self) -> &BTreeMap<String, CloudFilesNamespaceStartupConflict> {
        &self.state.startup_conflicts
    }

    pub fn conflict_count(&self) -> u64 {
        u64::try_from(
            self.state
                .conflicts
                .len()
                .saturating_add(self.state.startup_conflicts.len()),
        )
        .unwrap_or(u64::MAX)
    }

    pub fn unsupported_projection_count(&self) -> u64 {
        self.state.unsupported_projection_count
    }

    pub fn unsupported_projection_examples(&self) -> &BTreeMap<String, String> {
        &self.state.unsupported_projection_examples
    }

    pub fn full_scan_request(&self) -> Option<&CloudFilesFullScanRequest> {
        self.state.full_scan_request.as_ref()
    }

    pub fn has_suppression(&self, operation_id: &str) -> bool {
        self.state.suppressions.contains_key(operation_id)
    }

    pub fn cursor_for(
        &self,
        cursor: &CloudFilesNamespaceCursor,
    ) -> Result<Option<&CloudFilesNamespaceCursor>, CloudFilesNamespaceReconcileError> {
        Ok(self.state.cursors.get(&cursor.shard_key()?))
    }

    pub fn record_suppression(
        &mut self,
        suppression: CloudFilesNamespaceSuppression,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        self.validate_suppression(&suppression)?;
        let previous = self.state.clone();
        self.state
            .suppressions
            .insert(suppression.operation_id.clone(), suppression);
        self.persist_or_restore(previous)
    }

    /// Persist a pre-activation local difference without manufacturing a
    /// remote namespace action or permitting upload. Repeated scans replace
    /// evidence for the same canonical path instead of growing unboundedly.
    pub fn record_startup_conflict(
        &mut self,
        path: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<CloudFilesNamespaceStartupConflict, CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let path = canonical_sifs_path(&path.into())?;
        ensure_within_root(&path, &self.root)?;
        let detail = detail.into();
        if detail.trim().is_empty() {
            return Err(reconcile_error(
                "invalid-startup-conflict",
                "startup conflict detail cannot be empty",
            ));
        }
        let conflict = CloudFilesNamespaceStartupConflict {
            path: path.clone(),
            detail,
        };
        if self.state.startup_conflicts.get(&path) == Some(&conflict) {
            return Ok(conflict);
        }
        let previous = self.state.clone();
        self.state.startup_conflicts.insert(path, conflict.clone());
        self.persist_or_restore(previous)?;
        Ok(conflict)
    }

    pub fn resolve_startup_conflict(
        &mut self,
        path: &str,
    ) -> Result<bool, CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let path = canonical_sifs_path(path)?;
        ensure_within_root(&path, &self.root)?;
        if !self.state.startup_conflicts.contains_key(&path) {
            return Ok(false);
        }
        let previous = self.state.clone();
        self.state.startup_conflicts.remove(&path);
        self.persist_or_restore(previous)?;
        Ok(true)
    }

    /// Atomically records the no-echo token for a completed local mutation and
    /// advances the durable manifest to the authoritative response carried by
    /// that token. A later exact feed echo consumes the suppression and
    /// reapplies the same action idempotently.
    pub fn record_local_application(
        &mut self,
        suppression: CloudFilesNamespaceSuppression,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        self.validate_suppression(&suppression)?;
        let previous = self.state.clone();
        apply_materialized_action(&mut self.state, &suppression.expected_action);
        let mutations = manifest_mutations_for_action(&suppression.expected_action);
        let undo = apply_manifest_mutations_with_undo(&mut self.manifest, &mutations);
        self.state
            .suppressions
            .insert(suppression.operation_id.clone(), suppression);
        self.persist_manifest_or_restore(previous, mutations, undo)
    }

    /// Resolve a durable conflict. Applying the remote action uses the same
    /// persisted-before-effect recovery protocol as feed reconciliation;
    /// keeping the local state only removes the surfaced conflict record.
    pub fn resolve_conflict<E>(
        &mut self,
        event_id: &str,
        resolution: CloudFilesNamespaceConflictResolution,
        effects: &mut E,
    ) -> Result<CloudFilesNamespaceConflict, CloudFilesNamespaceReconcileError>
    where
        E: CloudFilesNamespaceEffectAdapter,
    {
        self.ensure_usable()?;
        let resumes_requested_resolution = self
            .state
            .inflight_effect
            .as_ref()
            .and_then(|pending| pending.resolving_conflict_event_id.as_deref())
            == Some(event_id);
        if resumes_requested_resolution
            && resolution != CloudFilesNamespaceConflictResolution::ApplyRemote
        {
            return Err(reconcile_error(
                "conflict-resolution-in-progress",
                format!(
                    "conflict `{event_id}` already has a durable apply-remote resolution in progress"
                ),
            ));
        }
        let requested_conflict = self.state.conflicts.get(event_id).cloned();
        self.recover_inflight(effects)?;
        if resumes_requested_resolution {
            return requested_conflict.ok_or_else(|| {
                reconcile_error(
                    "conflict-not-found",
                    format!("conflict `{event_id}` is not durable"),
                )
            });
        }
        let conflict = self.state.conflicts.get(event_id).cloned().ok_or_else(|| {
            reconcile_error(
                "conflict-not-found",
                format!("conflict `{event_id}` is not durable"),
            )
        })?;
        match resolution {
            CloudFilesNamespaceConflictResolution::KeepLocal => {
                let previous = self.state.clone();
                self.state.conflicts.remove(event_id);
                self.persist_or_restore(previous)?;
            }
            CloudFilesNamespaceConflictResolution::ApplyRemote => {
                let pending = CloudFilesPendingNamespaceEffect {
                    event_id: conflict.event_id.clone(),
                    shard_key: String::new(),
                    journal_sequence: 0,
                    action: conflict.action.clone(),
                    resolving_conflict_event_id: Some(conflict.event_id.clone()),
                };
                let mut projected = self.state.clone();
                projected.inflight_effect = Some(pending.clone());
                finish_inflight_state(&mut projected)?;
                ensure_state_fits(&projected)?;

                let previous = self.state.clone();
                self.state.inflight_effect = Some(pending);
                self.persist_or_restore(previous)?;
                effects
                    .apply(&conflict.action)
                    .map_err(|detail| reconcile_error("conflict-resolution-effect", detail))?;
                self.finish_inflight_effect()?;
            }
        }
        Ok(conflict)
    }

    /// Rewrites the canonical state snapshot after dropping event receipts
    /// already covered by durable cursors.
    pub fn compact_durable_state(&mut self) -> Result<u64, CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        self.persist()?;
        fs::metadata(&self.state_path)
            .map(|metadata| metadata.len())
            .map_err(io_error("inspect compacted namespace reconciliation state"))
    }

    pub fn poll_and_reconcile<F, E>(
        &mut self,
        initial_cursor: CloudFilesNamespaceCursor,
        feed: &mut F,
        effects: &mut E,
    ) -> Result<CloudFilesNamespaceReconcileProgress, CloudFilesNamespaceReconcileError>
    where
        F: CloudFilesNamespaceFeedAdapter,
        E: CloudFilesNamespaceEffectAdapter,
    {
        let cursor = self
            .cursor_for(&initial_cursor)?
            .cloned()
            .unwrap_or(initial_cursor);
        let batch = feed
            .poll(&cursor)
            .map_err(|detail| reconcile_error("namespace-feed", detail))?;
        self.reconcile_batch(batch, effects)
    }

    pub fn reconcile_batch<E>(
        &mut self,
        batch: CloudFilesNamespaceFeedBatch,
        effects: &mut E,
    ) -> Result<CloudFilesNamespaceReconcileProgress, CloudFilesNamespaceReconcileError>
    where
        E: CloudFilesNamespaceEffectAdapter,
    {
        self.ensure_usable()?;
        self.recover_inflight(effects)?;
        let shard_key = validate_batch(&batch, &self.state)?;
        let mut progress = CloudFilesNamespaceReconcileProgress::default();
        if let [CloudFilesNamespaceFeedEvent::RescanRequired { reason, detail }] =
            batch.events.as_slice()
        {
            let request = CloudFilesFullScanRequest {
                shard_key,
                cursor: batch.input_cursor,
                reason: reason.clone(),
                detail: detail.clone(),
            };
            let previous = self.state.clone();
            self.state.full_scan_request = Some(request.clone());
            self.persist_or_restore(previous)?;
            effects
                .request_full_scan(&request)
                .map_err(|detail| reconcile_error("request-full-scan-effect", detail))?;
            progress.full_scan_requested = true;
            return Ok(progress);
        }

        for event in &batch.events {
            let (provenance, action) = event_action(event, &self.root)?;
            if self.state.applied_events.contains_key(&provenance.event_id) {
                continue;
            }
            if self.suppression_matches(provenance, &action) {
                let operation_id = provenance
                    .operation_id
                    .clone()
                    .expect("matching suppression has an operation ID");
                let previous = self.state.clone();
                self.consume_matching_suppression(provenance, &action)?;
                apply_materialized_action(&mut self.state, &action);
                let mutations = manifest_mutations_for_action(&action);
                let undo = apply_manifest_mutations_with_undo(&mut self.manifest, &mutations);
                self.record_applied_event(&shard_key, provenance);
                self.persist_manifest_or_restore(previous, mutations, undo)?;
                progress.effects_suppressed += 1;
                progress.reconciled_local_operation_ids.push(operation_id);
                continue;
            }
            let local_states = self.local_states_for_action(&action, effects)?;
            if local_states
                .values()
                .any(|state| !matches!(state, CloudFilesLocalPathState::Clean))
            {
                let conflict = CloudFilesNamespaceConflict {
                    event_id: provenance.event_id.clone(),
                    action: action.clone(),
                    local_states,
                    detail: "remote namespace change intersects a dirty or pending local path"
                        .to_string(),
                };
                let previous = self.state.clone();
                self.state
                    .conflicts
                    .insert(provenance.event_id.clone(), conflict.clone());
                let mut completed_conflict = self.state.clone();
                completed_conflict.applied_events.insert(
                    provenance.event_id.clone(),
                    CloudFilesAppliedNamespaceEvent {
                        shard_key: shard_key.clone(),
                        journal_sequence: provenance.journal_sequence,
                    },
                );
                if let Err(error) = ensure_state_fits(&completed_conflict) {
                    self.state = previous;
                    return Err(error);
                }
                self.persist_or_restore(previous)?;
                effects
                    .record_conflict(&conflict)
                    .map_err(|detail| reconcile_error("record-conflict-effect", detail))?;
                let previous = self.state.clone();
                self.record_applied_event(&shard_key, provenance);
                self.persist_or_restore(previous)?;
                progress.conflicts_recorded += 1;
                continue;
            }

            let pending = CloudFilesPendingNamespaceEffect {
                event_id: provenance.event_id.clone(),
                shard_key: shard_key.clone(),
                journal_sequence: provenance.journal_sequence,
                action: action.clone(),
                resolving_conflict_event_id: None,
            };
            let mut completed_effect = self.state.clone();
            completed_effect.inflight_effect = Some(pending.clone());
            finish_inflight_state(&mut completed_effect)?;
            ensure_state_fits(&completed_effect)?;
            let previous = self.state.clone();
            self.state.inflight_effect = Some(pending);
            self.persist_or_restore(previous)?;
            effects
                .apply(&action)
                .map_err(|detail| reconcile_error("namespace-effect", detail))?;
            self.finish_inflight_effect()?;
            progress.effects_applied += 1;
        }

        let previous = self.state.clone();
        self.state
            .cursors
            .insert(shard_key.clone(), batch.next_cursor.clone());
        self.state.applied_events.retain(|_, event| {
            event.shard_key != shard_key
                || event.journal_sequence > batch.next_cursor.after_sequence
        });
        self.persist_or_restore(previous)?;
        progress.cursor_advanced = true;
        Ok(progress)
    }

    pub fn install_full_scan(
        &mut self,
        manifest: BTreeMap<String, CloudFilesReconciledManifestEntry>,
        cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
    ) -> Result<Vec<String>, CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        for (path, entry) in &manifest {
            if path != &entry.path || canonical_sifs_path(path)? != *path {
                return Err(reconcile_error(
                    "invalid-full-scan",
                    format!(
                        "manifest key `{path}` does not match entry `{}`",
                        entry.path
                    ),
                ));
            }
            ensure_within_root(path, &self.root)?;
            validate_manifest_entry(entry)?;
        }
        for (key, cursor) in &cursors {
            if key != &cursor.shard_key()? {
                return Err(reconcile_error(
                    "invalid-full-scan",
                    format!("cursor key `{key}` does not match its shard identity"),
                ));
            }
        }
        if self.state.inflight_effect.is_some() {
            return Err(reconcile_error(
                "namespace-effect-inflight",
                "cannot replace reconciliation state while a durable local effect is pending",
            ));
        }
        let previous = self.state.clone();
        let reconciled_local_operation_ids = self
            .state
            .suppressions
            .iter()
            .filter(|(_, suppression)| {
                suppression_action_matches_manifest(&suppression.expected_action, &manifest)
            })
            .map(|(operation_id, _)| operation_id.clone())
            .collect::<Vec<_>>();
        self.state.cursors = cursors;
        self.state.full_scan_request = None;
        self.state.inflight_effect = None;
        self.state.applied_events.clear();
        for operation_id in &reconciled_local_operation_ids {
            self.state.suppressions.remove(operation_id);
        }
        let mutations = manifest_transition(&self.manifest, &manifest);
        apply_materialized_manifest_mutations(&mut self.state, &mutations);
        let undo = apply_manifest_mutations_with_undo(&mut self.manifest, &mutations);
        self.persist_manifest_or_restore(previous, mutations, undo)?;
        Ok(reconciled_local_operation_ids)
    }

    /// Merge one bounded authoritative scan delta. Cursors are installed only
    /// with the terminal delta, after every preceding placeholder effect and
    /// manifest mutation is durable. Replaying a page after a crash is
    /// idempotent; skipping an unapplied page is not possible.
    pub fn install_full_scan_delta(
        &mut self,
        manifest: BTreeMap<String, CloudFilesReconciledManifestEntry>,
        removed_paths: Vec<String>,
        materialized_directories: Vec<String>,
        cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
        scan_complete: bool,
    ) -> Result<Vec<String>, CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        for (path, entry) in &manifest {
            if path != &entry.path || canonical_sifs_path(path)? != *path {
                return Err(reconcile_error(
                    "invalid-full-scan",
                    format!(
                        "manifest key `{path}` does not match entry `{}`",
                        entry.path
                    ),
                ));
            }
            ensure_within_root(path, &self.root)?;
            validate_manifest_entry(entry)?;
        }
        for path in &removed_paths {
            if canonical_sifs_path(path)? != *path {
                return Err(reconcile_error(
                    "invalid-full-scan",
                    format!("removed path `{path}` is not canonical"),
                ));
            }
            ensure_within_root(path, &self.root)?;
        }
        let mut materialized_directories = materialized_directories
            .into_iter()
            .map(|directory| {
                let canonical = canonical_sifs_path(&directory)?;
                if canonical != directory {
                    return Err(reconcile_error(
                        "invalid-full-scan",
                        format!("materialized directory `{directory}` is not canonical"),
                    ));
                }
                ensure_within_root(&canonical, &self.root)?;
                Ok(canonical)
            })
            .collect::<Result<Vec<_>, CloudFilesNamespaceReconcileError>>()?;
        if self.cursor_recovery_plan.is_some() {
            materialized_directories.sort();
            materialized_directories.dedup();
        }
        if scan_complete {
            for (key, cursor) in &cursors {
                if key != &cursor.shard_key()? {
                    return Err(reconcile_error(
                        "invalid-full-scan",
                        format!("cursor key `{key}` does not match its shard identity"),
                    ));
                }
            }
        } else if !cursors.is_empty() {
            return Err(reconcile_error(
                "invalid-full-scan",
                "a partial full-scan delta cannot advance namespace cursors",
            ));
        }
        if self.state.inflight_effect.is_some() {
            return Err(reconcile_error(
                "namespace-effect-inflight",
                "cannot merge reconciliation state while a durable local effect is pending",
            ));
        }

        let recovery_page =
            self.cursor_recovery_plan
                .as_ref()
                .map(|_| CloudFilesCursorRecoveryPage {
                    materialized_directories: materialized_directories.clone(),
                    cursors: cursors.clone(),
                    scan_complete,
                });

        let previous = self.state.clone();
        self.state
            .materialized_directories
            .extend(materialized_directories);
        let mut mutations = removed_paths
            .into_iter()
            .map(|path| CloudFilesManifestMutation::Remove { path })
            .collect::<Vec<_>>();
        mutations.extend(
            manifest
                .into_values()
                .map(|entry| CloudFilesManifestMutation::Upsert { entry }),
        );
        if self.cursor_recovery_plan.is_some() {
            mutations.sort_by(|left, right| {
                manifest_mutation_path(left).cmp(manifest_mutation_path(right))
            });
            if mutations
                .windows(2)
                .any(|pair| manifest_mutation_path(&pair[0]) == manifest_mutation_path(&pair[1]))
            {
                return Err(reconcile_error(
                    "invalid-full-scan",
                    "cursor recovery page contains multiple mutations for one path",
                ));
            }
        }
        apply_materialized_manifest_mutations(&mut self.state, &mutations);
        let undo = apply_manifest_mutations_with_undo(&mut self.manifest, &mutations);
        if scan_complete {
            for (key, cursor) in cursors {
                let replace = self
                    .state
                    .cursors
                    .get(&key)
                    .is_none_or(|current| cursor.after_sequence >= current.after_sequence);
                if replace {
                    self.state.cursors.insert(key, cursor);
                }
            }
            self.state.full_scan_request = None;
            self.state.applied_events.clear();
        }
        let reconciled_local_operation_ids = self
            .state
            .suppressions
            .iter()
            .filter(|(_, suppression)| {
                suppression_action_matches_manifest(&suppression.expected_action, &self.manifest)
            })
            .map(|(operation_id, _)| operation_id.clone())
            .collect::<Vec<_>>();
        for operation_id in &reconciled_local_operation_ids {
            self.state.suppressions.remove(operation_id);
        }
        if let Some(page) = recovery_page {
            self.persist_cursor_recovery_page_or_restore(previous, mutations, undo, page)?;
        } else {
            self.persist_manifest_or_restore(previous, mutations, undo)?;
        }
        Ok(reconciled_local_operation_ids)
    }

    /// Replays and finalizes a durable namespace effect before any full-scan
    /// delta is allowed to replace or merge reconciliation state.
    pub fn recover_inflight_effect<E>(
        &mut self,
        effects: &mut E,
    ) -> Result<(), CloudFilesNamespaceReconcileError>
    where
        E: CloudFilesNamespaceEffectAdapter,
    {
        let Some(inflight) = self.state.inflight_effect.clone() else {
            return Ok(());
        };
        let mut completed = self.state.clone();
        finish_inflight_state(&mut completed)?;
        ensure_state_fits(&completed)?;
        effects
            .apply(&inflight.action)
            .map_err(|detail| reconcile_error("recover-namespace-effect", detail))?;
        self.finish_inflight_effect()
    }

    fn recover_inflight<E>(
        &mut self,
        effects: &mut E,
    ) -> Result<(), CloudFilesNamespaceReconcileError>
    where
        E: CloudFilesNamespaceEffectAdapter,
    {
        self.recover_inflight_effect(effects)
    }

    fn finish_inflight_effect(&mut self) -> Result<(), CloudFilesNamespaceReconcileError> {
        let previous = self.state.clone();
        let action = finish_inflight_state(&mut self.state)?;
        let mutations = manifest_mutations_for_action(&action);
        let undo = apply_manifest_mutations_with_undo(&mut self.manifest, &mutations);
        self.persist_manifest_or_restore(previous, mutations, undo)
    }

    fn consume_matching_suppression(
        &mut self,
        provenance: &CloudFilesNamespaceProvenance,
        action: &CloudFilesNamespaceAction,
    ) -> Result<bool, CloudFilesNamespaceReconcileError> {
        let Some(operation_id) = provenance.operation_id.as_ref() else {
            return Ok(false);
        };
        let Some(suppression) = self.state.suppressions.get(operation_id) else {
            return Ok(false);
        };
        if suppression.expected_action != *action {
            return Ok(false);
        }
        self.state.suppressions.remove(operation_id);
        Ok(true)
    }

    fn suppression_matches(
        &self,
        provenance: &CloudFilesNamespaceProvenance,
        action: &CloudFilesNamespaceAction,
    ) -> bool {
        provenance
            .operation_id
            .as_ref()
            .and_then(|operation_id| self.state.suppressions.get(operation_id))
            .is_some_and(|suppression| suppression.expected_action == *action)
    }

    fn local_states_for_action<E>(
        &self,
        action: &CloudFilesNamespaceAction,
        effects: &mut E,
    ) -> Result<BTreeMap<String, CloudFilesLocalPathState>, CloudFilesNamespaceReconcileError>
    where
        E: CloudFilesNamespaceEffectAdapter,
    {
        action
            .paths()
            .into_iter()
            .map(|path| {
                effects
                    .local_path_state(path)
                    .map(|state| (path.to_string(), state))
                    .map_err(|detail| reconcile_error("local-path-state", detail))
            })
            .collect()
    }

    fn record_applied_event(
        &mut self,
        shard_key: &str,
        provenance: &CloudFilesNamespaceProvenance,
    ) {
        self.state.applied_events.insert(
            provenance.event_id.clone(),
            CloudFilesAppliedNamespaceEvent {
                shard_key: shard_key.to_string(),
                journal_sequence: provenance.journal_sequence,
            },
        );
    }

    fn validate_suppression(
        &self,
        suppression: &CloudFilesNamespaceSuppression,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        if suppression.operation_id.trim().is_empty() {
            return Err(reconcile_error(
                "invalid-suppression",
                "suppression operation ID cannot be empty",
            ));
        }
        validate_action(&suppression.expected_action, &self.root)?;
        if let Some(existing) = self.state.suppressions.get(&suppression.operation_id)
            && existing != suppression
        {
            return Err(reconcile_error(
                "suppression-id-reused",
                format!(
                    "operation ID `{}` already names a different namespace action",
                    suppression.operation_id
                ),
            ));
        }
        Ok(())
    }

    fn ensure_usable(&self) -> Result<(), CloudFilesNamespaceReconcileError> {
        if !self.usable {
            Err(reconcile_error(
                "reconciler-reopen-required",
                "a prior durable state write was uncertain; reopen before continuing",
            ))
        } else {
            self.manifest_store.ensure_usable()
        }
    }

    fn persist(&mut self) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        compact_reconciliation_state(&mut self.state);
        let bytes = encode_reconciliation_state(&self.state)?;
        let result = durable_file::write_file_atomic(
            &self.state_path,
            &bytes,
            DurableAtomicFileOptions::new(RECONCILIATION_STATE_FILE, RECONCILIATION_TEMP_LABEL)
                .sync_all(),
        );
        if let Err(source) = result {
            self.usable = false;
            return Err(storage_error("persist namespace reconciliation state")(
                source,
            ));
        }
        if let Err(error) = self.manifest_store.compact_if_needed(&self.manifest) {
            self.usable = false;
            return Err(error);
        }
        Ok(())
    }

    fn persist_or_restore(
        &mut self,
        previous: CloudFilesNamespaceReconciliationState,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        compact_reconciliation_state(&mut self.state);
        let bytes = match encode_reconciliation_state(&self.state) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.state = previous;
                return Err(error);
            }
        };
        if let Err(source) = durable_file::write_file_atomic(
            &self.state_path,
            &bytes,
            DurableAtomicFileOptions::new(RECONCILIATION_STATE_FILE, RECONCILIATION_TEMP_LABEL)
                .sync_all(),
        ) {
            self.usable = false;
            return Err(storage_error("persist namespace reconciliation state")(
                source,
            ));
        }
        if let Err(error) = self.manifest_store.compact_if_needed(&self.manifest) {
            self.usable = false;
            return Err(error);
        }
        Ok(())
    }

    fn persist_manifest_or_restore(
        &mut self,
        previous: CloudFilesNamespaceReconciliationState,
        mutations: Vec<CloudFilesManifestMutation>,
        undo: CloudFilesManifestUndo,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        update_unsupported_projection_summary(&mut self.state, &self.manifest, &undo);
        compact_reconciliation_state(&mut self.state);
        let bytes = match encode_reconciliation_state(&self.state) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.state = previous;
                restore_manifest(&mut self.manifest, undo);
                return Err(error);
            }
        };
        if let Err(error) = self
            .manifest_store
            .append_mutations(mutations, &self.manifest)
        {
            self.usable = false;
            return Err(error);
        }
        if let Err(source) = durable_file::write_file_atomic(
            &self.state_path,
            &bytes,
            DurableAtomicFileOptions::new(RECONCILIATION_STATE_FILE, RECONCILIATION_TEMP_LABEL)
                .sync_all(),
        ) {
            // The manifest transition may already be durable. Reopening will
            // conservatively replay the old cursor/suppression state against
            // that idempotent manifest projection.
            self.usable = false;
            return Err(storage_error("persist namespace reconciliation state")(
                source,
            ));
        }
        if let Err(error) = self.manifest_store.compact_if_needed(&self.manifest) {
            self.usable = false;
            return Err(error);
        }
        Ok(())
    }

    fn persist_cursor_recovery_page_or_restore(
        &mut self,
        mut previous: CloudFilesNamespaceReconciliationState,
        mutations: Vec<CloudFilesManifestMutation>,
        undo: CloudFilesManifestUndo,
        page: CloudFilesCursorRecoveryPage,
    ) -> Result<(), CloudFilesNamespaceReconcileError> {
        self.ensure_usable()?;
        let Some(plan) = self.cursor_recovery_plan.as_ref() else {
            self.state = previous;
            restore_manifest(&mut self.manifest, undo);
            return Err(reconcile_error(
                "cursor-recovery-page-without-plan",
                "cursor recovery page persistence requires an authorized plan",
            ));
        };
        let scope = self.state_path.parent().ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "cursor recovery state has no reconciliation scope",
            )
        })?;
        let rewrite =
            load_required_cursor_recovery_rewrite(&scope.join(CURSOR_RECOVERY_REWRITE_FILE), plan)?;
        if rewrite.phase != CloudFilesCursorRecoveryRewritePhase::ReconcileArmed {
            self.state = previous;
            restore_manifest(&mut self.manifest, undo);
            return Err(reconcile_error(
                "cursor-recovery-page-not-armed",
                "authoritative recovery pages may be persisted only while ReconcileArmed",
            ));
        }
        if inspect_cursor_recovery_page_intent(scope)?.is_some() {
            self.state = previous;
            restore_manifest(&mut self.manifest, undo);
            return Err(reconcile_error(
                "cursor-recovery-page-intent-live",
                "a prior cursor recovery page intent requires authorized reopen",
            ));
        }

        let (unsupported_count, unsupported_examples) =
            unsupported_projection_summary(&self.manifest);
        self.state.unsupported_projection_count = unsupported_count;
        self.state.unsupported_projection_examples = unsupported_examples;
        compact_reconciliation_state(&mut previous);
        compact_reconciliation_state(&mut self.state);
        let before_state = match encode_reconciliation_state(&previous) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.state = previous;
                restore_manifest(&mut self.manifest, undo);
                return Err(error);
            }
        };
        let after_state = match encode_reconciliation_state(&self.state) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.state = previous;
                restore_manifest(&mut self.manifest, undo);
                return Err(error);
            }
        };
        let mut before_manifest = self.manifest.clone();
        for (path, previous_entry) in &undo {
            match previous_entry {
                Some(entry) => {
                    before_manifest.insert(path.clone(), entry.clone());
                }
                None => {
                    before_manifest.remove(path);
                }
            }
        }
        let previous_manifest_entries = undo.iter().cloned().collect::<BTreeMap<_, _>>();
        if previous_manifest_entries.len() != mutations.len() {
            self.state = previous;
            restore_manifest(&mut self.manifest, undo);
            return Err(reconcile_error(
                "cursor-recovery-page-intent-transition",
                "cursor recovery page mutations do not have an exact unique preimage set",
            ));
        }
        let intent = CloudFilesCursorRecoveryPageIntent {
            schema_version: CURSOR_RECOVERY_PAGE_INTENT_SCHEMA_VERSION,
            recovery_id: plan.recovery_id.clone(),
            recovery_token: plan.recovery_token.clone(),
            nonce: plan.nonce.clone(),
            plan_fingerprint: cursor_recovery_plan_fingerprint(plan)?,
            mount: self.state.mount.clone(),
            root: self.root.clone(),
            before_state_fingerprint: cursor_recovery_bytes_fingerprint(&before_state),
            before_state,
            after_state_fingerprint: cursor_recovery_bytes_fingerprint(&after_state),
            after_state: after_state.clone(),
            before_manifest_fingerprint: cursor_recovery_manifest_fingerprint(&before_manifest)?,
            after_manifest_fingerprint: cursor_recovery_manifest_fingerprint(&self.manifest)?,
            mutations: mutations.clone(),
            previous_manifest_entries,
            materialized_directories: page.materialized_directories,
            cursors: page.cursors,
            scan_complete: page.scan_complete,
        };
        if let Err(error) = validate_cursor_recovery_page_intent(&intent, plan, &self.manifest) {
            self.state = previous;
            restore_manifest(&mut self.manifest, undo);
            return Err(error);
        }
        let intent_path = scope.join(CURSOR_RECOVERY_PAGE_INTENT_FILE);
        if let Err(error) = persist_cursor_recovery_page_intent(&intent_path, &intent) {
            self.usable = false;
            return Err(error);
        }
        if cursor_recovery_page_crash_is_armed(CursorRecoveryPageCrashCut::AfterIntent) {
            self.usable = false;
            return Err(reconcile_error(
                "injected-cursor-recovery-page-crash",
                "injected crash after page-intent durability and before manifest durability",
            ));
        }
        if let Err(error) = self
            .manifest_store
            .append_mutations(mutations, &self.manifest)
        {
            self.usable = false;
            return Err(error);
        }
        if cursor_recovery_page_crash_is_armed(CursorRecoveryPageCrashCut::AfterManifest) {
            self.usable = false;
            return Err(reconcile_error(
                "injected-cursor-recovery-page-crash",
                "injected crash after manifest durability and before recovery state durability",
            ));
        }
        if let Err(source) = durable_file::write_file_atomic(
            &self.state_path,
            &after_state,
            DurableAtomicFileOptions::new(RECONCILIATION_STATE_FILE, RECONCILIATION_TEMP_LABEL)
                .sync_all(),
        ) {
            self.usable = false;
            return Err(storage_error("persist cursor recovery page state")(source));
        }
        if cursor_recovery_page_crash_is_armed(CursorRecoveryPageCrashCut::AfterState) {
            self.usable = false;
            return Err(reconcile_error(
                "injected-cursor-recovery-page-crash",
                "injected crash after recovery state durability and before page-intent completion",
            ));
        }
        if let Err(error) = durable_file::remove_file_if_exists_and_sync_parent(&intent_path)
            .map_err(storage_error("complete cursor recovery page intent"))
        {
            self.usable = false;
            return Err(error);
        }
        if let Err(error) = self.manifest_store.compact_if_needed(&self.manifest) {
            self.usable = false;
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
std::thread_local! {
    static CURSOR_RECOVERY_PAGE_CRASH_CUT: std::cell::Cell<u8> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorRecoveryPageCrashCut {
    AfterIntent = 1,
    AfterManifest = 2,
    AfterState = 3,
}

#[cfg(test)]
fn arm_cursor_recovery_page_crash(cut: CursorRecoveryPageCrashCut) {
    CURSOR_RECOVERY_PAGE_CRASH_CUT.with(|armed| armed.set(cut as u8));
}

#[cfg(test)]
fn cursor_recovery_page_crash_is_armed(cut: CursorRecoveryPageCrashCut) -> bool {
    CURSOR_RECOVERY_PAGE_CRASH_CUT.with(|armed| {
        if armed.get() == cut as u8 {
            armed.set(0);
            true
        } else {
            false
        }
    })
}

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorRecoveryPageCrashCut {
    AfterIntent,
    AfterManifest,
    AfterState,
}

#[cfg(not(test))]
fn cursor_recovery_page_crash_is_armed(_cut: CursorRecoveryPageCrashCut) -> bool {
    false
}

/// Read-only Fresh-activation admission check. Opening this scope can create
/// state, remove abandoned stages, repair journals, or migrate legacy
/// manifests, so v3 first proves the exact mount/root directory does not exist
/// and establishes ownership before calling `open`.
pub(crate) fn fresh_reconciliation_scope_is_absent(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<bool, CloudFilesNamespaceReconcileError> {
    if mount.trim().is_empty() {
        return Err(reconcile_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(root)?;
    let scope = data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(RECONCILIATION_DIR)
        .join(mount_scope_directory(mount, &root)?);
    match fs::symlink_metadata(&scope) {
        Ok(_) => Ok(false),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(source) => Err(io_error("inspect namespace reconciliation scope")(source)),
    }
}

pub(crate) fn inspect_cursor_only_reconciliation_state(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<CloudFilesCursorOnlyReconciliationInspectionGuard, CloudFilesNamespaceReconcileError> {
    if mount.trim().is_empty() {
        return Err(reconcile_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(root)?;
    let module_runtime = data_dir.module_runtime_dir();
    let module_dir = module_runtime.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
    let reconciliation_root = module_dir.join(RECONCILIATION_DIR);
    for (label, path) in [
        ("module runtime", module_runtime.as_path()),
        ("Cloud Files module", module_dir.as_path()),
        ("namespace reconciliation", reconciliation_root.as_path()),
    ] {
        let metadata = fs::symlink_metadata(path)
            .map_err(io_error("inspect cursor-only reconciliation parent"))?;
        if !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("{label} path {} is not a plain directory", path.display()),
            ));
        }
    }
    let scope_name = mount_scope_directory(mount, &root)?;
    let scope = reconciliation_root.join(&scope_name);
    let metadata = fs::symlink_metadata(&scope)
        .map_err(io_error("inspect cursor-only reconciliation scope"))?;
    if !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            format!(
                "reconciliation scope {} is not a plain directory",
                scope.display()
            ),
        ));
    }
    for entry in fs::read_dir(&reconciliation_root)
        .map_err(io_error("list namespace reconciliation root"))?
    {
        let entry = entry.map_err(io_error("read namespace reconciliation root entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "reconciliation root contains a non-UTF-8 entry",
            )
        })?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(io_error("inspect namespace reconciliation root entry"))?;
        if name != scope_name || !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("unexpected reconciliation root entry `{name}`"),
            ));
        }
    }

    let lock_path = scope.join(RECONCILIATION_LOCK_FILE);
    let lock_metadata = fs::symlink_metadata(&lock_path).map_err(io_error(
        "inspect pre-existing cursor-only reconciliation lock",
    ))?;
    if !lock_metadata.file_type().is_file() || metadata_is_reparse(&lock_metadata) {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-lock",
            "reconciliation lock is not a plain pre-existing file",
        ));
    }
    let reconciliation_lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&lock_path)
        .map_err(io_error(
            "open pre-existing cursor-only reconciliation lock",
        ))?;
    reconciliation_lock.try_lock_exclusive().map_err(|source| {
        reconcile_error(
            "cursor-recovery-reconciliation-live",
            format!("namespace reconciler is live: {source}"),
        )
    })?;

    let mut raw_files = BTreeMap::new();
    let mut state = None;
    let mut saw_lock = false;
    let mut saw_manifest = false;
    let mut saw_materialization_audit = false;
    for entry in fs::read_dir(&scope).map_err(io_error("list cursor-only reconciliation scope"))? {
        let entry = entry.map_err(io_error("read cursor-only reconciliation entry"))?;
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "reconciliation scope contains a non-UTF-8 entry",
            )
        })?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(io_error("inspect cursor-only reconciliation entry"))?;
        if metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("reconciliation scope contains symlink/reparse evidence `{name}`"),
            ));
        }
        match name.as_str() {
            RECONCILIATION_STATE_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(
                    &path,
                    CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES,
                    "cursor-only reconciliation state",
                )?;
                let decoded: CloudFilesNamespaceReconciliationState =
                    decode_sobj(&bytes).map_err(|source| {
                        reconcile_error("decode-reconciliation-state", source.to_string())
                    })?;
                if encode_sobj(&decoded).map_err(|source| {
                    reconcile_error("encode-reconciliation-state", source.to_string())
                })? != bytes
                {
                    return Err(reconcile_error(
                        "cursor-recovery-reconciliation-noncanonical",
                        "reconciliation state bytes are not canonical",
                    ));
                }
                state = Some(decoded);
                raw_files.insert(name, bytes);
            }
            RECONCILIATION_LOCK_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(&path, 0, "cursor-only reconciliation lock")?;
                raw_files.insert(name, bytes);
                saw_lock = true;
            }
            MATERIALIZATION_AUDIT_INTENTS_FILE if metadata.file_type().is_file() => {
                let bytes = read_bounded_plain_file(
                    &path,
                    CURSOR_RECOVERY_STATE_IMAGE_MAX_BYTES,
                    "cursor-only materialization audit state",
                )?;
                let intents: CloudFilesMaterializationAuditIntentState = decode_sobj(&bytes)
                    .map_err(|source| {
                        reconcile_error("decode-materialization-audit-intents", source.to_string())
                    })?;
                if intents.schema_version != MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION
                    || intents.mount != mount
                    || intents.root != root
                    || !intents.directories.is_empty()
                    || encode_sobj(&intents).map_err(|source| {
                        reconcile_error("encode-materialization-audit-intents", source.to_string())
                    })? != bytes
                {
                    return Err(reconcile_error(
                        "cursor-recovery-materialization-audit-not-empty",
                        "materialization audit state is nonempty, mismatched, or noncanonical",
                    ));
                }
                raw_files.insert(name, bytes);
                saw_materialization_audit = true;
            }
            MANIFEST_DIR if metadata.file_type().is_dir() => {
                inspect_empty_cursor_recovery_manifest(&path, &mut raw_files)?;
                saw_manifest = true;
            }
            _ => {
                return Err(reconcile_error(
                    "cursor-recovery-reconciliation-topology",
                    format!("unexpected reconciliation recovery entry `{name}`"),
                ));
            }
        }
    }
    if !saw_lock || !saw_manifest || !saw_materialization_audit {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "reconciliation scope is missing its canonical lock, manifest, or audit state",
        ));
    }
    let state = state.ok_or_else(|| {
        reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "reconciliation scope is missing state.sobj",
        )
    })?;
    if state.schema_version != RECONCILIATION_SCHEMA_VERSION
        || state.mount != mount
        || state.root != root
        || state.cursors.is_empty()
        || !state.legacy_manifest.is_empty()
        || !state.suppressions.is_empty()
        || !state.conflicts.is_empty()
        || !state.startup_conflicts.is_empty()
        || !state.materialized_directories.is_empty()
        || state.unsupported_projection_count != 0
        || !state.unsupported_projection_examples.is_empty()
        || state.full_scan_request.is_some()
        || state.inflight_effect.is_some()
        || !state.applied_events.is_empty()
    {
        return Err(reconcile_error(
            "cursor-recovery-not-cursor-only",
            "reconciliation state contains non-cursor history or lacks a valid cursor set",
        ));
    }
    for (key, cursor) in &state.cursors {
        if key != &cursor.shard_key()? || cursor.authority_epoch == 0 {
            return Err(reconcile_error(
                "cursor-recovery-invalid-cursor",
                format!("cursor `{key}` has an invalid shard key or authority epoch"),
            ));
        }
    }
    Ok(CloudFilesCursorOnlyReconciliationInspectionGuard {
        inspection: CloudFilesCursorOnlyReconciliationInspection {
            scope_path: scope,
            raw_files,
            cursors: state.cursors,
        },
        _lock: reconciliation_lock,
    })
}

/// Opens only a recovery whose complete before/after images were already
/// authorized durably by the module. This is intentionally separate from the
/// public strict inspector: post-authorization state must never be mistaken
/// for a newly eligible cursor-only tree.
pub(crate) fn resume_authorized_cursor_recovery(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<CloudFilesCursorOnlyReconciliationInspectionGuard, CloudFilesNamespaceReconcileError> {
    if mount.trim().is_empty() {
        return Err(reconcile_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(root)?;
    let validated = validate_cursor_recovery_plan(plan, mount, &root)?;
    let module_runtime = data_dir.module_runtime_dir();
    let module_dir = module_runtime.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
    let reconciliation_root = module_dir.join(RECONCILIATION_DIR);
    for (label, path) in [
        ("module runtime", module_runtime.as_path()),
        ("Cloud Files module", module_dir.as_path()),
        ("namespace reconciliation", reconciliation_root.as_path()),
    ] {
        let metadata = fs::symlink_metadata(path)
            .map_err(io_error("inspect authorized reconciliation parent"))?;
        if !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("{label} path {} is not a plain directory", path.display()),
            ));
        }
    }
    let scope_name = mount_scope_directory(mount, &root)?;
    let scope = reconciliation_root.join(&scope_name);
    for entry in fs::read_dir(&reconciliation_root)
        .map_err(io_error("list authorized reconciliation root"))?
    {
        let entry = entry.map_err(io_error("read authorized reconciliation root entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "reconciliation root contains a non-UTF-8 entry",
            )
        })?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(io_error("inspect authorized reconciliation root entry"))?;
        if name != scope_name || !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("unexpected reconciliation root entry `{name}`"),
            ));
        }
    }
    let scope_metadata = fs::symlink_metadata(&scope)
        .map_err(io_error("inspect authorized reconciliation scope"))?;
    if !scope_metadata.file_type().is_dir() || metadata_is_reparse(&scope_metadata) {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "authorized reconciliation scope is not a plain directory",
        ));
    }
    let lock_path = scope.join(RECONCILIATION_LOCK_FILE);
    read_bounded_plain_file(&lock_path, 0, "authorized reconciliation lock")?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&lock_path)
        .map_err(io_error("open authorized reconciliation lock"))?;
    lock.try_lock_exclusive().map_err(|source| {
        reconcile_error(
            "cursor-recovery-reconciliation-live",
            format!("namespace reconciler is live: {source}"),
        )
    })?;
    let cut = inspect_authorized_cursor_recovery_cut(&scope, plan)?;
    Ok(CloudFilesCursorOnlyReconciliationInspectionGuard {
        inspection: CloudFilesCursorOnlyReconciliationInspection {
            scope_path: scope,
            raw_files: cut.raw_files,
            cursors: validated.original.cursors,
        },
        _lock: lock,
    })
}

/// Reopens a recovery after the deterministic rewrite has completed and the
/// authoritative scan may already have installed bounded page deltas. This
/// never accepts a pristine/torn rewrite cut and never repairs owner files.
pub(crate) fn open_authorized_cursor_recovery_progress(
    data_dir: &SinomoDataDir,
    mount: impl Into<String>,
    root: impl Into<String>,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<CloudFilesNamespaceReconciler, CloudFilesNamespaceReconcileError> {
    open_authorized_cursor_recovery_progress_inner(data_dir, mount.into(), root.into(), plan, false)
        .map(|(reconciler, _)| reconciler)
}

/// Acquires the reconciliation owner lock and returns a positive successor
/// guard only when the exact plan-bound Terminal cut is already durable. A
/// valid earlier progress cut returns `None` without changing any owner file;
/// malformed, torn, or ambiguous evidence fails closed.
pub(crate) fn acquire_authorized_cursor_recovery_terminal_successor(
    data_dir: &SinomoDataDir,
    mount: impl Into<String>,
    root: impl Into<String>,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<Option<CloudFilesCursorRecoveryTerminalSuccessorGuard>, CloudFilesNamespaceReconcileError>
{
    let mount = mount.into();
    if mount.trim().is_empty() {
        return Err(reconcile_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(&root.into())?;
    let validated = validate_cursor_recovery_plan(plan, &mount, &root)?;
    let reconciliation_dir = data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(RECONCILIATION_DIR)
        .join(mount_scope_directory(&mount, &root)?);
    let scope_metadata = fs::symlink_metadata(&reconciliation_dir)
        .map_err(io_error("inspect terminal successor reconciliation scope"))?;
    if !scope_metadata.file_type().is_dir() || metadata_is_reparse(&scope_metadata) {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "terminal successor reconciliation scope is not a plain directory",
        ));
    }
    let lock_path = reconciliation_dir.join(RECONCILIATION_LOCK_FILE);
    read_bounded_plain_file(&lock_path, 0, "terminal successor reconciliation lock")?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&lock_path)
        .map_err(io_error("open terminal successor reconciliation lock"))?;
    lock.try_lock_exclusive().map_err(|source| {
        reconcile_error(
            "cursor-recovery-reconciliation-live",
            format!("namespace reconciler is live: {source}"),
        )
    })?;

    let mut rewrite_stage = None;
    for entry in
        fs::read_dir(&reconciliation_dir).map_err(io_error("list terminal successor scope"))?
    {
        let entry = entry.map_err(io_error("read terminal successor scope entry"))?;
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "terminal successor scope contains a non-UTF-8 entry",
            )
        })?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(io_error("inspect terminal successor scope entry"))?;
        if metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-reconciliation-topology",
                format!("terminal successor contains reparse evidence `{name}`"),
            ));
        }
        let expected_final = matches!(
            name.as_str(),
            RECONCILIATION_STATE_FILE
                | RECONCILIATION_LOCK_FILE
                | MATERIALIZATION_AUDIT_INTENTS_FILE
                | CURSOR_RECOVERY_REWRITE_FILE
                | CURSOR_RECOVERY_PAGE_INTENT_FILE
        );
        if expected_final {
            if !metadata.file_type().is_file() {
                return Err(reconcile_error(
                    "cursor-recovery-reconciliation-topology",
                    format!("terminal successor owner entry `{name}` is not a plain file"),
                ));
            }
            continue;
        }
        if name == MANIFEST_DIR {
            if !metadata.file_type().is_dir() {
                return Err(reconcile_error(
                    "cursor-recovery-reconciliation-topology",
                    "terminal successor manifest is not a plain directory",
                ));
            }
            continue;
        }
        if looks_like_cursor_recovery_page_intent_temp(&name) {
            if !metadata.file_type().is_file()
                || !canonical_atomic_stage_name(
                    &name,
                    CURSOR_RECOVERY_PAGE_INTENT_FILE,
                    CURSOR_RECOVERY_PAGE_INTENT_TEMP_LABEL,
                )
            {
                return Err(reconcile_error(
                    "cursor-recovery-page-intent-topology",
                    "terminal successor contains malformed page-intent evidence",
                ));
            }
            continue;
        }
        if looks_like_cursor_recovery_temp(&name) {
            let kind = cursor_recovery_temp_kind(&name).ok_or_else(|| {
                reconcile_error(
                    "cursor-recovery-rewrite-torn",
                    format!("terminal successor has malformed durable stage `{name}`"),
                )
            })?;
            if !metadata.file_type().is_file()
                || kind != CursorRecoveryTempKind::Rewrite
                || rewrite_stage.is_some()
            {
                return Err(reconcile_error(
                    "cursor-recovery-rewrite-torn",
                    "terminal successor permits only one exact plain rewrite stage",
                ));
            }
            rewrite_stage = Some(path);
            continue;
        }
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            format!("terminal successor contains unexpected entry `{name}`"),
        ));
    }
    if inspect_cursor_recovery_page_intent(&reconciliation_dir)?.is_some() {
        // A canonical page intent is a legitimate earlier progress cut, but it
        // can never authorize TerminalPendingFinalize. The progress opener
        // owns its later deterministic repair.
        return Ok(None);
    }
    let state_path = reconciliation_dir.join(RECONCILIATION_STATE_FILE);
    let state_bytes = read_bounded_plain_file(
        &state_path,
        RECONCILIATION_STATE_MAX_BYTES,
        "terminal successor reconciliation state",
    )?;
    let state: CloudFilesNamespaceReconciliationState = decode_sobj(&state_bytes)
        .map_err(|source| reconcile_error("decode-reconciliation-state", source.to_string()))?;
    if encode_reconciliation_state(&state)? != state_bytes
        || !cursor_recovery_progress_state_is_valid(&state, &validated, false)?
    {
        return Err(reconcile_error(
            "cursor-recovery-progress-invalid",
            "terminal successor state is noncanonical or not authorized scan progress",
        ));
    }
    let terminal_state = cursor_recovery_progress_state_is_valid(&state, &validated, true)?
        && state.materialized_directories == BTreeSet::from([root.clone()]);
    let audit = load_required_cursor_recovery_audit(&reconciliation_dir, &mount, &root)?;
    let rewrite_path = reconciliation_dir.join(CURSOR_RECOVERY_REWRITE_FILE);
    let mut rewrite = load_required_cursor_recovery_rewrite(&rewrite_path, plan)?;

    if let Some(stage_path) = rewrite_stage {
        if !terminal_state || !audit.directories.is_empty() {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-torn",
                "terminal rewrite stage is not preceded by exact terminal owner state",
            ));
        }
        let terminal = CloudFilesCursorRecoveryRewrite::from_plan(
            plan,
            CloudFilesCursorRecoveryRewritePhase::Terminal,
        );
        let expected = encode_sobj(&terminal).map_err(|source| {
            reconcile_error("encode-cursor-recovery-rewrite", source.to_string())
        })?;
        let staged = read_bounded_plain_file(
            &stage_path,
            CURSOR_RECOVERY_REWRITE_MAX_BYTES,
            "terminal successor rewrite stage",
        )?;
        if staged != expected
            || !matches!(
                rewrite.phase,
                CloudFilesCursorRecoveryRewritePhase::ReconcileArmed
                    | CloudFilesCursorRecoveryRewritePhase::Terminal
            )
        {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-mismatch",
                "terminal successor rewrite stage is not its exact plan-bound successor",
            ));
        }
        // The stage is already fsynced. Atomically promote that exact entry and
        // sync the parent; never delete it and create a new power-loss gap.
        durable_file::install_staged_file(&stage_path, &rewrite_path, true)
            .map_err(storage_error("promote terminal recovery rewrite stage"))?;
        rewrite = terminal;
    }

    if rewrite.phase == CloudFilesCursorRecoveryRewritePhase::ReconcileArmed {
        // The remote scan may have installed its terminal state before the
        // worker finishes the observer/root audit and writes Terminal. That is
        // still a valid earlier progress cut and must resume the parked worker,
        // never be upgraded from state shape alone.
        return Ok(None);
    }
    if !terminal_state || !audit.directories.is_empty() {
        return Err(reconcile_error(
            "cursor-recovery-terminal-incomplete",
            "Terminal rewrite is not backed by exact root-only terminal owner state",
        ));
    }

    let (manifest_store, manifest) =
        CloudFilesManifestStore::open_authorized_cursor_recovery_terminal(&reconciliation_dir)?;
    validate_recovery_manifest(&manifest, &state, &root)?;
    Ok(Some(CloudFilesCursorRecoveryTerminalSuccessorGuard {
        reconciler: CloudFilesNamespaceReconciler {
            root,
            state_path,
            _lock: lock,
            cursor_recovery_plan: Some(plan.clone()),
            manifest_store,
            manifest,
            state,
            usable: true,
        },
    }))
}

/// Strict TerminalPendingFinalize opener. It is deliberately the positive
/// guard above, never the progress opener that may repair page/manifest state.
pub(crate) fn open_authorized_cursor_recovery_terminal_pending_finalize(
    data_dir: &SinomoDataDir,
    mount: impl Into<String>,
    root: impl Into<String>,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<CloudFilesNamespaceReconciler, CloudFilesNamespaceReconcileError> {
    acquire_authorized_cursor_recovery_terminal_successor(data_dir, mount, root, plan)?
        .map(CloudFilesCursorRecoveryTerminalSuccessorGuard::into_reconciler)
        .ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-not-terminal",
                "TerminalPendingFinalize recovery has only an earlier progress cut",
            )
        })
}

/// Reopens only the crash cut after the module durably recorded
/// NamespaceFinalized and the exact Terminal rewrite was already removed.
/// This is not an ordinary recovery opener: the missing rewrite is accepted
/// only with a terminal owner state, empty audit/page intent, and no rewrite
/// temporary evidence.
pub(crate) fn open_authorized_cursor_recovery_namespace_finalized(
    data_dir: &SinomoDataDir,
    mount: impl Into<String>,
    root: impl Into<String>,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<(CloudFilesNamespaceReconciler, bool), CloudFilesNamespaceReconcileError> {
    open_authorized_cursor_recovery_progress_inner(data_dir, mount.into(), root.into(), plan, true)
}

fn open_authorized_cursor_recovery_progress_inner(
    data_dir: &SinomoDataDir,
    mount: String,
    root: String,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    namespace_finalized: bool,
) -> Result<(CloudFilesNamespaceReconciler, bool), CloudFilesNamespaceReconcileError> {
    if mount.trim().is_empty() {
        return Err(reconcile_error(
            "invalid-mount",
            "mount identity cannot be empty",
        ));
    }
    let root = canonical_sifs_path(&root)?;
    let validated = validate_cursor_recovery_plan(plan, &mount, &root)?;
    let reconciliation_dir = data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(RECONCILIATION_DIR)
        .join(mount_scope_directory(&mount, &root)?);
    let scope_metadata = fs::symlink_metadata(&reconciliation_dir)
        .map_err(io_error("inspect authorized recovery progress scope"))?;
    if !scope_metadata.file_type().is_dir() || metadata_is_reparse(&scope_metadata) {
        return Err(reconcile_error(
            "cursor-recovery-reconciliation-topology",
            "authorized recovery progress scope is not a plain directory",
        ));
    }
    let lock_path = reconciliation_dir.join(RECONCILIATION_LOCK_FILE);
    read_bounded_plain_file(&lock_path, 0, "authorized recovery progress lock")?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&lock_path)
        .map_err(io_error("open authorized recovery progress lock"))?;
    lock.try_lock_exclusive().map_err(|source| {
        reconcile_error(
            "cursor-recovery-reconciliation-live",
            format!("namespace reconciler is live: {source}"),
        )
    })?;
    let rewrite_path = reconciliation_dir.join(CURSOR_RECOVERY_REWRITE_FILE);
    let rewrite = if namespace_finalized {
        match fs::symlink_metadata(&rewrite_path) {
            Ok(metadata) => {
                if !metadata.file_type().is_file() || metadata_is_reparse(&metadata) {
                    return Err(reconcile_error(
                        "cursor-recovery-rewrite-invalid",
                        "NamespaceFinalized rewrite is not a plain owner file",
                    ));
                }
                Some(load_required_cursor_recovery_rewrite(&rewrite_path, plan)?)
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                ensure_cursor_recovery_rewrite_absent(&reconciliation_dir, &rewrite_path)?;
                None
            }
            Err(source) => {
                return Err(io_error(
                    "inspect NamespaceFinalized cursor recovery rewrite",
                )(source));
            }
        }
    } else {
        Some(load_required_cursor_recovery_rewrite(&rewrite_path, plan)?)
    };
    let namespace_rewrite_absent = namespace_finalized && rewrite.is_none();
    let state_path = reconciliation_dir.join(RECONCILIATION_STATE_FILE);
    let state_bytes = read_bounded_plain_file(
        &state_path,
        RECONCILIATION_STATE_MAX_BYTES,
        "authorized cursor recovery progress state",
    )?;
    let state: CloudFilesNamespaceReconciliationState = decode_sobj(&state_bytes)
        .map_err(|source| reconcile_error("decode-reconciliation-state", source.to_string()))?;
    if encode_sobj(&state)
        .map_err(|source| reconcile_error("encode-reconciliation-state", source.to_string()))?
        != state_bytes
    {
        return Err(reconcile_error(
            "cursor-recovery-progress-invalid",
            "authorized cursor recovery progress state is noncanonical",
        ));
    }
    let audit = load_required_cursor_recovery_audit(&reconciliation_dir, &mount, &root)?;
    let (manifest_store, manifest, migrated) =
        CloudFilesManifestStore::open(&reconciliation_dir, BTreeMap::new())?;
    if migrated {
        return Err(reconcile_error(
            "cursor-recovery-manifest-migration",
            "authorized recovery progress may not migrate a legacy manifest",
        ));
    }
    let state = if namespace_finalized {
        if inspect_cursor_recovery_page_intent(&reconciliation_dir)?.is_some() {
            return Err(reconcile_error(
                "cursor-recovery-page-intent-live",
                "NamespaceFinalized recovery cannot retain a page transaction",
            ));
        }
        state
    } else {
        repair_authorized_cursor_recovery_page_intent(
            &reconciliation_dir,
            &state_path,
            &state_bytes,
            plan,
            &manifest,
        )?
    };
    let require_terminal = if let Some(rewrite) = rewrite {
        let rewrite = resume_terminal_rewrite_stage(
            &reconciliation_dir,
            &rewrite_path,
            plan,
            rewrite,
            cursor_recovery_progress_state_is_valid(&state, &validated, true)?,
            audit.directories.is_empty(),
        )?;
        rewrite.phase == CloudFilesCursorRecoveryRewritePhase::Terminal
    } else {
        true
    };
    if !cursor_recovery_progress_state_is_valid(&state, &validated, require_terminal)? {
        return Err(reconcile_error(
            "cursor-recovery-progress-invalid",
            "recovery state did not evolve solely through the authoritative scan protocol",
        ));
    }
    if require_terminal && !audit.directories.is_empty() {
        return Err(reconcile_error(
            "cursor-recovery-terminal-audit-pending",
            "Terminal recovery progress still has materialization audit intents",
        ));
    }
    validate_recovery_manifest(&manifest, &state, &root)?;
    Ok((
        CloudFilesNamespaceReconciler {
            root,
            state_path,
            _lock: lock,
            cursor_recovery_plan: Some(plan.clone()),
            manifest_store,
            manifest,
            state,
            usable: true,
        },
        namespace_rewrite_absent,
    ))
}

fn ensure_cursor_recovery_rewrite_absent(
    scope: &Path,
    rewrite_path: &Path,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    match fs::symlink_metadata(rewrite_path) {
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(io_error("inspect finalized cursor recovery rewrite")(
                source,
            ));
        }
        Ok(_) => {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-present",
                "NamespaceFinalized recovery still has its Terminal rewrite",
            ));
        }
    }
    for entry in fs::read_dir(scope).map_err(io_error("list finalized recovery scope"))? {
        let entry = entry.map_err(io_error("read finalized recovery scope entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "finalized recovery scope contains a non-UTF-8 entry",
            )
        })?;
        if cursor_recovery_temp_kind(&name) == Some(CursorRecoveryTempKind::Rewrite)
            || name.starts_with(&format!(
                ".{CURSOR_RECOVERY_REWRITE_FILE}.{CURSOR_RECOVERY_REWRITE_TEMP_LABEL}-"
            ))
        {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-temp-present",
                "NamespaceFinalized recovery retains rewrite temporary evidence",
            ));
        }
    }
    Ok(())
}

/// Exact plan-bound evidence that the lock-owning reconciler persisted
/// Terminal. This is deliberately not itself finalization authorization: the
/// module owner must explicitly convert it only after synchronous host Ready
/// observation has completed.
pub(crate) struct CloudFilesCursorRecoveryTerminalProof {
    recovery_id: String,
    recovery_token: String,
    nonce: String,
    root: String,
    terminal_rewrite_fingerprint: String,
}

/// Caller-owned proof that host Ready was synchronously observed after the
/// inner Terminal marker. Its fields and constructor stay private so ordinary
/// namespace code cannot fabricate a broad marker-delete request.
pub(crate) struct CloudFilesCursorRecoveryReadyToFinalize {
    terminal: CloudFilesCursorRecoveryTerminalProof,
}

impl CloudFilesCursorRecoveryTerminalProof {
    /// Call only after the outer/module Ready transition and its synchronous
    /// host acknowledgement are durable. The explicit type transition keeps
    /// that ordering visible at the worker/module API boundary.
    pub(crate) fn authorize_after_host_ready_observed(
        self,
    ) -> CloudFilesCursorRecoveryReadyToFinalize {
        CloudFilesCursorRecoveryReadyToFinalize { terminal: self }
    }
}

fn cursor_recovery_terminal_proof(
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<CloudFilesCursorRecoveryTerminalProof, CloudFilesNamespaceReconcileError> {
    let terminal_bytes = encode_sobj(&CloudFilesCursorRecoveryRewrite::from_plan(
        plan,
        CloudFilesCursorRecoveryRewritePhase::Terminal,
    ))
    .map_err(|source| reconcile_error("encode-cursor-recovery-rewrite", source.to_string()))?;
    Ok(CloudFilesCursorRecoveryTerminalProof {
        recovery_id: plan.recovery_id.clone(),
        recovery_token: plan.recovery_token.clone(),
        nonce: plan.nonce.clone(),
        root: plan.root.clone(),
        terminal_rewrite_fingerprint: cursor_recovery_bytes_fingerprint(&terminal_bytes),
    })
}

fn validate_cursor_recovery_ready_to_finalize(
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    root: &str,
    ready: &CloudFilesCursorRecoveryReadyToFinalize,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    let expected = cursor_recovery_terminal_proof(plan)?;
    if ready.terminal.recovery_id != expected.recovery_id
        || ready.terminal.recovery_token != expected.recovery_token
        || ready.terminal.nonce != expected.nonce
        || ready.terminal.root != root
        || ready.terminal.terminal_rewrite_fingerprint != expected.terminal_rewrite_fingerprint
    {
        return Err(reconcile_error(
            "cursor-recovery-ready-proof-mismatch",
            "cursor recovery finalization proof does not match the exact Terminal plan",
        ));
    }
    Ok(())
}

/// Removes only the exact plan-bound Terminal marker after the outer/module
/// owner has durably committed Ready. An armed marker, changed plan, pending
/// audit, or nonterminal state is never deleted.
pub(crate) fn finalize_authorized_cursor_recovery_after_ready(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    ready: CloudFilesCursorRecoveryReadyToFinalize,
) -> Result<bool, CloudFilesNamespaceReconcileError> {
    let mut reconciler = open_authorized_cursor_recovery_progress(data_dir, mount, root, plan)?;
    reconciler.finalize_authorized_cursor_recovery_after_ready(plan, ready)
}
fn cursor_recovery_progress_state_is_valid(
    state: &CloudFilesNamespaceReconciliationState,
    plan: &ValidatedCursorRecoveryPlan,
    require_terminal: bool,
) -> Result<bool, CloudFilesNamespaceReconcileError> {
    if state.schema_version != RECONCILIATION_SCHEMA_VERSION
        || state.mount != plan.expected.mount
        || state.root != plan.expected.root
        || !state.legacy_manifest.is_empty()
        || !state.suppressions.is_empty()
        || !state.conflicts.is_empty()
        || !state.startup_conflicts.is_empty()
        || !state.materialized_directories.contains(&state.root)
        || state.inflight_effect.is_some()
        || !state.applied_events.is_empty()
    {
        return Ok(false);
    }
    for directory in &state.materialized_directories {
        if canonical_sifs_path(directory)? != *directory {
            return Ok(false);
        }
        ensure_within_root(directory, &state.root)?;
    }
    for (path, kind) in &state.unsupported_projection_examples {
        if canonical_sifs_path(path)? != *path
            || !matches!(kind.as_str(), "symlink" | "hardlink" | "artifact")
        {
            return Ok(false);
        }
        ensure_within_root(path, &state.root)?;
    }
    if state.unsupported_projection_examples.len() as u64 > state.unsupported_projection_count {
        return Ok(false);
    }
    if require_terminal {
        if state.cursors.is_empty() || state.full_scan_request.is_some() {
            return Ok(false);
        }
    } else if state.cursors.is_empty() {
        if state.full_scan_request != plan.expected.full_scan_request {
            return Ok(false);
        }
    } else if state.full_scan_request.is_some() {
        return Ok(false);
    }
    for (key, cursor) in &state.cursors {
        if key != &cursor.shard_key()? || cursor.authority_epoch == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_recovery_manifest(
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    state: &CloudFilesNamespaceReconciliationState,
    root: &str,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    for (path, entry) in manifest {
        if path != &entry.path || canonical_sifs_path(path)? != *path {
            return Err(reconcile_error(
                "cursor-recovery-progress-manifest",
                "recovery manifest contains a noncanonical or mismatched path",
            ));
        }
        ensure_within_root(path, root)?;
        validate_manifest_entry(entry)?;
    }
    let (unsupported_count, unsupported_examples) = unsupported_projection_summary(manifest);
    if state.unsupported_projection_count != unsupported_count
        || state.unsupported_projection_examples != unsupported_examples
    {
        return Err(reconcile_error(
            "cursor-recovery-progress-manifest",
            "recovery manifest does not match its durable projection summary",
        ));
    }
    for directory in &state.materialized_directories {
        if directory == root {
            continue;
        }
        if !manifest
            .get(directory)
            .is_some_and(|entry| matches!(entry.kind.as_str(), "directory" | "mount"))
        {
            return Err(reconcile_error(
                "cursor-recovery-progress-manifest",
                format!(
                    "materialized directory `{directory}` lacks matching authoritative manifest evidence"
                ),
            ));
        }
    }
    Ok(())
}

fn load_required_cursor_recovery_audit(
    scope: &Path,
    mount: &str,
    root: &str,
) -> Result<CloudFilesMaterializationAuditIntentState, CloudFilesNamespaceReconcileError> {
    let path = scope.join(MATERIALIZATION_AUDIT_INTENTS_FILE);
    let bytes = read_bounded_plain_file(
        &path,
        MATERIALIZATION_AUDIT_INTENTS_MAX_BYTES,
        "cursor recovery materialization audit state",
    )?;
    let audit: CloudFilesMaterializationAuditIntentState =
        decode_sobj(&bytes).map_err(|source| {
            reconcile_error("decode-materialization-audit-intents", source.to_string())
        })?;
    if audit.schema_version != MATERIALIZATION_AUDIT_INTENTS_SCHEMA_VERSION
        || audit.mount != mount
        || audit.root != root
        || encode_sobj(&audit).map_err(|source| {
            reconcile_error("encode-materialization-audit-intents", source.to_string())
        })? != bytes
    {
        return Err(reconcile_error(
            "cursor-recovery-progress-audit",
            "cursor recovery audit state is noncanonical or has changed identity",
        ));
    }
    for directory in &audit.directories {
        if canonical_sifs_path(directory)? != *directory {
            return Err(reconcile_error(
                "cursor-recovery-progress-audit",
                "cursor recovery audit contains a noncanonical directory",
            ));
        }
        ensure_within_root(directory, root)?;
    }
    Ok(audit)
}

fn load_required_cursor_recovery_rewrite(
    path: &Path,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
) -> Result<CloudFilesCursorRecoveryRewrite, CloudFilesNamespaceReconcileError> {
    let bytes = read_bounded_plain_file(
        path,
        CURSOR_RECOVERY_REWRITE_MAX_BYTES,
        "cursor recovery rewrite marker",
    )?;
    let rewrite: CloudFilesCursorRecoveryRewrite = decode_sobj(&bytes)
        .map_err(|source| reconcile_error("decode-cursor-recovery-rewrite", source.to_string()))?;
    if encode_sobj(&rewrite)
        .map_err(|source| reconcile_error("encode-cursor-recovery-rewrite", source.to_string()))?
        != bytes
        || !rewrite.matches_plan(plan)
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-mismatch",
            "cursor recovery rewrite marker is noncanonical or belongs to another plan",
        ));
    }
    Ok(rewrite)
}

fn resume_terminal_rewrite_stage(
    scope: &Path,
    rewrite_path: &Path,
    plan: &CloudFilesCursorRecoveryReconciliationPlan,
    rewrite: CloudFilesCursorRecoveryRewrite,
    terminal_state: bool,
    audit_empty: bool,
) -> Result<CloudFilesCursorRecoveryRewrite, CloudFilesNamespaceReconcileError> {
    let mut stage = None;
    for entry in fs::read_dir(scope).map_err(io_error("list cursor recovery scope"))? {
        let entry = entry.map_err(io_error("read cursor recovery scope entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "cursor recovery scope contains a non-UTF-8 entry",
            )
        })?;
        if !looks_like_cursor_recovery_temp(&name) {
            continue;
        }
        let kind = cursor_recovery_temp_kind(&name).ok_or_else(|| {
            reconcile_error(
                "cursor-recovery-rewrite-torn",
                format!("cursor recovery has a noncanonical durable stage `{name}`"),
            )
        })?;
        if stage.is_some() {
            return Err(reconcile_error(
                "cursor-recovery-rewrite-torn",
                "cursor recovery has multiple unresolved durable stages",
            ));
        }
        stage = Some((kind, entry.path()));
    }
    let Some((kind, path)) = stage else {
        return Ok(rewrite);
    };
    if kind != CursorRecoveryTempKind::Rewrite || !terminal_state || !audit_empty {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-torn",
            "only an exact terminal-marker stage may follow authoritative scan progress",
        ));
    }
    let terminal = CloudFilesCursorRecoveryRewrite::from_plan(
        plan,
        CloudFilesCursorRecoveryRewritePhase::Terminal,
    );
    let expected_bytes = encode_sobj(&terminal)
        .map_err(|source| reconcile_error("encode-cursor-recovery-rewrite", source.to_string()))?;
    let staged_bytes = read_bounded_plain_file(
        &path,
        CURSOR_RECOVERY_REWRITE_MAX_BYTES,
        "cursor recovery terminal marker stage",
    )?;
    if staged_bytes != expected_bytes
        || !matches!(
            rewrite.phase,
            CloudFilesCursorRecoveryRewritePhase::ReconcileArmed
                | CloudFilesCursorRecoveryRewritePhase::Terminal
        )
    {
        return Err(reconcile_error(
            "cursor-recovery-rewrite-mismatch",
            "terminal marker stage does not match its exact plan-bound predecessor",
        ));
    }
    durable_file::remove_file_if_exists_and_sync_parent(&path).map_err(storage_error(
        "remove replayed cursor recovery terminal stage",
    ))?;
    persist_cursor_recovery_rewrite(rewrite_path, &terminal)?;
    Ok(terminal)
}

fn inspect_empty_cursor_recovery_manifest(
    manifest_dir: &Path,
    raw_files: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    let metadata = fs::symlink_metadata(manifest_dir)
        .map_err(io_error("inspect cursor-only manifest directory"))?;
    if !metadata.file_type().is_dir() || metadata_is_reparse(&metadata) {
        return Err(reconcile_error(
            "cursor-recovery-manifest-topology",
            "manifest path is not a plain directory",
        ));
    }
    let mut saw_index = false;
    let mut saw_journal = false;
    for entry in fs::read_dir(manifest_dir).map_err(io_error("list cursor-only manifest"))? {
        let entry = entry.map_err(io_error("read cursor-only manifest entry"))?;
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-manifest-topology",
                "manifest contains a non-UTF-8 entry",
            )
        })?;
        let metadata =
            fs::symlink_metadata(&path).map_err(io_error("inspect cursor-only manifest entry"))?;
        if !metadata.file_type().is_file() || metadata_is_reparse(&metadata) {
            return Err(reconcile_error(
                "cursor-recovery-manifest-topology",
                format!("manifest entry `{name}` is not a plain file"),
            ));
        }
        match name.as_str() {
            MANIFEST_INDEX_FILE => {
                if metadata.len() > MANIFEST_INDEX_MAX_BYTES {
                    return Err(reconcile_error(
                        "cursor-recovery-manifest-too-large",
                        "manifest index exceeds its recovery bound",
                    ));
                }
                let bytes = fs::read(&path).map_err(io_error("read cursor-only manifest index"))?;
                let index: CloudFilesManifestIndex = decode_sobj(&bytes).map_err(|source| {
                    reconcile_error("decode-manifest-index", source.to_string())
                })?;
                if index.schema_version != MANIFEST_SCHEMA_VERSION
                    || index.generation != 1
                    || index.applied_through_sequence != 0
                    || !index.chunks.is_empty()
                    || encode_sobj(&index).map_err(|source| {
                        reconcile_error("encode-manifest-index", source.to_string())
                    })? != bytes
                {
                    return Err(reconcile_error(
                        "cursor-recovery-manifest-not-empty",
                        "manifest index is nonempty or noncanonical",
                    ));
                }
                raw_files.insert(format!("{MANIFEST_DIR}/{name}"), bytes);
                saw_index = true;
            }
            MANIFEST_JOURNAL_FILE => {
                if metadata.len() > MANIFEST_JOURNAL_MAX_BYTES {
                    return Err(reconcile_error(
                        "cursor-recovery-manifest-too-large",
                        "manifest journal exceeds its owner bound",
                    ));
                }
                let bytes =
                    fs::read(&path).map_err(io_error("read cursor-only manifest journal"))?;
                if !bytes.is_empty() {
                    return Err(reconcile_error(
                        "cursor-recovery-manifest-not-empty",
                        "manifest journal contains retained history",
                    ));
                }
                raw_files.insert(format!("{MANIFEST_DIR}/{name}"), bytes);
                saw_journal = true;
            }
            _ => {
                return Err(reconcile_error(
                    "cursor-recovery-manifest-not-empty",
                    format!("manifest contains history, temporary, or unknown entry `{name}`"),
                ));
            }
        }
    }
    if !saw_index || !saw_journal {
        return Err(reconcile_error(
            "cursor-recovery-manifest-topology",
            "manifest is missing its canonical empty index or journal",
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn suppression_action_matches_manifest(
    action: &CloudFilesNamespaceAction,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> bool {
    match action {
        CloudFilesNamespaceAction::Upsert {
            path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
        } => manifest.get(path).is_some_and(|entry| {
            entry.object_id == *object_id
                && entry.version == *version
                && entry.content_hash == *content_hash
                && entry.content_size_bytes == *content_size_bytes
                && entry.kind == *kind
        }),
        CloudFilesNamespaceAction::Remove { path, .. } => !manifest.contains_key(path),
        CloudFilesNamespaceAction::Rename {
            from_path,
            to_path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
        } => {
            !manifest.contains_key(from_path)
                && manifest.get(to_path).is_some_and(|entry| {
                    entry.object_id == *object_id
                        && entry.version == *version
                        && entry.content_hash == *content_hash
                        && entry.content_size_bytes == *content_size_bytes
                        && entry.kind == *kind
                })
        }
    }
}

fn finish_inflight_state(
    state: &mut CloudFilesNamespaceReconciliationState,
) -> Result<CloudFilesNamespaceAction, CloudFilesNamespaceReconcileError> {
    let inflight = state.inflight_effect.clone().ok_or_else(|| {
        reconcile_error("missing-inflight-effect", "no namespace effect is pending")
    })?;
    let action = inflight.action.clone();
    if let Some(conflict_event_id) = inflight.resolving_conflict_event_id {
        state.conflicts.remove(&conflict_event_id);
    } else {
        state.applied_events.insert(
            inflight.event_id,
            CloudFilesAppliedNamespaceEvent {
                shard_key: inflight.shard_key,
                journal_sequence: inflight.journal_sequence,
            },
        );
    }
    apply_materialized_action(state, &action);
    state.inflight_effect = None;
    Ok(action)
}

fn apply_materialized_action(
    state: &mut CloudFilesNamespaceReconciliationState,
    action: &CloudFilesNamespaceAction,
) {
    match action {
        CloudFilesNamespaceAction::Upsert { path, kind, .. } => {
            if !matches!(kind.as_str(), "directory" | "mount") {
                remove_materialized_subtree(&mut state.materialized_directories, path);
            }
        }
        CloudFilesNamespaceAction::Remove { path, .. } => {
            remove_materialized_subtree(&mut state.materialized_directories, path);
        }
        CloudFilesNamespaceAction::Rename {
            from_path,
            to_path,
            kind,
            ..
        } => {
            if matches!(kind.as_str(), "directory" | "mount") {
                rekey_materialized_subtree(&mut state.materialized_directories, from_path, to_path);
            } else {
                remove_materialized_subtree(&mut state.materialized_directories, from_path);
                remove_materialized_subtree(&mut state.materialized_directories, to_path);
            }
        }
    }
}

fn apply_materialized_manifest_mutations(
    state: &mut CloudFilesNamespaceReconciliationState,
    mutations: &[CloudFilesManifestMutation],
) {
    for mutation in mutations {
        match mutation {
            CloudFilesManifestMutation::Remove { path } => {
                remove_materialized_subtree(&mut state.materialized_directories, path);
            }
            CloudFilesManifestMutation::Upsert { entry }
                if !matches!(entry.kind.as_str(), "directory" | "mount") =>
            {
                remove_materialized_subtree(&mut state.materialized_directories, &entry.path);
            }
            CloudFilesManifestMutation::Upsert { .. } => {}
        }
    }
}

fn remove_materialized_subtree(directories: &mut BTreeSet<String>, prefix: &str) {
    directories.retain(|directory| !sifs_path_is_at_or_below(directory, prefix));
}

fn rekey_materialized_subtree(directories: &mut BTreeSet<String>, from_path: &str, to_path: &str) {
    let moved = directories
        .iter()
        .filter(|directory| sifs_path_is_at_or_below(directory, from_path))
        .cloned()
        .collect::<Vec<_>>();
    for directory in moved {
        directories.remove(&directory);
        let suffix = directory.strip_prefix(from_path).unwrap_or_default();
        directories.insert(format!("{to_path}{suffix}"));
    }
}

fn sifs_path_is_at_or_below(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn compact_reconciliation_state(state: &mut CloudFilesNamespaceReconciliationState) {
    let cursors = &state.cursors;
    state.applied_events.retain(|_, event| {
        cursors
            .get(&event.shard_key)
            .is_none_or(|cursor| event.journal_sequence > cursor.after_sequence)
    });
}

fn ensure_state_fits(
    state: &CloudFilesNamespaceReconciliationState,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    let mut compacted = state.clone();
    compact_reconciliation_state(&mut compacted);
    encode_reconciliation_state(&compacted).map(|_| ())
}

fn encode_reconciliation_state(
    state: &CloudFilesNamespaceReconciliationState,
) -> Result<Vec<u8>, CloudFilesNamespaceReconcileError> {
    let bytes = encode_sobj(state)
        .map_err(|source| reconcile_error("encode-reconciliation-state", source.to_string()))?;
    let state_bytes = u64::try_from(bytes.len()).map_err(|_| {
        reconcile_error(
            "reconciliation-state-too-large",
            "encoded namespace reconciliation state length overflowed",
        )
    })?;
    if state_bytes > RECONCILIATION_STATE_MAX_BYTES {
        return Err(reconcile_error(
            "reconciliation-state-too-large",
            format!(
                "encoded namespace reconciliation state is {state_bytes} bytes; max is {RECONCILIATION_STATE_MAX_BYTES}"
            ),
        ));
    }
    Ok(bytes)
}

fn remove_abandoned_state_stages(
    reconciliation_dir: &Path,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    remove_abandoned_atomic_stages(
        reconciliation_dir,
        RECONCILIATION_STATE_FILE,
        RECONCILIATION_TEMP_LABEL,
    )
}

fn cursor_recovery_evidence_exists(
    reconciliation_dir: &Path,
) -> Result<bool, CloudFilesNamespaceReconcileError> {
    for entry in
        fs::read_dir(reconciliation_dir).map_err(io_error("list namespace reconciliation scope"))?
    {
        let entry = entry.map_err(io_error("read namespace reconciliation scope entry"))?;
        let name = entry.file_name().into_string().map_err(|_| {
            reconcile_error(
                "cursor-recovery-reconciliation-topology",
                "reconciliation scope contains a non-UTF-8 entry",
            )
        })?;
        if name == CURSOR_RECOVERY_REWRITE_FILE
            || name == CURSOR_RECOVERY_PAGE_INTENT_FILE
            || looks_like_cursor_recovery_temp(&name)
            || looks_like_cursor_recovery_page_intent_temp(&name)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn remove_abandoned_atomic_stages(
    reconciliation_dir: &Path,
    file_name: &str,
    temp_label: &str,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    let prefix = format!(".{file_name}.{temp_label}-");
    for entry in
        fs::read_dir(reconciliation_dir).map_err(io_error("list namespace reconciliation scope"))?
    {
        let entry = entry.map_err(io_error("read namespace reconciliation scope entry"))?;
        let file_name = entry.file_name();
        if !file_name.to_string_lossy().starts_with(&prefix) {
            continue;
        }
        durable_file::remove_file_if_exists_and_sync_parent(&entry.path())
            .map_err(storage_error("remove abandoned namespace state stage"))?;
    }
    Ok(())
}

fn ensure_reconciliation_scope(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<PathBuf, CloudFilesNamespaceReconcileError> {
    let runtime_dir = data_dir.module_runtime_dir();
    fs::create_dir_all(&runtime_dir).map_err(io_error("create module runtime directory"))?;
    let module_dir = runtime_dir.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
    durable_file::ensure_private_directory(&module_dir)
        .map_err(storage_error("create Cloud Files module runtime directory"))?;
    let reconciliation_root = module_dir.join(RECONCILIATION_DIR);
    durable_file::ensure_private_directory(&reconciliation_root)
        .map_err(storage_error("create namespace reconciliation root"))?;
    let reconciliation_dir = reconciliation_root.join(mount_scope_directory(mount, root)?);
    durable_file::ensure_private_directory(&reconciliation_dir)
        .map_err(storage_error("create namespace reconciliation scope"))?;
    Ok(reconciliation_dir)
}

fn encode_materialization_audit_intents(
    state: &CloudFilesMaterializationAuditIntentState,
) -> Result<Vec<u8>, CloudFilesNamespaceReconcileError> {
    let bytes = encode_sobj(state).map_err(|source| {
        reconcile_error("encode-materialization-audit-intents", source.to_string())
    })?;
    let byte_len = u64::try_from(bytes.len()).map_err(|_| {
        reconcile_error(
            "materialization-audit-intents-too-large",
            "encoded materialization audit intent length overflowed",
        )
    })?;
    if byte_len > MATERIALIZATION_AUDIT_INTENTS_MAX_BYTES {
        return Err(reconcile_error(
            "materialization-audit-intents-too-large",
            format!(
                "encoded materialization audit intents are {byte_len} bytes; max is {MATERIALIZATION_AUDIT_INTENTS_MAX_BYTES}"
            ),
        ));
    }
    Ok(bytes)
}

fn load_materialization_audit_intents(
    path: &Path,
) -> Result<Option<CloudFilesMaterializationAuditIntentState>, CloudFilesNamespaceReconcileError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("inspect materialization audit intents")(source)),
    };
    if metadata.len() > MATERIALIZATION_AUDIT_INTENTS_MAX_BYTES {
        return Err(reconcile_error(
            "materialization-audit-intents-too-large",
            format!(
                "materialization audit intent state is {} bytes; max is {MATERIALIZATION_AUDIT_INTENTS_MAX_BYTES}",
                metadata.len()
            ),
        ));
    }
    let bytes = fs::read(path).map_err(io_error("read materialization audit intents"))?;
    decode_sobj(&bytes).map(Some).map_err(|source| {
        reconcile_error("decode-materialization-audit-intents", source.to_string())
    })
}

fn load_reconciliation_state(
    path: &Path,
) -> Result<Option<CloudFilesNamespaceReconciliationState>, CloudFilesNamespaceReconcileError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io_error("inspect namespace reconciliation state")(source)),
    };
    if metadata.len() > RECONCILIATION_STATE_MAX_BYTES {
        return Err(reconcile_error(
            "reconciliation-state-too-large",
            format!(
                "namespace reconciliation state is {} bytes; max is {RECONCILIATION_STATE_MAX_BYTES}",
                metadata.len()
            ),
        ));
    }
    let bytes = fs::read(path).map_err(io_error("read namespace reconciliation state"))?;
    decode_sobj(&bytes)
        .map(Some)
        .map_err(|source| reconcile_error("decode-reconciliation-state", source.to_string()))
}

fn validate_batch(
    batch: &CloudFilesNamespaceFeedBatch,
    state: &CloudFilesNamespaceReconciliationState,
) -> Result<String, CloudFilesNamespaceReconcileError> {
    let shard_key = batch.input_cursor.shard_key()?;
    if batch.next_cursor.shard_key()? != shard_key
        || batch.next_cursor.authority_epoch != batch.input_cursor.authority_epoch
        || batch.next_cursor.after_sequence < batch.input_cursor.after_sequence
    {
        return Err(reconcile_error(
            "invalid-feed-batch",
            "feed batch changed shard/epoch or rewound its cursor",
        ));
    }
    if let Some(current) = state.cursors.get(&shard_key)
        && current != &batch.input_cursor
    {
        return Err(reconcile_error(
            "cursor-mismatch",
            "feed input cursor does not match the durable last-reconciled cursor",
        ));
    }
    let rescan = matches!(
        batch.events.as_slice(),
        [CloudFilesNamespaceFeedEvent::RescanRequired { .. }]
    );
    if batch
        .events
        .iter()
        .any(|event| matches!(event, CloudFilesNamespaceFeedEvent::RescanRequired { .. }))
        && !rescan
    {
        return Err(reconcile_error(
            "invalid-feed-batch",
            "RescanRequired must be the only event in its batch",
        ));
    }
    if rescan {
        if let [CloudFilesNamespaceFeedEvent::RescanRequired { reason, .. }] =
            batch.events.as_slice()
            && reason.trim().is_empty()
        {
            return Err(reconcile_error(
                "invalid-feed-batch",
                "RescanRequired reason cannot be empty",
            ));
        }
        return Ok(shard_key);
    }
    let mut order = None;
    let mut event_ids = BTreeSet::new();
    for event in &batch.events {
        let provenance = event_provenance(event).ok_or_else(|| {
            reconcile_error("invalid-feed-batch", "ordinary event lacks provenance")
        })?;
        if provenance.event_id.trim().is_empty()
            || !event_ids.insert(provenance.event_id.clone())
            || provenance
                .operation_id
                .as_ref()
                .is_some_and(|operation_id| operation_id.trim().is_empty())
            || provenance.journal_sequence <= batch.input_cursor.after_sequence
            || provenance.journal_sequence > batch.next_cursor.after_sequence
        {
            return Err(reconcile_error(
                "invalid-feed-batch",
                "feed event ID/sequence is empty, duplicated, or outside its cursor range",
            ));
        }
        let next_order = (provenance.journal_sequence, provenance.journal_ordinal);
        if order.is_some_and(|previous| previous >= next_order) {
            return Err(reconcile_error(
                "invalid-feed-batch",
                "feed events are not strictly ordered by journal sequence/ordinal",
            ));
        }
        order = Some(next_order);
        event_action(event, &state.root)?;
    }
    if batch.journal_head_sequence < batch.next_cursor.after_sequence {
        return Err(reconcile_error(
            "invalid-feed-batch",
            "feed journal head precedes its next cursor",
        ));
    }
    Ok(shard_key)
}

fn event_action<'event>(
    event: &'event CloudFilesNamespaceFeedEvent,
    root: &str,
) -> Result<
    (
        &'event CloudFilesNamespaceProvenance,
        CloudFilesNamespaceAction,
    ),
    CloudFilesNamespaceReconcileError,
> {
    let (provenance, action) = match event {
        CloudFilesNamespaceFeedEvent::Upsert {
            path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
            provenance,
        } => (
            provenance,
            CloudFilesNamespaceAction::Upsert {
                path: canonical_sifs_path(path)?,
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                content_size_bytes: *content_size_bytes,
                kind: kind.clone(),
            },
        ),
        CloudFilesNamespaceFeedEvent::Remove {
            path,
            object_id,
            version,
            content_hash,
            provenance,
        } => (
            provenance,
            CloudFilesNamespaceAction::Remove {
                path: canonical_sifs_path(path)?,
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
            },
        ),
        CloudFilesNamespaceFeedEvent::Rename {
            from_path,
            to_path,
            object_id,
            version,
            content_hash,
            content_size_bytes,
            kind,
            provenance,
        } => (
            provenance,
            CloudFilesNamespaceAction::Rename {
                from_path: canonical_sifs_path(from_path)?,
                to_path: canonical_sifs_path(to_path)?,
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
                content_size_bytes: *content_size_bytes,
                kind: kind.clone(),
            },
        ),
        CloudFilesNamespaceFeedEvent::RescanRequired { .. } => {
            return Err(reconcile_error(
                "invalid-feed-batch",
                "RescanRequired cannot be converted to a local namespace action",
            ));
        }
    };
    validate_action(&action, root)?;
    Ok((provenance, action))
}

fn event_provenance(
    event: &CloudFilesNamespaceFeedEvent,
) -> Option<&CloudFilesNamespaceProvenance> {
    match event {
        CloudFilesNamespaceFeedEvent::Upsert { provenance, .. }
        | CloudFilesNamespaceFeedEvent::Remove { provenance, .. }
        | CloudFilesNamespaceFeedEvent::Rename { provenance, .. } => Some(provenance),
        CloudFilesNamespaceFeedEvent::RescanRequired { .. } => None,
    }
}

fn validate_action(
    action: &CloudFilesNamespaceAction,
    root: &str,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    for path in action.paths() {
        ensure_within_root(path, root)?;
    }
    let (object_id, version, content_hash, kind) = match action {
        CloudFilesNamespaceAction::Upsert {
            object_id,
            version,
            content_hash,
            kind,
            ..
        }
        | CloudFilesNamespaceAction::Rename {
            object_id,
            version,
            content_hash,
            kind,
            ..
        } => (object_id, version, content_hash, Some(kind)),
        CloudFilesNamespaceAction::Remove {
            object_id,
            version,
            content_hash,
            ..
        } => (object_id, version, content_hash, None),
    };
    if object_id.trim().is_empty()
        || version.trim().is_empty()
        || content_hash
            .as_ref()
            .is_some_and(|hash| hash.trim().is_empty())
        || kind.is_some_and(|kind| kind.trim().is_empty())
    {
        return Err(reconcile_error(
            "invalid-action",
            "namespace actions require nonempty object/version/kind/hash identities",
        ));
    }
    if let CloudFilesNamespaceAction::Rename {
        from_path, to_path, ..
    } = action
        && from_path == to_path
    {
        return Err(reconcile_error(
            "invalid-action",
            "rename source and destination paths must differ",
        ));
    }
    Ok(())
}

fn validate_manifest_entry(
    entry: &CloudFilesReconciledManifestEntry,
) -> Result<(), CloudFilesNamespaceReconcileError> {
    if entry.object_id.trim().is_empty()
        || entry.version.trim().is_empty()
        || entry.kind.trim().is_empty()
        || entry
            .content_hash
            .as_ref()
            .is_some_and(|hash| hash.trim().is_empty())
    {
        return Err(reconcile_error(
            "invalid-full-scan",
            "manifest entries require nonempty object/version/kind/hash identities",
        ));
    }
    Ok(())
}

fn mount_scope_directory(
    mount: &str,
    root: &str,
) -> Result<String, CloudFilesNamespaceReconcileError> {
    let identity = encode_sobj(&(mount, root))
        .map_err(|source| reconcile_error("encode-mount-scope", source.to_string()))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(MOUNT_SCOPE_DOMAIN);
    hasher.update(&identity);
    Ok(format!("scope-{}", hasher.finalize().to_hex()))
}

fn canonical_sifs_path(path: &str) -> Result<String, CloudFilesNamespaceReconcileError> {
    let path = path.trim();
    if path.is_empty() || !path.starts_with('/') || path.contains('\0') {
        return Err(reconcile_error(
            "invalid-path",
            format!("SIFS path `{path}` must be absolute, nonempty, and NUL-free"),
        ));
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if components.pop().is_none() {
                    return Err(reconcile_error(
                        "invalid-path",
                        format!("SIFS path `{path}` escapes its namespace root"),
                    ));
                }
            }
            component => components.push(component),
        }
    }
    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

fn sifs_parent_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

fn ensure_within_root(path: &str, root: &str) -> Result<(), CloudFilesNamespaceReconcileError> {
    if root == "/" || path == root || path.starts_with(&format!("{root}/")) {
        Ok(())
    } else {
        Err(reconcile_error(
            "path-outside-root",
            format!("SIFS path `{path}` is outside reconciler root `{root}`"),
        ))
    }
}

fn reconcile_error(
    code: &'static str,
    detail: impl Into<String>,
) -> CloudFilesNamespaceReconcileError {
    CloudFilesNamespaceReconcileError {
        code,
        detail: detail.into(),
    }
}

fn io_error(
    operation: &'static str,
) -> impl FnOnce(std::io::Error) -> CloudFilesNamespaceReconcileError {
    move |source| reconcile_error("reconciliation-io", format!("{operation}: {source}"))
}

fn storage_error(
    operation: &'static str,
) -> impl FnOnce(durable_file::DurableFileError) -> CloudFilesNamespaceReconcileError {
    move |source| reconcile_error("reconciliation-storage", format!("{operation}: {source}"))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(_name: &str) -> Self {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            // Reconciliation fixtures add hashed scope names and atomic temp
            // suffixes below this root. Keep the root short enough for hosts
            // that still enforce the legacy Windows MAX_PATH limit.
            let path = std::env::temp_dir().join(format!("cfr-{}-{suffix}", std::process::id()));
            fs::create_dir_all(&path).expect("test root");
            Self(path)
        }

        fn data_dir(&self) -> SinomoDataDir {
            SinomoDataDir::new(&self.0)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn fresh_scope_probe_is_read_only_and_rejects_even_an_empty_scope() {
        let root = TestRoot::new("fresh-scope-probe");
        let data_dir = root.data_dir();
        let reconciliation_root = data_dir
            .module_runtime_dir()
            .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
            .join(RECONCILIATION_DIR);

        assert!(
            fresh_reconciliation_scope_is_absent(&data_dir, "mount-a", "/services")
                .expect("probe absent scope")
        );
        assert!(
            !reconciliation_root.exists(),
            "read-only Fresh admission must not create reconciliation directories"
        );

        let scope = reconciliation_root.join(
            mount_scope_directory("mount-a", "/services").expect("exact reconciliation scope name"),
        );
        fs::create_dir_all(&scope).expect("empty retained reconciliation scope");
        assert!(
            !fresh_reconciliation_scope_is_absent(&data_dir, "mount-a", "/services")
                .expect("probe retained scope")
        );
        assert!(scope.exists(), "probe must retain existing evidence");
    }

    #[derive(Default)]
    struct FakeEffects {
        states: BTreeMap<String, CloudFilesLocalPathState>,
        applied: Vec<CloudFilesNamespaceAction>,
        conflicts: Vec<CloudFilesNamespaceConflict>,
        rescans: Vec<CloudFilesFullScanRequest>,
        apply_results: VecDeque<Result<(), String>>,
        conflict_results: VecDeque<Result<(), String>>,
    }

    impl CloudFilesNamespaceEffectAdapter for FakeEffects {
        fn local_path_state(&mut self, path: &str) -> Result<CloudFilesLocalPathState, String> {
            Ok(self
                .states
                .get(path)
                .cloned()
                .unwrap_or(CloudFilesLocalPathState::Clean))
        }

        fn apply(&mut self, action: &CloudFilesNamespaceAction) -> Result<(), String> {
            self.applied.push(action.clone());
            self.apply_results.pop_front().unwrap_or(Ok(()))
        }

        fn record_conflict(
            &mut self,
            conflict: &CloudFilesNamespaceConflict,
        ) -> Result<(), String> {
            self.conflicts.push(conflict.clone());
            self.conflict_results.pop_front().unwrap_or(Ok(()))
        }

        fn request_full_scan(&mut self, request: &CloudFilesFullScanRequest) -> Result<(), String> {
            self.rescans.push(request.clone());
            Ok(())
        }
    }

    fn cursor(after_sequence: u64) -> CloudFilesNamespaceCursor {
        CloudFilesNamespaceCursor {
            file_system_id: "sifs".to_string(),
            shard_id: "services".to_string(),
            authority_epoch: 7,
            after_sequence,
        }
    }

    fn provenance(
        event_id: &str,
        sequence: u64,
        operation_id: Option<&str>,
    ) -> CloudFilesNamespaceProvenance {
        CloudFilesNamespaceProvenance {
            event_id: event_id.to_string(),
            journal_sequence: sequence,
            journal_ordinal: 0,
            operation_id: operation_id.map(str::to_string),
        }
    }

    fn upsert(
        event_id: &str,
        sequence: u64,
        operation_id: Option<&str>,
    ) -> CloudFilesNamespaceFeedEvent {
        CloudFilesNamespaceFeedEvent::Upsert {
            path: "/services/report.txt".to_string(),
            object_id: "01object".to_string(),
            version: format!("01version{sequence}"),
            content_hash: Some(format!("blake3:{sequence}")),
            content_size_bytes: Some(7),
            kind: "file".to_string(),
            provenance: provenance(event_id, sequence, operation_id),
        }
    }

    fn batch(
        input: u64,
        next: u64,
        events: Vec<CloudFilesNamespaceFeedEvent>,
    ) -> CloudFilesNamespaceFeedBatch {
        CloudFilesNamespaceFeedBatch {
            input_cursor: cursor(input),
            next_cursor: cursor(next),
            events,
            journal_head_sequence: next,
            has_more: false,
        }
    }

    fn open_reconciler(root: &TestRoot) -> CloudFilesNamespaceReconciler {
        CloudFilesNamespaceReconciler::open(&root.data_dir(), "mount-a", "/services")
            .expect("open reconciler")
    }

    struct CursorRecoveryFixture {
        root: TestRoot,
        scope: PathBuf,
        plan: CloudFilesCursorRecoveryReconciliationPlan,
    }

    fn cursor_recovery_fixture(name: &str) -> CursorRecoveryFixture {
        let root = TestRoot::new(name);
        let mut reconciler = open_reconciler(&root);
        let scope = reconciler
            .state_path()
            .parent()
            .expect("reconciliation scope")
            .to_path_buf();
        let boundary = cursor(41);
        reconciler
            .state
            .cursors
            .insert(boundary.shard_key().expect("shard key"), boundary);
        reconciler.persist().expect("persist cursor-only state");
        drop(reconciler);
        drop(
            CloudFilesMaterializationAuditIntentStore::open(
                &root.data_dir(),
                "mount-a",
                "/services",
            )
            .expect("create empty audit state"),
        );
        let guard =
            inspect_cursor_only_reconciliation_state(&root.data_dir(), "mount-a", "/services")
                .expect("inspect exact cursor-only state");
        let plan = guard
            .authoritative_reconciliation_plan("recovery-a", "token-a", "nonce-a")
            .expect("build recovery plan");
        drop(guard);
        CursorRecoveryFixture { root, scope, plan }
    }

    fn persist_armed_rewrite(fixture: &CursorRecoveryFixture) {
        persist_cursor_recovery_rewrite(
            &fixture.scope.join(CURSOR_RECOVERY_REWRITE_FILE),
            &CloudFilesCursorRecoveryRewrite::from_plan(
                &fixture.plan,
                CloudFilesCursorRecoveryRewritePhase::ReconcileArmed,
            ),
        )
        .expect("persist armed rewrite");
    }

    fn recovery_stage_path(
        fixture: &CursorRecoveryFixture,
        file_name: &str,
        label: &str,
        sequence: u64,
    ) -> PathBuf {
        fixture.scope.join(format!(
            ".{file_name}.{label}-{}-{sequence}",
            std::process::id()
        ))
    }

    fn write_recovery_stage(
        fixture: &CursorRecoveryFixture,
        file_name: &str,
        label: &str,
        sequence: u64,
        bytes: &[u8],
    ) -> PathBuf {
        let path = recovery_stage_path(fixture, file_name, label, sequence);
        fs::write(&path, bytes).expect("write recovery stage");
        path
    }

    fn write_recovery_state(fixture: &CursorRecoveryFixture, bytes: &[u8]) {
        fs::write(fixture.scope.join(RECONCILIATION_STATE_FILE), bytes)
            .expect("write recovery state cut");
    }

    fn write_recovery_audit(fixture: &CursorRecoveryFixture, bytes: &[u8]) {
        fs::write(
            fixture.scope.join(MATERIALIZATION_AUDIT_INTENTS_FILE),
            bytes,
        )
        .expect("write recovery audit cut");
    }

    fn open_armed_cursor_recovery(
        fixture: &CursorRecoveryFixture,
    ) -> CloudFilesNamespaceReconciler {
        let guard = resume_authorized_cursor_recovery(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("open cursor recovery rewrite cut");
        guard
            .apply_authoritative_reconciliation_plan(&fixture.plan)
            .expect("apply cursor recovery rewrite");
        drop(guard);
        open_authorized_cursor_recovery_progress(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("open armed cursor recovery progress")
    }

    fn unsupported_recovery_entry(path: &str) -> CloudFilesReconciledManifestEntry {
        CloudFilesReconciledManifestEntry {
            path: path.to_string(),
            object_id: format!("object-{path}"),
            version: "version-unsupported".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "symlink".to_string(),
        }
    }

    fn inject_unsupported_recovery_page_cut(
        fixture: &CursorRecoveryFixture,
        cut: CursorRecoveryPageCrashCut,
        path: &str,
    ) {
        let mut reconciler = open_armed_cursor_recovery(fixture);
        let entry = unsupported_recovery_entry(path);
        arm_cursor_recovery_page_crash(cut);
        let error = reconciler
            .install_full_scan_delta(
                BTreeMap::from([(entry.path.clone(), entry)]),
                Vec::new(),
                vec!["/services".to_string()],
                BTreeMap::new(),
                false,
            )
            .expect_err("inject cursor recovery page crash cut");
        assert_eq!(error.code, "injected-cursor-recovery-page-crash");
    }

    #[test]
    fn authorized_cursor_recovery_accepts_only_the_three_plan_bound_rewrite_cuts() {
        let fixture = cursor_recovery_fixture("recovery-exact-cuts");
        drop(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .expect("accept pristine authorized cut"),
        );

        persist_armed_rewrite(&fixture);
        assert!(
            inspect_cursor_only_reconciliation_state(
                &fixture.root.data_dir(),
                "mount-a",
                "/services"
            )
            .is_err(),
            "public inspection must reject an armed marker before transformation"
        );
        drop(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .expect("accept original state plus armed marker"),
        );

        write_recovery_state(&fixture, &fixture.plan.expected_state);
        drop(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .expect("accept expected state plus old audit plus marker"),
        );

        write_recovery_audit(&fixture, &fixture.plan.expected_audit);
        drop(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .expect("accept exact expected state and audit plus marker"),
        );

        let before_state = fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE))
            .expect("state before strict inspection");
        assert!(
            inspect_cursor_only_reconciliation_state(
                &fixture.root.data_dir(),
                "mount-a",
                "/services"
            )
            .is_err(),
            "public inspection must reject armed/transformed recovery evidence"
        );
        assert_eq!(
            fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE)).expect("state after reject"),
            before_state,
            "rejected public inspection must be immutable"
        );
        fs::remove_file(fixture.scope.join(CURSOR_RECOVERY_REWRITE_FILE))
            .expect("remove marker only to isolate transformed-state admission");
        assert!(
            inspect_cursor_only_reconciliation_state(
                &fixture.root.data_dir(),
                "mount-a",
                "/services"
            )
            .is_err(),
            "public inspection must reject a transformed image even without its marker"
        );
        assert_eq!(
            fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE))
                .expect("transformed state retained"),
            before_state
        );
    }

    #[test]
    fn cursor_recovery_replays_each_exact_atomic_stage() {
        let rewrite = cursor_recovery_fixture("recovery-rewrite-stage");
        let armed_bytes = encode_sobj(&CloudFilesCursorRecoveryRewrite::from_plan(
            &rewrite.plan,
            CloudFilesCursorRecoveryRewritePhase::ReconcileArmed,
        ))
        .expect("encode armed rewrite");
        let rewrite_stage = write_recovery_stage(
            &rewrite,
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
            1,
            &armed_bytes,
        );
        let guard = resume_authorized_cursor_recovery(
            &rewrite.root.data_dir(),
            "mount-a",
            "/services",
            &rewrite.plan,
        )
        .expect("accept exact rewrite stage");
        guard
            .apply_authoritative_reconciliation_plan(&rewrite.plan)
            .expect("replay rewrite stage");
        assert!(!rewrite_stage.exists());
        drop(guard);

        let state = cursor_recovery_fixture("recovery-state-stage");
        persist_armed_rewrite(&state);
        let state_stage = write_recovery_stage(
            &state,
            RECONCILIATION_STATE_FILE,
            CURSOR_RECOVERY_STATE_TEMP_LABEL,
            2,
            &state.plan.expected_state,
        );
        let guard = resume_authorized_cursor_recovery(
            &state.root.data_dir(),
            "mount-a",
            "/services",
            &state.plan,
        )
        .expect("accept exact state stage");
        guard
            .apply_authoritative_reconciliation_plan(&state.plan)
            .expect("replay state stage");
        assert!(!state_stage.exists());
        drop(guard);

        let audit = cursor_recovery_fixture("recovery-audit-stage");
        persist_armed_rewrite(&audit);
        write_recovery_state(&audit, &audit.plan.expected_state);
        let audit_stage = write_recovery_stage(
            &audit,
            MATERIALIZATION_AUDIT_INTENTS_FILE,
            CURSOR_RECOVERY_AUDIT_TEMP_LABEL,
            3,
            &audit.plan.expected_audit,
        );
        let guard = resume_authorized_cursor_recovery(
            &audit.root.data_dir(),
            "mount-a",
            "/services",
            &audit.plan,
        )
        .expect("accept exact audit stage");
        guard
            .apply_authoritative_reconciliation_plan(&audit.plan)
            .expect("replay audit stage");
        assert!(!audit_stage.exists());
        drop(guard);
    }

    #[test]
    fn cursor_recovery_rejects_wrong_predecessor_multiple_and_forged_stages_without_mutation() {
        let fixture = cursor_recovery_fixture("recovery-invalid-stages");
        persist_armed_rewrite(&fixture);
        write_recovery_state(&fixture, &fixture.plan.expected_state);
        let wrong_predecessor = write_recovery_stage(
            &fixture,
            RECONCILIATION_STATE_FILE,
            CURSOR_RECOVERY_STATE_TEMP_LABEL,
            11,
            &fixture.plan.expected_state,
        );
        let state_before = fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE))
            .expect("state before wrong predecessor");
        let marker_before = fs::read(fixture.scope.join(CURSOR_RECOVERY_REWRITE_FILE))
            .expect("marker before wrong predecessor");
        assert!(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .is_err(),
            "state stage after an already-installed expected state must stop"
        );
        assert_eq!(
            fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE)).expect("unchanged state"),
            state_before
        );
        assert_eq!(
            fs::read(fixture.scope.join(CURSOR_RECOVERY_REWRITE_FILE)).expect("unchanged marker"),
            marker_before
        );
        assert!(wrong_predecessor.exists());

        let multiple = cursor_recovery_fixture("recovery-multiple-stages");
        let armed_bytes = encode_sobj(&CloudFilesCursorRecoveryRewrite::from_plan(
            &multiple.plan,
            CloudFilesCursorRecoveryRewritePhase::ReconcileArmed,
        ))
        .expect("encode armed marker");
        let first = write_recovery_stage(
            &multiple,
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
            12,
            &armed_bytes,
        );
        let second = write_recovery_stage(
            &multiple,
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
            13,
            &armed_bytes,
        );
        assert!(
            resume_authorized_cursor_recovery(
                &multiple.root.data_dir(),
                "mount-a",
                "/services",
                &multiple.plan,
            )
            .is_err(),
            "multiple exact stages must stop"
        );
        assert!(first.exists() && second.exists());

        let forged = cursor_recovery_fixture("recovery-forged-stage");
        let forged_stage = write_recovery_stage(
            &forged,
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
            14,
            b"forged-plan-stage",
        );
        let original_state =
            fs::read(forged.scope.join(RECONCILIATION_STATE_FILE)).expect("original state");
        assert!(
            resume_authorized_cursor_recovery(
                &forged.root.data_dir(),
                "mount-a",
                "/services",
                &forged.plan,
            )
            .is_err()
        );
        assert_eq!(
            fs::read(forged.scope.join(RECONCILIATION_STATE_FILE)).expect("unchanged state"),
            original_state
        );
        assert!(forged_stage.exists());
    }

    #[test]
    fn forged_cursor_recovery_plan_and_torn_cut_are_rejected_immutably() {
        let fixture = cursor_recovery_fixture("recovery-forged-plan");
        persist_armed_rewrite(&fixture);
        write_recovery_audit(&fixture, &fixture.plan.expected_audit);
        let state_before =
            fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE)).expect("state before torn cut");
        let audit_before = fs::read(fixture.scope.join(MATERIALIZATION_AUDIT_INTENTS_FILE))
            .expect("audit before torn cut");
        assert!(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .is_err(),
            "expected audit may never precede expected state"
        );
        assert_eq!(
            fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE)).expect("state after reject"),
            state_before
        );
        assert_eq!(
            fs::read(fixture.scope.join(MATERIALIZATION_AUDIT_INTENTS_FILE))
                .expect("audit after reject"),
            audit_before
        );

        write_recovery_audit(&fixture, &fixture.plan.original_audit);
        let mut forged = fixture.plan.clone();
        forged.expected_state.push(0);
        assert!(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &forged,
            )
            .is_err(),
            "forged deterministic after-image must stop"
        );
        assert_eq!(
            fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE)).expect("unchanged state"),
            state_before
        );
    }

    #[test]
    fn ordinary_open_rejects_recovery_temp_evidence_without_removing_it() {
        let fixture = cursor_recovery_fixture("recovery-ordinary-open-temp");
        let stage = write_recovery_stage(
            &fixture,
            CURSOR_RECOVERY_REWRITE_FILE,
            CURSOR_RECOVERY_REWRITE_TEMP_LABEL,
            21,
            b"retained-stage-evidence",
        );
        assert!(
            inspect_cursor_only_reconciliation_state(
                &fixture.root.data_dir(),
                "mount-a",
                "/services"
            )
            .is_err(),
            "public inspection must reject recovery temp evidence"
        );
        assert!(
            stage.exists(),
            "public inspection must retain temp evidence"
        );
        let error = match CloudFilesNamespaceReconciler::open(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
        ) {
            Ok(_) => panic!("ordinary open must not consume recovery evidence"),
            Err(error) => error,
        };
        assert_eq!(error.code, "cursor-recovery-requires-authorized-open");
        assert_eq!(
            fs::read(&stage).expect("retained recovery stage"),
            b"retained-stage-evidence"
        );
    }

    #[test]
    fn malformed_recovery_temp_name_is_never_treated_as_resumable() {
        let fixture = cursor_recovery_fixture("recovery-malformed-temp");
        let malformed = fixture.scope.join(format!(
            ".{CURSOR_RECOVERY_REWRITE_FILE}.{CURSOR_RECOVERY_REWRITE_TEMP_LABEL}-not-a-sequence"
        ));
        fs::write(&malformed, b"forged").expect("write malformed recovery stage");
        assert!(
            resume_authorized_cursor_recovery(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .is_err()
        );
        assert!(malformed.exists());
        assert!(
            CloudFilesNamespaceReconciler::open(&fixture.root.data_dir(), "mount-a", "/services")
                .is_err()
        );
        assert!(malformed.exists());
    }

    #[test]
    fn recovery_progress_terminal_and_finalize_are_plan_bound_and_idempotent() {
        let fixture = cursor_recovery_fixture("recovery-terminal");
        let guard = resume_authorized_cursor_recovery(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("open pristine cut");
        guard
            .apply_authoritative_reconciliation_plan(&fixture.plan)
            .expect("apply exact rewrite");
        drop(guard);

        let mut reconciler = open_authorized_cursor_recovery_progress(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("open armed progress");
        assert!(
            reconciler
                .mark_authorized_cursor_recovery_terminal(&fixture.plan)
                .is_err(),
            "armed initial state is not terminal"
        );
        let original: CloudFilesNamespaceReconciliationState =
            decode_sobj(&fixture.plan.original_state).expect("decode original state");
        reconciler
            .install_full_scan_delta(
                BTreeMap::new(),
                Vec::new(),
                vec!["/services".to_string()],
                original.cursors,
                true,
            )
            .expect("install terminal authoritative delta");
        let mut audits = CloudFilesMaterializationAuditIntentStore::open(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
        )
        .expect("open recovery audit store");
        audits
            .acknowledge_directories(["/services"])
            .expect("acknowledge terminal root audit");
        drop(audits);

        let terminal = reconciler
            .mark_authorized_cursor_recovery_terminal(&fixture.plan)
            .expect("persist exact terminal phase");
        let replay_terminal = reconciler
            .resume_authorized_cursor_recovery_terminal_proof(&fixture.plan)
            .expect("reconstruct finalization capability");
        assert!(
            CloudFilesNamespaceReconciler::open(&fixture.root.data_dir(), "mount-a", "/services")
                .is_err(),
            "ordinary open remains blocked through Terminal"
        );
        assert!(
            reconciler
                .finalize_authorized_cursor_recovery_after_ready(
                    &fixture.plan,
                    terminal.authorize_after_host_ready_observed(),
                )
                .expect("finalize Terminal marker")
        );
        assert!(
            !reconciler
                .finalize_authorized_cursor_recovery_after_ready(
                    &fixture.plan,
                    replay_terminal.authorize_after_host_ready_observed(),
                )
                .expect("idempotent finalization replay")
        );
        drop(reconciler);
        drop(
            CloudFilesNamespaceReconciler::open(&fixture.root.data_dir(), "mount-a", "/services")
                .expect("ordinary open after exact finalized Terminal"),
        );
    }

    #[test]
    fn partial_authoritative_scan_progress_reopens_only_under_armed_plan() {
        let fixture = cursor_recovery_fixture("recovery-partial-progress");
        let guard = resume_authorized_cursor_recovery(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("open pristine cut");
        guard
            .apply_authoritative_reconciliation_plan(&fixture.plan)
            .expect("apply exact rewrite");
        drop(guard);
        let mut reconciler = open_authorized_cursor_recovery_progress(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("open armed recovery");
        let entry = CloudFilesReconciledManifestEntry {
            path: "/services/remote.txt".to_string(),
            object_id: "object-remote".to_string(),
            version: "version-remote".to_string(),
            content_hash: Some("blake3:remote".to_string()),
            content_size_bytes: Some(6),
            kind: "file".to_string(),
        };
        reconciler
            .install_full_scan_delta(
                BTreeMap::from([(entry.path.clone(), entry)]),
                Vec::new(),
                vec!["/services".to_string()],
                BTreeMap::new(),
                false,
            )
            .expect("install partial authoritative page");
        drop(reconciler);
        let reopened = open_authorized_cursor_recovery_progress(
            &fixture.root.data_dir(),
            "mount-a",
            "/services",
            &fixture.plan,
        )
        .expect("reopen partial scan progress");
        assert!(reopened.cursors().is_empty());
        assert!(reopened.full_scan_request().is_some());
        assert!(reopened.manifest().contains_key("/services/remote.txt"));
    }

    #[test]
    fn cursor_recovery_page_intent_repairs_only_the_three_exact_durable_cuts() {
        for (name, cut, manifest_committed, state_committed) in [
            (
                "recovery-page-before-before",
                CursorRecoveryPageCrashCut::AfterIntent,
                false,
                false,
            ),
            (
                "recovery-page-before-after",
                CursorRecoveryPageCrashCut::AfterManifest,
                true,
                false,
            ),
            (
                "recovery-page-after-after",
                CursorRecoveryPageCrashCut::AfterState,
                true,
                true,
            ),
        ] {
            let fixture = cursor_recovery_fixture(name);
            let path = "/services/unsupported-link";
            inject_unsupported_recovery_page_cut(&fixture, cut, path);
            let intent_path = fixture.scope.join(CURSOR_RECOVERY_PAGE_INTENT_FILE);
            assert!(intent_path.exists(), "crash cut retains its durable intent");

            let state_at_cut = fs::read(fixture.scope.join(RECONCILIATION_STATE_FILE))
                .expect("read recovery state at injected cut");
            let decoded_at_cut: CloudFilesNamespaceReconciliationState =
                decode_sobj(&state_at_cut).expect("decode state at injected cut");
            assert_eq!(
                decoded_at_cut.unsupported_projection_count,
                if state_committed { 1 } else { 0 },
                "state image must identify the injected cut"
            );

            let reopened = open_authorized_cursor_recovery_progress(
                &fixture.root.data_dir(),
                "mount-a",
                "/services",
                &fixture.plan,
            )
            .expect("repair exact cursor recovery page cut");
            assert_eq!(reopened.manifest().contains_key(path), manifest_committed);
            assert_eq!(
                reopened.unsupported_projection_count(),
                if manifest_committed { 1 } else { 0 }
            );
            assert_eq!(
                reopened
                    .unsupported_projection_examples()
                    .get(path)
                    .map(String::as_str),
                manifest_committed.then_some("symlink")
            );
            assert!(reopened.cursors().is_empty());
            assert!(reopened.full_scan_request().is_some());
            assert!(
                !intent_path.exists(),
                "exact repair must fsync-remove its completed page intent"
            );
        }
    }

    #[test]
    fn forged_or_multiple_cursor_recovery_page_intents_stop_immutably() {
        let forged = cursor_recovery_fixture("recovery-page-forged-plan");
        let forged_path = "/services/forged-link";
        inject_unsupported_recovery_page_cut(
            &forged,
            CursorRecoveryPageCrashCut::AfterManifest,
            forged_path,
        );
        let intent_path = forged.scope.join(CURSOR_RECOVERY_PAGE_INTENT_FILE);
        let mut intent: CloudFilesCursorRecoveryPageIntent =
            decode_sobj(&fs::read(&intent_path).expect("read valid page intent before forging"))
                .expect("decode valid page intent");
        intent.nonce.push_str("-forged");
        let forged_bytes =
            encode_cursor_recovery_page_intent(&intent).expect("encode forged intent");
        fs::write(&intent_path, &forged_bytes).expect("install forged page intent");
        let state_before = fs::read(forged.scope.join(RECONCILIATION_STATE_FILE))
            .expect("state before forged intent admission");
        let journal_path = forged.scope.join(MANIFEST_DIR).join(MANIFEST_JOURNAL_FILE);
        let journal_before = fs::read(&journal_path).expect("journal before forged admission");
        assert!(
            open_authorized_cursor_recovery_progress(
                &forged.root.data_dir(),
                "mount-a",
                "/services",
                &forged.plan,
            )
            .is_err(),
            "forged plan binding must stop"
        );
        assert_eq!(
            fs::read(forged.scope.join(RECONCILIATION_STATE_FILE)).expect("unchanged state"),
            state_before
        );
        assert_eq!(
            fs::read(&intent_path).expect("retained forged intent"),
            forged_bytes
        );
        assert_eq!(
            fs::read(&journal_path).expect("unchanged journal"),
            journal_before
        );

        let multiple = cursor_recovery_fixture("recovery-page-multiple");
        inject_unsupported_recovery_page_cut(
            &multiple,
            CursorRecoveryPageCrashCut::AfterIntent,
            "/services/multiple-link",
        );
        let final_path = multiple.scope.join(CURSOR_RECOVERY_PAGE_INTENT_FILE);
        let final_bytes = fs::read(&final_path).expect("read retained page intent");
        let temp_path = recovery_stage_path(
            &multiple,
            CURSOR_RECOVERY_PAGE_INTENT_FILE,
            CURSOR_RECOVERY_PAGE_INTENT_TEMP_LABEL,
            91,
        );
        fs::write(&temp_path, &final_bytes).expect("write duplicate page-intent stage");
        let state_before = fs::read(multiple.scope.join(RECONCILIATION_STATE_FILE))
            .expect("state before multiple-intent admission");
        assert!(
            open_authorized_cursor_recovery_progress(
                &multiple.root.data_dir(),
                "mount-a",
                "/services",
                &multiple.plan,
            )
            .is_err(),
            "multiple page intent objects must stop"
        );
        assert_eq!(
            fs::read(multiple.scope.join(RECONCILIATION_STATE_FILE)).expect("unchanged state"),
            state_before
        );
        assert_eq!(
            fs::read(&final_path).expect("retained final intent"),
            final_bytes
        );
        assert_eq!(
            fs::read(&temp_path).expect("retained intent stage"),
            final_bytes
        );
    }

    #[test]
    fn ordinary_and_public_open_reject_page_intent_evidence_without_a_plan() {
        let root = TestRoot::new("recovery-page-no-plan");
        let reconciler = open_reconciler(&root);
        let scope = reconciler
            .state_path()
            .parent()
            .expect("reconciliation scope")
            .to_path_buf();
        let state_before = fs::read(reconciler.state_path()).expect("state before page evidence");
        drop(reconciler);
        let evidence_path = scope.join(CURSOR_RECOVERY_PAGE_INTENT_FILE);
        fs::write(&evidence_path, b"unbound-page-intent").expect("write unbound page intent");

        assert!(
            inspect_cursor_only_reconciliation_state(&root.data_dir(), "mount-a", "/services")
                .is_err(),
            "public inspection must reject page-intent evidence"
        );
        let error =
            match CloudFilesNamespaceReconciler::open(&root.data_dir(), "mount-a", "/services") {
                Ok(_) => panic!("ordinary open must reject page-intent evidence"),
                Err(error) => error,
            };
        assert_eq!(error.code, "cursor-recovery-requires-authorized-open");
        assert_eq!(
            fs::read(scope.join(RECONCILIATION_STATE_FILE)).expect("unchanged state"),
            state_before
        );
        assert_eq!(
            fs::read(&evidence_path).expect("retained unbound page intent"),
            b"unbound-page-intent"
        );
    }

    #[test]
    fn cursor_advances_only_after_effect_and_manifest_are_durable() {
        let root = TestRoot::new("cursor-order");
        let mut reconciler = open_reconciler(&root);
        assert_eq!(
            reconciler.state_path(),
            root.data_dir()
                .module_runtime_dir()
                .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
                .join(RECONCILIATION_DIR)
                .join(
                    mount_scope_directory("mount-a", "/services")
                        .expect("mount scope directory"),
                )
                .join(RECONCILIATION_STATE_FILE)
        );
        let mut effects = FakeEffects::default();

        let progress = reconciler
            .reconcile_batch(batch(0, 1, vec![upsert("event-1", 1, None)]), &mut effects)
            .expect("reconcile");
        assert_eq!(progress.effects_applied, 1);
        assert!(progress.cursor_advanced);
        assert_eq!(effects.applied.len(), 1);
        assert_eq!(
            reconciler
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("persisted")
                .after_sequence,
            1
        );
        assert_eq!(
            reconciler
                .manifest()
                .get("/services/report.txt")
                .expect("manifest")
                .version,
            "01version1"
        );
        drop(reconciler);
        let reopened = open_reconciler(&root);
        assert_eq!(reopened.manifest().len(), 1);
        assert_eq!(
            reopened
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("persisted")
                .after_sequence,
            1
        );
    }

    #[test]
    fn crash_after_effect_replays_idempotently_before_cursor_advance() {
        let root = TestRoot::new("effect-crash");
        let mut reconciler = open_reconciler(&root);
        let mut crashing = FakeEffects {
            apply_results: VecDeque::from([Err("simulated crash after effect".to_string())]),
            ..FakeEffects::default()
        };
        let error = reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-crash", 1, None)]),
                &mut crashing,
            )
            .expect_err("effect outcome uncertain");
        assert_eq!(error.code, "namespace-effect");
        assert!(reconciler.cursors().is_empty());
        let error = reconciler
            .install_full_scan(BTreeMap::new(), BTreeMap::new())
            .expect_err("full scan cannot discard pending effect");
        assert_eq!(error.code, "namespace-effect-inflight");
        drop(reconciler);

        let mut reopened = open_reconciler(&root);
        let mut recovery = FakeEffects::default();
        let progress = reopened
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-crash", 1, None)]),
                &mut recovery,
            )
            .expect("recover and replay batch");
        assert_eq!(recovery.applied.len(), 1);
        assert_eq!(progress.effects_applied, 0);
        assert!(progress.cursor_advanced);
        assert_eq!(
            reopened
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("persisted")
                .after_sequence,
            1
        );
    }

    #[test]
    fn matching_operation_suppression_consumes_echo_without_local_effect() {
        let root = TestRoot::new("suppression");
        let mut reconciler = open_reconciler(&root);
        let action = event_action(&upsert("ignored", 1, Some("op-local")), "/services")
            .expect("action")
            .1;
        reconciler
            .record_suppression(CloudFilesNamespaceSuppression {
                operation_id: "op-local".to_string(),
                expected_action: action,
            })
            .expect("suppression");
        let mut effects = FakeEffects::default();

        let progress = reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-echo", 1, Some("op-local"))]),
                &mut effects,
            )
            .expect("suppress echo");
        assert_eq!(progress.effects_suppressed, 1);
        assert_eq!(
            progress.reconciled_local_operation_ids,
            vec!["op-local".to_string()]
        );
        assert!(effects.applied.is_empty());
        assert!(reconciler.manifest().contains_key("/services/report.txt"));
    }

    #[test]
    fn dirty_or_pending_paths_record_conflict_and_consume_cursor() {
        let root = TestRoot::new("dirty-conflict");
        let mut reconciler = open_reconciler(&root);
        let mut effects = FakeEffects {
            states: BTreeMap::from([(
                "/services/report.txt".to_string(),
                CloudFilesLocalPathState::Pending {
                    operation_id: "op-pending".to_string(),
                },
            )]),
            ..FakeEffects::default()
        };

        let progress = reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-conflict", 1, None)]),
                &mut effects,
            )
            .expect("record conflict");
        assert_eq!(progress.conflicts_recorded, 1);
        assert!(effects.applied.is_empty());
        assert_eq!(effects.conflicts.len(), 1);
        assert!(reconciler.conflicts().contains_key("event-conflict"));
        assert_eq!(
            reconciler
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("persisted")
                .after_sequence,
            1
        );

        effects.states.insert(
            "/services/report.txt".to_string(),
            CloudFilesLocalPathState::Dirty {
                detail: "local bytes have not been staged".to_string(),
            },
        );
        reconciler
            .reconcile_batch(
                batch(1, 2, vec![upsert("event-dirty", 2, None)]),
                &mut effects,
            )
            .expect("record dirty conflict");
        assert!(reconciler.conflicts().contains_key("event-dirty"));
        assert_eq!(effects.conflicts.len(), 2);
    }

    #[test]
    fn clean_remove_is_one_effect_and_removes_the_manifest_entry() {
        let root = TestRoot::new("remove");
        let mut reconciler = open_reconciler(&root);
        let mut effects = FakeEffects::default();
        reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-upsert", 1, None)]),
                &mut effects,
            )
            .expect("seed manifest");
        let remove = CloudFilesNamespaceFeedEvent::Remove {
            path: "/services/report.txt".to_string(),
            object_id: "01object".to_string(),
            version: "01version1".to_string(),
            content_hash: Some("blake3:1".to_string()),
            provenance: provenance("event-remove", 2, None),
        };

        reconciler
            .reconcile_batch(batch(1, 2, vec![remove]), &mut effects)
            .expect("remove");
        assert!(matches!(
            effects.applied.last(),
            Some(CloudFilesNamespaceAction::Remove { .. })
        ));
        assert!(!reconciler.manifest().contains_key("/services/report.txt"));
        assert_eq!(
            reconciler
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("persisted")
                .after_sequence,
            2
        );
    }

    #[test]
    fn sparse_materialization_is_durable_and_directory_rename_delete_rekeys_ledger() {
        let root = TestRoot::new("materialized-ledger");
        let mut reconciler = open_reconciler(&root);
        let directory = CloudFilesReconciledManifestEntry {
            path: "/services/d".to_string(),
            object_id: "01directory".to_string(),
            version: "01directoryversion".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "directory".to_string(),
        };
        let child = CloudFilesReconciledManifestEntry {
            path: "/services/d/file.txt".to_string(),
            object_id: "01child".to_string(),
            version: "01childversion".to_string(),
            content_hash: Some("blake3:child".to_string()),
            content_size_bytes: Some(5),
            kind: "file".to_string(),
        };
        reconciler
            .record_materialized_placeholder("/services", directory)
            .expect("record root page child");
        reconciler
            .record_materialized_placeholder("/services/d", child)
            .expect("record nested page child");
        drop(reconciler);

        let mut reopened = open_reconciler(&root);
        assert!(reopened.materialized_directories().contains("/services"));
        assert!(reopened.materialized_directories().contains("/services/d"));
        assert!(reopened.manifest().contains_key("/services/d/file.txt"));
        let rename = CloudFilesNamespaceFeedEvent::Rename {
            from_path: "/services/d".to_string(),
            to_path: "/services/e".to_string(),
            object_id: "01directory".to_string(),
            version: "01directoryversion2".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "directory".to_string(),
            provenance: provenance("rename-materialized", 1, None),
        };
        reopened
            .reconcile_batch(batch(0, 1, vec![rename]), &mut FakeEffects::default())
            .expect("rename materialized directory");
        assert!(!reopened.materialized_directories().contains("/services/d"));
        assert!(reopened.materialized_directories().contains("/services/e"));

        let remove = CloudFilesNamespaceFeedEvent::Remove {
            path: "/services/e".to_string(),
            object_id: "01directory".to_string(),
            version: "01directoryversion2".to_string(),
            content_hash: None,
            provenance: provenance("remove-materialized", 2, None),
        };
        reopened
            .reconcile_batch(batch(1, 2, vec![remove]), &mut FakeEffects::default())
            .expect("remove materialized directory");
        assert!(!reopened.materialized_directories().contains("/services/e"));
        assert!(reopened.materialized_directories().contains("/services"));
    }

    #[test]
    fn crash_during_conflict_effect_does_not_advance_cursor_and_retries_after_restart() {
        let root = TestRoot::new("conflict-crash");
        let mut reconciler = open_reconciler(&root);
        let states = BTreeMap::from([(
            "/services/report.txt".to_string(),
            CloudFilesLocalPathState::Dirty {
                detail: "unstaged local bytes".to_string(),
            },
        )]);
        let mut crashing = FakeEffects {
            states: states.clone(),
            conflict_results: VecDeque::from([Err(
                "simulated crash while surfacing conflict".to_string()
            )]),
            ..FakeEffects::default()
        };

        let error = reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-conflict-crash", 1, None)]),
                &mut crashing,
            )
            .expect_err("conflict effect outcome uncertain");
        assert_eq!(error.code, "record-conflict-effect");
        assert!(reconciler.cursors().is_empty());
        drop(reconciler);

        let mut reopened = open_reconciler(&root);
        let mut recovery = FakeEffects {
            states,
            ..FakeEffects::default()
        };
        let progress = reopened
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-conflict-crash", 1, None)]),
                &mut recovery,
            )
            .expect("retry conflict effect");
        assert_eq!(progress.conflicts_recorded, 1);
        assert_eq!(recovery.conflicts.len(), 1);
        assert!(progress.cursor_advanced);
    }

    #[test]
    fn rename_is_one_effect_and_rescan_never_advances_cursor() {
        let root = TestRoot::new("rename-rescan");
        let mut reconciler = open_reconciler(&root);
        let rename = CloudFilesNamespaceFeedEvent::Rename {
            from_path: "/services/old.txt".to_string(),
            to_path: "/services/new.txt".to_string(),
            object_id: "01object".to_string(),
            version: "01version".to_string(),
            content_hash: Some("blake3:value".to_string()),
            content_size_bytes: Some(5),
            kind: "file".to_string(),
            provenance: provenance("event-rename", 1, None),
        };
        let mut effects = FakeEffects::default();
        reconciler
            .reconcile_batch(batch(0, 1, vec![rename]), &mut effects)
            .expect("rename");
        assert!(matches!(
            effects.applied.as_slice(),
            [CloudFilesNamespaceAction::Rename { .. }]
        ));
        assert!(!reconciler.manifest().contains_key("/services/old.txt"));
        assert!(reconciler.manifest().contains_key("/services/new.txt"));

        let rescan = CloudFilesNamespaceFeedEvent::RescanRequired {
            reason: "journal-compaction-gap".to_string(),
            detail: "cursor fell behind retention".to_string(),
        };
        let progress = reconciler
            .reconcile_batch(batch(1, 1, vec![rescan]), &mut effects)
            .expect("rescan request");
        assert!(progress.full_scan_requested);
        assert!(!progress.cursor_advanced);
        assert_eq!(effects.rescans.len(), 1);
        assert!(reconciler.full_scan_request().is_some());
        assert_eq!(
            reconciler
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("persisted")
                .after_sequence,
            1
        );
    }

    #[test]
    fn local_application_updates_manifest_atomically_and_exact_echo_is_suppressed_after_restart() {
        let root = TestRoot::new("local-application");
        let mut reconciler = open_reconciler(&root);
        let event = upsert("event-local", 1, Some("op-local"));
        let action = event_action(&event, "/services").expect("action").1;
        reconciler
            .record_local_application(CloudFilesNamespaceSuppression {
                operation_id: "op-local".to_string(),
                expected_action: action,
            })
            .expect("record local application");
        assert_eq!(
            reconciler
                .manifest()
                .get("/services/report.txt")
                .expect("immediate manifest generation")
                .version,
            "01version1"
        );
        drop(reconciler);

        let mut reopened = open_reconciler(&root);
        assert_eq!(
            reopened
                .manifest()
                .get("/services/report.txt")
                .expect("durable manifest generation")
                .version,
            "01version1"
        );
        let mut effects = FakeEffects::default();
        let progress = reopened
            .reconcile_batch(batch(0, 1, vec![event]), &mut effects)
            .expect("consume exact echo");
        assert_eq!(progress.effects_suppressed, 1);
        assert!(effects.applied.is_empty());
        drop(reopened);

        let reopened = open_reconciler(&root);
        assert_eq!(
            reopened
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("durable echo cursor")
                .after_sequence,
            1
        );
    }

    #[test]
    fn apply_remote_conflict_resolution_recovers_across_restart() {
        let root = TestRoot::new("conflict-resolution-restart");
        let mut reconciler = open_reconciler(&root);
        let mut dirty = FakeEffects {
            states: BTreeMap::from([(
                "/services/report.txt".to_string(),
                CloudFilesLocalPathState::Dirty {
                    detail: "local edit".to_string(),
                },
            )]),
            ..FakeEffects::default()
        };
        reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-resolve", 1, None)]),
                &mut dirty,
            )
            .expect("record conflict");
        let mut uncertain = FakeEffects {
            apply_results: VecDeque::from([Err("simulated post-effect crash".to_string())]),
            ..FakeEffects::default()
        };
        let error = reconciler
            .resolve_conflict(
                "event-resolve",
                CloudFilesNamespaceConflictResolution::ApplyRemote,
                &mut uncertain,
            )
            .expect_err("resolution effect is uncertain");
        assert_eq!(error.code, "conflict-resolution-effect");
        drop(reconciler);

        let mut reopened = open_reconciler(&root);
        let mut recovery = FakeEffects::default();
        reopened
            .resolve_conflict(
                "event-resolve",
                CloudFilesNamespaceConflictResolution::ApplyRemote,
                &mut recovery,
            )
            .expect("resume durable resolution");
        assert_eq!(recovery.applied.len(), 1);
        assert!(!reopened.conflicts().contains_key("event-resolve"));
        assert_eq!(
            reopened
                .manifest()
                .get("/services/report.txt")
                .expect("resolved manifest")
                .version,
            "01version1"
        );
        drop(reopened);

        let reopened = open_reconciler(&root);
        assert!(!reopened.conflicts().contains_key("event-resolve"));
        assert!(reopened.manifest().contains_key("/services/report.txt"));
    }

    #[test]
    fn startup_conflicts_are_path_bounded_durable_and_resolvable() {
        let root = TestRoot::new("startup-conflict");
        let mut reconciler = open_reconciler(&root);
        reconciler
            .record_startup_conflict("/services/./untracked.txt", "first scan")
            .expect("record startup conflict");
        reconciler
            .record_startup_conflict("/services/untracked.txt", "newer scan evidence")
            .expect("replace startup conflict evidence");
        assert_eq!(reconciler.conflict_count(), 1);
        assert_eq!(
            reconciler
                .startup_conflicts()
                .get("/services/untracked.txt")
                .expect("startup conflict")
                .detail,
            "newer scan evidence"
        );
        drop(reconciler);

        let mut reopened = open_reconciler(&root);
        assert_eq!(reopened.conflict_count(), 1);
        assert!(
            reopened
                .resolve_startup_conflict("/services/untracked.txt")
                .expect("resolve startup conflict")
        );
        assert!(
            !reopened
                .resolve_startup_conflict("/services/untracked.txt")
                .expect("idempotent resolution")
        );
        drop(reopened);
        assert_eq!(open_reconciler(&root).conflict_count(), 0);
    }

    #[test]
    fn unsupported_projection_summary_is_bounded_incremental_and_durable() {
        let root = TestRoot::new("unsupported-projection-summary");
        let mut reconciler = open_reconciler(&root);
        let entries = (0..40)
            .map(|index| {
                let path = format!("/services/link-{index:03}");
                (
                    path.clone(),
                    CloudFilesReconciledManifestEntry {
                        path,
                        object_id: format!("object-{index}"),
                        version: format!("version-{index}"),
                        content_hash: None,
                        content_size_bytes: None,
                        kind: "symlink".to_string(),
                    },
                )
            })
            .collect();
        reconciler
            .install_full_scan_delta(entries, Vec::new(), Vec::new(), BTreeMap::new(), false)
            .expect("install unsupported page");
        assert_eq!(reconciler.unsupported_projection_count(), 40);
        assert_eq!(
            reconciler.unsupported_projection_examples().len(),
            CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT
        );

        let replacement_path = "/services/link-000".to_string();
        reconciler
            .install_full_scan_delta(
                BTreeMap::from([(
                    replacement_path.clone(),
                    CloudFilesReconciledManifestEntry {
                        path: replacement_path,
                        object_id: "file-object".to_string(),
                        version: "file-version".to_string(),
                        content_hash: Some("blake3:file".to_string()),
                        content_size_bytes: Some(4),
                        kind: "file".to_string(),
                    },
                )]),
                vec!["/services/link-001".to_string()],
                Vec::new(),
                BTreeMap::new(),
                false,
            )
            .expect("replace and remove unsupported entries");
        assert_eq!(reconciler.unsupported_projection_count(), 38);
        assert!(reconciler.unsupported_projection_examples().len() <= 16);
        drop(reconciler);

        let reopened = open_reconciler(&root);
        assert_eq!(reopened.unsupported_projection_count(), 38);
        assert_eq!(
            reopened.unsupported_projection_examples().len(),
            CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT
        );
    }

    #[test]
    fn mount_and_root_replacements_use_isolated_reconciliation_scopes() {
        let root = TestRoot::new("scope-isolation");
        let mut original = open_reconciler(&root);
        let original_state = original.state_path().to_path_buf();
        original
            .record_startup_conflict("/services/local.txt", "owned-root difference")
            .expect("record original conflict");
        drop(original);

        let replacement =
            CloudFilesNamespaceReconciler::open(&root.data_dir(), "mount-a", "/replacement")
                .expect("open replacement root");
        assert_ne!(replacement.state_path(), original_state);
        assert_eq!(replacement.conflict_count(), 0);
        drop(replacement);

        let moved_mount =
            CloudFilesNamespaceReconciler::open(&root.data_dir(), "mount-b", "/services")
                .expect("open replacement mount path");
        assert_ne!(moved_mount.state_path(), original_state);
        assert_eq!(moved_mount.conflict_count(), 0);
        drop(moved_mount);

        let reopened = open_reconciler(&root);
        assert_eq!(reopened.state_path(), original_state);
        assert_eq!(reopened.conflict_count(), 1);
    }

    #[test]
    fn bounded_snapshot_churn_compacts_receipts_and_restarts_at_latest_cursor() {
        let root = TestRoot::new("bounded-state-churn");
        let mut reconciler = open_reconciler(&root);
        let mut effects = FakeEffects::default();
        for sequence in 1..=64 {
            reconciler
                .reconcile_batch(
                    batch(
                        sequence - 1,
                        sequence,
                        vec![upsert(&format!("event-{sequence}"), sequence, None)],
                    ),
                    &mut effects,
                )
                .expect("reconcile churn event");
        }
        assert!(reconciler.state.applied_events.is_empty());
        let compacted_bytes = reconciler
            .compact_durable_state()
            .expect("compact durable snapshot");
        assert!(compacted_bytes < 64 * 1024);
        drop(reconciler);

        let reopened = open_reconciler(&root);
        assert_eq!(
            reopened
                .cursor_for(&cursor(0))
                .expect("cursor")
                .expect("latest cursor")
                .after_sequence,
            64
        );
        assert_eq!(reopened.manifest().len(), 1);
    }

    #[test]
    fn chunked_materialized_manifest_exceeds_legacy_state_cap_and_reopens() {
        let root = TestRoot::new("chunked-large-manifest");
        let mut reconciler = open_reconciler(&root);
        const ENTRIES: usize = 4_096;
        const PAGE: usize = 32;
        for first in (0..ENTRIES).step_by(PAGE) {
            let page = (first..(first + PAGE).min(ENTRIES))
                .map(|index| {
                    let path = format!("/services/materialized-{index:05}.txt");
                    (
                        path.clone(),
                        CloudFilesReconciledManifestEntry {
                            path,
                            object_id: format!("object-{index:05}"),
                            version: format!("version-{index:05}"),
                            content_hash: Some(format!("blake3:{index:064x}")),
                            content_size_bytes: Some(index as u64),
                            kind: "file".to_string(),
                        },
                    )
                })
                .collect();
            reconciler
                .install_full_scan_delta(page, Vec::new(), Vec::new(), BTreeMap::new(), false)
                .expect("install bounded materialized page");
        }
        assert_eq!(reconciler.manifest().len(), ENTRIES);
        let state_bytes = fs::metadata(reconciler.state_path())
            .expect("state metadata")
            .len();
        assert!(
            state_bytes < RECONCILIATION_STATE_MAX_BYTES,
            "materialized entries must not be embedded in the state snapshot"
        );
        reconciler
            .compact_durable_state()
            .expect("compact chunked manifest");
        let manifest_dir = reconciler
            .state_path()
            .parent()
            .expect("scope")
            .join(MANIFEST_DIR);
        let chunk_count = fs::read_dir(&manifest_dir)
            .expect("manifest directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("chunk-"))
            .count();
        assert!(chunk_count > 1, "large manifests must span bounded chunks");
        drop(reconciler);

        let reopened = open_reconciler(&root);
        assert_eq!(reopened.manifest().len(), ENTRIES);
        assert_eq!(
            reopened
                .manifest()
                .get("/services/materialized-04095.txt")
                .expect("last materialized entry")
                .version,
            "version-04095"
        );
    }

    #[test]
    fn oversized_state_is_rejected_before_write_and_prior_snapshot_survives() {
        let root = TestRoot::new("state-bound");
        let mut reconciler = open_reconciler(&root);
        let mut effects = FakeEffects::default();
        reconciler
            .reconcile_batch(
                batch(0, 1, vec![upsert("event-seed", 1, None)]),
                &mut effects,
            )
            .expect("seed state");
        let prior = fs::read(reconciler.state_path()).expect("prior state bytes");
        let error = reconciler
            .record_startup_conflict(
                "/services/too-large.txt",
                "x".repeat(RECONCILIATION_STATE_MAX_BYTES as usize),
            )
            .expect_err("oversized state must fail before write");
        assert_eq!(error.code, "reconciliation-state-too-large");
        assert_eq!(
            fs::read(reconciler.state_path()).expect("unchanged state bytes"),
            prior
        );
        assert_eq!(reconciler.conflict_count(), 0);
        drop(reconciler);

        let reopened = open_reconciler(&root);
        assert!(reopened.manifest().contains_key("/services/report.txt"));
        assert_eq!(reopened.conflict_count(), 0);
    }

    #[test]
    fn orphaned_atomic_snapshot_stage_is_ignored_after_crash() {
        let root = TestRoot::new("snapshot-crash");
        let mut reconciler = open_reconciler(&root);
        reconciler
            .record_startup_conflict("/services/local.txt", "durable evidence")
            .expect("persist active state");
        let state_path = reconciler.state_path().to_path_buf();
        let orphan = state_path
            .parent()
            .expect("state parent")
            .join(".state.sobj.cloud-files-state-crash-orphan");
        let mut staged =
            durable_file::create_private_staged_file(&orphan).expect("create orphaned stage");
        use std::io::Write as _;
        staged.write_all(b"not installed").expect("write orphan");
        staged.sync_all().expect("sync orphan");
        drop(staged);
        drop(reconciler);

        let reopened = open_reconciler(&root);
        assert_eq!(reopened.conflict_count(), 1);
        assert!(state_path.exists());
        assert!(!orphan.exists());
    }
}
