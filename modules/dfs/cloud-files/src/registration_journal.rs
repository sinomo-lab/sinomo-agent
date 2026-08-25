#![cfg_attr(not(windows), allow(dead_code))]

use std::path::PathBuf;

use crate::protocol::{
    CloudFilesModuleError, CloudFilesMountRequest, CloudFilesRegistrationReceipt,
    CloudFilesUnregisterRequest, SINOMO_DFS_CLOUD_FILES_INTERFACE_V2,
    SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
};
use serde::{Deserialize, Serialize};
use sinomo_module_api::{decode_sobj, encode_sobj};

pub(crate) const REGISTRATION_IDENTITY_MAX_BYTES: usize = 64 * 1024;
const REGISTRATION_JOURNAL_SCHEMA_VERSION: u32 = 1;
const CURSOR_RECOVERY_REGISTRATION_JOURNAL_SCHEMA_VERSION: u32 = 2;
const REGISTRATION_IDENTITY_SCHEMA: &str = "sinomo.dfs.cloud-files.registration.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RegistrationPhase {
    Prepared,
    Registered,
    StateOwned,
    Ready,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationRecoveryAction {
    ClearArtifacts,
    RollBackRegistration,
    ReuseOwnedRegistration,
    ReuseOwnedState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationOwnershipDisposition {
    Release,
    Retain,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CursorRecoveryJournalProvenance {
    pub(crate) recovery_id: String,
    pub(crate) recovery_token: String,
    pub(crate) recovery_cycle: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RegistrationRootIdentity {
    schema: String,
    interface_version: String,
    registration_nonce: String,
    sync_root: PathBuf,
    sync_root_file_id: u64,
    root_path: String,
    provider_name: String,
    provider_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RegistrationJournal {
    pub(crate) schema_version: u32,
    pub(crate) interface_version: String,
    pub(crate) phase: RegistrationPhase,
    pub(crate) registration_nonce: String,
    pub(crate) sync_root: PathBuf,
    pub(crate) sync_root_file_id: u64,
    pub(crate) root_path: String,
    pub(crate) provider_name: String,
    pub(crate) provider_version: String,
    pub(crate) provider_id: String,
    pub(crate) root_identity: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cursor_recovery: Option<CursorRecoveryJournalProvenance>,
}

impl RegistrationJournal {
    pub(crate) fn prepared(
        request: &CloudFilesMountRequest,
        provider_id: impl Into<String>,
        registration_nonce: impl Into<String>,
        sync_root_file_id: u64,
    ) -> Result<Self, CloudFilesModuleError> {
        let provider_id = provider_id.into();
        let registration_nonce = registration_nonce.into();
        validate_nonce(&registration_nonce)?;
        let identity = RegistrationRootIdentity {
            schema: REGISTRATION_IDENTITY_SCHEMA.to_string(),
            interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
            registration_nonce: registration_nonce.clone(),
            sync_root: request.sync_root.clone(),
            sync_root_file_id,
            root_path: request.root_path.clone(),
            provider_name: request.provider_name.clone(),
            provider_id: provider_id.clone(),
        };
        let root_identity = encode_sobj(&identity).map_err(|source| CloudFilesModuleError {
            code: "registration-identity-encode".to_string(),
            detail: format!("encode nonce-bound Cloud Files registration identity: {source}"),
        })?;
        if root_identity.len() > REGISTRATION_IDENTITY_MAX_BYTES {
            return Err(CloudFilesModuleError {
                code: "registration-identity-too-large".to_string(),
                detail: format!(
                    "Cloud Files registration identity is {} bytes; max is {REGISTRATION_IDENTITY_MAX_BYTES}",
                    root_identity.len()
                ),
            });
        }
        Ok(Self {
            schema_version: REGISTRATION_JOURNAL_SCHEMA_VERSION,
            interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
            phase: RegistrationPhase::Prepared,
            registration_nonce,
            sync_root: request.sync_root.clone(),
            sync_root_file_id,
            root_path: request.root_path.clone(),
            provider_name: request.provider_name.clone(),
            provider_version: request.provider_version.clone(),
            provider_id,
            root_identity,
            cursor_recovery: None,
        })
    }

    pub(crate) fn cursor_recovery_state_owned(
        request: &CloudFilesMountRequest,
        provider_id: impl Into<String>,
        registration_nonce: impl Into<String>,
        sync_root_file_id: u64,
        recovery_id: impl Into<String>,
        recovery_token: impl Into<String>,
        recovery_cycle: u64,
    ) -> Result<Self, CloudFilesModuleError> {
        let mut journal =
            Self::prepared(request, provider_id, registration_nonce, sync_root_file_id)?;
        journal.phase = RegistrationPhase::StateOwned;
        journal.cursor_recovery = Some(CursorRecoveryJournalProvenance {
            recovery_id: recovery_id.into(),
            recovery_token: recovery_token.into(),
            recovery_cycle,
        });
        journal.schema_version = CURSOR_RECOVERY_REGISTRATION_JOURNAL_SCHEMA_VERSION;
        journal.validate(&journal.provider_id)?;
        Ok(journal)
    }

    pub(crate) fn bind_cursor_recovery(
        &mut self,
        recovery_id: impl Into<String>,
        recovery_token: impl Into<String>,
        recovery_cycle: u64,
    ) {
        self.cursor_recovery = Some(CursorRecoveryJournalProvenance {
            recovery_id: recovery_id.into(),
            recovery_token: recovery_token.into(),
            recovery_cycle,
        });
        self.schema_version = CURSOR_RECOVERY_REGISTRATION_JOURNAL_SCHEMA_VERSION;
    }

    pub(crate) fn clear_cursor_recovery(&mut self) {
        self.cursor_recovery = None;
        self.schema_version = REGISTRATION_JOURNAL_SCHEMA_VERSION;
    }

    pub(crate) fn validate(&self, expected_provider_id: &str) -> Result<(), CloudFilesModuleError> {
        if !matches!(
            (self.schema_version, self.cursor_recovery.is_some()),
            (REGISTRATION_JOURNAL_SCHEMA_VERSION, false)
                | (CURSOR_RECOVERY_REGISTRATION_JOURNAL_SCHEMA_VERSION, true)
        ) || !matches!(
            self.interface_version.as_str(),
            SINOMO_DFS_CLOUD_FILES_INTERFACE_V2 | SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        ) || self.provider_id != expected_provider_id
        {
            return Err(CloudFilesModuleError {
                code: "registration-journal-version-mismatch".to_string(),
                detail: "Cloud Files registration journal schema, interface, or provider ID is unsupported"
                    .to_string(),
            });
        }
        validate_nonce(&self.registration_nonce)?;
        if self.cursor_recovery.as_ref().is_some_and(|provenance| {
            provenance.recovery_id.trim().is_empty()
                || !valid_v1_blake3_token(&provenance.recovery_token)
        }) {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-journal-provenance".to_string(),
                detail: "Cloud Files registration journal has malformed cursor-recovery provenance"
                    .to_string(),
            });
        }
        if self.root_identity.len() > REGISTRATION_IDENTITY_MAX_BYTES {
            return Err(CloudFilesModuleError {
                code: "registration-identity-too-large".to_string(),
                detail: format!(
                    "Cloud Files registration identity is {} bytes; max is {REGISTRATION_IDENTITY_MAX_BYTES}",
                    self.root_identity.len()
                ),
            });
        }
        let observed =
            decode_sobj::<RegistrationRootIdentity>(&self.root_identity).map_err(|source| {
                CloudFilesModuleError {
                    code: "registration-identity-decode".to_string(),
                    detail: format!(
                        "decode nonce-bound Cloud Files registration identity: {source}"
                    ),
                }
            })?;
        let expected = RegistrationRootIdentity {
            schema: REGISTRATION_IDENTITY_SCHEMA.to_string(),
            interface_version: self.interface_version.clone(),
            registration_nonce: self.registration_nonce.clone(),
            sync_root: self.sync_root.clone(),
            sync_root_file_id: self.sync_root_file_id,
            root_path: self.root_path.clone(),
            provider_name: self.provider_name.clone(),
            provider_id: self.provider_id.clone(),
        };
        if observed != expected {
            return Err(CloudFilesModuleError {
                code: "registration-journal-identity-mismatch".to_string(),
                detail:
                    "Cloud Files registration journal fields do not match its durable root identity"
                        .to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn matches_mount_request(&self, request: &CloudFilesMountRequest) -> bool {
        self.sync_root == request.sync_root
            && self.root_path == request.root_path
            && self.provider_name == request.provider_name
    }

    pub(crate) fn matches_unregister_request(&self, request: &CloudFilesUnregisterRequest) -> bool {
        self.sync_root == request.sync_root
            && self.root_path == request.root_path
            && self.provider_name == request.provider_name
    }

    pub(crate) fn matches_receipt(&self, receipt: &CloudFilesRegistrationReceipt) -> bool {
        receipt.registration_identity_interface_version() == self.interface_version
            && receipt.sync_root == self.sync_root
            && receipt.root_path == self.root_path
            && receipt.provider_name == self.provider_name
    }

    pub(crate) fn mount_ownership_evidence_verified(
        &self,
        receipt: Option<&CloudFilesRegistrationReceipt>,
    ) -> Result<bool, CloudFilesModuleError> {
        if receipt.is_some_and(|receipt| !self.matches_receipt(receipt)) {
            return Err(CloudFilesModuleError {
                code: "registration-artifact-mismatch".to_string(),
                detail: format!(
                    "durable receipt and nonce journal identify different Cloud Files registrations for {}",
                    self.sync_root.display()
                ),
            });
        }
        match self.phase {
            RegistrationPhase::Prepared
                if self.interface_version == SINOMO_DFS_CLOUD_FILES_INTERFACE_V2 =>
            {
                Ok(true)
            }
            RegistrationPhase::Registered | RegistrationPhase::StateOwned => Ok(true),
            RegistrationPhase::Ready if receipt.is_some() => Ok(true),
            RegistrationPhase::Ready => Err(CloudFilesModuleError {
                code: "registration-receipt-missing".to_string(),
                detail: format!(
                    "Ready Cloud Files state ownership for {} has no matching durable readiness receipt",
                    self.sync_root.display()
                ),
            }),
            RegistrationPhase::Prepared => Ok(receipt.is_some()),
        }
    }

    /// Whether this phase is a durable ownership boundary for writable local
    /// state. Deployed v2 opened and repaired writable stores before it wrote
    /// either Prepared or Registered, so both legacy phases are conservatively
    /// ownership-bearing. v3 moved all mutable opens after StateOwned; only its
    /// Prepared phase is therefore rollback-safe.
    pub(crate) fn owns_writable_state(&self) -> bool {
        if self.interface_version == SINOMO_DFS_CLOUD_FILES_INTERFACE_V2 {
            return matches!(
                self.phase,
                RegistrationPhase::Prepared
                    | RegistrationPhase::Registered
                    | RegistrationPhase::StateOwned
                    | RegistrationPhase::Ready
            );
        }
        matches!(
            self.phase,
            RegistrationPhase::Registered
                | RegistrationPhase::StateOwned
                | RegistrationPhase::Ready
        )
    }

    pub(crate) fn recovery_action(
        &self,
        registration_present: bool,
        receipt_verified: bool,
    ) -> RegistrationRecoveryAction {
        if self.owns_writable_state() && receipt_verified {
            if registration_present {
                RegistrationRecoveryAction::ReuseOwnedRegistration
            } else {
                RegistrationRecoveryAction::ReuseOwnedState
            }
        } else if !registration_present {
            RegistrationRecoveryAction::ClearArtifacts
        } else {
            RegistrationRecoveryAction::RollBackRegistration
        }
    }

    pub(crate) fn state_preservation_disposition(
        &self,
        receipt: Option<&CloudFilesRegistrationReceipt>,
        current_sync_root_file_id: Option<u64>,
    ) -> Result<RegistrationOwnershipDisposition, CloudFilesModuleError> {
        // A preserve-state request is itself crash-replayable ownership intent.
        // Even v3 Prepared is normalized to StateOwned: destructively releasing
        // it and crashing before the typed response would make a retry unable
        // to distinguish a safely released attempt from missing evidence.
        let current_sync_root_file_id =
            current_sync_root_file_id.ok_or_else(|| CloudFilesModuleError {
                code: "sync-root-file-id-unavailable".to_string(),
                detail: format!(
                    "cannot verify the current file ID for retained Cloud Files root {}",
                    self.sync_root.display()
                ),
            })?;
        if current_sync_root_file_id != self.sync_root_file_id {
            return Err(CloudFilesModuleError {
                code: "sync-root-file-id-changed".to_string(),
                detail: format!(
                    "refusing to retain Cloud Files state ownership for {} because its stable file ID changed",
                    self.sync_root.display()
                ),
            });
        }
        let receipt = match (self.phase, receipt) {
            (RegistrationPhase::Ready, None) => {
                return Err(CloudFilesModuleError {
                    code: "registration-receipt-missing".to_string(),
                    detail: format!(
                        "refusing to retain Ready Cloud Files state ownership for {} without its durable readiness receipt",
                        self.sync_root.display()
                    ),
                });
            }
            (
                RegistrationPhase::Prepared
                | RegistrationPhase::Registered
                | RegistrationPhase::StateOwned,
                None,
            ) => None,
            (_, receipt) => receipt,
        };
        if receipt.is_some_and(|receipt| !self.matches_receipt(receipt)) {
            return Err(CloudFilesModuleError {
                code: "registration-artifact-mismatch".to_string(),
                detail: format!(
                    "refusing to retain Cloud Files state ownership for {} because its nonce journal and receipt identify different registrations",
                    self.sync_root.display()
                ),
            });
        }
        Ok(RegistrationOwnershipDisposition::Retain)
    }
}

fn valid_v1_blake3_token(value: &str) -> bool {
    value
        .strip_prefix("v1:blake3:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn validate_nonce(nonce: &str) -> Result<(), CloudFilesModuleError> {
    if nonce.len() != 32
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CloudFilesModuleError {
            code: "registration-nonce-invalid".to_string(),
            detail: "Cloud Files registration nonce must be 128 bits encoded as lowercase hex"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writable_request() -> CloudFilesMountRequest {
        let mut request = CloudFilesMountRequest::new(PathBuf::from(r"C:\\Users\\test\\sinomo"));
        request.root_path = "/projects".to_string();
        request.read_only = false;
        request.sync_local_changes = true;
        request.provider_name = "Sinomo SIFS".to_string();
        request.provider_version = "2.0.0".to_string();
        request
    }

    fn journal() -> RegistrationJournal {
        RegistrationJournal::prepared(
            &writable_request(),
            "a318f9ce140b41bc9e44d0f154459584",
            "00112233445566778899aabbccddeeff",
            42,
        )
        .expect("journal")
    }

    #[test]
    fn cursor_recovery_journal_is_canonical_exact_v3_state_owned_evidence() {
        let request = writable_request();
        let journal = RegistrationJournal::cursor_recovery_state_owned(
            &request,
            "a318f9ce140b41bc9e44d0f154459584",
            "00112233445566778899aabbccddeeff",
            42,
            "cursor-recovery-test",
            format!("v1:blake3:{}", "a".repeat(64)),
            0,
        )
        .expect("cursor recovery journal");

        assert_eq!(
            journal.schema_version,
            CURSOR_RECOVERY_REGISTRATION_JOURNAL_SCHEMA_VERSION
        );
        assert_eq!(
            journal.interface_version,
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        );
        assert_eq!(journal.phase, RegistrationPhase::StateOwned);
        assert_eq!(journal.sync_root, request.sync_root);
        assert_eq!(journal.root_path, request.root_path);
        assert_eq!(journal.provider_name, request.provider_name);
        assert_eq!(journal.provider_version, request.provider_version);
        assert_eq!(journal.sync_root_file_id, 42);
        journal
            .validate("a318f9ce140b41bc9e44d0f154459584")
            .expect("exact recovery journal validates");

        let identity = decode_sobj::<RegistrationRootIdentity>(&journal.root_identity)
            .expect("decode recovery identity");
        assert_eq!(identity.schema, REGISTRATION_IDENTITY_SCHEMA);
        assert_eq!(
            identity.interface_version,
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        );
        assert_eq!(identity.registration_nonce, journal.registration_nonce);
        assert_eq!(identity.sync_root_file_id, journal.sync_root_file_id);
        assert_eq!(identity.provider_id, journal.provider_id);
        assert_eq!(
            encode_sobj(&identity).expect("canonical recovery identity"),
            journal.root_identity
        );
        assert!(journal.owns_writable_state());
        assert!(
            journal
                .mount_ownership_evidence_verified(None)
                .expect("StateOwned recovery does not require a Ready receipt")
        );
        assert_eq!(
            journal.recovery_action(false, true),
            RegistrationRecoveryAction::ReuseOwnedState
        );
        assert_eq!(
            journal
                .state_preservation_disposition(None, Some(42))
                .expect("exact recovery ownership is retainable"),
            RegistrationOwnershipDisposition::Retain
        );

        let encoded = encode_sobj(&journal).expect("encode recovery journal");
        let decoded =
            decode_sobj::<RegistrationJournal>(&encoded).expect("decode recovery journal");
        assert_eq!(decoded, journal);
        assert_eq!(
            encode_sobj(&decoded).expect("canonical recovery journal"),
            encoded
        );

        let mut normalized = journal;
        normalized.clear_cursor_recovery();
        assert_eq!(
            normalized.schema_version,
            REGISTRATION_JOURNAL_SCHEMA_VERSION
        );
        assert!(normalized.cursor_recovery.is_none());
        normalized
            .validate("a318f9ce140b41bc9e44d0f154459584")
            .expect("ordinary normalized journal validates");
    }

    #[test]
    fn cursor_recovery_journal_rejects_every_nonce_bound_field_mismatch() {
        let journal = RegistrationJournal::cursor_recovery_state_owned(
            &writable_request(),
            "a318f9ce140b41bc9e44d0f154459584",
            "00112233445566778899aabbccddeeff",
            42,
            "cursor-recovery-test",
            format!("v1:blake3:{}", "a".repeat(64)),
            0,
        )
        .expect("cursor recovery journal");

        let mut mismatches = Vec::new();
        let mut changed = journal.clone();
        changed.sync_root = PathBuf::from(r"C:\Users\test\other");
        mismatches.push(changed);
        let mut changed = journal.clone();
        changed.sync_root_file_id += 1;
        mismatches.push(changed);
        let mut changed = journal.clone();
        changed.root_path = "/other".to_string();
        mismatches.push(changed);
        let mut changed = journal.clone();
        changed.provider_name = "Other Provider".to_string();
        mismatches.push(changed);
        let mut changed = journal.clone();
        changed.provider_id = "ffeeddccbbaa99887766554433221100".to_string();
        mismatches.push(changed);
        let mut changed = journal.clone();
        changed.registration_nonce = "ffeeddccbbaa99887766554433221100".to_string();
        mismatches.push(changed);

        for changed in mismatches {
            assert!(
                changed.validate(&changed.provider_id).is_err(),
                "changing a nonce-bound field must not validate: {changed:?}"
            );
        }
        assert_eq!(
            journal
                .validate("ffeeddccbbaa99887766554433221100")
                .expect_err("a different expected provider ID must fail")
                .code,
            "registration-journal-version-mismatch"
        );
        assert_eq!(
            RegistrationJournal::cursor_recovery_state_owned(
                &writable_request(),
                "a318f9ce140b41bc9e44d0f154459584",
                "00112233445566778899AABBCCDDEEFF",
                42,
                "cursor-recovery-test",
                format!("v1:blake3:{}", "a".repeat(64)),
                0,
            )
            .expect_err("uppercase nonce is not canonical")
            .code,
            "registration-nonce-invalid"
        );
    }

    #[test]
    fn nonce_bound_identity_covers_provider_mount_and_sifs_root() {
        let baseline = journal();
        baseline
            .validate("a318f9ce140b41bc9e44d0f154459584")
            .expect("valid journal");

        let mut changed = writable_request();
        changed.root_path = "/other".to_string();
        let other_root = RegistrationJournal::prepared(
            &changed,
            baseline.provider_id.clone(),
            baseline.registration_nonce.clone(),
            baseline.sync_root_file_id,
        )
        .expect("other root");
        assert_ne!(baseline.root_identity, other_root.root_identity);

        changed = writable_request();
        changed.provider_name = "Other Provider".to_string();
        let other_provider = RegistrationJournal::prepared(
            &changed,
            baseline.provider_id.clone(),
            baseline.registration_nonce.clone(),
            baseline.sync_root_file_id,
        )
        .expect("other provider");
        assert_ne!(baseline.root_identity, other_provider.root_identity);

        changed = writable_request();
        changed.sync_root = PathBuf::from(r"C:\\Users\\test\\other");
        let other_mount = RegistrationJournal::prepared(
            &changed,
            baseline.provider_id.clone(),
            baseline.registration_nonce.clone(),
            baseline.sync_root_file_id,
        )
        .expect("other mount");
        assert_ne!(baseline.root_identity, other_mount.root_identity);

        let other_nonce = RegistrationJournal::prepared(
            &writable_request(),
            baseline.provider_id.clone(),
            "ffeeddccbbaa99887766554433221100",
            baseline.sync_root_file_id,
        )
        .expect("other nonce");
        assert_ne!(baseline.root_identity, other_nonce.root_identity);

        let other_file_id = RegistrationJournal::prepared(
            &writable_request(),
            baseline.provider_id.clone(),
            baseline.registration_nonce.clone(),
            43,
        )
        .expect("other file ID");
        assert_ne!(baseline.root_identity, other_file_id.root_identity);
    }

    #[test]
    fn journal_validation_rejects_identity_field_tampering() {
        let mut journal = journal();
        journal.root_path = "/tampered".to_string();

        let error = journal
            .validate("a318f9ce140b41bc9e44d0f154459584")
            .expect_err("tampering must fail closed");

        assert_eq!(error.code, "registration-journal-identity-mismatch");
    }

    #[test]
    fn v3_lifecycle_reuses_registered_or_later_ownership_with_actual_presence() {
        let mut journal = journal();
        journal.phase = RegistrationPhase::Prepared;
        assert!(!journal.owns_writable_state());
        assert_eq!(
            journal.recovery_action(true, true),
            RegistrationRecoveryAction::RollBackRegistration
        );
        for phase in [
            RegistrationPhase::Registered,
            RegistrationPhase::StateOwned,
            RegistrationPhase::Ready,
        ] {
            journal.phase = phase;
            assert!(journal.owns_writable_state());
            assert_eq!(
                journal.recovery_action(true, false),
                RegistrationRecoveryAction::RollBackRegistration
            );
            assert_eq!(
                journal.recovery_action(true, true),
                RegistrationRecoveryAction::ReuseOwnedRegistration
            );
            assert_eq!(
                journal.recovery_action(false, true),
                RegistrationRecoveryAction::ReuseOwnedState
            );
            assert_eq!(
                journal.recovery_action(false, false),
                RegistrationRecoveryAction::ClearArtifacts
            );
        }
    }

    #[test]
    fn legacy_v2_prepared_and_registered_are_conservatively_owned() {
        let mut journal = journal();
        journal.interface_version = SINOMO_DFS_CLOUD_FILES_INTERFACE_V2.to_string();
        let mut identity = decode_sobj::<RegistrationRootIdentity>(&journal.root_identity)
            .expect("decode identity");
        identity.interface_version = SINOMO_DFS_CLOUD_FILES_INTERFACE_V2.to_string();
        journal.root_identity = encode_sobj(&identity).expect("encode legacy identity");
        journal
            .validate("a318f9ce140b41bc9e44d0f154459584")
            .expect("legacy v2 journal remains recoverable");
        for phase in [
            RegistrationPhase::Prepared,
            RegistrationPhase::Registered,
            RegistrationPhase::StateOwned,
        ] {
            journal.phase = phase;
            assert!(journal.owns_writable_state(), "{phase:?}");
            let verified = journal
                .mount_ownership_evidence_verified(None)
                .expect("receiptless legacy ownership remains recoverable");
            assert!(verified, "{phase:?}");
            assert_eq!(
                journal.recovery_action(false, verified),
                RegistrationRecoveryAction::ReuseOwnedState
            );
        }
        journal.phase = RegistrationPhase::Ready;
        assert!(journal.owns_writable_state());
        assert_eq!(
            journal
                .mount_ownership_evidence_verified(None)
                .expect_err("Ready still requires its durable readiness receipt")
                .code,
            "registration-receipt-missing"
        );
        let receipt = CloudFilesRegistrationReceipt {
            interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_V2.to_string(),
            registration_identity_interface_version: None,
            sync_root: journal.sync_root.clone(),
            root_path: journal.root_path.clone(),
            provider_name: journal.provider_name.clone(),
        };
        assert!(
            journal
                .mount_ownership_evidence_verified(Some(&receipt))
                .expect("matching legacy Ready receipt remains recoverable")
        );
    }

    #[test]
    fn preserve_normalizes_v3_prepared_and_retains_exact_owned_state() {
        let mut journal = journal();
        let receipt = CloudFilesRegistrationReceipt {
            interface_version: journal.interface_version.clone(),
            registration_identity_interface_version: None,
            sync_root: journal.sync_root.clone(),
            root_path: journal.root_path.clone(),
            provider_name: journal.provider_name.clone(),
        };
        journal.phase = RegistrationPhase::Prepared;
        assert_eq!(
            journal
                .state_preservation_disposition(None, Some(journal.sync_root_file_id))
                .expect("preserve intent makes v3 Prepared replayably retained"),
            RegistrationOwnershipDisposition::Retain
        );

        for phase in [RegistrationPhase::Registered, RegistrationPhase::StateOwned] {
            journal.phase = phase;
            assert_eq!(
                journal
                    .state_preservation_disposition(None, Some(journal.sync_root_file_id))
                    .expect("owned pre-Ready state does not require a readiness receipt"),
                RegistrationOwnershipDisposition::Retain
            );
        }

        journal.phase = RegistrationPhase::Ready;
        assert_eq!(
            journal
                .state_preservation_disposition(Some(&receipt), Some(journal.sync_root_file_id),)
                .expect("exact Ready ownership"),
            RegistrationOwnershipDisposition::Retain
        );
    }

    #[test]
    fn conditional_preserve_fails_closed_for_missing_mismatched_or_replaced_ready_evidence() {
        let mut journal = journal();
        journal.phase = RegistrationPhase::Ready;
        let receipt = CloudFilesRegistrationReceipt {
            interface_version: journal.interface_version.clone(),
            registration_identity_interface_version: None,
            sync_root: journal.sync_root.clone(),
            root_path: journal.root_path.clone(),
            provider_name: journal.provider_name.clone(),
        };

        assert_eq!(
            journal
                .state_preservation_disposition(None, Some(journal.sync_root_file_id))
                .expect_err("missing receipt"),
            CloudFilesModuleError {
                code: "registration-receipt-missing".to_string(),
                detail: format!(
                    "refusing to retain Ready Cloud Files state ownership for {} without its durable readiness receipt",
                    journal.sync_root.display()
                ),
            }
        );
        let mut mismatched = receipt.clone();
        mismatched.root_path = "/other".to_string();
        assert_eq!(
            journal
                .state_preservation_disposition(Some(&mismatched), Some(journal.sync_root_file_id),)
                .expect_err("mismatched receipt")
                .code,
            "registration-artifact-mismatch"
        );
        assert_eq!(
            journal
                .state_preservation_disposition(
                    Some(&receipt),
                    Some(journal.sync_root_file_id + 1),
                )
                .expect_err("replaced root")
                .code,
            "sync-root-file-id-changed"
        );
    }

    #[test]
    fn ready_receipt_must_match_exact_provider_mount_and_root() {
        let journal = journal();
        let mut receipt = CloudFilesRegistrationReceipt {
            interface_version: journal.interface_version.clone(),
            registration_identity_interface_version: None,
            sync_root: journal.sync_root.clone(),
            root_path: journal.root_path.clone(),
            provider_name: journal.provider_name.clone(),
        };
        assert!(journal.matches_receipt(&receipt));

        receipt.root_path = "/other".to_string();
        assert!(!journal.matches_receipt(&receipt));
    }

    #[test]
    fn upgraded_v3_receipt_keeps_the_exact_v2_registration_identity() {
        let mut journal = journal();
        journal.interface_version = SINOMO_DFS_CLOUD_FILES_INTERFACE_V2.to_string();
        let mut identity = decode_sobj::<RegistrationRootIdentity>(&journal.root_identity)
            .expect("decode identity");
        identity.interface_version = SINOMO_DFS_CLOUD_FILES_INTERFACE_V2.to_string();
        journal.root_identity = encode_sobj(&identity).expect("encode legacy identity");
        journal.phase = RegistrationPhase::Ready;
        let receipt = CloudFilesRegistrationReceipt {
            interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
            registration_identity_interface_version: Some(
                SINOMO_DFS_CLOUD_FILES_INTERFACE_V2.to_string(),
            ),
            sync_root: journal.sync_root.clone(),
            root_path: journal.root_path.clone(),
            provider_name: journal.provider_name.clone(),
        };

        assert!(journal.matches_receipt(&receipt));
        assert!(
            journal
                .mount_ownership_evidence_verified(Some(&receipt))
                .expect("v3 cleanup receipt binds the unchanged v2 identity")
        );
    }

    #[test]
    fn provider_version_update_preserves_nonce_bound_root_identity() {
        let baseline = journal();
        let mut updated_request = writable_request();
        updated_request.provider_version = "2.1.0".to_string();
        let updated = RegistrationJournal::prepared(
            &updated_request,
            baseline.provider_id.clone(),
            baseline.registration_nonce.clone(),
            baseline.sync_root_file_id,
        )
        .expect("updated provider journal");

        assert_eq!(baseline.root_identity, updated.root_identity);
        assert!(baseline.matches_mount_request(&updated_request));
    }
}
