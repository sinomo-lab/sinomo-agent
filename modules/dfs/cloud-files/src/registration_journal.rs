#![cfg_attr(not(windows), allow(dead_code))]

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sinomo_dfs_cloud_files_api::{
    CloudFilesModuleError, CloudFilesMountRequest, CloudFilesRegistrationReceipt,
    CloudFilesUnregisterRequest, SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
};
use sinomo_module_api::{decode_sobj, encode_sobj};

pub(crate) const REGISTRATION_IDENTITY_MAX_BYTES: usize = 64 * 1024;
const REGISTRATION_JOURNAL_SCHEMA_VERSION: u32 = 1;
const REGISTRATION_IDENTITY_SCHEMA: &str = "sinomo.dfs.cloud-files.registration.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RegistrationPhase {
    Prepared,
    Registered,
    Ready,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationRecoveryAction {
    ClearArtifacts,
    RollBackRegistration,
    ReuseReadyRegistration,
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
        })
    }

    pub(crate) fn validate(&self, expected_provider_id: &str) -> Result<(), CloudFilesModuleError> {
        if self.schema_version != REGISTRATION_JOURNAL_SCHEMA_VERSION
            || self.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
            || self.provider_id != expected_provider_id
        {
            return Err(CloudFilesModuleError {
                code: "registration-journal-version-mismatch".to_string(),
                detail: "Cloud Files registration journal schema, interface, or provider ID is unsupported"
                    .to_string(),
            });
        }
        validate_nonce(&self.registration_nonce)?;
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
        receipt.interface_version == self.interface_version
            && receipt.sync_root == self.sync_root
            && receipt.root_path == self.root_path
            && receipt.provider_name == self.provider_name
    }

    pub(crate) fn recovery_action(
        &self,
        registration_present: bool,
        receipt_verified: bool,
    ) -> RegistrationRecoveryAction {
        if !registration_present {
            RegistrationRecoveryAction::ClearArtifacts
        } else if self.phase == RegistrationPhase::Ready && receipt_verified {
            RegistrationRecoveryAction::ReuseReadyRegistration
        } else {
            RegistrationRecoveryAction::RollBackRegistration
        }
    }
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
    fn lifecycle_reuses_only_receipted_ready_registration() {
        let mut journal = journal();
        for phase in [RegistrationPhase::Prepared, RegistrationPhase::Registered] {
            journal.phase = phase;
            assert_eq!(
                journal.recovery_action(true, true),
                RegistrationRecoveryAction::RollBackRegistration
            );
        }
        journal.phase = RegistrationPhase::Ready;
        assert_eq!(
            journal.recovery_action(true, false),
            RegistrationRecoveryAction::RollBackRegistration
        );
        assert_eq!(
            journal.recovery_action(true, true),
            RegistrationRecoveryAction::ReuseReadyRegistration
        );
        assert_eq!(
            journal.recovery_action(false, true),
            RegistrationRecoveryAction::ClearArtifacts
        );
    }

    #[test]
    fn ready_receipt_must_match_exact_provider_mount_and_root() {
        let journal = journal();
        let mut receipt = CloudFilesRegistrationReceipt {
            interface_version: journal.interface_version.clone(),
            sync_root: journal.sync_root.clone(),
            root_path: journal.root_path.clone(),
            provider_name: journal.provider_name.clone(),
        };
        assert!(journal.matches_receipt(&receipt));

        receipt.root_path = "/other".to_string();
        assert!(!journal.matches_receipt(&receipt));
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
