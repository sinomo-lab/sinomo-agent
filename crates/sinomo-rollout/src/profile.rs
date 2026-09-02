use std::fs;
use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeIdentity;
use thiserror::Error;

pub const MODULE_DISTRIBUTION_PROFILE_RELATIVE_PATH: &str = "distribution/module-profile.json";
pub const MODULE_DISTRIBUTION_PROFILE_SCHEMA_VERSION: u16 = 1;
pub const MODULE_DISTRIBUTION_PROFILE_SIGNATURE_SCHEME: &str = "local-ed25519";
const MODULE_DISTRIBUTION_PROFILE_MAX_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleDistributionProfile {
    pub schema_version: u16,
    pub profile_id: String,
    pub profile_version: String,
    pub modules: Vec<ModuleDistributionEntry>,
    pub signer_public_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl ModuleDistributionProfile {
    pub fn new(profile_id: impl Into<String>, profile_version: impl Into<String>) -> Self {
        Self {
            schema_version: MODULE_DISTRIBUTION_PROFILE_SCHEMA_VERSION,
            profile_id: profile_id.into(),
            profile_version: profile_version.into(),
            modules: Vec::new(),
            signer_public_key: String::new(),
            signature: None,
        }
    }

    pub fn with_module(mut self, module: ModuleDistributionEntry) -> Self {
        self.modules.push(module);
        self
    }

    pub fn validate(&self) -> Result<(), ModuleDistributionProfileError> {
        if self.schema_version != MODULE_DISTRIBUTION_PROFILE_SCHEMA_VERSION {
            return Err(ModuleDistributionProfileError::Invalid(format!(
                "unsupported schema version {}",
                self.schema_version
            )));
        }
        for (value, label) in [
            (&self.profile_id, "profile id"),
            (&self.profile_version, "profile version"),
            (&self.signer_public_key, "signer public key"),
        ] {
            if value.trim().is_empty() || value != value.trim() {
                return Err(ModuleDistributionProfileError::Invalid(format!(
                    "{label} must be non-empty and normalized"
                )));
            }
        }
        let mut ids = std::collections::BTreeSet::new();
        for module in &self.modules {
            module.validate()?;
            if !ids.insert(module.module_id.as_str()) {
                return Err(ModuleDistributionProfileError::Invalid(format!(
                    "duplicate module id {}",
                    module.module_id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleDistributionEntry {
    pub module_id: String,
    pub interface_version: String,
    pub package: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
    pub cache_key: String,
    /// Platform-neutral library stem. Rolling profiles that already contain a
    /// complete `.so`, `.dylib`, or `.dll` name remain accepted.
    pub library_name: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub platforms: Vec<String>,
    #[serde(default)]
    pub default_enabled: bool,
}

impl ModuleDistributionEntry {
    pub fn validate(&self) -> Result<(), ModuleDistributionProfileError> {
        for (value, label) in [
            (&self.module_id, "module id"),
            (&self.interface_version, "interface version"),
            (&self.package, "package"),
            (&self.cache_key, "cache key"),
            (&self.library_name, "library name"),
            (&self.source, "source"),
        ] {
            if value.trim().is_empty() || value != value.trim() {
                return Err(ModuleDistributionProfileError::Invalid(format!(
                    "{label} must be non-empty and normalized"
                )));
            }
        }
        Ok(())
    }

    pub fn applies_to_current_platform(&self) -> bool {
        self.platforms.is_empty()
            || self.platforms.iter().any(|platform| {
                platform == std::env::consts::OS
                    || (platform == "unix" && cfg!(unix))
                    || (platform == "windows" && cfg!(windows))
            })
    }
}

pub fn sign_module_distribution_profile(
    profile: &mut ModuleDistributionProfile,
    identity: &NodeIdentity,
) -> Result<(), ModuleDistributionProfileError> {
    profile.signer_public_key = identity.public_key.clone();
    profile.signature = None;
    let payload = module_distribution_profile_payload(profile)?;
    let key_pair = identity
        .key_pair()
        .map_err(|error| ModuleDistributionProfileError::Signature(error.to_string()))?;
    profile.signature = Some(format!(
        "{}:{}",
        MODULE_DISTRIBUTION_PROFILE_SIGNATURE_SCHEME,
        URL_SAFE_NO_PAD.encode(key_pair.sign(&payload).as_ref())
    ));
    Ok(())
}

pub fn verify_module_distribution_profile(
    profile: &ModuleDistributionProfile,
) -> Result<(), ModuleDistributionProfileError> {
    profile.validate()?;
    let signature = profile
        .signature
        .as_deref()
        .ok_or_else(|| ModuleDistributionProfileError::Signature("missing signature".into()))?;
    let encoded = signature
        .strip_prefix(&format!(
            "{}:",
            MODULE_DISTRIBUTION_PROFILE_SIGNATURE_SCHEME
        ))
        .ok_or_else(|| {
            ModuleDistributionProfileError::Signature("unsupported signature scheme".into())
        })?;
    let public_key = URL_SAFE_NO_PAD
        .decode(profile.signer_public_key.as_bytes())
        .map_err(|_| ModuleDistributionProfileError::Signature("invalid public key".into()))?;
    let signature = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|_| ModuleDistributionProfileError::Signature("invalid signature bytes".into()))?;
    let payload = module_distribution_profile_payload(profile)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(&payload, &signature)
        .map_err(|_| ModuleDistributionProfileError::Signature("signature mismatch".into()))
}

pub fn write_module_distribution_profile(
    root: &Path,
    profile: &ModuleDistributionProfile,
) -> Result<PathBuf, ModuleDistributionProfileError> {
    verify_module_distribution_profile(profile)?;
    let path = root.join(MODULE_DISTRIBUTION_PROFILE_RELATIVE_PATH);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| ModuleDistributionProfileError::Io {
            path: parent.to_path_buf(),
            detail: error.to_string(),
        })?;
    }
    let bytes = serde_json::to_vec_pretty(profile)
        .map_err(|error| ModuleDistributionProfileError::Object(error.to_string()))?;
    fs::write(&path, bytes).map_err(|error| ModuleDistributionProfileError::Io {
        path: path.clone(),
        detail: error.to_string(),
    })?;
    Ok(path)
}

pub fn read_module_distribution_profile(
    root: &Path,
) -> Result<Option<ModuleDistributionProfile>, ModuleDistributionProfileError> {
    let path = root.join(MODULE_DISTRIBUTION_PROFILE_RELATIVE_PATH);
    if !path.exists() {
        return Ok(None);
    }
    let metadata = fs::metadata(&path).map_err(|error| ModuleDistributionProfileError::Io {
        path: path.clone(),
        detail: error.to_string(),
    })?;
    if metadata.len() > MODULE_DISTRIBUTION_PROFILE_MAX_BYTES {
        return Err(ModuleDistributionProfileError::Invalid(format!(
            "profile is {} bytes; maximum is {MODULE_DISTRIBUTION_PROFILE_MAX_BYTES}",
            metadata.len()
        )));
    }
    let bytes = fs::read(&path).map_err(|error| ModuleDistributionProfileError::Io {
        path: path.clone(),
        detail: error.to_string(),
    })?;
    let profile = serde_json::from_slice(&bytes)
        .map_err(|error| ModuleDistributionProfileError::Object(error.to_string()))?;
    verify_module_distribution_profile(&profile)?;
    Ok(Some(profile))
}

fn module_distribution_profile_payload(
    profile: &ModuleDistributionProfile,
) -> Result<Vec<u8>, ModuleDistributionProfileError> {
    let mut unsigned = profile.clone();
    unsigned.signature = None;
    serde_json::to_vec(&unsigned)
        .map_err(|error| ModuleDistributionProfileError::Object(error.to_string()))
}

#[derive(Debug, Error)]
pub enum ModuleDistributionProfileError {
    #[error("invalid module distribution profile: {0}")]
    Invalid(String),
    #[error("module distribution profile signature: {0}")]
    Signature(String),
    #[error("module distribution profile object: {0}")]
    Object(String),
    #[error("module distribution profile I/O at {path}: {detail}")]
    Io { path: PathBuf, detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_profile_roundtrips_and_rejects_tampering() {
        let root = sinomo_testdir::TestDir::new("module-distribution-profile");
        let identity =
            sinomo_core::node::load_or_create_main_node_identity(&root).expect("identity");
        let mut profile =
            ModuleDistributionProfile::new("default", "1").with_module(ModuleDistributionEntry {
                module_id: "module.external".to_string(),
                interface_version: "1.0.0".to_string(),
                package: "external-module".to_string(),
                features: Vec::new(),
                cache_key: "module/external".to_string(),
                library_name: "external_module".to_string(),
                source: "profile:default".to_string(),
                platforms: Vec::new(),
                default_enabled: true,
            });
        sign_module_distribution_profile(&mut profile, &identity).expect("sign");
        write_module_distribution_profile(&root, &profile).expect("write");
        assert_eq!(
            read_module_distribution_profile(&root).expect("read"),
            Some(profile.clone())
        );
        profile.modules[0].package.push_str("-tampered");
        assert!(verify_module_distribution_profile(&profile).is_err());
    }

    #[test]
    fn repository_distribution_profile_is_present_and_verified() {
        let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root");
        let profile = read_module_distribution_profile(repository_root)
            .expect("read repository distribution profile")
            .expect("repository distribution profile");

        assert_eq!(profile.profile_id, "sinomo-default");
        assert!(profile.modules.iter().any(|module| module.default_enabled));
        assert!(profile.modules.iter().any(|module| !module.default_enabled));
        assert!(profile.modules.iter().any(|module| {
            module.module_id == "module.evolution"
                && module.package == "sinomo-evolution"
                && module.default_enabled
        }));
    }
}
