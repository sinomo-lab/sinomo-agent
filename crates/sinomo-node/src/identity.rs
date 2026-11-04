use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const NODE_IDENTITY_OBJECT_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIdentityPaths {
    pub main: PathBuf,
    pub shadow: PathBuf,
}

impl NodeIdentityPaths {
    pub fn new(data_dir: &Path) -> Self {
        let root = data_dir.join("identity");
        Self {
            main: root.join("main-ed25519.sobj"),
            shadow: root.join("shadow-ed25519.sobj"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub public_key: String,
    private_key_pkcs8: String,
    created_at: String,
}

impl NodeIdentity {
    pub fn key_pair(&self) -> Result<Ed25519KeyPair, NodeIdentityError> {
        let pkcs8 = URL_SAFE_NO_PAD
            .decode(self.private_key_pkcs8.as_bytes())
            .map_err(|source| NodeIdentityError::Parse {
                detail: format!("invalid node identity private key encoding: {source}"),
            })?;
        Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|source| NodeIdentityError::Parse {
            detail: format!("invalid node identity private key: {source}"),
        })
    }
}

pub fn load_or_create_main_node_identity(
    data_dir: &Path,
) -> Result<NodeIdentity, NodeIdentityError> {
    load_or_create_node_identity(&NodeIdentityPaths::new(data_dir).main)
}

pub fn load_or_create_shadow_node_identity(
    data_dir: &Path,
) -> Result<NodeIdentity, NodeIdentityError> {
    load_or_create_node_identity(&NodeIdentityPaths::new(data_dir).shadow)
}

pub fn load_or_create_node_identity(path: &Path) -> Result<NodeIdentity, NodeIdentityError> {
    if path.exists() {
        return read_object(path);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| NodeIdentityError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let rng = SystemRandom::new();
    let pkcs8 =
        Ed25519KeyPair::generate_pkcs8(&rng).map_err(|source| NodeIdentityError::Parse {
            detail: format!("failed to generate node identity key: {source}"),
        })?;
    let key_pair =
        Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).map_err(|source| NodeIdentityError::Parse {
            detail: format!("failed to load generated node identity key: {source}"),
        })?;
    let identity = NodeIdentity {
        public_key: URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
        private_key_pkcs8: URL_SAFE_NO_PAD.encode(pkcs8.as_ref()),
        created_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string()),
    };
    write_object(path, &identity)?;
    Ok(identity)
}

fn read_object<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, NodeIdentityError> {
    let bytes = fs::read(path).map_err(|source| NodeIdentityError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_ext::from_bytes(&bytes).map_err(|source| NodeIdentityError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_object<T: Serialize>(path: &Path, value: &T) -> Result<(), NodeIdentityError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| NodeIdentityError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_ext::to_bytes(value, NODE_IDENTITY_OBJECT_VERSION, LengthMode::U32)
        .map_err(|source| NodeIdentityError::Object {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    options
        .open(path)
        .and_then(|mut file| std::io::Write::write_all(&mut file, &content))
        .map_err(|source| NodeIdentityError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum NodeIdentityError {
    #[error("node identity parse error: {detail}")]
    Parse { detail: String },
    #[error("node identity I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("node identity JSON error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("node identity Sinomo object error at {path}: {detail}")]
    Object { path: String, detail: String },
}
