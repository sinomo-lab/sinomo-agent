//! Durable local content storage for DFS blobs.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

const HASH_PREFIX: &str = "blake3:";
const BLAKE3_HEX_LEN: usize = 64;

#[derive(Debug, Error)]
pub enum ContentStoreError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("manifest serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid content hash: {0}")]
    InvalidHash(String),
    #[error("staged content not found for intent: {0}")]
    MissingStaged(String),
}

pub type Result<T> = std::result::Result<T, ContentStoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifest {
    pub object_id: String,
    pub version: String,
    pub content_hash: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct LocalContentStore {
    root: PathBuf,
}

impl LocalContentStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        create_dir_all(root.join("blobs"))?;
        create_dir_all(root.join("objects"))?;
        create_dir_all(root.join("staging"))?;
        Ok(Self { root })
    }

    pub fn stage_bytes(&self, intent_id: &str, bytes: &[u8]) -> Result<String> {
        let hash = content_hash(bytes);
        let staging_path = self.staging_path(intent_id);
        write_file_atomic(&staging_path, bytes)?;
        Ok(hash)
    }

    pub fn commit_staged(
        &self,
        intent_id: &str,
        object_id: &str,
        version: &str,
    ) -> Result<ContentManifest> {
        let staging_path = self.staging_path(intent_id);
        if !staging_path.exists() {
            return Err(ContentStoreError::MissingStaged(intent_id.to_owned()));
        }

        let bytes = read_file(&staging_path)?;
        let manifest = self.write_object_version(object_id, version, &bytes)?;
        remove_file(&staging_path)?;
        Ok(manifest)
    }

    pub fn put_object_version(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        self.write_object_version(object_id, version, bytes)
    }

    pub fn read_blob(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.blob_path(hash)?;
        read_file(&path)
    }

    pub fn read_manifest(&self, object_id: &str, version: &str) -> Result<ContentManifest> {
        let path = self.manifest_path(object_id, version);
        let bytes = read_file(&path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn manifests_for_object(&self, object_id: &str) -> Result<Vec<ContentManifest>> {
        let object_root = self.root.join("objects").join(object_id);
        if !object_root.exists() {
            return Ok(Vec::new());
        }

        let mut manifests: Vec<ContentManifest> = Vec::new();
        for entry in read_dir(&object_root)? {
            let path = entry.path().join("manifest.json");
            if path.is_file() {
                manifests.push(serde_json::from_slice(&read_file(&path)?)?);
            }
        }
        manifests.sort_by(|left, right| left.version.cmp(&right.version));
        Ok(manifests)
    }

    pub fn has_blob(&self, hash: &str) -> Result<bool> {
        let path = self.blob_path(hash)?;
        Ok(path.is_file())
    }

    fn write_object_version(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        let hash = content_hash(bytes);
        let blob_path = self.blob_path(&hash)?;
        if !blob_path.exists() {
            write_file_atomic(&blob_path, bytes)?;
        }

        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: hash,
            size_bytes: bytes.len() as u64,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        write_file_atomic(&self.manifest_path(object_id, version), &manifest_bytes)?;
        Ok(manifest)
    }

    fn blob_path(&self, hash: &str) -> Result<PathBuf> {
        let hex = hash_hex(hash)?;
        Ok(self.root.join("blobs").join(&hex[..2]).join(hex))
    }

    fn manifest_path(&self, object_id: &str, version: &str) -> PathBuf {
        self.root
            .join("objects")
            .join(object_id)
            .join(version)
            .join("manifest.json")
    }

    fn staging_path(&self, intent_id: &str) -> PathBuf {
        self.root.join("staging").join(intent_id)
    }
}

fn content_hash(bytes: &[u8]) -> String {
    format!("{HASH_PREFIX}{}", blake3::hash(bytes).to_hex())
}

fn hash_hex(hash: &str) -> Result<&str> {
    let Some(hex) = hash.strip_prefix(HASH_PREFIX) else {
        return Err(ContentStoreError::InvalidHash(hash.to_owned()));
    };
    if hex.len() != BLAKE3_HEX_LEN || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ContentStoreError::InvalidHash(hash.to_owned()));
    }
    if hex.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(ContentStoreError::InvalidHash(hash.to_owned()));
    }
    Ok(hex)
}

fn create_dir_all(path: PathBuf) -> Result<()> {
    fs::create_dir_all(&path).map_err(|source| ContentStoreError::Io { path, source })
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }

    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, bytes).map_err(|source| ContentStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    fs::rename(&tmp_path, path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn put_object_version_writes_blob_and_manifest() {
        let root = temp_root("put_object_version_writes_blob_and_manifest");
        let store = LocalContentStore::new(&root).expect("create store");

        let manifest = store
            .put_object_version("object-a", "v1", b"hello dfs")
            .expect("put object version");

        assert_eq!(manifest.object_id, "object-a");
        assert_eq!(manifest.version, "v1");
        assert_eq!(manifest.size_bytes, 9);
        assert!(manifest.content_hash.starts_with(HASH_PREFIX));
        assert_eq!(
            manifest.content_hash.len(),
            HASH_PREFIX.len() + BLAKE3_HEX_LEN
        );
        assert!(store.has_blob(&manifest.content_hash).expect("has blob"));
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            b"hello dfs"
        );
        assert_eq!(
            store
                .read_manifest("object-a", "v1")
                .expect("read manifest"),
            manifest
        );
        assert_eq!(
            store
                .manifests_for_object("object-a")
                .expect("object manifests"),
            vec![manifest]
        );

        cleanup(&root);
    }

    #[test]
    fn stage_and_commit_moves_staged_bytes_into_object_version() {
        let root = temp_root("stage_and_commit_moves_staged_bytes_into_object_version");
        let store = LocalContentStore::new(&root).expect("create store");

        let staged_hash = store
            .stage_bytes("intent-1", b"pending content")
            .expect("stage bytes");
        let staging_path = root.join("staging").join("intent-1");
        assert!(staging_path.is_file());

        let manifest = store
            .commit_staged("intent-1", "object-b", "v2")
            .expect("commit staged");

        assert_eq!(manifest.content_hash, staged_hash);
        assert!(!staging_path.exists());
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            b"pending content"
        );

        cleanup(&root);
    }

    #[test]
    fn rejects_invalid_hashes() {
        let root = temp_root("rejects_invalid_hashes");
        let store = LocalContentStore::new(&root).expect("create store");

        assert!(matches!(
            store.has_blob("sha256:abc"),
            Err(ContentStoreError::InvalidHash(_))
        ));
        assert!(matches!(
            store.read_blob(
                "blake3:AF1349B9F5F9A1A6A0404DEA36D2518CFBBAF2E24C9EAEF1F5D3B3F9B8A3E9F0"
            ),
            Err(ContentStoreError::InvalidHash(_))
        ));

        cleanup(&root);
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-dfs-{name}-{nanos}"))
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
