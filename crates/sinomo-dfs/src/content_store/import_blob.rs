//! Bounded verification and atomic merge of current-format immutable blobs.
//! Original codecs, inventory scheduling and source retirement are not here.
use super::inline_bindings::InlineFileIdentity;
use super::*;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

fn invalid(message: &str) -> ContentStoreError {
    ContentStoreError::InvalidContentLayout {
        message: message.into(),
    }
}

enum Source {
    Staged(durable_file::SyncedPrivateStagedFile),
    Existing(PathBuf),
}
impl Source {
    fn path(&self) -> &Path {
        match self {
            Self::Staged(file) => file.path(),
            Self::Existing(path) => path,
        }
    }
}

/// Retained verification work, subordinate to one caller-owned inventory unit.
/// Recreating it after restart rehashes through bounded steps; no persisted
/// claim or caller-provided digest can skip byte verification.
pub struct ContentImportBlobVerifier {
    source: Source,
    reader: fs::File,
    identity: InlineFileIdentity,
    expected_hash: String,
    expected_size: u64,
    position: u64,
    hasher: blake3::Hasher,
    verified: bool,
}

/// Constructible only by complete byte verification and identity validation.
pub struct ContentStoreVerifiedImportBlob {
    source: Source,
    reader: fs::File,
    identity: InlineFileIdentity,
    hash: String,
    size: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ContentImportBlobPublication {
    Published,
    CurrentDeleted,
    /// Current immutable bytes must be verified without holding a writer lock.
    VerifyCurrent {
        path: PathBuf,
    },
}

impl ContentImportBlobVerifier {
    pub fn new(
        file: durable_file::SyncedPrivateStagedFile,
        expected_hash: String,
        expected_size: u64,
    ) -> Result<Self> {
        durable_file::read_synced_private_staged_file_bounded(&file, 0)
            .map_err(content_durable_file_error)?;
        Self::open(Source::Staged(file), expected_hash, expected_size)
    }

    /// Retain a read-only existing inode. This never syncs, rewrites, or confers
    /// private-stage authority on current content.
    pub fn existing(path: &Path, expected_hash: String, expected_size: u64) -> Result<Self> {
        Self::open(
            Source::Existing(path.to_owned()),
            expected_hash,
            expected_size,
        )
    }

    fn open(source: Source, expected_hash: String, expected_size: u64) -> Result<Self> {
        hash_hex(&expected_hash)?;
        let reader =
            fs::File::open(source.path()).map_err(|source_error| ContentStoreError::Io {
                path: source.path().to_owned(),
                source: source_error,
            })?;
        let metadata = reader
            .metadata()
            .map_err(|source_error| ContentStoreError::Io {
                path: source.path().to_owned(),
                source: source_error,
            })?;
        let identity = InlineFileIdentity::from_metadata(&metadata)?;
        if metadata.len() != expected_size || InlineFileIdentity::read(source.path())? != identity {
            return Err(invalid(
                "import blob identity or length changed while opening",
            ));
        }
        Ok(Self {
            source,
            reader,
            identity,
            expected_hash,
            expected_size,
            position: 0,
            hasher: blake3::Hasher::new(),
            verified: false,
        })
    }

    pub fn verified_bytes(&self) -> u64 {
        self.position
    }

    /// Hash at most max_bytes; cancellation belongs to the owning inventory,
    /// never an individual frontend request. A depleted time budget yields.
    pub fn step(
        &mut self,
        max_bytes: u64,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<bool> {
        if max_bytes == 0 {
            return Err(invalid(
                "import verification requires a nonzero byte budget",
            ));
        }
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Ok(false);
        }
        self.validate_identity()?;
        let mut remaining = max_bytes;
        let mut bytes = [0; 64 * 1024];
        while self.position < self.expected_size && remaining != 0 {
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                return Ok(false);
            }
            let count = (self.expected_size - self.position)
                .min(remaining)
                .min(bytes.len() as u64) as usize;
            self.reader
                .read_exact(&mut bytes[..count])
                .map_err(|source| ContentStoreError::Io {
                    path: self.source.path().to_owned(),
                    source,
                })?;
            self.hasher.update(&bytes[..count]);
            self.position += count as u64;
            remaining -= count as u64;
        }
        self.validate_identity()?;
        if self.position == self.expected_size {
            let actual = format!("blake3:{}", self.hasher.finalize().to_hex());
            if actual != self.expected_hash {
                return Err(invalid(
                    "import blob bytes do not match their immutable hash",
                ));
            }
            self.verified = true;
        }
        Ok(self.verified)
    }

    fn validate_identity(&self) -> Result<()> {
        validate_identity(&self.source, &self.reader, &self.identity)
    }

    pub fn finish(self) -> Result<ContentStoreVerifiedImportBlob> {
        if !self.verified {
            return Err(invalid("import blob verification is incomplete"));
        }
        self.validate_identity()?;
        Ok(ContentStoreVerifiedImportBlob {
            source: self.source,
            reader: self.reader,
            identity: self.identity,
            hash: self.expected_hash,
            size: self.expected_size,
        })
    }
}

fn validate_identity(
    source: &Source,
    reader: &fs::File,
    expected: &InlineFileIdentity,
) -> Result<()> {
    if let Source::Staged(file) = source {
        durable_file::read_synced_private_staged_file_bounded(file, 0)
            .map_err(content_durable_file_error)?;
    }
    let metadata = reader.metadata().map_err(|error| ContentStoreError::Io {
        path: source.path().to_owned(),
        source: error,
    })?;
    if InlineFileIdentity::from_metadata(&metadata)? != *expected
        || InlineFileIdentity::read(source.path())? != *expected
    {
        return Err(invalid(
            "verified import blob changed its retained file identity",
        ));
    }
    Ok(())
}

#[cfg(feature = "storage-conversion")]
impl ContentStoreStagingWriter {
    pub fn import_verified_blob(
        &self,
        blob: ContentStoreVerifiedImportBlob,
    ) -> Result<ContentImportBlobPublication> {
        let identity = self
            .current_import_identity()
            .ok_or_else(|| invalid("live blob import requires an inventory owner"))?;
        self.publish_verified_blob_inner(blob, Some(identity))
    }

    /// Publish verified bytes into the existing private candidate capability.
    /// This does not reset inventory state, claim coverage, or grant metadata authority.
    pub fn publish_verified_candidate_blob(
        &self,
        blob: ContentStoreVerifiedImportBlob,
    ) -> Result<ContentImportBlobPublication> {
        if self.current_import_identity().is_some() {
            return Err(invalid(
                "a live import handle cannot claim private candidate publication",
            ));
        }
        self.publish_verified_blob_inner(blob, None)
    }

    fn publish_verified_blob_inner(
        &self,
        blob: ContentStoreVerifiedImportBlob,
        inventory: Option<&str>,
    ) -> Result<ContentImportBlobPublication> {
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        if let Some(identity) = inventory {
            crate::content_store::preparation::require_pending_inventory(
                &reservation.sentinel,
                identity,
            )?;
        }
        if self.inline_blob_deleted(&blob.hash)? {
            return Ok(ContentImportBlobPublication::CurrentDeleted);
        }
        validate_identity(&blob.source, &blob.reader, &blob.identity)?;
        if let Some(entry) = self.inline_blob_entry(&blob.hash)? {
            if entry.size_bytes != blob.size || self.open_inline_blob(&blob.hash, 0, 0)?.is_none() {
                return Err(invalid(
                    "current inline blob conflicts with verified import",
                ));
            }
            return Ok(ContentImportBlobPublication::Published);
        }
        let destination = self.blob_path(&blob.hash)?;
        match &blob.source {
            Source::Existing(path) => {
                if path != &destination {
                    return Err(invalid(
                        "existing import proof names another current destination",
                    ));
                }
                // The actual destination inode was verified outside this lock,
                // and its same identity has just been revalidated under it.
                Ok(ContentImportBlobPublication::Published)
            }
            Source::Staged(file) => {
                if file.path() == destination.as_path() {
                    return Err(invalid(
                        "current content cannot masquerade as a private import stage",
                    ));
                }
                match fs::symlink_metadata(&destination) {
                    Ok(metadata) => {
                        if !metadata.is_file() {
                            return Err(invalid(
                                "current blob destination is not a regular immutable file",
                            ));
                        }
                        return Ok(ContentImportBlobPublication::VerifyCurrent {
                            path: destination,
                        });
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(ContentStoreError::Io {
                            path: destination,
                            source,
                        });
                    }
                }
                self.ensure_quota_delta(ContentStoreQuotaDelta {
                    blob_bytes: blob.size,
                    ..ContentStoreQuotaDelta::default()
                })?;
                self.publish_prehashed_temporary_blob(file, &destination)?;
                let installed = InlineFileIdentity::read(&destination)?;
                let retained =
                    InlineFileIdentity::from_metadata(&blob.reader.metadata().map_err(
                        |source| ContentStoreError::Io {
                            path: destination.clone(),
                            source,
                        },
                    )?)?;
                if installed != retained {
                    return Err(invalid(
                        "blob publication did not install the verified retained inode",
                    ));
                }
                Ok(ContentImportBlobPublication::Published)
            }
        }
    }
}

#[cfg(all(test, feature = "storage-conversion"))]
mod tests {
    use super::*;
    fn staged(path: &Path, bytes: &[u8]) -> durable_file::SyncedPrivateStagedFile {
        let mut file = durable_file::create_private_staged_file(path).unwrap();
        file.write_all(bytes).unwrap();
        durable_file::sync_private_staged_file(path, file).unwrap()
    }
    fn finish(mut verifier: ContentImportBlobVerifier) -> ContentStoreVerifiedImportBlob {
        let cancelled = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !verifier.step(4096, deadline, &cancelled).unwrap() {
            assert!(Instant::now() < deadline);
        }
        verifier.finish().unwrap()
    }
    #[test]
    fn import_blob_verification_is_bounded_and_restart_rehashes_real_bytes() {
        let dir = sinomo_testdir::TestDir::new("bounded-import-verifier");
        let path = dir.path().join("private");
        let bytes = vec![0x5c; 128 * 1024];
        let hash = content_hash(&bytes);
        let mut verifier =
            ContentImportBlobVerifier::new(staged(&path, &bytes), hash.clone(), bytes.len() as u64)
                .unwrap();
        assert!(
            !verifier
                .step(
                    4096,
                    Instant::now() + Duration::from_secs(5),
                    &AtomicBool::new(false)
                )
                .unwrap()
        );
        assert_eq!(verifier.verified_bytes(), 4096);
        drop(verifier);
        let reopened = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let synced = durable_file::sync_private_staged_file(&path, reopened).unwrap();
        let restarted = ContentImportBlobVerifier::new(synced, hash, bytes.len() as u64).unwrap();
        assert_eq!(
            restarted.verified_bytes(),
            0,
            "a checkpoint cannot assert a hash was verified"
        );
        let verified = finish(restarted);
        assert_eq!(verified.size, bytes.len() as u64);
    }
    #[test]
    fn import_blob_refuses_mutation_and_corrupt_existing_content_without_rewrite() {
        let dir = sinomo_testdir::TestDir::new("import-verifier-corruption");
        let path = dir.path().join("private");
        let bytes = vec![0x23; 64 * 1024];
        let hash = content_hash(&bytes);
        let mut verifier =
            ContentImportBlobVerifier::new(staged(&path, &bytes), hash.clone(), bytes.len() as u64)
                .unwrap();
        assert!(
            !verifier
                .step(
                    4096,
                    Instant::now() + Duration::from_secs(5),
                    &AtomicBool::new(false)
                )
                .unwrap()
        );
        fs::write(&path, vec![0x31; bytes.len()]).unwrap();
        assert!(
            verifier
                .step(
                    4096,
                    Instant::now() + Duration::from_secs(5),
                    &AtomicBool::new(false)
                )
                .is_err()
        );
        let store = LocalContentStore::new(dir.path().join("current")).unwrap();
        store
            .put_object_version("object", "version", &bytes)
            .unwrap();
        let current = store.blob_path(&hash).unwrap();
        fs::write(&current, vec![0x11; bytes.len()]).unwrap();
        let before = fs::read(&current).unwrap();
        let mut existing =
            ContentImportBlobVerifier::existing(&current, hash, bytes.len() as u64).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match existing.step(4096, deadline, &AtomicBool::new(false)) {
                Err(_) => break,
                Ok(false) => assert!(Instant::now() < deadline),
                Ok(true) => panic!("corrupt current bytes cannot mint a verified token"),
            }
        }
        assert_eq!(fs::read(current).unwrap(), before);
    }
    #[test]
    fn import_blob_existing_winner_is_verified_outside_the_publication_lock() {
        let dir = sinomo_testdir::TestDir::new("import-blob-existing");
        let root = dir.path().join("current");
        let store = LocalContentStore::new(&root).unwrap();
        let bytes = vec![0x72; 64 * 1024];
        let hash = content_hash(&bytes);
        store
            .put_object_version("existing", "version", &bytes)
            .unwrap();
        store.begin_inventory_import("inventory").unwrap();
        let writer = ContentStoreStagingWriter::open_import_target(&root, "inventory").unwrap();
        let proof = finish(
            ContentImportBlobVerifier::new(
                staged(&dir.path().join("private"), &bytes),
                hash.clone(),
                bytes.len() as u64,
            )
            .unwrap(),
        );
        let ContentImportBlobPublication::VerifyCurrent { path } =
            writer.import_verified_blob(proof).unwrap()
        else {
            panic!("existing current file requires its own bounded proof")
        };
        let existing = finish(
            ContentImportBlobVerifier::existing(&path, hash.clone(), bytes.len() as u64).unwrap(),
        );
        assert_eq!(
            writer.import_verified_blob(existing).unwrap(),
            ContentImportBlobPublication::Published
        );
        assert_eq!(store.read_blob(&hash).unwrap(), bytes);
    }
    #[test]
    fn private_candidate_verified_blob_keeps_mode_and_current_layout_durability() {
        let dir = sinomo_testdir::TestDir::new("candidate-verified-blob");
        let root = dir.path().join("candidate");
        let writer = ContentStoreStagingWriter::open_candidate(&root).unwrap();
        let bytes = vec![0x46; 64 * 1024];
        let hash = content_hash(&bytes);
        let proof = finish(
            ContentImportBlobVerifier::new(
                staged(&dir.path().join("private"), &bytes),
                hash.clone(),
                bytes.len() as u64,
            )
            .unwrap(),
        );
        assert_eq!(
            writer.publish_verified_candidate_blob(proof).unwrap(),
            ContentImportBlobPublication::Published
        );
        assert_eq!(writer.read_blob(&hash).unwrap(), bytes);
        let incarnation = writer.store_incarnation().unwrap();
        writer.begin_content_root_catalog_scan().unwrap();
        let marker = writer.content_reservation_batch_sentinel_path();
        let original = fs::read(&marker).unwrap();
        writer
            .certify_current_layout_durability(&incarnation)
            .unwrap();
        assert_eq!(
            fs::read(&marker).unwrap(),
            original,
            "certification must not restamp a newer causal epoch"
        );
        assert!(
            writer
                .certify_current_layout_durability("another-incarnation")
                .is_err()
        );
        writer.begin_inventory_import("live-mode").unwrap();
        drop(writer);
        let live = ContentStoreStagingWriter::open_import_target(&root, "live-mode").unwrap();
        let proof = finish(
            ContentImportBlobVerifier::new(
                staged(&dir.path().join("private2"), &bytes),
                hash,
                bytes.len() as u64,
            )
            .unwrap(),
        );
        assert!(live.publish_verified_candidate_blob(proof).is_err());
    }
}
