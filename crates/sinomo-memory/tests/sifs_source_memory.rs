use sinomo_memory::{
    EvidenceCoverage, IndexRootPolicy, IndexedFileMemoryStore, LocalIndexRootConfig,
    MemoryAccessContext, MemoryError, MemoryQuery, MemorySourceConnector, SifsSourceConnector,
};
use sinomo_runtime::core::Id;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::vfs::{
    VfsAppendOptions, VfsBackend, VfsDirEntry, VfsError, VfsFileRead, VfsMetadata,
};
use sinomo_runtime::vfs::{VfsHandle, VfsRemoveOptions, VfsWriteOptions};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct DerivedRoot(PathBuf);
impl DerivedRoot {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("sifs-source-memory-{}", Id::generate())))
    }
}
impl Drop for DerivedRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn query(text: &str) -> MemoryQuery {
    MemoryQuery {
        task_hint: Some(text.into()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    }
}

#[test]
fn canonical_vfs_sources_reindex_and_reopen_without_local_shadow_files() {
    // A generic VFS fixture exercises connector composition; real daemon/SIFS
    // transport and cross-node durability are verified by the Docker consumer lane.
    let derived = DerivedRoot::new();
    let source = VfsHandle::memory();
    source.create_dir_all("/roaming/notes").unwrap();
    source
        .write("/roaming/notes/note.md", b"alpha shared memory\n")
        .unwrap();
    let store =
        IndexedFileMemoryStore::new(&derived.0).with_sifs_vfs(source.clone(), "fixture-namespace");
    let root = store
        .register_sifs_root(LocalIndexRootConfig::new(
            "/roaming/notes",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    assert_eq!(root.connector_id, "sinomo.memory.sifs/v1");
    assert_eq!(store.index_root(&root.root_id).unwrap().published_files, 1);
    let first = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query("alpha"))
        .unwrap();
    assert_eq!(first.evidence.len(), 1);
    assert_eq!(first.evidence[0].content, "alpha shared memory");
    assert_eq!(
        first.evidence[0].anchor.source_version.file_ref.path_hint,
        "note.md"
    );
    let native_object = source
        .stat("/roaming/notes/note.md")
        .unwrap()
        .object_id
        .unwrap();
    assert_eq!(first.files[0].source.platform_identity, native_object);
    assert!(
        !derived.0.join("roaming").exists(),
        "canonical files must never be mirrored into derived local paths"
    );

    let reopened =
        IndexedFileMemoryStore::new(&derived.0).with_sifs_vfs(source.clone(), "fixture-namespace");
    assert_eq!(
        reopened
            .retrieve(&MemoryAccessContext::cluster_local(), &query("alpha"))
            .unwrap()
            .evidence,
        first.evidence
    );
    source
        .write("/roaming/notes/note.md", b"beta new shared memory\n")
        .unwrap();
    let stale = reopened
        .retrieve(&MemoryAccessContext::cluster_local(), &query("alpha"))
        .unwrap();
    assert!(stale.evidence.is_empty());
    assert!(matches!(
        stale.coverage,
        EvidenceCoverage::Approximate { .. }
    ));
    assert_eq!(
        reopened.index_root(&root.root_id).unwrap().published_files,
        1
    );
    let current = reopened
        .retrieve(&MemoryAccessContext::cluster_local(), &query("beta"))
        .unwrap();
    assert_eq!(current.evidence[0].content, "beta new shared memory");
    assert_ne!(
        current.evidence[0].anchor.source_version.version_id,
        first.evidence[0].anchor.source_version.version_id
    );

    source.remove("/roaming/notes/note.md").unwrap();
    assert!(
        reopened
            .retrieve(&MemoryAccessContext::cluster_local(), &query("beta"))
            .unwrap()
            .evidence
            .is_empty()
    );
    assert_eq!(
        reopened.index_root(&root.root_id).unwrap().tombstoned_files,
        1
    );
    assert!(
        reopened
            .retrieve(&MemoryAccessContext::cluster_local(), &query("beta"))
            .unwrap()
            .evidence
            .is_empty()
    );
}

#[test]
fn sifs_connector_is_lazy_and_never_falls_back_to_local_sources() {
    let derived = DerivedRoot::new();
    let data_dir = SinomoDataDir::new(derived.0.join("node"));
    let lazy = IndexedFileMemoryStore::from_data_dir(&data_dir);
    assert!(!lazy.has_registered_roots().unwrap());
    assert!(
        !data_dir.root().exists(),
        "constructing a store must not open or initialize DFS"
    );

    let source = VfsHandle::memory();
    source.create_dir_all("/notes").unwrap();
    source.write("/notes/note.md", b"canonical memory").unwrap();
    let configured =
        IndexedFileMemoryStore::new(&derived.0).with_sifs_vfs(source, "fixture-namespace");
    let root = configured
        .register_sifs_root(LocalIndexRootConfig::new(
            "/notes",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    configured.index_root(&root.root_id).unwrap();
    let missing = IndexedFileMemoryStore::new(&derived.0);
    assert!(matches!(
        missing.index_root(&root.root_id),
        Err(MemoryError::InvalidIndexRoot { .. })
    ));
    assert!(matches!(
        missing.retrieve(&MemoryAccessContext::cluster_local(), &query("canonical")),
        Err(MemoryError::InvalidIndexRoot { .. })
    ));
}

#[test]
fn sifs_source_pages_are_bounded_root_bound_and_reject_replaced_roots() {
    let derived = DerivedRoot::new();
    let source = VfsHandle::memory();
    for root in ["/one", "/two"] {
        source.create_dir_all(root).unwrap();
        for index in 0..3 {
            source
                .write(format!("{root}/{index}.md"), b"source")
                .unwrap();
        }
    }
    let store =
        IndexedFileMemoryStore::new(&derived.0).with_sifs_vfs(source.clone(), "fixture-namespace");
    let one = store
        .register_sifs_root(LocalIndexRootConfig::new(
            "/one",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    let two = store
        .register_sifs_root(LocalIndexRootConfig::new(
            "/two",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    let first = store.scan_local_root_page(&one.root_id, None, 1).unwrap();
    assert_eq!(first.sources.len(), 1);
    assert!(first.continuation.is_some());
    assert!(matches!(
        store.scan_local_root_page(&two.root_id, first.continuation.as_ref(), 1),
        Err(MemoryError::InvalidMemoryContinuation { .. })
    ));
    let mut cursor = first.continuation;
    let mut seen = 1;
    while let Some(next) = cursor {
        let page = store
            .scan_local_root_page(&one.root_id, Some(&next), 1)
            .unwrap();
        assert!(page.sources.len() <= 1);
        seen += page.sources.len();
        cursor = page.continuation;
    }
    assert_eq!(seen, 3);
    source.rename("/one", "/old-one").unwrap();
    source.create_dir_all("/one").unwrap();
    assert!(matches!(
        store.index_root(&one.root_id),
        Err(MemoryError::SourceChanged { .. })
    ));
}

#[cfg(unix)]
#[test]
fn sifs_source_contract_rejects_symlink_roots_without_following_them() {
    let fixture = DerivedRoot::new();
    let source = fixture.0.join("source");
    std::fs::create_dir_all(source.join("allowed")).unwrap();
    std::os::unix::fs::symlink("allowed", source.join("linked")).unwrap();
    let store = IndexedFileMemoryStore::new(fixture.0.join("index"))
        .with_sifs_vfs(VfsHandle::local(source).unwrap(), "fixture-namespace");
    assert!(
        store
            .register_sifs_root(LocalIndexRootConfig::new(
                "/linked",
                IndexRootPolicy::cluster_local()
            ))
            .is_err()
    );
}

#[derive(Debug)]
struct UnscopedOnlySource {
    ordinary_calls: Arc<AtomicUsize>,
}

impl UnscopedOnlySource {
    fn unsupported<T>(&self) -> Result<T, VfsError> {
        self.ordinary_calls.fetch_add(1, Ordering::Relaxed);
        Err(VfsError::Unsupported {
            backend: "unscoped-test".into(),
            detail: "ordinary operation must not substitute for scoped capability".into(),
        })
    }
}

// No no-link overrides: the VFS's actual capability boundary rejects them.
impl VfsBackend for UnscopedOnlySource {
    fn backend_name(&self) -> &'static str {
        "unscoped-test"
    }
    fn stat(&self, _: &str) -> Result<VfsMetadata, VfsError> {
        self.unsupported()
    }
    fn list(&self, _: &str) -> Result<Vec<VfsDirEntry>, VfsError> {
        self.unsupported()
    }
    fn read(&self, _: &str) -> Result<Vec<u8>, VfsError> {
        self.unsupported()
    }
    fn read_prefix(&self, _: &str, _: u64) -> Result<VfsFileRead, VfsError> {
        self.unsupported()
    }
    fn write(&self, _: &str, _: &[u8], _: &VfsWriteOptions) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn append(&self, _: &str, _: &[u8], _: &VfsAppendOptions) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn create_dir_all(&self, _: &str) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn rename(&self, _: &str, _: &str) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn remove(&self, _: &str, _: &VfsRemoveOptions) -> Result<(), VfsError> {
        self.unsupported()
    }
}

#[test]
fn sifs_source_requires_actual_scoped_vfs_capability() {
    let fixture = DerivedRoot::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = VfsHandle::new(
        Arc::new(UnscopedOnlySource {
            ordinary_calls: calls.clone(),
        }),
        "/",
    )
    .unwrap();
    let store = IndexedFileMemoryStore::new(&fixture.0).with_sifs_vfs(source, "fixture-namespace");
    assert!(matches!(
        store.register_sifs_root(LocalIndexRootConfig::new(
            "/notes",
            IndexRootPolicy::cluster_local()
        )),
        Err(MemoryError::Vfs { .. })
    ));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "a weak backend must not get an ordinary stat/read fallback"
    );
}

#[test]
fn sifs_snapshot_rejects_forged_identity_length_and_media_type() {
    let fixture = DerivedRoot::new();
    let source = VfsHandle::memory();
    source.create_dir_all("/notes").unwrap();
    source
        .write("/notes/note.md", b"canonical source bytes")
        .unwrap();
    let store =
        IndexedFileMemoryStore::new(&fixture.0).with_sifs_vfs(source.clone(), "fixture-namespace");
    let root = store
        .register_sifs_root(LocalIndexRootConfig::new(
            "/notes",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    let candidate = store
        .scan_local_root_page(&root.root_id, None, 16)
        .unwrap()
        .sources
        .remove(0);
    let connector = SifsSourceConnector::from_vfs(source, "fixture-namespace");
    assert_eq!(
        connector.acquire_snapshot(&root, &candidate).unwrap().bytes,
        b"canonical source bytes"
    );
    let mut wrong_identity = candidate.clone();
    wrong_identity.file_ref.object_id = Id::generate();
    let mut wrong_length = candidate.clone();
    wrong_length.byte_length += 1;
    let mut wrong_media_type = candidate;
    wrong_media_type.media_type = "application/json".into();
    for forged in [wrong_identity, wrong_length, wrong_media_type] {
        assert!(matches!(
            connector.acquire_snapshot(&root, &forged),
            Err(MemoryError::InvalidSourceCandidate { .. })
        ));
    }
}

#[derive(Debug)]
struct ExactReadBoundSource(VfsHandle);

impl ExactReadBoundSource {
    fn unsupported<T>(&self) -> Result<T, VfsError> {
        Err(VfsError::Unsupported {
            backend: self.backend_name().into(),
            detail: "only scoped stat and exact read are available".into(),
        })
    }
}

impl VfsBackend for ExactReadBoundSource {
    fn backend_name(&self) -> &'static str {
        "exact-read-bound-test"
    }
    fn stat(&self, _: &str) -> Result<VfsMetadata, VfsError> {
        self.unsupported()
    }
    fn stat_beneath_no_links(&self, root: &str, path: &str) -> Result<VfsMetadata, VfsError> {
        self.0.scoped(root)?.stat_beneath_no_links(path)
    }
    fn list(&self, _: &str) -> Result<Vec<VfsDirEntry>, VfsError> {
        self.unsupported()
    }
    fn read(&self, _: &str) -> Result<Vec<u8>, VfsError> {
        self.unsupported()
    }
    fn read_prefix(&self, _: &str, _: u64) -> Result<VfsFileRead, VfsError> {
        self.unsupported()
    }
    fn read_prefix_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        max_bytes: u64,
    ) -> Result<sinomo_runtime::vfs::VfsOpenedFileRead, VfsError> {
        let scoped = self.0.scoped(root)?;
        let size = scoped.stat_beneath_no_links(path)?.size_bytes.unwrap();
        assert_eq!(max_bytes, size, "request the actual tiny source length");
        scoped.read_prefix_beneath_no_links(path, max_bytes)
    }
    fn write(&self, _: &str, _: &[u8], _: &VfsWriteOptions) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn append(&self, _: &str, _: &[u8], _: &VfsAppendOptions) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn create_dir_all(&self, _: &str) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn rename(&self, _: &str, _: &str) -> Result<(), VfsError> {
        self.unsupported()
    }
    fn remove(&self, _: &str, _: &VfsRemoveOptions) -> Result<(), VfsError> {
        self.unsupported()
    }
}

#[test]
fn tiny_source_uses_actual_read_length_with_a_large_configured_root_limit() {
    let fixture = DerivedRoot::new();
    let source = VfsHandle::memory();
    source.create_dir_all("/notes").unwrap();
    source
        .write("/notes/note.md", b"tiny source bytes")
        .unwrap();
    let bounded = VfsHandle::new(Arc::new(ExactReadBoundSource(source.clone())), "/").unwrap();
    let store = IndexedFileMemoryStore::new(&fixture.0)
        .with_sifs_vfs(bounded.clone(), "read-bound-namespace");
    let mut config = LocalIndexRootConfig::new("/notes", IndexRootPolicy::cluster_local());
    config.max_source_bytes = 128 * 1024 * 1024;
    let root = store.register_sifs_root(config).unwrap();
    let candidate = SifsSourceConnector::from_vfs(source, "read-bound-namespace")
        .scan_page(&root, None, 16)
        .unwrap()
        .sources
        .remove(0);
    let snapshot = SifsSourceConnector::from_vfs(bounded, "read-bound-namespace")
        .acquire_snapshot(&root, &candidate)
        .unwrap();
    assert_eq!(snapshot.bytes, b"tiny source bytes");
    assert_eq!(snapshot.candidate, candidate);
}

#[test]
fn identical_native_anchors_cannot_cross_source_namespaces() {
    let fixture = DerivedRoot::new();
    let source = VfsHandle::memory();
    source.create_dir_all("/services").unwrap();
    source
        .write("/services/note.md", b"namespace memory needle")
        .unwrap();
    let original_metadata = source.stat("/services").unwrap();
    let original =
        IndexedFileMemoryStore::new(&fixture.0).with_sifs_vfs(source.clone(), "cluster-a");
    let old_root = original
        .register_sifs_root(LocalIndexRootConfig::new(
            "/services",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    let candidate = original
        .scan_local_root_page(&old_root.root_id, None, 16)
        .unwrap()
        .sources
        .remove(0);
    original.index_root(&old_root.root_id).unwrap();
    assert_eq!(
        original
            .retrieve(&MemoryAccessContext::cluster_local(), &query("needle"))
            .unwrap()
            .evidence
            .len(),
        1
    );

    // Keep the same native directory/file IDs and bytes, as deterministic
    // bootstrap anchors can do across clusters; only the namespace changes.
    let switched =
        IndexedFileMemoryStore::new(&fixture.0).with_sifs_vfs(source.clone(), "cluster-b");
    assert_eq!(source.stat("/services").unwrap(), original_metadata);
    assert!(matches!(
        switched.index_root(&old_root.root_id),
        Err(MemoryError::SourceChanged { .. })
    ));
    let other_connector = SifsSourceConnector::from_vfs(source.clone(), "cluster-b");
    assert!(matches!(
        other_connector.acquire_snapshot(&old_root, &candidate),
        Err(MemoryError::SourceChanged { .. })
    ));
    assert!(
        switched
            .retrieve(&MemoryAccessContext::cluster_local(), &query("needle"))
            .unwrap()
            .evidence
            .is_empty()
    );
    let new_root = switched
        .register_sifs_root(LocalIndexRootConfig::new(
            "/services",
            IndexRootPolicy::cluster_local(),
        ))
        .unwrap();
    assert_ne!(new_root.root_id, old_root.root_id);
    switched.index_root(&new_root.root_id).unwrap();
    let current = switched
        .retrieve(&MemoryAccessContext::cluster_local(), &query("needle"))
        .unwrap();
    assert_eq!(current.evidence.len(), 1);
    assert_eq!(
        current.evidence[0].anchor.source_version.file_ref.root_id,
        new_root.root_id
    );

    let unbound = IndexedFileMemoryStore::new(fixture.0.join("unbound")).with_sifs_vfs(source, "");
    assert!(matches!(
        unbound.register_sifs_root(LocalIndexRootConfig::new(
            "/services",
            IndexRootPolicy::cluster_local()
        )),
        Err(MemoryError::InvalidIndexRoot { .. })
    ));
}
