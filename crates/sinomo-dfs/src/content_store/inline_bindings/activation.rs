use super::*;

impl LocalContentStore {
    /// Initializes only the current catalog. Existing content roots must be
    /// converted by the storage preparation provider before opening.
    pub(in crate::content_store) fn initialize_inline_bindings(&self) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        let root = &self.inline_store.owner.root;
        if catalog_path(root).exists() {
            let catalog = read_catalog(root, &reservation.sentinel.store_incarnation)?;
            return certify_catalog(root, &catalog);
        }
        let bindings = root.join(INLINE_BINDING_DIR);
        durable_file::ensure_directory(&bindings).map_err(content_durable_file_error)?;
        durable_file::ensure_directory(&bindings.join("generations"))
            .map_err(content_durable_file_error)?;
        let generation = generation_root(root, 1);
        durable_file::ensure_directory(&generation).map_err(content_durable_file_error)?;
        let lease_path = generation.join("readers.lock");
        let mut lease = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lease_path)
            .map_err(|source| ContentStoreError::Io {
                path: lease_path.clone(),
                source,
            })?;
        durable_file::sync_open_file(&mut lease, &lease_path, durable_file::DurableFileSync::All)
            .map_err(content_durable_file_error)?;
        sync_dir(&generation)?;
        let writer =
            FrameAppender::open(&segment_path(root, 1, 0), frame_limits()).map_err(inline_error)?;
        if writer.durable_prefix().end() != 0 {
            return Err(inline_error("unpublished initial catalog contains records"));
        }
        let catalog = InlineCatalog {
            format_version: INLINE_FORMAT_VERSION,
            store_incarnation: reservation.sentinel.store_incarnation.clone(),
            generation: 1,
            segments: vec![InlineSegment {
                id: 0,
                sealed_end: None,
                sealed_digest: None,
            }],
        };
        publish_catalog(root, &catalog)?;
        certify_catalog(root, &catalog)
    }
}
