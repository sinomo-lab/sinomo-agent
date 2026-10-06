# DFS attachments

This optional R1 component owns file-associated annotations and derived
artifacts: bounded catalogs, producer budgets, generation claims, uploads,
conditional publication and resumable cleanup. Catalogs and payloads remain
ordinary distributed filesystem objects; the component does not own metadata
authority, placement, replication or physical content garbage collection.

`DfsAttachmentsExt` supplies the attachment operations for callers with a
canonical `NetworkedDfsService`. `DfsAttachmentService` borrows that service and
uses its public distributed operations. The shared data contracts remain in
`sinomo-api::dfs::attachments`.

Object annotations use filesystem/object identity. Derived artifacts additionally
name an exact source revision. Rename preserves identity; source-path reuse does
not transfer annotations. Catalog compare-and-swap is the publication point,
and uncertain retries retain the same operation identity and preconditions.

Attachment cleanup uses core object lifecycle evidence and durable change
cursors. Disabled attachment processing does not weaken the filesystem's
retention roots or grant deletion authority. The persisted catalog and staging
layouts remain compatible with existing attachment data.

See [the file attachment contract](../../docs/developer/file-attachments.md)
for API bounds, storage identity, producer ownership and indexing rules.
