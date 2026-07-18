use super::{IndexGenerationId, IndexPartitionId, IndexRootId, IndexRootPolicy, RouterManifestId};
use crate::{MemoryError, decode_sobj, encode_sobj};
use serde::{Deserialize, Serialize};
use sinomo_runtime::core::Id;
use sinomo_runtime::storage::durable_file::{
    DurableAtomicFileOptions, write_file_create_new_atomic,
};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

pub(super) const INDEXED_SCHEMA_VERSION: u32 = 2;
/// This sentinel cannot be produced by the lexical tokenizer.
pub(super) const ALL_PARTITIONS_TERM: &str = "\0all-partitions";

const MAX_ROUTER_SHARDS: usize = 257;
const MAX_ROUTER_TREE_HEIGHT: u16 = 32;
const MAX_ROUTER_LEAF_RECORDS: usize = 64;
const MAX_ROUTER_INTERNAL_CHILDREN: usize = 64;
const MAX_ROUTER_RECORD_BYTES: usize = 1024 * 1024;
const MAX_ROUTER_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_ROUTER_SCAN_INSPECTED: usize = 4_096;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct LexicalRouterManifest {
    pub(super) schema_version: u32,
    pub(super) manifest_id: RouterManifestId,
    pub(super) shards: BTreeMap<String, RouterNodeRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub(super) published_at: OffsetDateTime,
}

impl LexicalRouterManifest {
    pub(super) fn empty() -> Self {
        let shards = BTreeMap::new();
        Self {
            schema_version: INDEXED_SCHEMA_VERSION,
            manifest_id: canonical_manifest_id(INDEXED_SCHEMA_VERSION, &shards),
            shards,
            published_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    pub(super) fn refresh_id(&mut self) {
        self.manifest_id = canonical_manifest_id(self.schema_version, &self.shards);
    }

    pub(super) fn validate(self, path: &Path) -> Result<Self, MemoryError> {
        if self.schema_version != INDEXED_SCHEMA_VERSION {
            return Err(MemoryError::UnsupportedSchema {
                path: path.display().to_string(),
                schema_version: self.schema_version,
            });
        }
        if self.shards.len() > MAX_ROUTER_SHARDS {
            return Err(invalid_router_object(
                path,
                format!(
                    "router manifest has {} shards, exceeding the {MAX_ROUTER_SHARDS} bound",
                    self.shards.len()
                ),
            ));
        }
        for (shard_key, root) in &self.shards {
            validate_shard_key(path, shard_key)?;
            validate_node_ref(path, root)?;
            validate_reference_shard(path, shard_key, root)?;
        }
        let expected_id = canonical_manifest_id(self.schema_version, &self.shards);
        if self.manifest_id != expected_id {
            return Err(invalid_router_object(
                path,
                format!(
                    "router manifest id is not canonical: expected {expected_id}, got {}",
                    self.manifest_id
                ),
            ));
        }
        Ok(self)
    }
}

fn canonical_manifest_id(
    schema_version: u32,
    shards: &BTreeMap<String, RouterNodeRef>,
) -> RouterManifestId {
    Id::derived(
        "memory-lexical-router-manifest",
        serde_json::to_vec(&(schema_version, shards)).expect("router manifest is serializable"),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct RouterPosting {
    pub(super) partition_id: IndexPartitionId,
    pub(super) generation_id: IndexGenerationId,
    pub(super) root_id: IndexRootId,
    pub(super) authorization_epoch: Id,
    pub(super) policy: IndexRootPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct RouterKey {
    pub(super) term: String,
    pub(super) partition_id: IndexPartitionId,
}

impl RouterKey {
    pub(super) fn new(term: impl Into<String>, partition_id: IndexPartitionId) -> Self {
        Self {
            term: term.into(),
            partition_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RouterRecord {
    key: RouterKey,
    posting: RouterPosting,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct RouterNodeRef {
    page_id: Id,
    first_key: RouterKey,
    last_key: RouterKey,
    height: u16,
    record_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct RouterPageEnvelope {
    schema_version: u32,
    page_id: Id,
    node: RouterNode,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
enum RouterNode {
    Leaf { records: Vec<RouterRecord> },
    Internal { children: Vec<RouterNodeRef> },
}

pub(super) type RouterMutationBatch = BTreeMap<RouterKey, Option<RouterPosting>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RouterScanWatermark {
    pub(super) partition_id: IndexPartitionId,
    pub(super) generation_id: IndexGenerationId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RouterScanPage {
    pub(super) eligible_postings: Vec<RouterPosting>,
    pub(super) last_inspected: Option<RouterScanWatermark>,
    pub(super) inspected_count: usize,
    pub(super) exhausted: bool,
}

#[derive(Clone, Debug)]
pub(super) struct RouterTreeStore {
    shards_dir: PathBuf,
}

impl RouterTreeStore {
    pub(super) fn new(shards_dir: impl Into<PathBuf>) -> Self {
        Self {
            shards_dir: shards_dir.into(),
        }
    }

    pub(super) fn apply(
        &self,
        shard_key: &str,
        root: Option<&RouterNodeRef>,
        mutations: &RouterMutationBatch,
    ) -> Result<Option<RouterNodeRef>, MemoryError> {
        validate_shard_key(&self.shards_dir, shard_key)?;
        if mutations.is_empty() {
            return Ok(root.cloned());
        }
        for (key, posting) in mutations {
            if router_shard_key(&key.term) != shard_key {
                return Err(invalid_router_object(
                    &self.shards_dir,
                    format!(
                        "router mutation for term {:?} does not belong to shard {shard_key}",
                        key.term
                    ),
                ));
            }
            if posting
                .as_ref()
                .is_some_and(|posting| posting.partition_id != key.partition_id)
            {
                return Err(invalid_router_object(
                    &self.shards_dir,
                    "router mutation key and posting partition ids differ",
                ));
            }
        }

        let mut rewritten = match root {
            Some(root) => self.rewrite_node(shard_key, root, mutations)?,
            None => {
                let records = mutations
                    .iter()
                    .filter_map(|(key, posting)| {
                        posting.as_ref().map(|posting| RouterRecord {
                            key: key.clone(),
                            posting: posting.clone(),
                        })
                    })
                    .collect::<Vec<_>>();
                self.write_leaf_chunks(shard_key, &records)?
            }
        };
        while rewritten.len() > 1 {
            rewritten = self.write_internal_chunks(shard_key, &rewritten)?;
        }
        Ok(rewritten.pop())
    }

    pub(super) fn scan_term<F>(
        &self,
        shard_key: &str,
        root: &RouterNodeRef,
        term: &str,
        start_partition_id: Option<&IndexPartitionId>,
        start_exclusive: bool,
        eligible_limit: usize,
        max_inspected: usize,
        mut authorize: F,
    ) -> Result<RouterScanPage, MemoryError>
    where
        F: FnMut(&RouterPosting) -> Result<bool, MemoryError>,
    {
        validate_shard_key(&self.shards_dir, shard_key)?;
        if router_shard_key(term) != shard_key {
            return Err(invalid_router_object(
                &self.shards_dir,
                format!("term {term:?} does not belong to router shard {shard_key}"),
            ));
        }
        if eligible_limit == 0 || max_inspected == 0 || max_inspected > MAX_ROUTER_SCAN_INSPECTED {
            return Err(MemoryError::InvalidMemoryPageLimit {
                surface: "lexical router scan work",
                limit: max_inspected,
                max: MAX_ROUTER_SCAN_INSPECTED,
            });
        }
        let height_pages = usize::from(root.height).saturating_add(1);
        let max_pages = max_inspected.saturating_add(1).saturating_mul(height_pages);
        let mut state = RouterScanState {
            page: RouterScanPage {
                eligible_postings: Vec::with_capacity(eligible_limit.min(256)),
                last_inspected: None,
                inspected_count: 0,
                exhausted: true,
            },
            eligible_limit,
            max_inspected,
            remaining_pages: max_pages,
        };
        self.scan_node(
            shard_key,
            root,
            term,
            start_partition_id,
            start_exclusive,
            &mut authorize,
            &mut state,
        )?;
        Ok(state.page)
    }

    fn rewrite_node(
        &self,
        shard_key: &str,
        reference: &RouterNodeRef,
        mutations: &RouterMutationBatch,
    ) -> Result<Vec<RouterNodeRef>, MemoryError> {
        match self.load_node(shard_key, reference)? {
            RouterNode::Leaf { records } => {
                let mut records = records
                    .into_iter()
                    .map(|record| (record.key, record.posting))
                    .collect::<BTreeMap<_, _>>();
                for (key, posting) in mutations {
                    match posting {
                        Some(posting) => {
                            records.insert(key.clone(), posting.clone());
                        }
                        None => {
                            records.remove(key);
                        }
                    }
                }
                let records = records
                    .into_iter()
                    .map(|(key, posting)| RouterRecord { key, posting })
                    .collect::<Vec<_>>();
                self.write_leaf_chunks(shard_key, &records)
            }
            RouterNode::Internal { children } => {
                let mut child_mutations = vec![RouterMutationBatch::new(); children.len()];
                for (key, posting) in mutations {
                    let child_index = children
                        .partition_point(|child| child.last_key < *key)
                        .min(children.len().saturating_sub(1));
                    child_mutations[child_index].insert(key.clone(), posting.clone());
                }

                let mut rewritten_children = Vec::new();
                for (child, mutations) in children.iter().zip(child_mutations) {
                    if mutations.is_empty() {
                        rewritten_children.push(child.clone());
                    } else {
                        rewritten_children.extend(self.rewrite_node(shard_key, child, &mutations)?);
                    }
                }
                self.write_internal_chunks(shard_key, &rewritten_children)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_node<F>(
        &self,
        shard_key: &str,
        reference: &RouterNodeRef,
        term: &str,
        start_partition_id: Option<&IndexPartitionId>,
        start_exclusive: bool,
        authorize: &mut F,
        state: &mut RouterScanState,
    ) -> Result<(), MemoryError>
    where
        F: FnMut(&RouterPosting) -> Result<bool, MemoryError>,
    {
        if state.stopped()
            || reference.last_key.term.as_str() < term
            || reference.first_key.term.as_str() > term
            || reference_is_before_start(reference, term, start_partition_id, start_exclusive)
        {
            return Ok(());
        }
        if state.remaining_pages == 0 {
            state.page.exhausted = false;
            return Ok(());
        }
        state.remaining_pages -= 1;
        match self.load_node(shard_key, reference)? {
            RouterNode::Leaf { records } => {
                for record in records {
                    if state.stopped() {
                        break;
                    }
                    match record.key.term.as_str().cmp(term) {
                        std::cmp::Ordering::Less => continue,
                        std::cmp::Ordering::Greater => break,
                        std::cmp::Ordering::Equal => {}
                    }
                    if start_partition_id.is_some_and(|start| {
                        record.key.partition_id < *start
                            || (start_exclusive && record.key.partition_id == *start)
                    }) {
                        continue;
                    }
                    state.page.inspected_count = state.page.inspected_count.saturating_add(1);
                    state.page.last_inspected = Some(RouterScanWatermark {
                        partition_id: record.posting.partition_id.clone(),
                        generation_id: record.posting.generation_id.clone(),
                    });
                    if authorize(&record.posting)? {
                        state.page.eligible_postings.push(record.posting);
                    }
                    if state.stopped() {
                        state.page.exhausted = false;
                    }
                }
            }
            RouterNode::Internal { children } => {
                for child in children {
                    if state.stopped() || child.first_key.term.as_str() > term {
                        break;
                    }
                    if child.last_key.term.as_str() < term {
                        continue;
                    }
                    self.scan_node(
                        shard_key,
                        &child,
                        term,
                        start_partition_id,
                        start_exclusive,
                        authorize,
                        state,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn write_leaf_chunks(
        &self,
        shard_key: &str,
        records: &[RouterRecord],
    ) -> Result<Vec<RouterNodeRef>, MemoryError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut references = Vec::new();
        for records in records.chunks(MAX_ROUTER_LEAF_RECORDS) {
            references.extend(self.write_leaf_group(shard_key, records)?);
        }
        Ok(references)
    }

    fn write_leaf_group(
        &self,
        shard_key: &str,
        records: &[RouterRecord],
    ) -> Result<Vec<RouterNodeRef>, MemoryError> {
        let node = RouterNode::Leaf {
            records: records.to_vec(),
        };
        let encoded = self.encode_node(shard_key, node)?;
        if encoded.bytes.len() <= MAX_ROUTER_PAGE_BYTES {
            let reference = encoded.reference.clone();
            self.persist_encoded_page(encoded)?;
            return Ok(vec![reference]);
        }
        if records.len() == 1 {
            return Err(MemoryError::SobjSerialize {
                path: encoded.path.display().to_string(),
                detail: format!(
                    "router leaf page is {} bytes, exceeding the {MAX_ROUTER_PAGE_BYTES} byte bound",
                    encoded.bytes.len()
                ),
            });
        }
        let middle = records.len() / 2;
        let mut left = self.write_leaf_group(shard_key, &records[..middle])?;
        left.extend(self.write_leaf_group(shard_key, &records[middle..])?);
        Ok(left)
    }

    fn write_internal_chunks(
        &self,
        shard_key: &str,
        children: &[RouterNodeRef],
    ) -> Result<Vec<RouterNodeRef>, MemoryError> {
        if children.is_empty() {
            return Ok(Vec::new());
        }
        let mut references = Vec::new();
        for children in children.chunks(MAX_ROUTER_INTERNAL_CHILDREN) {
            references.extend(self.write_internal_group(shard_key, children)?);
        }
        Ok(references)
    }

    fn write_internal_group(
        &self,
        shard_key: &str,
        children: &[RouterNodeRef],
    ) -> Result<Vec<RouterNodeRef>, MemoryError> {
        let node = RouterNode::Internal {
            children: children.to_vec(),
        };
        let encoded = self.encode_node(shard_key, node)?;
        if encoded.bytes.len() <= MAX_ROUTER_PAGE_BYTES {
            let reference = encoded.reference.clone();
            self.persist_encoded_page(encoded)?;
            return Ok(vec![reference]);
        }
        if children.len() == 1 {
            return Err(MemoryError::SobjSerialize {
                path: encoded.path.display().to_string(),
                detail: format!(
                    "router internal page is {} bytes, exceeding the {MAX_ROUTER_PAGE_BYTES} byte bound",
                    encoded.bytes.len()
                ),
            });
        }
        let middle = children.len() / 2;
        let mut left = self.write_internal_group(shard_key, &children[..middle])?;
        left.extend(self.write_internal_group(shard_key, &children[middle..])?);
        Ok(left)
    }

    fn encode_node(
        &self,
        shard_key: &str,
        node: RouterNode,
    ) -> Result<EncodedRouterPage, MemoryError> {
        let placeholder = self.shards_dir.join(shard_key).join("pages");
        let reference = node_reference(&placeholder, &node)?;
        let envelope = RouterPageEnvelope {
            schema_version: INDEXED_SCHEMA_VERSION,
            page_id: reference.page_id.clone(),
            node,
        };
        let path = self.page_path(shard_key, &reference.page_id);
        let bytes = encode_sobj(&path, &envelope)?;
        Ok(EncodedRouterPage {
            shard_key: shard_key.to_string(),
            path,
            envelope,
            reference,
            bytes,
        })
    }

    fn persist_encoded_page(&self, encoded: EncodedRouterPage) -> Result<(), MemoryError> {
        if encoded.bytes.len() > MAX_ROUTER_PAGE_BYTES {
            return Err(MemoryError::SobjSerialize {
                path: encoded.path.display().to_string(),
                detail: format!(
                    "router page is {} bytes, exceeding the {MAX_ROUTER_PAGE_BYTES} byte bound",
                    encoded.bytes.len()
                ),
            });
        }
        if encoded.path.is_file() {
            let existing =
                self.load_node_from_path(&encoded.shard_key, &encoded.path, &encoded.reference)?;
            if existing == encoded.envelope.node {
                return Ok(());
            }
            return Err(invalid_router_object(
                &encoded.path,
                "content-addressed router page collides with different content",
            ));
        }
        let parent = encoded
            .path
            .parent()
            .ok_or_else(|| MemoryError::DurableFile {
                path: encoded.path.display().to_string(),
                detail: "router page has no parent directory".to_string(),
            })?;
        fs::create_dir_all(parent).map_err(|source| MemoryError::Io {
            path: parent.display().to_string(),
            source,
        })?;
        match write_file_create_new_atomic(
            &encoded.path,
            &encoded.bytes,
            DurableAtomicFileOptions::new("memory-router-page.sobj", "memory-router-page"),
        ) {
            Ok(()) => Ok(()),
            Err(_source) if encoded.path.is_file() => {
                let existing = self.load_node_from_path(
                    &encoded.shard_key,
                    &encoded.path,
                    &encoded.reference,
                )?;
                if existing == encoded.envelope.node {
                    Ok(())
                } else {
                    Err(invalid_router_object(
                        &encoded.path,
                        "content-addressed router page collides with different content",
                    ))
                }
            }
            Err(source) => Err(MemoryError::DurableFile {
                path: encoded.path.display().to_string(),
                detail: source.to_string(),
            }),
        }
    }

    fn load_node(
        &self,
        shard_key: &str,
        reference: &RouterNodeRef,
    ) -> Result<RouterNode, MemoryError> {
        if reference.height > MAX_ROUTER_TREE_HEIGHT {
            return Err(invalid_router_object(
                &self.shards_dir,
                format!(
                    "router tree height {} exceeds the {MAX_ROUTER_TREE_HEIGHT} bound",
                    reference.height
                ),
            ));
        }
        let path = self.page_path(shard_key, &reference.page_id);
        self.load_node_from_path(shard_key, &path, reference)
    }

    fn load_node_from_path(
        &self,
        shard_key: &str,
        path: &Path,
        reference: &RouterNodeRef,
    ) -> Result<RouterNode, MemoryError> {
        let length = fs::metadata(path)
            .map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?
            .len();
        if length > MAX_ROUTER_PAGE_BYTES as u64 {
            return Err(invalid_router_object(
                path,
                format!(
                    "router page is {length} bytes, exceeding the {MAX_ROUTER_PAGE_BYTES} byte bound"
                ),
            ));
        }
        let bytes = fs::read(path).map_err(|source| MemoryError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let envelope: RouterPageEnvelope = decode_sobj(path, &bytes)?;
        if envelope.schema_version != INDEXED_SCHEMA_VERSION {
            return Err(MemoryError::UnsupportedSchema {
                path: path.display().to_string(),
                schema_version: envelope.schema_version,
            });
        }
        let canonical_page_id = canonical_page_id(&envelope.node);
        if envelope.page_id != canonical_page_id || reference.page_id != canonical_page_id {
            return Err(invalid_router_object(
                path,
                format!(
                    "router page id is not canonical: expected {canonical_page_id}, envelope {}, reference {}",
                    envelope.page_id, reference.page_id
                ),
            ));
        }
        let actual_reference = node_reference(path, &envelope.node)?;
        if &actual_reference != reference {
            return Err(invalid_router_object(
                path,
                "router page reference metadata does not match page content",
            ));
        }
        validate_node_shard(path, shard_key, &envelope.node)?;
        Ok(envelope.node)
    }

    fn page_path(&self, shard_key: &str, page_id: &Id) -> PathBuf {
        self.shards_dir
            .join(shard_key)
            .join("pages")
            .join(format!("{page_id}.sobj"))
    }

    #[cfg(test)]
    fn page_ids(
        &self,
        shard_key: &str,
        root: &RouterNodeRef,
    ) -> Result<(BTreeSet<Id>, BTreeSet<Id>), MemoryError> {
        let mut all = BTreeSet::new();
        let mut leaves = BTreeSet::new();
        self.collect_page_ids(shard_key, root, &mut all, &mut leaves)?;
        Ok((all, leaves))
    }

    #[cfg(test)]
    fn collect_page_ids(
        &self,
        shard_key: &str,
        reference: &RouterNodeRef,
        all: &mut BTreeSet<Id>,
        leaves: &mut BTreeSet<Id>,
    ) -> Result<(), MemoryError> {
        all.insert(reference.page_id.clone());
        match self.load_node(shard_key, reference)? {
            RouterNode::Leaf { .. } => {
                leaves.insert(reference.page_id.clone());
            }
            RouterNode::Internal { children } => {
                for child in children {
                    self.collect_page_ids(shard_key, &child, all, leaves)?;
                }
            }
        }
        Ok(())
    }
}

struct RouterScanState {
    page: RouterScanPage,
    eligible_limit: usize,
    max_inspected: usize,
    remaining_pages: usize,
}

impl RouterScanState {
    fn stopped(&self) -> bool {
        self.page.eligible_postings.len() >= self.eligible_limit
            || self.page.inspected_count >= self.max_inspected
    }
}

struct EncodedRouterPage {
    shard_key: String,
    path: PathBuf,
    envelope: RouterPageEnvelope,
    reference: RouterNodeRef,
    bytes: Vec<u8>,
}

fn node_reference(path: &Path, node: &RouterNode) -> Result<RouterNodeRef, MemoryError> {
    match node {
        RouterNode::Leaf { records } => {
            if records.is_empty() || records.len() > MAX_ROUTER_LEAF_RECORDS {
                return Err(invalid_router_object(
                    path,
                    format!(
                        "router leaf has {} records; expected 1..={MAX_ROUTER_LEAF_RECORDS}",
                        records.len()
                    ),
                ));
            }
            for pair in records.windows(2) {
                if pair[0].key >= pair[1].key {
                    return Err(invalid_router_object(
                        path,
                        "router leaf records are not strictly ordered",
                    ));
                }
            }
            for record in records {
                if record.key.partition_id != record.posting.partition_id {
                    return Err(invalid_router_object(
                        path,
                        "router leaf key and posting partition ids differ",
                    ));
                }
                let record_bytes = encode_sobj(path, record)?;
                if record_bytes.len() > MAX_ROUTER_RECORD_BYTES {
                    return Err(MemoryError::SobjSerialize {
                        path: path.display().to_string(),
                        detail: format!(
                            "router record is {} bytes, exceeding the {MAX_ROUTER_RECORD_BYTES} byte bound",
                            record_bytes.len()
                        ),
                    });
                }
            }
            Ok(RouterNodeRef {
                page_id: canonical_page_id(node),
                first_key: records.first().expect("nonempty leaf").key.clone(),
                last_key: records.last().expect("nonempty leaf").key.clone(),
                height: 0,
                record_count: records.len() as u64,
            })
        }
        RouterNode::Internal { children } => {
            if children.is_empty() || children.len() > MAX_ROUTER_INTERNAL_CHILDREN {
                return Err(invalid_router_object(
                    path,
                    format!(
                        "router internal page has {} children; expected 1..={MAX_ROUTER_INTERNAL_CHILDREN}",
                        children.len()
                    ),
                ));
            }
            let child_height = children[0].height;
            for child in children {
                validate_node_ref(path, child)?;
                if child.height != child_height {
                    return Err(invalid_router_object(
                        path,
                        "router internal page has children at different heights",
                    ));
                }
            }
            for pair in children.windows(2) {
                if pair[0].last_key >= pair[1].first_key {
                    return Err(invalid_router_object(
                        path,
                        "router internal child ranges overlap or are unordered",
                    ));
                }
            }
            let height = child_height
                .checked_add(1)
                .ok_or_else(|| invalid_router_object(path, "router tree height overflow"))?;
            if height > MAX_ROUTER_TREE_HEIGHT {
                return Err(invalid_router_object(
                    path,
                    format!(
                        "router tree height {height} exceeds the {MAX_ROUTER_TREE_HEIGHT} bound"
                    ),
                ));
            }
            let record_count = children.iter().try_fold(0_u64, |total, child| {
                total
                    .checked_add(child.record_count)
                    .ok_or_else(|| invalid_router_object(path, "router tree record count overflow"))
            })?;
            Ok(RouterNodeRef {
                page_id: canonical_page_id(node),
                first_key: children
                    .first()
                    .expect("nonempty internal")
                    .first_key
                    .clone(),
                last_key: children.last().expect("nonempty internal").last_key.clone(),
                height,
                record_count,
            })
        }
    }
}

fn validate_node_ref(path: &Path, reference: &RouterNodeRef) -> Result<(), MemoryError> {
    if reference.first_key > reference.last_key {
        return Err(invalid_router_object(
            path,
            "router page reference has an inverted key range",
        ));
    }
    if reference.height > MAX_ROUTER_TREE_HEIGHT {
        return Err(invalid_router_object(
            path,
            format!(
                "router page reference height {} exceeds the {MAX_ROUTER_TREE_HEIGHT} bound",
                reference.height
            ),
        ));
    }
    if reference.record_count == 0 {
        return Err(invalid_router_object(
            path,
            "router page reference has zero records",
        ));
    }
    Ok(())
}

fn validate_reference_shard(
    path: &Path,
    shard_key: &str,
    reference: &RouterNodeRef,
) -> Result<(), MemoryError> {
    if router_shard_key(&reference.first_key.term) != shard_key
        || router_shard_key(&reference.last_key.term) != shard_key
    {
        return Err(invalid_router_object(
            path,
            format!("router page reference crosses the {shard_key} shard boundary"),
        ));
    }
    Ok(())
}

fn validate_node_shard(path: &Path, shard_key: &str, node: &RouterNode) -> Result<(), MemoryError> {
    match node {
        RouterNode::Leaf { records } => {
            for record in records {
                if router_shard_key(&record.key.term) != shard_key {
                    return Err(invalid_router_object(
                        path,
                        format!(
                            "router leaf term {:?} does not belong to shard {shard_key}",
                            record.key.term
                        ),
                    ));
                }
            }
        }
        RouterNode::Internal { children } => {
            for child in children {
                validate_reference_shard(path, shard_key, child)?;
            }
        }
    }
    Ok(())
}

fn reference_is_before_start(
    reference: &RouterNodeRef,
    term: &str,
    start_partition_id: Option<&IndexPartitionId>,
    start_exclusive: bool,
) -> bool {
    let Some(start) = start_partition_id else {
        return false;
    };
    reference.last_key.term == term
        && (reference.last_key.partition_id < *start
            || (start_exclusive && reference.last_key.partition_id == *start))
}

fn canonical_page_id(node: &RouterNode) -> Id {
    Id::derived(
        "memory-lexical-router-page",
        serde_json::to_vec(&(INDEXED_SCHEMA_VERSION, node)).expect("router page is serializable"),
    )
}

pub(super) fn router_shard_key(term: &str) -> String {
    if term == ALL_PARTITIONS_TERM {
        return "all".to_string();
    }
    format!("{:02x}", blake3::hash(term.as_bytes()).as_bytes()[0])
}

fn validate_shard_key(path: &Path, shard_key: &str) -> Result<(), MemoryError> {
    let valid = shard_key == "all"
        || (shard_key.len() == 2
            && shard_key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    if valid {
        Ok(())
    } else {
        Err(invalid_router_object(
            path,
            format!("invalid lexical router shard key {shard_key:?}"),
        ))
    }
}

fn invalid_router_object(path: &Path, detail: impl Into<String>) -> MemoryError {
    MemoryError::SobjDeserialize {
        path: path.display().to_string(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("sinomo-memory-router-{name}-{}", Id::generate()));
            fs::create_dir_all(&path).expect("create router test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn posting(partition_id: Id, generation: &str) -> RouterPosting {
        RouterPosting {
            partition_id,
            generation_id: Id::derived("test-router-generation", generation),
            root_id: Id::derived("test-router-root", b"root"),
            authorization_epoch: Id::derived("test-router-epoch", b"epoch"),
            policy: IndexRootPolicy::cluster_local(),
        }
    }

    fn put(term: &str, partition_id: Id, generation: &str) -> (RouterKey, Option<RouterPosting>) {
        (
            RouterKey::new(term, partition_id.clone()),
            Some(posting(partition_id, generation)),
        )
    }

    #[test]
    fn replacing_one_popular_posting_structurally_shares_other_leaves() {
        let directory = TestDirectory::new("sharing");
        let store = RouterTreeStore::new(&directory.0);
        let term = "shared";
        let shard = router_shard_key(term);
        let mut initial = RouterMutationBatch::new();
        for index in 0..(MAX_ROUTER_LEAF_RECORDS * 4) {
            let partition = Id::derived("test-router-partition", index.to_le_bytes());
            let (key, value) = put(term, partition, "old");
            initial.insert(key, value);
        }
        let old_root = store
            .apply(&shard, None, &initial)
            .expect("build router tree")
            .expect("nonempty root");
        let (_, old_leaves) = store.page_ids(&shard, &old_root).expect("old page ids");
        assert!(old_leaves.len() >= 4);

        let replaced_key = initial
            .keys()
            .nth(initial.len() / 2)
            .expect("middle router key")
            .clone();
        let replacement = BTreeMap::from([put(term, replaced_key.partition_id.clone(), "new")]);
        let new_root = store
            .apply(&shard, Some(&old_root), &replacement)
            .expect("replace one posting")
            .expect("nonempty replacement root");
        let (_, new_leaves) = store.page_ids(&shard, &new_root).expect("new page ids");
        let shared_leaves = old_leaves.intersection(&new_leaves).count();
        assert_eq!(old_leaves.len() - shared_leaves, 1);
        assert_eq!(new_leaves.len() - shared_leaves, 1);

        let old = store
            .scan_term(
                &shard,
                &old_root,
                term,
                Some(&replaced_key.partition_id),
                false,
                1,
                1,
                |_| Ok(true),
            )
            .expect("scan pinned old root");
        let new = store
            .scan_term(
                &shard,
                &new_root,
                term,
                Some(&replaced_key.partition_id),
                false,
                1,
                1,
                |_| Ok(true),
            )
            .expect("scan new root");
        assert_eq!(
            old.eligible_postings[0].generation_id,
            posting(replaced_key.partition_id.clone(), "old").generation_id
        );
        assert_eq!(
            new.eligible_postings[0].generation_id,
            posting(replaced_key.partition_id, "new").generation_id
        );
    }

    #[test]
    fn colliding_terms_remain_exact_and_deletion_is_local() {
        let directory = TestDirectory::new("collision");
        let store = RouterTreeStore::new(&directory.0);
        let mut by_shard = BTreeMap::<String, String>::new();
        let (first, second, shard) = (0_u64..)
            .find_map(|index| {
                let term = format!("collision-{index}");
                let shard = router_shard_key(&term);
                by_shard
                    .insert(shard.clone(), term.clone())
                    .map(|previous| (previous, term, shard))
            })
            .expect("two terms share a hash shard");
        let first_partition = Id::derived("test-router-partition", b"first");
        let second_partition = Id::derived("test-router-partition", b"second");
        let initial = BTreeMap::from([
            put(&first, first_partition.clone(), "first"),
            put(&second, second_partition.clone(), "second"),
        ]);
        let root = store
            .apply(&shard, None, &initial)
            .expect("build colliding term tree")
            .expect("nonempty root");
        let first_results = store
            .scan_term(&shard, &root, &first, None, false, 8, 8, |_| Ok(true))
            .expect("scan first term");
        assert_eq!(first_results.eligible_postings.len(), 1);
        assert_eq!(
            first_results.eligible_postings[0].partition_id,
            first_partition
        );

        let deleted = BTreeMap::from([(RouterKey::new(&first, first_partition), None)]);
        let root = store
            .apply(&shard, Some(&root), &deleted)
            .expect("delete one colliding term")
            .expect("second term remains");
        assert!(
            store
                .scan_term(&shard, &root, &first, None, false, 8, 8, |_| Ok(true))
                .expect("scan deleted term")
                .eligible_postings
                .is_empty()
        );
        let second_results = store
            .scan_term(&shard, &root, &second, None, false, 8, 8, |_| Ok(true))
            .expect("scan retained term");
        assert_eq!(second_results.eligible_postings.len(), 1);
        assert_eq!(
            second_results.eligible_postings[0].partition_id,
            second_partition
        );
    }

    #[test]
    fn denied_postings_consume_bounded_work_and_return_an_exclusive_watermark() {
        let directory = TestDirectory::new("denied-work");
        let store = RouterTreeStore::new(&directory.0);
        let term = "shared";
        let shard = router_shard_key(term);
        let mut initial = RouterMutationBatch::new();
        for index in 0_u64..3 {
            let partition = Id::derived("test-router-partition", index.to_le_bytes());
            let (key, value) = put(term, partition, "denied");
            initial.insert(key, value);
        }
        let root = store
            .apply(&shard, None, &initial)
            .expect("build denied router tree")
            .expect("nonempty root");

        let first = store
            .scan_term(&shard, &root, term, None, false, 1, 1, |_| Ok(false))
            .expect("bounded denied scan");
        assert!(first.eligible_postings.is_empty());
        assert_eq!(first.inspected_count, 1);
        assert!(!first.exhausted);
        let first_watermark = first.last_inspected.expect("denied scan watermark");

        let second = store
            .scan_term(
                &shard,
                &root,
                term,
                Some(&first_watermark.partition_id),
                true,
                1,
                1,
                |_| Ok(false),
            )
            .expect("resume denied scan exclusively");
        assert!(second.eligible_postings.is_empty());
        assert_eq!(second.inspected_count, 1);
        assert!(!second.exhausted);
        assert_ne!(
            second.last_inspected.expect("second denied watermark"),
            first_watermark
        );
    }

    #[test]
    fn oversized_router_page_fails_closed_before_allocation() {
        let directory = TestDirectory::new("oversized");
        let store = RouterTreeStore::new(&directory.0);
        let term = "shared";
        let shard = router_shard_key(term);
        let partition = Id::derived("test-router-partition", b"one");
        let root = store
            .apply(&shard, None, &BTreeMap::from([put(term, partition, "one")]))
            .expect("write router page")
            .expect("nonempty root");
        let page = store.page_path(&shard, &root.page_id);
        fs::write(&page, vec![0_u8; MAX_ROUTER_PAGE_BYTES + 1])
            .expect("corrupt page with oversized bytes");
        let error = store
            .scan_term(&shard, &root, term, None, false, 1, 1, |_| Ok(true))
            .expect_err("oversized page must fail closed");
        assert!(error.to_string().contains("exceeding"));
    }
}
