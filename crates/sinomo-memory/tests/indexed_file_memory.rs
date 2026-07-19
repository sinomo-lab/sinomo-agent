use sinomo_memory::{
    EvidenceCoverage, IndexRootPolicy, IndexedFileMemoryStore, LocalIndexRootConfig,
    LocalScanCursor, LocalScanFrame, MemoryAccessContext, MemoryQuery, MemorySourceCatalog,
    MemoryTraceStore, SourceFreshness,
};
use sinomo_runtime::core::{Id, MemoryScope, TrustDomain};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

struct TestDirectories {
    base: PathBuf,
    source: PathBuf,
    memory: PathBuf,
}

impl TestDirectories {
    fn new(name: &str) -> Self {
        let base = std::env::temp_dir().join(format!("sinomo-memory-{name}-{}", Id::generate()));
        let source = base.join("source");
        let memory = base.join("state").join("memory");
        fs::create_dir_all(&source).expect("create source fixture");
        Self {
            base,
            source,
            memory,
        }
    }
}

impl Drop for TestDirectories {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

#[test]
fn local_markdown_round_trip_restarts_and_replaces_one_generation() {
    let dirs = TestDirectories::new("round-trip");
    let note = dirs.source.join("note.md");
    fs::write(&note, "alpha memory\nbeta line\n").expect("write source fixture");

    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    let first = store
        .index_root(&root.root_id)
        .expect("index first version");
    assert_eq!(first.published_files, 1);
    assert_eq!(first.published_generations.len(), 1);

    let query = MemoryQuery {
        task_hint: Some("alpha".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let before_serving = serving_files(&dirs.memory);
    let first_bundle = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("retrieve first version");
    assert_eq!(first_bundle.evidence.len(), 1);
    assert_eq!(first_bundle.evidence[0].content, "alpha memory");
    assert_eq!(first_bundle.coverage, EvidenceCoverage::Complete);
    assert_eq!(before_serving, serving_files(&dirs.memory));

    let restarted = IndexedFileMemoryStore::new(&dirs.memory);
    let after_restart = restarted
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("retrieve after restart");
    assert_eq!(after_restart.evidence, first_bundle.evidence);

    fs::write(&note, "gamma replacement memory\nbeta line\n").expect("replace source");
    let stale = restarted
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("stale current-only query is non-fatal");
    assert!(stale.evidence.is_empty());
    assert!(matches!(
        stale.coverage,
        EvidenceCoverage::Approximate { .. }
    ));

    let replacement = restarted
        .index_root(&root.root_id)
        .expect("publish replacement");
    assert_eq!(replacement.published_files, 1);
    assert_ne!(
        replacement.published_generations[0],
        first.published_generations[0]
    );
    let old = restarted
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("query removed term");
    assert!(old.evidence.is_empty());
    let replacement_query = MemoryQuery {
        task_hint: Some("gamma".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let current = restarted
        .retrieve(&MemoryAccessContext::cluster_local(), &replacement_query)
        .expect("query replacement");
    assert_eq!(current.evidence[0].content, "gamma replacement memory");

    let trace_page = restarted
        .trace_page(None, 2)
        .expect("paged retrieval trace");
    assert_eq!(trace_page.items.len(), 2);
    assert!(trace_page.continuation.is_some());

    let generation_files = files_below(&dirs.memory.join("partitions"))
        .into_iter()
        .filter(|path| path.to_string_lossy().contains("/generations/"))
        .count();
    assert_eq!(generation_files, 2, "immutable generations are retained");
}

#[test]
fn scan_and_query_continuations_are_bounded_and_snapshot_pinned() {
    let dirs = TestDirectories::new("continuation");
    fs::write(dirs.source.join("a.md"), "first evidence\n").expect("write a");
    fs::write(dirs.source.join("b.txt"), "second evidence\n").expect("write b");
    fs::write(dirs.source.join("ignored.bin"), [0_u8, 1, 2]).expect("write ignored");

    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    let mut cursor = None;
    let mut scanned_sources = Vec::new();
    while scanned_sources.len() < 2 {
        let page = store
            .scan_local_root_page(&root.root_id, cursor.as_ref(), 1)
            .expect("scan bounded traversal page");
        scanned_sources.extend(page.sources);
        cursor = page.continuation;
        assert!(
            cursor.is_some() || scanned_sources.len() == 2,
            "scan completed before returning both included sources"
        );
    }
    assert_ne!(
        scanned_sources[0].file_ref.object_id,
        scanned_sources[1].file_ref.object_id
    );

    store.index_root(&root.root_id).expect("index root");
    let query = MemoryQuery {
        max_partitions: 1,
        max_records: 16,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let first = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first bounded query");
    assert!(matches!(
        first.coverage,
        EvidenceCoverage::BudgetTruncated { .. }
    ));
    let continuation = first.continuation.clone().expect("honest continuation");
    let second = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: Some(continuation),
                ..query
            },
        )
        .expect("second bounded query");
    assert_eq!(first.evidence.len(), 1);
    assert_eq!(second.evidence.len(), 1);
    assert_ne!(
        first.evidence[0].anchor.source_version.file_ref.object_id,
        second.evidence[0].anchor.source_version.file_ref.object_id
    );

    let roots = store.root_page(None, 1).expect("paged roots");
    assert_eq!(roots.items, vec![root]);
}

#[cfg(unix)]
#[test]
fn local_rename_preserves_object_identity_but_replacement_does_not() {
    let dirs = TestDirectories::new("rename-identity");
    let first_path = dirs.source.join("first.md");
    let renamed_path = dirs.source.join("renamed.md");
    fs::write(&first_path, "rename identity evidence\n").expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("initial index");
    let first = store
        .source_page(&root.root_id, None, 8)
        .expect("initial sources")
        .items
        .into_iter()
        .next()
        .expect("initial source");

    fs::rename(&first_path, &renamed_path).expect("rename source");
    let rename = store.index_root(&root.root_id).expect("index rename");
    assert_eq!(rename.published_files, 1);
    assert_eq!(rename.tombstoned_files, 0);
    let renamed = store
        .source_page(&root.root_id, None, 8)
        .expect("renamed sources")
        .items
        .into_iter()
        .find(|source| source.freshness == SourceFreshness::Current)
        .expect("current renamed source");
    assert_eq!(
        renamed.version.file_ref.object_id,
        first.version.file_ref.object_id
    );
    assert_eq!(renamed.version.file_ref.path_hint, "renamed.md");

    let replacement_path = dirs.source.join("replacement.tmp");
    fs::write(&replacement_path, "replacement inode evidence\n").expect("stage replacement");
    fs::rename(&replacement_path, &renamed_path).expect("atomically replace source");
    let replacement = store.index_root(&root.root_id).expect("index replacement");
    assert_eq!(replacement.published_files, 1);
    assert_eq!(replacement.tombstoned_files, 1);
    let sources = store
        .source_page(&root.root_id, None, 8)
        .expect("replacement sources")
        .items;
    assert_eq!(sources.len(), 2);
    assert_eq!(
        sources
            .iter()
            .filter(|source| source.freshness == SourceFreshness::Current)
            .count(),
        1
    );
    assert_eq!(
        sources
            .iter()
            .filter(|source| source.freshness == SourceFreshness::Deleted)
            .count(),
        1
    );
}

#[test]
fn query_selectors_do_not_grant_source_scope_authority() {
    let dirs = TestDirectories::new("access-context");
    fs::write(dirs.source.join("private.md"), "task private evidence\n")
        .expect("write private source");
    let task_scope = MemoryScope::Task {
        task_id: "task-private".to_string(),
    };
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy {
                scope: task_scope.clone(),
                visibility: TrustDomain::new("trust.local").expect("trust domain"),
                allow_model_context: true,
                allow_relation_traversal: false,
            },
        ))
        .expect("register private root");
    store.index_root(&root.root_id).expect("index private root");
    let query = MemoryQuery {
        task_id: Some("task-private".to_string()),
        task_hint: Some("private".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };

    let denied = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("selector-only query");
    assert!(denied.evidence.is_empty());

    let mut authorized = MemoryAccessContext::cluster_local();
    authorized.allowed_scopes.push(task_scope);
    let allowed = store
        .retrieve(&authorized, &query)
        .expect("authorized query");
    assert_eq!(allowed.evidence.len(), 1);
    assert_eq!(allowed.evidence[0].content, "task private evidence");
}

#[test]
fn lease_and_policy_changes_publish_distinct_immutable_generations() {
    let dirs = TestDirectories::new("lease-policy-generation");
    let path = dirs.source.join("policy.md");
    let content = "stable content under changing lease and policy\n";
    fs::write(&path, content).expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let mut config = LocalIndexRootConfig::new(&dirs.source, IndexRootPolicy::cluster_local());
    let root = store
        .register_local_root(config.clone())
        .expect("register root");
    let first = store.index_root(&root.root_id).expect("first index");

    std::thread::sleep(std::time::Duration::from_millis(2));
    fs::write(&path, content).expect("touch source without changing content");
    let touched = store
        .index_root(&root.root_id)
        .expect("publish changed lease");
    assert_eq!(touched.published_files, 1);
    assert_ne!(
        first.published_generations[0],
        touched.published_generations[0]
    );
    let query = MemoryQuery {
        task_hint: Some("stable".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    assert_eq!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("query touched source")
            .evidence
            .len(),
        1
    );

    let task_scope = MemoryScope::Task {
        task_id: "tightened-task".to_string(),
    };
    config.policy.scope = task_scope.clone();
    store
        .register_local_root(config)
        .expect("tighten root policy");
    assert!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("old policy is fenced before reindex")
            .evidence
            .is_empty()
    );
    let mut task_access = MemoryAccessContext::cluster_local();
    task_access.allowed_scopes.push(task_scope.clone());
    assert!(
        store
            .retrieve(&task_access, &query)
            .expect("new policy has no generation before reindex")
            .evidence
            .is_empty()
    );
    let tightened = store
        .index_root(&root.root_id)
        .expect("publish tightened policy");
    assert_eq!(tightened.published_files, 1);
    assert_ne!(
        touched.published_generations[0],
        tightened.published_generations[0]
    );
    assert!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("cluster access after tightening")
            .evidence
            .is_empty()
    );
    assert_eq!(
        store
            .retrieve(&task_access, &query)
            .expect("task access after tightening")
            .evidence
            .len(),
        1
    );
}

#[test]
fn local_cursor_and_candidate_cannot_escape_the_registered_root() {
    let dirs = TestDirectories::new("root-confinement");
    fs::write(dirs.source.join("inside.md"), "inside evidence\n").expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    let malicious = LocalScanCursor {
        frames: vec![LocalScanFrame {
            relative_dir: "../".to_string(),
            after_name: None,
        }],
    };
    assert!(
        store
            .scan_local_root_page(&root.root_id, Some(&malicious), 1)
            .is_err()
    );

    let mut candidate = store
        .scan_local_root_page(&root.root_id, None, 1)
        .expect("valid scan")
        .sources
        .into_iter()
        .next()
        .expect("source candidate");
    candidate.absolute_path = dirs.base.join("outside.md");
    fs::write(&candidate.absolute_path, "outside evidence\n").expect("write outside file");
    assert!(store.publish_source_candidate(&candidate).is_err());
}

#[test]
fn evidence_continuation_resumes_at_the_first_unreturned_anchor() {
    let dirs = TestDirectories::new("anchor-continuation");
    fs::write(
        dirs.source.join("two-lines.md"),
        "shared first line\nshared second line\n",
    )
    .expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let query = MemoryQuery {
        task_hint: Some("shared".to_string()),
        max_records: 1,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let first = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first evidence page");
    assert_eq!(first.evidence[0].content, "shared first line");
    let second = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: first.continuation,
                ..query
            },
        )
        .expect("second evidence page");
    assert_eq!(second.evidence[0].content, "shared second line");
}

#[test]
fn evidence_is_ranked_before_record_limit_is_applied() {
    let dirs = TestDirectories::new("rank-before-limit");
    fs::write(dirs.source.join("weak.md"), "alpha weak evidence\n").expect("write weak");
    fs::write(
        dirs.source.join("strong.md"),
        "alpha beta strong evidence\n",
    )
    .expect("write strong");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index sources");
    let query = MemoryQuery {
        task_hint: Some("alpha beta".to_string()),
        max_records: 1,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };

    let first = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first ranked page");
    assert_eq!(first.trace.searched_partitions, 2);
    assert_eq!(first.evidence.len(), 1);
    assert_eq!(first.evidence[0].score, 200);
    assert_eq!(first.evidence[0].content, "alpha beta strong evidence");
    assert!(matches!(
        first.coverage,
        EvidenceCoverage::BudgetTruncated { .. }
    ));

    let second = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: first.continuation,
                ..query
            },
        )
        .expect("second ranked page");
    assert_eq!(second.evidence.len(), 1);
    assert_eq!(second.evidence[0].score, 100);
    assert_eq!(second.evidence[0].content, "alpha weak evidence");
    assert_eq!(second.coverage, EvidenceCoverage::Complete);
}

#[test]
fn ranked_evidence_continuations_preserve_later_partition_windows() {
    let dirs = TestDirectories::new("ranked-window-continuation");
    for index in 0..4 {
        fs::write(
            dirs.source.join(format!("note-{index}.md")),
            format!("shared evidence {index}\n"),
        )
        .expect("write source");
    }
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index sources");
    let mut query = MemoryQuery {
        task_hint: Some("shared".to_string()),
        max_partitions: 2,
        max_records: 1,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let mut returned = Vec::new();

    for _ in 0..8 {
        let page = store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("ranked evidence page");
        returned.extend(
            page.evidence
                .iter()
                .map(|evidence| evidence.anchor.source_version.file_ref.object_id.clone()),
        );
        match page.continuation {
            Some(continuation) => query.continuation = Some(continuation),
            None => break,
        }
    }

    assert_eq!(returned.len(), 4);
    assert_eq!(returned.iter().cloned().collect::<BTreeSet<_>>().len(), 4);
}

#[test]
fn evidence_continuation_position_is_authenticated() {
    let dirs = TestDirectories::new("continuation-authenticator");
    fs::write(
        dirs.source.join("two-lines.md"),
        "signed first line\nsigned second line\n",
    )
    .expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let query = MemoryQuery {
        task_hint: Some("signed".to_string()),
        max_records: 1,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let continuation = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first page")
        .continuation
        .expect("continuation");

    let mut anchor_tampered = continuation.clone();
    anchor_tampered.next_anchor = anchor_tampered.next_anchor.saturating_add(1);
    assert!(matches!(
        store.retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: Some(anchor_tampered),
                ..query.clone()
            },
        ),
        Err(sinomo_memory::MemoryError::InvalidMemoryContinuation { .. })
    ));

    let mut partition_tampered = continuation;
    partition_tampered.next_partition_id = Id::generate();
    assert!(matches!(
        store.retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: Some(partition_tampered),
                ..query
            },
        ),
        Err(sinomo_memory::MemoryError::InvalidMemoryContinuation { .. })
    ));
}

#[test]
fn pinned_continuation_cannot_bypass_local_policy_revocation() {
    let dirs = TestDirectories::new("continuation-policy-revocation");
    fs::write(
        dirs.source.join("revoked.md"),
        "revocable first line\nrevocable second line\n",
    )
    .expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let mut config = LocalIndexRootConfig::new(&dirs.source, IndexRootPolicy::cluster_local());
    let root = store
        .register_local_root(config.clone())
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let query = MemoryQuery {
        task_hint: Some("revocable".to_string()),
        max_records: 1,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let continuation = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first page")
        .continuation
        .expect("continuation");

    config.policy.allow_model_context = false;
    store
        .register_local_root(config)
        .expect("revoke model-context exposure locally");
    let resumed = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: Some(continuation),
                ..query
            },
        )
        .expect("old snapshot is reauthorized against current policy");
    assert!(resumed.evidence.is_empty());
    assert_eq!(resumed.trace.routed_partitions, 0);
    assert_eq!(resumed.trace.searched_partitions, 0);
}

#[test]
fn continuation_reads_its_immutable_router_snapshot_after_publication() {
    let dirs = TestDirectories::new("router-snapshot");
    fs::write(dirs.source.join("a.md"), "shared alpha\n").expect("write alpha");
    fs::write(dirs.source.join("b.md"), "shared beta\n").expect("write beta");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index two files");
    let query = MemoryQuery {
        task_hint: Some("shared".to_string()),
        max_partitions: 1,
        max_records: 8,
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    let first = store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first page");
    let continuation = first.continuation.expect("continuation");

    fs::write(dirs.source.join("c.md"), "shared gamma\n").expect("write gamma");
    store
        .index_root(&root.root_id)
        .expect("publish a newer router manifest");

    let resumed = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                continuation: Some(continuation),
                ..query
            },
        )
        .expect("resume pinned router snapshot");
    assert_eq!(resumed.evidence.len(), 1);
    assert!(!resumed.evidence[0].content.contains("gamma"));
}

#[test]
fn tightening_root_extensions_unroutes_previously_indexed_sources() {
    let dirs = TestDirectories::new("extension-policy");
    fs::write(dirs.source.join("included.txt"), "extension evidence\n").expect("write text source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let mut config = LocalIndexRootConfig::new(&dirs.source, IndexRootPolicy::cluster_local());
    let root = store
        .register_local_root(config.clone())
        .expect("register text root");
    store.index_root(&root.root_id).expect("index text source");
    let query = MemoryQuery {
        task_hint: Some("extension".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    assert_eq!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("query included source")
            .evidence
            .len(),
        1
    );

    config.include_extensions = vec!["md".to_string()];
    store
        .register_local_root(config)
        .expect("restrict root extensions");
    let report = store
        .index_root(&root.root_id)
        .expect("reconcile restricted root");
    assert_eq!(report.tombstoned_files, 1);
    assert!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("query after extension restriction")
            .evidence
            .is_empty()
    );
}

#[test]
fn lowering_source_size_limit_fences_and_tombstones_oversized_sources() {
    let dirs = TestDirectories::new("source-size-policy");
    fs::write(
        dirs.source.join("bounded.md"),
        "content that becomes too large for the tightened root\n",
    )
    .expect("write bounded source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let mut config = LocalIndexRootConfig::new(&dirs.source, IndexRootPolicy::cluster_local());
    config.max_source_bytes = 1_024;
    let root = store
        .register_local_root(config.clone())
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let query = MemoryQuery {
        task_hint: Some("content".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    assert_eq!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("query source")
            .evidence
            .len(),
        1
    );

    config.max_source_bytes = 8;
    store
        .register_local_root(config)
        .expect("tighten source size limit");
    assert!(
        store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("oversized source is fenced")
            .evidence
            .is_empty()
    );
    let report = store
        .index_root(&root.root_id)
        .expect("reconcile oversized source");
    assert_eq!(report.tombstoned_files, 1);
}

#[test]
fn continuation_budget_can_increase_without_changing_query_identity() {
    let dirs = TestDirectories::new("continuation-budget");
    fs::write(dirs.source.join("budget.md"), "budget evidence\n").expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let first = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                task_hint: Some("budget".to_string()),
                max_records: 0,
                max_bytes: 128 * 1024,
                ..MemoryQuery::default()
            },
        )
        .expect("zero-record page");
    let continuation = first.continuation.expect("budget continuation");
    let resumed = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                task_hint: Some("budget".to_string()),
                max_records: 1,
                max_bytes: 128 * 1024,
                continuation: Some(continuation),
                ..MemoryQuery::default()
            },
        )
        .expect("resume with larger budget");
    assert_eq!(resumed.evidence.len(), 1);
}

#[test]
fn long_lexical_terms_keep_exact_full_token_identity() {
    let dirs = TestDirectories::new("long-lexical-term");
    let prefix = "a".repeat(128);
    let first_term = format!("{prefix}x");
    let second_term = format!("{prefix}y");
    fs::write(
        dirs.source.join("long.md"),
        format!("{first_term} first\n{second_term} second\n"),
    )
    .expect("write long terms");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let bundle = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                task_hint: Some(first_term),
                max_bytes: 128 * 1024,
                ..MemoryQuery::default()
            },
        )
        .expect("query exact long term");
    assert_eq!(bundle.evidence.len(), 1);
    assert!(bundle.evidence[0].content.ends_with(" first"));
    assert_eq!(bundle.coverage, EvidenceCoverage::Complete);
}

#[test]
fn returned_evidence_bundle_never_exceeds_its_byte_budget() {
    let dirs = TestDirectories::new("evidence-byte-budget");
    fs::write(dirs.source.join("budget.md"), "bounded evidence\n").expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let zero_budget = store.retrieve(
        &MemoryAccessContext::cluster_local(),
        &MemoryQuery {
            task_hint: Some("bounded".to_string()),
            max_bytes: 0,
            ..MemoryQuery::default()
        },
    );
    assert!(matches!(
        zero_budget,
        Err(sinomo_memory::MemoryError::MemoryEvidenceBundleTooLarge { limit: 0, .. })
    ));

    let limit = 128 * 1024;
    let bundle = store
        .retrieve(
            &MemoryAccessContext::cluster_local(),
            &MemoryQuery {
                task_hint: Some("bounded".to_string()),
                max_bytes: limit,
                ..MemoryQuery::default()
            },
        )
        .expect("bounded result");
    assert!(serde_json::to_vec(&bundle).expect("serialize bundle").len() <= limit);
}

#[test]
fn trace_append_discards_uncommitted_torn_tail() {
    let dirs = TestDirectories::new("trace-torn-tail");
    fs::write(dirs.source.join("trace.md"), "trace evidence\n").expect("write source");
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register root");
    store.index_root(&root.root_id).expect("index source");
    let query = MemoryQuery {
        task_hint: Some("trace".to_string()),
        max_bytes: 128 * 1024,
        ..MemoryQuery::default()
    };
    store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("first traced query");
    let trace_path = dirs.memory.join("traces").join("retrieval.sobjlog");
    let mut trace_file = fs::OpenOptions::new()
        .append(true)
        .open(&trace_path)
        .expect("open trace log");
    trace_file
        .write_all(&128_u64.to_le_bytes())
        .and_then(|()| trace_file.write_all(b"torn"))
        .expect("append torn trace tail");
    trace_file.sync_all().expect("sync torn tail");

    store
        .retrieve(&MemoryAccessContext::cluster_local(), &query)
        .expect("append after recovery");
    let traces = store.trace_page(None, 8).expect("page recovered trace log");
    assert_eq!(traces.items.len(), 2);
    assert!(traces.continuation.is_none());
}

#[test]
fn large_root_uses_bounded_pages_reports_and_query_continuations() {
    let dirs = TestDirectories::new("large-root");
    for index in 0..257 {
        fs::write(
            dirs.source.join(format!("note-{index:04}.md")),
            format!("shared synthetic evidence {index}\n"),
        )
        .expect("write synthetic source");
    }
    let store = IndexedFileMemoryStore::new(&dirs.memory);
    let root = store
        .register_local_root(LocalIndexRootConfig::new(
            &dirs.source,
            IndexRootPolicy::cluster_local(),
        ))
        .expect("register large root");
    assert!(store.scan_local_root_page(&root.root_id, None, 0).is_err());
    assert!(
        store
            .scan_local_root_page(&root.root_id, None, 257)
            .is_err()
    );
    assert!(store.root_page(None, 0).is_err());

    let mut cursor = None;
    let mut scanned = 0usize;
    loop {
        let page = store
            .scan_local_root_page(&root.root_id, cursor.as_ref(), 17)
            .expect("bounded source page");
        assert!(page.sources.len() <= 17);
        scanned += page.sources.len();
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(scanned, 257);

    let report = store.index_root(&root.root_id).expect("index large root");
    assert_eq!(report.published_files, 257);
    assert_eq!(report.published_generation_count, 257);
    assert_eq!(report.published_generations.len(), 256);
    assert!(report.published_generations_truncated);

    let popular_term_ids = collect_evidence_object_ids(
        &store,
        MemoryQuery {
            task_hint: Some("shared".to_string()),
            max_partitions: 7,
            max_records: 32,
            max_bytes: 256 * 1024,
            ..MemoryQuery::default()
        },
    );
    assert_eq!(popular_term_ids.len(), 257);

    let authorized_enumeration_ids = collect_evidence_object_ids(
        &store,
        MemoryQuery {
            max_partitions: 7,
            max_records: 32,
            max_bytes: 256 * 1024,
            ..MemoryQuery::default()
        },
    );
    assert_eq!(authorized_enumeration_ids, popular_term_ids);
}

fn collect_evidence_object_ids(
    store: &IndexedFileMemoryStore,
    mut query: MemoryQuery,
) -> BTreeSet<Id> {
    let mut object_ids = BTreeSet::new();
    for page_index in 0..128 {
        let bundle = store
            .retrieve(&MemoryAccessContext::cluster_local(), &query)
            .expect("retrieve paged router evidence");
        for evidence in &bundle.evidence {
            object_ids.insert(evidence.anchor.source_version.file_ref.object_id.clone());
        }
        match bundle.continuation {
            Some(continuation) => {
                assert!(matches!(
                    bundle.coverage,
                    EvidenceCoverage::BudgetTruncated { .. }
                ));
                query.continuation = Some(continuation);
            }
            None => {
                assert_eq!(bundle.coverage, EvidenceCoverage::Complete);
                return object_ids;
            }
        }
        assert!(page_index < 127, "router pagination did not terminate");
    }
    unreachable!("bounded pagination loop either returns or fails its page bound")
}

fn serving_files(memory_root: &Path) -> BTreeMap<PathBuf, blake3::Hash> {
    ["catalog", "partitions", "routing"]
        .into_iter()
        .flat_map(|directory| files_below(&memory_root.join(directory)))
        .map(|path| {
            let hash = blake3::hash(&fs::read(&path).expect("read serving file"));
            (path, hash)
        })
        .collect()
}

fn files_below(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry.expect("read directory entry").path();
        if path.is_dir() {
            files.extend(files_below(&path));
        } else if path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    files
}
