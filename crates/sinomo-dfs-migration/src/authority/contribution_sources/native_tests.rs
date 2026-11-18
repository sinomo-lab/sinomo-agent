//! Native multi-node reads of contribution sources. The services shard is
//! the source S on its own primary; root and users are targets on another
//! node. An old S is modeled as a shard whose storage record turned
//! Converting, with ordinary opens held so only migration reads its copy;
//! its capture is taken the way its own conversion takes it.
use super::*;
use crate::authority::native_fixtures::canonical_migration_runtime;
use crate::metadata::canonical_generation::CapturedMetadataUnit;
use crate::namespace_counts::NamespaceCountMaintenanceLimits;
use sinomo_dfs::test_support::DfsTestRuntime;
use sinomo_dfs::{
    DfsMetadataConversionSource, DfsMetadataShardConversion, DfsNamespaceJobKind, DfsObjectKind,
    DfsStoragePreparationScope,
};
use std::sync::atomic::{AtomicBool, Ordering};

const SOURCE: &str = "services";

struct Cluster {
    native: DfsTestRuntime,
    nodes: Vec<NodeId>,
    held: Arc<AtomicBool>,
    fs: DfsFileSystemId,
    contexts: Vec<sinomo_module::ComponentContext>,
}

/// Two nodes: `services` on the second, root and users on the first, which
/// also holds the cluster-KV voter and replicates services. The second node
/// can stop while KV and the targets stay available.
fn start(name: &str) -> Cluster {
    let nodes = DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    let held = Arc::new(AtomicBool::new(false));
    let factory_held = Arc::clone(&held);
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    move |context, scope, lifecycle| {
                        let binding = storage_preparation_provider(context, scope, lifecycle)?;
                        let provider = binding.provider;
                        let held = Arc::clone(&factory_held);
                        Ok(sinomo_dfs::DfsStoragePreparationProviderBinding {
                            readiness: binding.readiness,
                            provider: Arc::new(move |root, unit, budget| {
                                if held.load(Ordering::Acquire)
                                    && matches!(
                                        unit,
                                        DfsStoragePreparationScope::Metadata(_)
                                            | DfsStoragePreparationScope::MetadataGeneration { .. }
                                    )
                                {
                                    return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                                        processed_records: 0,
                                    });
                                }
                                provider(root, unit, budget)
                            }),
                        })
                    },
                ))
                .unwrap(),
            ),
            Arc::new(staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    let fs = native.file_system().file_system_id;
    let replicas = nodes
        .iter()
        .filter(|node| **node != nodes[1])
        .cloned()
        .collect();
    native
        .reassign_metadata_shard(&fs, SOURCE, nodes[1].clone(), replicas)
        .unwrap();
    Cluster {
        native,
        nodes,
        held,
        fs,
        contexts: Vec::new(),
    }
}

/// S owns a file, an alias of it, an alias of a users file and a directory.
fn write_namespace(cluster: &Cluster) {
    let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
    let options = || sinomo_dfs::DfsWriteOptions::replicated_eventual(cluster.nodes.clone());
    client
        .posix_write_file("/users/doc", b"users bytes", options())
        .unwrap();
    client
        .posix_write_file("/services/file", b"services bytes", options())
        .unwrap();
    client
        .posix_hardlink("/services/file", "/services/alias")
        .unwrap();
    client
        .posix_hardlink("/users/doc", "/services/doc-alias")
        .unwrap();
    client.posix_mkdir("/services/dir").unwrap();
}

fn owner(cluster: &Cluster, node: usize) -> Arc<MigrationOwner> {
    let context = cluster
        .native
        .component_context(&cluster.nodes[node])
        .unwrap();
    canonical_migration_runtime(&context)
}

/// Stop every node and reopen it with ordinary metadata opens held, so the
/// migration owners are the only readers of the shard copies.
fn hold_ordinary_opens(cluster: &mut Cluster) {
    for node in cluster.nodes.clone().iter().rev() {
        cluster.native.stop_node(node).unwrap();
    }
    cluster.held.store(true, Ordering::Release);
    for node in cluster.nodes.clone() {
        let result = cluster.native.reopen_node_with_startup_action(
            node,
            Box::new(|_| Err("ordinary opens held while migration reads".into())),
        );
        let failure = result.unwrap_err();
        assert!(
            failure.contains("ordinary opens held while migration reads"),
            "{failure}"
        );
    }
    cluster.contexts = cluster
        .nodes
        .iter()
        .map(|node| cluster.native.component_context(node).unwrap())
        .collect();
}

fn shard(owner: &MigrationOwner, fs: &DfsFileSystemId, shard_id: &str) -> DfsMetadataShardConfig {
    owner
        .authority
        .assignments(std::slice::from_ref(fs))
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == shard_id)
        .unwrap()
}

/// Close `shard_id` the way its conversion does first: its storage record
/// turns Converting under an unsealed attempt at its current epoch.
fn close(owner: &MigrationOwner, fs: &DfsFileSystemId, shard_id: &str) {
    let key = sinomo_dfs::metadata_shard_storage_key(fs, shard_id);
    let (rows, _) = owner
        .authority
        .read_exact(std::slice::from_ref(&key))
        .unwrap();
    let before = rows[0]
        .entry
        .clone()
        .expect("a born shard carries a storage record");
    let config = shard(owner, fs, shard_id);
    let source = DfsMetadataConversionSource {
        source_identity: "contribution-test-source".into(),
        source_format: None,
        source_journal_sequence: 1,
        source_state_fingerprint: "contribution-test-state".into(),
        source_cut_digest: "a".repeat(64),
        pending_mapping_digest: "b".repeat(64),
    };
    let attempt_id = sinomo_dfs::metadata_shard_conversion_attempt_id(
        fs,
        shard_id,
        &config.assignment,
        config.epoch,
        &config.primary_node_id,
        &source,
        None,
    )
    .unwrap();
    let record = DfsMetadataShardStorage::converting(
        &config,
        DfsMetadataShardConversion {
            attempt_id,
            epoch: config.epoch,
            primary_node_id: config.primary_node_id.clone(),
            takeover: None,
            source,
            seal: None,
            contributions_digest: None,
        },
    );
    record.validate_against(&config).unwrap();
    let committed = owner
        .authority
        .commit(KvTxnRequest::new(
            KvRequestId::generate(),
            KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: key.clone(),
                    value: before.value.clone(),
                }],
                success: vec![KvOp::Put {
                    key,
                    value: sinomo_core::kv::encode_kv_value(&record).unwrap(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        ))
        .unwrap();
    NativeImportAuthority::require_committed(&committed).unwrap();
}

/// The converter's bounded capture of `shard_id`'s local copy, step by step,
/// into the stage its conversion reads. The copy is current-format, so the
/// per-shard capture intent asks the converter to treat it as a source.
fn capture(
    owner: &MigrationOwner,
    fs: &DfsFileSystemId,
    shard_id: &str,
) -> Arc<CapturedMetadataUnit> {
    let root = owner.metadata_root.join(shard_id);
    let id = DfsMetadataShardId::new(shard_id);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let (progress, captured) = owner
            .metadata
            .capture_shard_conversion_source(&root, &root, &id, fs, &[], 256)
            .unwrap();
        if let Some(captured) = captured {
            return captured;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the capture did not complete: {progress:?}"
        );
        std::thread::yield_now();
    }
}

fn read(cluster: &Cluster, reader: &MigrationOwner, target: &str) -> SourceContributionRead {
    let source = NativeContributionSource::new(Arc::clone(&reader.authority));
    let target = shard(reader, &cluster.fs, target);
    let from = shard(reader, &cluster.fs, SOURCE);
    block_native(
        &cluster.native.runtime_handle(),
        source.read(&target, &from),
    )
    .unwrap()
}

fn answered(read: SourceContributionRead) -> SourceContributionAnswer {
    match read {
        SourceContributionRead::Answered(answer) => answer,
        waiting => panic!("the source did not answer: {waiting:?}"),
    }
}

fn assert_waits_on_source(read: &SourceContributionRead, cluster: &Cluster) {
    let SourceContributionRead::Waiting { nodes, keys } = read else {
        panic!("an unanswered source must wait, not answer: {read:?}");
    };
    assert!(nodes.contains(&cluster.nodes[1]), "{nodes:?}");
    for key in [
        sinomo_dfs::metadata_shard_key(&cluster.fs, SOURCE),
        sinomo_dfs::metadata_shard_write_set_key(&cluster.fs, SOURCE),
        sinomo_dfs::metadata_shard_storage_key(&cluster.fs, SOURCE),
    ] {
        assert!(keys.contains(&key), "{key} missing from {keys:?}");
    }
}

fn limits() -> NamespaceCountMaintenanceLimits {
    NamespaceCountMaintenanceLimits {
        max_objects: 100_000,
        max_encoded_bytes: 64 * 1024 * 1024,
        deadline: None,
    }
}

fn paths(answer: &SourceContributionAnswer) -> (BTreeSet<String>, BTreeSet<(String, String)>) {
    (
        answer
            .contributions
            .aliases
            .iter()
            .map(|alias| alias.alias_path.clone())
            .collect(),
        answer
            .contributions
            .child_directories
            .iter()
            .map(|child| (child.parent_path.clone(), child.child_path.clone()))
            .collect(),
    )
}

#[test]
fn native_old_source_answers_from_its_primary_capture() {
    let mut cluster = start("contribution-source-old-image");
    write_namespace(&cluster);
    let reader = owner(&cluster, 0);
    let open = answered(read(&cluster, &reader, "root"));
    drop(reader);
    hold_ordinary_opens(&mut cluster);
    let (reader, primary) = (owner(&cluster, 0), owner(&cluster, 1));
    close(&reader, &cluster.fs, SOURCE);
    // Until S's conversion records its capture, S's primary has no image
    // answer, and the source waits on S rather than reading as empty.
    assert_waits_on_source(&read(&cluster, &reader, "root"), &cluster);
    let captured = capture(&primary, &cluster.fs, SOURCE);
    let old = answered(read(&cluster, &reader, "root"));
    let catalog = reader
        .authority
        .assignments(std::slice::from_ref(&cluster.fs))
        .unwrap();
    let expected = crate::namespace_counts::contributions::contributions_from_captured(
        &captured,
        &catalog,
        "root",
        limits(),
    )
    .unwrap();
    assert_eq!(
        old.contributions,
        finish_answer(expected, Vec::new(), String::new()).contributions
    );
    assert!(old.running_jobs_touching_target.is_empty(), "{old:?}");
    // The old image's answer names the same contributions as S's current
    // tables did before S closed, read from a different basis.
    assert_eq!(old.contributions, open.contributions);
    assert_ne!(old.basis, open.basis);
    let (aliases, children) = paths(&old);
    assert_eq!(
        aliases,
        BTreeSet::from(["/services/alias".into(), "/services/doc-alias".into()])
    );
    assert_eq!(children, BTreeSet::from([("/".into(), "/services".into())]));
    assert!(
        old.basis.contains("old-image") && old.basis.contains(&captured.source.source_identity),
        "{}",
        old.basis
    );
    assert_eq!(
        answered(read(&cluster, &reader, "root")),
        old,
        "an unchanged image answers identically"
    );
    let users = answered(read(&cluster, &reader, "users"));
    assert_eq!(
        users.contributions.aliases, old.contributions.aliases,
        "every live alias, whatever its target"
    );
    assert!(
        users.contributions.child_directories.is_empty(),
        "/services's parent belongs to root"
    );
    drop((reader, primary, captured));
    cluster.contexts.clear();
    cluster.native.close().unwrap();
}

#[test]
fn native_open_source_answers_from_current_tables() {
    let cluster = start("contribution-source-current-tables");
    write_namespace(&cluster);
    let reader = owner(&cluster, 0);
    let first = answered(read(&cluster, &reader, "root"));
    let (aliases, children) = paths(&first);
    assert_eq!(
        aliases,
        BTreeSet::from(["/services/alias".into(), "/services/doc-alias".into()])
    );
    assert_eq!(children, BTreeSet::from([("/".into(), "/services".into())]));
    assert!(first.running_jobs_touching_target.is_empty(), "{first:?}");
    assert!(first.basis.contains("current-tables"), "{}", first.basis);
    let doc = first
        .contributions
        .aliases
        .iter()
        .find(|alias| alias.alias_path == "/services/doc-alias")
        .unwrap();
    let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
    let users = shard(&reader, &cluster.fs, "users");
    let DfsMetadataQueryResult::StatAtPathV2 {
        object: Some(target),
    } = client
        .query_metadata_authority(
            &users,
            DfsMetadataSelection::StatAtPathV2 {
                path: "/users/doc".into(),
            },
        )
        .unwrap()
    else {
        panic!("the users file is live");
    };
    assert_eq!(doc.target_object_id, target.object_id);
    assert_eq!(
        answered(read(&cluster, &reader, "root")),
        first,
        "unchanged tables answer identically"
    );
    client
        .posix_hardlink("/services/file", "/services/second-alias")
        .unwrap();
    drop(client);
    let second = answered(read(&cluster, &reader, "root"));
    assert_ne!(second.basis, first.basis, "a write in S moves its prefix");
    assert_eq!(second.contributions.aliases.len(), 3);
    drop(reader);
    cluster.native.close().unwrap();
}

#[test]
fn native_unreachable_source_primary_waits_and_answers_on_return() {
    let mut cluster = start("contribution-source-unreachable");
    write_namespace(&cluster);
    hold_ordinary_opens(&mut cluster);
    let (reader, primary) = (owner(&cluster, 0), owner(&cluster, 1));
    close(&reader, &cluster.fs, SOURCE);
    capture(&primary, &cluster.fs, SOURCE);
    let answer = answered(read(&cluster, &reader, "root"));
    drop(primary);
    cluster.contexts.remove(1);
    let source_primary = cluster.nodes[1].clone();
    cluster.native.stop_node(&source_primary).unwrap();
    assert_waits_on_source(&read(&cluster, &reader, "root"), &cluster);
    let result = cluster.native.reopen_node_with_startup_action(
        source_primary,
        Box::new(|_| Err("ordinary opens held while migration reads".into())),
    );
    assert!(
        result
            .unwrap_err()
            .contains("ordinary opens held while migration reads")
    );
    // S's primary answers again from the capture its conversion recorded.
    assert_eq!(answered(read(&cluster, &reader, "root")), answer);
    drop(reader);
    cluster.contexts.clear();
    cluster.native.close().unwrap();
}

/// A legacy cross-shard hardlink creation coordinated by S: the canonical
/// object stays at `/services/file`, the alias belongs at `alias_path`.
/// Current DFS refuses to run its completion frame, so the job keeps its
/// recorded frames until S's conversion settles it.
fn legacy_creation_job(
    file: &DfsObjectMetadata,
    alias_path: &str,
    destination: &NodeId,
) -> DfsNamespaceJobRecord {
    let mut alias = file.clone();
    alias.object_id = sinomo_dfs::DfsObjectId::generate();
    alias.path = alias_path.into();
    alias.kind = DfsObjectKind::Hardlink;
    alias.link_target_object_id = Some(file.object_id.clone());
    alias.namespace_link_count = None;
    alias.version = sinomo_dfs::DfsVersion::generate();
    let now = time::OffsetDateTime::now_utc();
    DfsNamespaceJobRecord {
        job_id: format!("legacy-creation{}", alias_path.replace('/', "-")),
        file_system_id: file.file_system_id.clone(),
        origin: None,
        kind: DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        status: DfsNamespaceJobStatus::Running,
        root_path: file.path.clone(),
        target_path: Some(alias_path.into()),
        pending_frames: vec![
            DfsNamespaceJobFrame::CrossShardHardlinkComplete {
                target_object_id: file.object_id.clone(),
            },
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias,
                target_object_id: file.object_id.clone(),
                destination_primary_node_id: destination.clone(),
            },
        ],
        processed_object_count: 0,
        created_at: now,
        updated_at: now,
        completed_at: None,
        failure_reason: None,
    }
}

#[test]
fn native_old_source_reports_only_its_jobs_touching_the_target() {
    let mut cluster = start("contribution-source-legacy-jobs");
    write_namespace(&cluster);
    {
        let reader = owner(&cluster, 0);
        let services = shard(&reader, &cluster.fs, SOURCE);
        let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
        let DfsMetadataQueryResult::StatAtPathV2 { object: Some(file) } = client
            .query_metadata_authority(
                &services,
                DfsMetadataSelection::StatAtPathV2 {
                    path: "/services/file".into(),
                },
            )
            .unwrap()
        else {
            panic!("the services file is live");
        };
        drop(client);
        let to_root = legacy_creation_job(&file, "/root-alias", &cluster.nodes[0]);
        let to_users = legacy_creation_job(&file, "/users/users-alias", &cluster.nodes[0]);
        let payload = sinomo_dfs::DfsPreparedOperationPayload::for_metadata_effects(
            sinomo_dfs::DfsPreparedOperationScope {
                file_system_id: cluster.fs.clone(),
                shard_id: DfsMetadataShardId::new(SOURCE),
                owner_epoch: services.epoch,
                operation_id: "contribution-test-legacy-jobs".into(),
            },
            Vec::new(),
            Vec::new(),
            vec![
                sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceJob { job: to_root },
                sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceJob { job: to_users },
            ],
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        cluster
            .native
            .with_main_maintenance_service(&cluster.nodes[1], |service| {
                sinomo_dfs::test_support::publish_current_payload(service, payload).unwrap();
            })
            .unwrap();
    }
    hold_ordinary_opens(&mut cluster);
    let (reader, primary) = (owner(&cluster, 0), owner(&cluster, 1));
    close(&reader, &cluster.fs, SOURCE);
    let captured = capture(&primary, &cluster.fs, SOURCE);
    let running = captured
        .projected_rows
        .namespace_jobs
        .iter()
        .filter(|job| job.status == DfsNamespaceJobStatus::Running)
        .map(|job| job.job_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        running,
        BTreeSet::from([
            "legacy-creation-root-alias".into(),
            "legacy-creation-users-users-alias".into()
        ]),
        "the old image holds both legacy jobs as recorded"
    );
    let catalog = reader
        .authority
        .assignments(std::slice::from_ref(&cluster.fs))
        .unwrap();
    assert!(
        crate::namespace_counts::contributions::contributions_from_captured(
            &captured,
            &catalog,
            "root",
            limits()
        )
        .is_err(),
        "the census refuses a source with unsettled jobs"
    );
    for (target, job_id) in [
        ("root", "legacy-creation-root-alias"),
        ("users", "legacy-creation-users-users-alias"),
    ] {
        let answer = answered(read(&cluster, &reader, target));
        assert_eq!(
            answer.running_jobs_touching_target,
            vec![SourceJobTouchingTarget {
                job_id: job_id.into(),
                family: "hardlink-across-shard-primaries".into(),
                // S's own conversion settles a legacy creation.
                phase: "cross-shard-hardlink-adopt;settled-by-own-conversion".into(),
            }],
            "only the job whose alias path {target} owns is reported"
        );
        let expected = crate::namespace_counts::contributions::contributions_beside_unsettled_work(
            &captured,
            &catalog,
            target,
            limits(),
        )
        .unwrap();
        assert_eq!(
            answer.contributions,
            finish_answer(expected, Vec::new(), String::new()).contributions
        );
    }
    drop((reader, primary, captured));
    cluster.contexts.clear();
    cluster.native.close().unwrap();
}
