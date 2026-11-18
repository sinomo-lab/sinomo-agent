//! Work between closed shards that each shard's own conversion settles, from
//! captured rows and scripted round answers.
use super::*;
use crate::authority::contribution_source::SourceHoldings;
use crate::authority::contribution_sources::TargetScope;
use crate::namespace_counts::tests::{object, source};
use crate::namespace_counts::{
    NamespaceCountAliasContribution, NamespaceCountMaintenanceLimits,
    NamespaceCountMaintenanceSource, ShardCountContributions,
};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsMetadataShardAssignment, DfsMetadataShardId, DfsNamespaceJobOrigin,
    DfsNamespaceObjectPrecondition, DfsObjectId, DfsVersion,
};

const CURRENT: &str = r#"{"read":"current-tables"}"#;
const OLD: &str = r#"{"read":"old-image"}"#;

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn catalog() -> Vec<DfsMetadataShardConfig> {
    let config = |shard: &str, assignment| DfsMetadataShardConfig {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        shard_id: shard.into(),
        primary_node_id: node(&format!("closed-{shard}")),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment,
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    };
    vec![
        config("root", DfsMetadataShardAssignment::Root),
        config(
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".into(),
            },
        ),
        config(
            "users",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/users".into(),
            },
        ),
    ]
}

fn shard<'a>(catalog: &'a [DfsMetadataShardConfig], id: &str) -> &'a DfsMetadataShardConfig {
    catalog.iter().find(|shard| shard.shard_id == id).unwrap()
}

fn alias(name: &str, path: &str, target: &DfsObjectId) -> DfsObjectMetadata {
    let mut alias = object(name, path, sinomo_dfs::DfsObjectKind::Hardlink);
    alias.link_target_object_id = Some(target.clone());
    alias
}

/// `shard`'s capture: `objects`, and `jobs` and `requests` in its rows.
fn capture(
    root: &Path,
    catalog: &[DfsMetadataShardConfig],
    shard: &str,
    objects: Vec<DfsObjectMetadata>,
    jobs: Vec<DfsNamespaceJobRecord>,
    requests: Vec<DfsNamespaceRequestRecord>,
) -> CapturedMetadataUnit {
    let captured = source(root, catalog, shard, objects).captured;
    let mut rows = captured.projected_rows.clone();
    rows.namespace_jobs = jobs;
    rows.namespace_jobs
        .sort_by(|left, right| left.job_id.cmp(&right.job_id));
    rows.namespace_requests = requests;
    CapturedMetadataUnit::from_test_projection(
        captured.source.clone(),
        rows,
        captured.proven_v7_terminals.clone(),
    )
    .unwrap()
}

fn job(
    job_id: &str,
    kind: DfsNamespaceJobKind,
    root_path: &str,
    target_path: Option<&str>,
    pending_frames: Vec<DfsNamespaceJobFrame>,
) -> DfsNamespaceJobRecord {
    DfsNamespaceJobRecord {
        job_id: job_id.into(),
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        origin: Some(DfsNamespaceJobOrigin {
            requester_node_id: node("closed-requester"),
            request_id: format!("request-{job_id}"),
            operation_id: format!("operation-{job_id}"),
        }),
        kind,
        status: DfsNamespaceJobStatus::Running,
        root_path: root_path.into(),
        target_path: target_path.map(Into::into),
        pending_frames,
        processed_object_count: 0,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
        completed_at: None,
        failure_reason: None,
    }
}

/// The Pending outer request of `job`, a hardlink of `existing` at `link`.
fn request(
    job: &DfsNamespaceJobRecord,
    shard: &str,
    existing: &str,
    link: &str,
) -> DfsNamespaceRequestRecord {
    let origin = job.origin.clone().unwrap();
    DfsNamespaceRequestRecord {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        requester_node_id: origin.requester_node_id,
        request_id: origin.request_id,
        operation_id: origin.operation_id,
        authority_shard_id: DfsMetadataShardId::new(shard),
        operation: sinomo_dfs::DfsNamespaceOperation::Hardlink {
            existing_path: existing.into(),
            link_path: link.into(),
        },
        command_digest: "closed-command".into(),
        status: DfsNamespaceRequestStatus::Pending,
        result: None,
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    }
}

/// A legacy creation of `canonical` at `alias`: the adopt frame is still
/// pending when `adopt_pending`.
fn legacy_creation(
    job_id: &str,
    canonical: &DfsObjectMetadata,
    alias: &DfsObjectMetadata,
    adopt_pending: bool,
) -> DfsNamespaceJobRecord {
    let mut frames = vec![DfsNamespaceJobFrame::CrossShardHardlinkComplete {
        target_object_id: canonical.object_id.clone(),
    }];
    if adopt_pending {
        frames.push(DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
            alias: alias.clone(),
            target_object_id: canonical.object_id.clone(),
            destination_primary_node_id: node("closed-services"),
        });
    }
    job(
        job_id,
        DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        &canonical.path,
        Some(&alias.path),
        frames,
    )
}

/// Every other shard answers the shard's questions holding nothing, from
/// `basis`, and `services` reports `aliases` as its live aliases.
fn round(
    questions: &RoundQuestions,
    basis: &str,
    aliases: &[&DfsObjectMetadata],
) -> ContributionRound {
    questions
        .per_source
        .iter()
        .map(|(source, question)| {
            let aliases = if source == "services" { aliases } else { &[] };
            let answer = SourceContributionAnswer {
                contributions: ShardCountContributions {
                    source_shard: DfsMetadataShardId::new(source),
                    target_shard: DfsMetadataShardId::new("users"),
                    aliases: aliases.iter().map(|alias| contribution(alias)).collect(),
                    child_directories: Vec::new(),
                    moving_directories: Vec::new(),
                },
                running_jobs_touching_target: Vec::new(),
                basis: basis.into(),
                held: SourceHoldings {
                    answered: question.len() as u64,
                    ..Default::default()
                },
            };
            (source.clone(), answer)
        })
        .collect()
}

fn contribution(alias: &DfsObjectMetadata) -> NamespaceCountAliasContribution {
    NamespaceCountAliasContribution {
        file_system_id: alias.file_system_id.clone(),
        alias_object_id: alias.object_id.clone(),
        alias_path: alias.path.clone(),
        target_object_id: alias.link_target_object_id.clone().unwrap(),
    }
}

fn limits() -> NamespaceCountMaintenanceLimits {
    NamespaceCountMaintenanceLimits {
        max_objects: 1000,
        max_encoded_bytes: 16 * 1024 * 1024,
        deadline: None,
    }
}

/// A legacy creation of the users shard whose alias the services shard owns
/// no longer stops users' conversion: it waits for the rounds and settles
/// from the live aliases services reports, by the same rule from services'
/// old image as from its current tables. The adopted alias live at its path
/// completes the job and its request and is counted; an alias services never
/// adopted aborts the job and rejects its request; the adopted identity live
/// at another path fails closed. With the adopt frame popped, the alias must
/// be live at its path.
#[test]
fn a_legacy_creation_whose_alias_another_shard_owns_settles_from_its_live_aliases() {
    let root = sinomo_testdir::TestDir::new("closed-remote-creation");
    let catalog = catalog();
    let users = shard(&catalog, "users");
    let doc = object("closed-doc", "/users/doc", sinomo_dfs::DfsObjectKind::File);
    let link = alias("closed-link", "/services/link", &doc.object_id);
    let objects = vec![
        object(
            "closed-users",
            "/users",
            sinomo_dfs::DfsObjectKind::Directory,
        ),
        doc.clone(),
    ];
    for adopt_pending in [true, false] {
        let creation = legacy_creation("closed-creation", &doc, &link, adopt_pending);
        let captured = capture(
            root.path().join(format!("{adopt_pending}")).as_path(),
            &catalog,
            "users",
            objects.clone(),
            vec![creation.clone()],
            vec![request(&creation, "users", "/users/doc", "/services/link")],
        );
        let settled = settle_own_jobs(&catalog, users, &captured).unwrap();
        assert_eq!(settled.pending_pairs, vec![creation.job_id.clone()]);
        let questions = RoundQuestions::new(
            &catalog,
            users,
            &settled_unit(&captured, &settled).unwrap(),
            &settled,
        )
        .unwrap();
        let mut moved = link.clone();
        moved.path = "/services/elsewhere".into();
        for basis in [OLD, CURRENT] {
            for (live, completed) in [(Some(&link), true), (None, false)] {
                if !adopt_pending && !completed {
                    let error = settle_pairs(&questions, &round(&questions, basis, &[]))
                        .unwrap_err()
                        .to_string();
                    assert!(error.contains("holds no live alias at its path"), "{error}");
                    continue;
                }
                let answers = round(&questions, basis, live.as_slice());
                let unit = with_pair_settlement(&captured, &settled, &questions, &answers).unwrap();
                let rows = &unit.projected_rows;
                let jobs = rows
                    .namespace_jobs
                    .iter()
                    .map(|job| {
                        (
                            job.job_id.as_str(),
                            job.status.clone(),
                            job.pending_frames.len(),
                        )
                    })
                    .collect::<Vec<_>>();
                let expected =
                    completed.then_some(("closed-creation", DfsNamespaceJobStatus::Completed, 0));
                assert_eq!(
                    jobs,
                    expected.into_iter().collect::<Vec<_>>(),
                    "{basis} {completed}"
                );
                let [request] = rows.namespace_requests.as_slice() else {
                    panic!("one request")
                };
                assert_eq!(request.status, DfsNamespaceRequestStatus::Completed);
                assert_eq!(
                    matches!(request.result, Some(DfsNamespaceResult::Applied)),
                    completed,
                    "{:?}",
                    request.result
                );
                let counted = NamespaceCountMaintenanceSource {
                    captured: Arc::new(unit),
                };
                let plan =
                    crate::namespace_counts::contributions::plan_shard_namespace_count_maintenance(
                        &catalog,
                        &counted,
                        &[answers["services"].contributions.clone()],
                        limits(),
                    )
                    .unwrap();
                let count = plan
                    .counted_objects(&counted, limits())
                    .unwrap()
                    .into_iter()
                    .find(|object| object.object_id == doc.object_id)
                    .unwrap()
                    .namespace_link_count;
                assert_eq!(count, Some(if completed { 2 } else { 1 }), "{basis}");
            }
            let error = settle_pairs(&questions, &round(&questions, basis, &[&moved]))
                .unwrap_err()
                .to_string();
            let expected = if adopt_pending {
                "adopted alias identity elsewhere"
            } else {
                "holds no live alias at its path"
            };
            assert!(error.contains(expected), "{error}");
        }
    }
}

/// A closed shard's old image marks the jobs its own conversion settles from
/// its rows: a legacy creation, a fence-commit-only creation and a fence it
/// keeps on its own canonical target, each touching the target shard. Their
/// outer requests are left to them; other work touching the target is
/// reported as before. The target settles the marked jobs per pair and is
/// still held by the rest.
#[test]
fn an_old_image_marks_the_jobs_its_own_conversion_settles() {
    let root = sinomo_testdir::TestDir::new("closed-marked");
    let catalog = catalog();
    let file = object(
        "closed-file",
        "/services/file",
        sinomo_dfs::DfsObjectKind::File,
    );
    let mut committed = file.clone();
    committed.version = DfsVersion::from_legacy_named("closed-file-committed").unwrap();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: file.object_id.clone(),
        version: file.version.clone(),
    };
    let creation = legacy_creation(
        "closed-legacy-creation",
        &file,
        &alias("closed-users-link", "/users/link", &file.object_id),
        true,
    );
    let commit = job(
        "closed-local-commit",
        DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        "/services/file",
        Some("/users/other-link"),
        vec![DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
            fence_job_id: "closed-local-fence".into(),
            source_path: "/users/other-link".into(),
            expected_target: expected.clone(),
            committed_target: committed.clone(),
        }],
    );
    let mut fence = job(
        "closed-kept-fence",
        DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
        "/services/file",
        None,
        vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path: "/users/alias".into(),
            expected_target: expected,
            committed_target: committed,
        }],
    );
    fence.origin = None;
    let scan = job(
        "closed-clear",
        DfsNamespaceJobKind::ClearDirectoryRecursive,
        "/services/tree",
        None,
        vec![DfsNamespaceJobFrame::ScanDirectory {
            path: "/users/tree".into(),
            cursor: None,
        }],
    );
    let captured = capture(
        root.path(),
        &catalog,
        "services",
        vec![
            object(
                "closed-services",
                "/services",
                sinomo_dfs::DfsObjectKind::Directory,
            ),
            file.clone(),
        ],
        vec![
            creation.clone(),
            commit.clone(),
            fence.clone(),
            scan.clone(),
        ],
        vec![
            request(&creation, "services", "/services/file", "/users/link"),
            request(&scan, "services", "/services/file", "/users/scan-link"),
        ],
    );
    let rows = &captured.projected_rows;
    let scope = TargetScope {
        file_system_id: &file.file_system_id,
        catalog: &catalog,
        target: "users",
    };
    let work = scope.unsettled_work(rows).unwrap();
    let reported = |work: &[SourceJobTouchingTarget]| {
        work.iter()
            .map(|work| work.job_id.clone())
            .collect::<BTreeSet<_>>()
    };
    let requester = node("closed-requester");
    let scan_request = format!("{requester}:request-closed-clear");
    let creation_request = format!("{requester}:request-closed-legacy-creation");
    assert!(reported(&work).contains(&creation_request), "{work:?}");
    assert!(reported(&work).contains(&scan_request), "{work:?}");
    let marked = mark_settled_by_own_conversion(rows, &catalog, "services", work).unwrap();
    assert_eq!(
        reported(&marked),
        BTreeSet::from([
            "closed-clear".to_owned(),
            "closed-kept-fence".to_owned(),
            "closed-legacy-creation".to_owned(),
            "closed-local-commit".to_owned(),
            scan_request,
        ]),
        "the settled jobs' request is left to its job"
    );
    let users_capture = capture(
        root.path().join("users").as_path(),
        &catalog,
        "users",
        vec![object(
            "closed-users",
            "/users",
            sinomo_dfs::DfsObjectKind::Directory,
        )],
        Vec::new(),
        Vec::new(),
    );
    let users = shard(&catalog, "users");
    let settled = settle_own_jobs(&catalog, users, &users_capture).unwrap();
    let questions = RoundQuestions::new(&catalog, users, &users_capture, &settled).unwrap();
    for work in &marked {
        let settled_by_source = [&creation, &commit, &fence]
            .iter()
            .any(|job| job.job_id == work.job_id);
        assert_eq!(
            marked_settled_by_own_conversion(work),
            settled_by_source,
            "{work:?}"
        );
        assert_eq!(
            questions.settles_per_pair(work),
            settled_by_source,
            "{work:?}"
        );
    }
}

/// Every other shard answers from `basis` holding everything it is asked.
fn holding_round(questions: &RoundQuestions, basis: &str) -> ContributionRound {
    questions
        .per_source
        .iter()
        .map(|(source, question)| {
            let answer = SourceContributionAnswer {
                contributions: ShardCountContributions {
                    source_shard: DfsMetadataShardId::new(source),
                    target_shard: DfsMetadataShardId::new("users"),
                    aliases: Vec::new(),
                    child_directories: Vec::new(),
                    moving_directories: Vec::new(),
                },
                running_jobs_touching_target: Vec::new(),
                basis: basis.into(),
                held: SourceHoldings {
                    answered: question.len() as u64,
                    objects: question.objects.clone(),
                    directories: question.directories.clone(),
                    jobs: question.jobs.clone(),
                },
            };
            (source.clone(), answer)
        })
        .collect()
}

/// An alias job of users whose only step left commits a fence that closed
/// services holds stays Running in users' image, with its outer request
/// Pending, once services has recorded its conversion: users does not wait
/// for services to convert, and its counts admit the kept job. Before
/// services has recorded its conversion, users waits for that record.
#[test]
fn a_fence_commit_on_a_closed_shard_that_recorded_its_conversion_stays_running() {
    let root = sinomo_testdir::TestDir::new("closed-kept-commit");
    let catalog = catalog();
    let users = shard(&catalog, "users");
    let canonical = object(
        "closed-file",
        "/services/file",
        sinomo_dfs::DfsObjectKind::File,
    );
    let mut committed = canonical.clone();
    committed.version = DfsVersion::from_legacy_named("closed-file-committed").unwrap();
    let mutation = job(
        "closed-unlink",
        DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
        "/users/alias",
        Some("/users/alias"),
        vec![DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
            fence_job_id: "closed-services-fence".into(),
            source_path: "/users/alias".into(),
            expected_target: DfsNamespaceObjectPrecondition {
                object_id: canonical.object_id.clone(),
                version: canonical.version.clone(),
            },
            committed_target: committed,
        }],
    );
    let captured = capture(
        root.path(),
        &catalog,
        "users",
        vec![
            object(
                "closed-users",
                "/users",
                sinomo_dfs::DfsObjectKind::Directory,
            ),
            alias("closed-kept", "/users/kept", &canonical.object_id),
        ],
        vec![mutation.clone()],
        vec![request(&mutation, "users", "/users/alias", "/users/alias")],
    );
    let settled = settle_own_jobs(&catalog, users, &captured).unwrap();
    assert_eq!(settled.pending_pairs, vec![mutation.job_id.clone()]);
    let questions = RoundQuestions::new(
        &catalog,
        users,
        &settled_unit(&captured, &settled).unwrap(),
        &settled,
    )
    .unwrap();
    for basis in [OLD, r#"{"read":"old-image","storage":"absent"}"#] {
        assert!(
            matches!(
                settle_pairs(&questions, &holding_round(&questions, basis)).unwrap(),
                PairDecision::Wait(evidence) if evidence.contains("has not recorded its conversion")
            ),
            "{basis}"
        );
    }
    let recorded = holding_round(&questions, r#"{"read":"old-image","storage":"blake3:00"}"#);
    let unit = with_pair_settlement(&captured, &settled, &questions, &recorded).unwrap();
    let rows = &unit.projected_rows;
    assert_eq!(rows.namespace_jobs, vec![mutation]);
    assert_eq!(
        rows.namespace_requests
            .iter()
            .map(|request| request.status.clone())
            .collect::<Vec<_>>(),
        vec![DfsNamespaceRequestStatus::Pending]
    );
    let counted = NamespaceCountMaintenanceSource {
        captured: Arc::new(unit),
    };
    crate::namespace_counts::contributions::plan_shard_namespace_count_maintenance(
        &catalog,
        &counted,
        &[],
        limits(),
    )
    .unwrap();
}
