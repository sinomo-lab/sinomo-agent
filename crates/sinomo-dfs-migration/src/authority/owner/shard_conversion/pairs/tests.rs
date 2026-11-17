//! Per-pair checks and settlements of one shard's conversion, from captured
//! rows and scripted round answers.
use super::*;
use crate::authority::contribution_source::{SourceHoldings, SourceJobTouchingTarget};
use crate::namespace_counts::tests::{object, source};
use crate::namespace_counts::{NamespaceCountMaintenanceLimits, ShardCountContributions};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsMetadataShardAssignment, DfsMetadataShardId, DfsNamespaceJobOrigin,
    DfsNamespaceObjectPrecondition, DfsObjectId, DfsObjectKind, DfsVersion,
};

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn config(shard: &str, assignment: DfsMetadataShardAssignment) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        shard_id: shard.into(),
        primary_node_id: node(&format!("pairs-{shard}")),
        replica_node_ids: Vec::new(),
        epoch: 1,
        assignment,
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    }
}

fn catalog() -> Vec<DfsMetadataShardConfig> {
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

fn users(catalog: &[DfsMetadataShardConfig]) -> &DfsMetadataShardConfig {
    catalog
        .iter()
        .find(|shard| shard.shard_id == "users")
        .unwrap()
}

fn id(name: &str) -> DfsObjectId {
    DfsObjectId::from_legacy_named(name).unwrap()
}

fn alias(name: &str, path: &str, target: &DfsObjectId) -> DfsObjectMetadata {
    let mut alias = object(name, path, DfsObjectKind::Hardlink);
    alias.link_target_object_id = Some(target.clone());
    alias
}

/// The users shard's capture: its root directory, `objects`, and `jobs`
/// and `requests` in its rows.
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
            requester_node_id: node("pairs-requester"),
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

/// The Pending outer request of `job`.
fn request(
    job: &DfsNamespaceJobRecord,
    operation: sinomo_dfs::DfsNamespaceOperation,
) -> DfsNamespaceRequestRecord {
    let origin = job.origin.clone().unwrap();
    DfsNamespaceRequestRecord {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        requester_node_id: origin.requester_node_id,
        request_id: origin.request_id,
        operation_id: origin.operation_id,
        authority_shard_id: DfsMetadataShardId::new("users"),
        operation,
        command_digest: "pairs-command".into(),
        status: DfsNamespaceRequestStatus::Pending,
        result: None,
        posix_coordination_result: None,
        recovery_attempts: 0,
        last_recovery_error: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn hardlink(existing: &str, link: &str) -> sinomo_dfs::DfsNamespaceOperation {
    sinomo_dfs::DfsNamespaceOperation::Hardlink {
        existing_path: existing.into(),
        link_path: link.into(),
    }
}

/// A Running fence the users shard holds over its canonical `target` for the
/// alias path `source_path`, and the generation the fence commits.
fn fence(
    fence_job_id: &str,
    source_path: &str,
    target: &DfsObjectMetadata,
) -> (DfsNamespaceJobRecord, DfsObjectMetadata) {
    let mut committed = target.clone();
    committed.namespace_link_count = Some(target.namespace_link_count.unwrap_or(1) + 1);
    committed.changed_at_unix_nanos = 1;
    committed.version = DfsVersion::from_legacy_named(format!("{fence_job_id}-committed")).unwrap();
    let mut fence = job(
        fence_job_id,
        DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
        &target.path,
        None,
        vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path: source_path.into(),
            expected_target: DfsNamespaceObjectPrecondition {
                object_id: target.object_id.clone(),
                version: target.version.clone(),
            },
            committed_target: committed.clone(),
        }],
    );
    fence.origin = None;
    (fence, committed)
}

fn fence_commit(
    fence_job_id: &str,
    source_path: &str,
    target: &DfsObjectMetadata,
    committed: &DfsObjectMetadata,
) -> DfsNamespaceJobFrame {
    DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
        fence_job_id: fence_job_id.into(),
        source_path: source_path.into(),
        expected_target: DfsNamespaceObjectPrecondition {
            object_id: target.object_id.clone(),
            version: target.version.clone(),
        },
        committed_target: committed.clone(),
    }
}

fn answer(
    source: &str,
    basis: &str,
    held: SourceHoldings,
    jobs: Vec<SourceJobTouchingTarget>,
) -> SourceContributionAnswer {
    SourceContributionAnswer {
        contributions: ShardCountContributions {
            source_shard: DfsMetadataShardId::new(source),
            target_shard: DfsMetadataShardId::new("users"),
            aliases: Vec::new(),
            child_directories: Vec::new(),
        },
        running_jobs_touching_target: jobs,
        basis: basis.into(),
        held,
    }
}

const CURRENT: &str = r#"{"read":"current-tables"}"#;
const OLD: &str = r#"{"read":"old-image"}"#;

/// Every source answers its question holding exactly `holds`.
fn round(questions: &RoundQuestions, holds: &[(&str, &str)], basis: &str) -> ContributionRound {
    questions
        .per_source
        .iter()
        .map(|(source, question)| {
            let has = |entry: &str| holds.contains(&(source.as_str(), entry));
            let held = SourceHoldings {
                answered: question.len() as u64,
                objects: question
                    .objects
                    .iter()
                    .filter(|id| has(id.as_str()))
                    .cloned()
                    .collect(),
                directories: question
                    .directories
                    .iter()
                    .filter(|path| has(path))
                    .cloned()
                    .collect(),
                jobs: question
                    .jobs
                    .iter()
                    .filter(|job| has(job))
                    .cloned()
                    .collect(),
            };
            (source.clone(), answer(source, basis, held, Vec::new()))
        })
        .collect()
}

fn limits() -> NamespaceCountMaintenanceLimits {
    NamespaceCountMaintenanceLimits {
        max_objects: 1000,
        max_encoded_bytes: 16 * 1024 * 1024,
        deadline: None,
    }
}

fn questions_of(
    captured: &CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    shard: &str,
) -> (SettledJobs, RoundQuestions) {
    let target = catalog
        .iter()
        .find(|config| config.shard_id == shard)
        .unwrap();
    let settled = settle_own_jobs(catalog, target, captured).unwrap();
    let questions = RoundQuestions::new(
        catalog,
        target,
        &settled_unit(captured, &settled).unwrap(),
        &settled,
    )
    .unwrap();
    (settled, questions)
}

/// An alias whose target is none of the shard's rows is asked of every other
/// shard: held by one, the round settles; held by none, the shard's own rows
/// included, it is a dangling alias that stops this shard with its alias and
/// target named.
#[test]
fn an_alias_target_no_shard_holds_is_a_typed_stop() {
    let root = sinomo_testdir::TestDir::new("pairs-dangling");
    let catalog = catalog();
    let local = object("pairs-local-file", "/users/file", DfsObjectKind::File);
    let remote = id("pairs-remote-target");
    let captured = capture(
        root.path(),
        &catalog,
        "users",
        vec![
            object("pairs-users", "/users", DfsObjectKind::Directory),
            local.clone(),
            alias("pairs-local-alias", "/users/local-alias", &local.object_id),
            alias("pairs-remote-alias", "/users/remote-alias", &remote),
        ],
        Vec::new(),
        Vec::new(),
    );
    let (_, questions) = questions_of(&captured, &catalog, "users");
    for source in ["root", "services"] {
        assert_eq!(
            questions.for_source(source).objects,
            vec![remote.clone()],
            "{source} is asked for the foreign target only"
        );
    }
    let held = round(&questions, &[("services", remote.as_str())], CURRENT);
    assert!(matches!(
        settle_pairs(&questions, &held).unwrap(),
        PairDecision::Settled(_)
    ));
    let error = settle_pairs(&questions, &round(&questions, &[], CURRENT))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("dangling alias /users/remote-alias") && error.contains(remote.as_str()),
        "{error}"
    );
}

/// The routing hash of a path, as DFS's name-hash ranges assign it.
fn name_hash(path: &str) -> u64 {
    path.bytes().fold(0xcbf2_9ce4_8422_2325, |hash: u64, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Under hash routing a file's parent can be another shard's. Its owner is
/// asked whether the parent is a live directory there; absent, the shard's
/// conversion stops.
#[test]
fn a_parent_its_owner_does_not_hold_is_a_typed_stop() {
    let root = sinomo_testdir::TestDir::new("pairs-parent");
    let hash = name_hash("/docs/file");
    let catalog = vec![
        config("root", DfsMetadataShardAssignment::Root),
        config(
            "hashed",
            DfsMetadataShardAssignment::NameHashRange {
                start_inclusive: hash,
                end_exclusive: hash.wrapping_add(1),
            },
        ),
    ];
    let file = object("pairs-hashed-file", "/docs/file", DfsObjectKind::File);
    let owner = |path: &str| {
        sinomo_dfs::select_metadata_shard_config_for_path(&file.file_system_id, &catalog, path)
            .unwrap()
            .unwrap()
            .shard_id
    };
    assert_eq!(
        (owner("/docs/file"), owner("/docs")),
        ("hashed".to_owned(), "root".to_owned())
    );
    let captured = capture(
        root.path(),
        &catalog,
        "hashed",
        vec![file],
        Vec::new(),
        Vec::new(),
    );
    let (_, questions) = questions_of(&captured, &catalog, "hashed");
    assert_eq!(
        questions.for_source("root").directories,
        vec!["/docs".to_owned()]
    );
    assert!(matches!(
        settle_pairs(
            &questions,
            &round(&questions, &[("root", "/docs")], CURRENT)
        )
        .unwrap(),
        PairDecision::Settled(_)
    ));
    let error = settle_pairs(&questions, &round(&questions, &[], CURRENT))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("/docs/file has no live parent directory /docs in shard root"),
        "{error}"
    );
}

/// A settled legacy hardlink job's outer request settles with it: Applied
/// for a job settled Completed, Rejected for an aborted one. Left Pending,
/// the shard's settled rows could not be counted.
#[test]
fn a_settled_hardlink_job_settles_its_outer_request() {
    let root = sinomo_testdir::TestDir::new("pairs-request");
    let catalog = catalog();
    let target = id("pairs-services-file");
    let link = alias("pairs-adopted-alias", "/users/alias", &target);
    let creation = job(
        "pairs-legacy-creation",
        DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        "/services/file",
        Some("/users/alias"),
        vec![
            DfsNamespaceJobFrame::CrossShardHardlinkComplete {
                target_object_id: target.clone(),
            },
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias: link.clone(),
                target_object_id: target.clone(),
                destination_primary_node_id: node("pairs-users"),
            },
        ],
    );
    let pending = request(&creation, hardlink("/services/file", "/users/alias"));
    for (adopted, completed) in [(true, true), (false, false)] {
        let mut objects = vec![object("pairs-users", "/users", DfsObjectKind::Directory)];
        if adopted {
            objects.push(link.clone());
        }
        let captured = capture(
            root.path().join(format!("{adopted}")).as_path(),
            &catalog,
            "users",
            objects,
            vec![creation.clone()],
            vec![pending.clone()],
        );
        let settled = settle_own_jobs(&catalog, users(&catalog), &captured).unwrap();
        let unit = settled_unit(&captured, &settled).unwrap();
        let [request] = unit.projected_rows.namespace_requests.as_slice() else {
            panic!("one request")
        };
        assert_eq!(request.status, DfsNamespaceRequestStatus::Completed);
        assert_eq!(
            matches!(request.result, Some(DfsNamespaceResult::Applied)),
            completed,
            "{:?}",
            request.result
        );
        assert!(matches!(
            request.result,
            Some(DfsNamespaceResult::Applied | DfsNamespaceResult::Rejected { .. })
        ));
        let counted = crate::namespace_counts::NamespaceCountMaintenanceSource {
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
}

/// The fence rule, from the coordinator's recorded phase.
#[test]
fn a_target_fence_settles_from_its_coordinators_phase() {
    for (phase, outcome) in [
        (
            CoordinatorPhase::Running("cross-shard-hardlink-target-fence-commit;fence=f"),
            FenceOutcome::Apply,
        ),
        (
            CoordinatorPhase::Running("cross-shard-hardlink-alias-commit;fence=f"),
            FenceOutcome::Keep,
        ),
        (
            CoordinatorPhase::Running("cross-shard-hardlink-target-fence-install;fence=f"),
            FenceOutcome::Keep,
        ),
        (CoordinatorPhase::SettledCompleted, FenceOutcome::Apply),
        (CoordinatorPhase::SettledAborted, FenceOutcome::Drop),
        (CoordinatorPhase::Unknown, FenceOutcome::Keep),
    ] {
        assert_eq!(settle_target_fence(phase), outcome, "{phase:?}");
    }
}

/// A fence the shard holds for another shard's alias job settles per pair,
/// from the phase that shard reports: past the alias change, the committed
/// target is applied and the fence dropped; before it, the fence is kept and
/// the shard counts from its pre-fence rows.
#[test]
fn a_remote_coordinators_phase_applies_or_keeps_the_fence() {
    let root = sinomo_testdir::TestDir::new("pairs-remote-fence");
    let catalog = catalog();
    let doc = object("pairs-doc", "/users/doc", DfsObjectKind::File);
    let (held, committed) = fence("pairs-fence", "/services/alias", &doc);
    let captured = capture(
        root.path(),
        &catalog,
        "users",
        vec![
            object("pairs-users", "/users", DfsObjectKind::Directory),
            doc.clone(),
        ],
        vec![held.clone()],
        Vec::new(),
    );
    let (settled, questions) = questions_of(&captured, &catalog, "users");
    assert_eq!(
        settled.pending_pairs,
        vec![held.job_id.clone()],
        "the fence waits for its pair"
    );
    for (phase, applied) in [
        (
            "cross-shard-hardlink-target-fence-commit;fence=pairs-fence",
            true,
        ),
        ("cross-shard-hardlink-alias-commit;fence=pairs-fence", false),
    ] {
        let mut answers = round(&questions, &[], CURRENT);
        answers
            .get_mut("services")
            .unwrap()
            .running_jobs_touching_target
            .push(SourceJobTouchingTarget {
                job_id: "services-alias-job".into(),
                family: "hardlink-alias-mutation-across-shard-primaries".into(),
                phase: phase.into(),
            });
        let unit = with_pair_settlement(&captured, &settled, &questions, &answers).unwrap();
        let rows = &unit.projected_rows;
        let current = rows
            .objects
            .iter()
            .find(|object| object.object_id == doc.object_id)
            .unwrap();
        assert_eq!(
            rows.namespace_jobs
                .iter()
                .any(|job| job.job_id == held.job_id),
            !applied,
            "{phase}"
        );
        assert_eq!(*current == committed, applied, "{phase}");
        // Past the alias change the services alias is live and counted; before
        // it, it is not, and the kept fence leaves the count from before it.
        let contributions = applied
            .then(|| ShardCountContributions {
                source_shard: DfsMetadataShardId::new("services"),
                target_shard: DfsMetadataShardId::new("users"),
                aliases: vec![crate::namespace_counts::NamespaceCountAliasContribution {
                    file_system_id: sinomo_dfs::sifs_file_system_id(),
                    alias_object_id: id("pairs-services-alias"),
                    alias_path: "/services/alias".into(),
                    target_object_id: doc.object_id.clone(),
                }],
                child_directories: Vec::new(),
            })
            .into_iter()
            .collect::<Vec<_>>();
        let counted = crate::namespace_counts::NamespaceCountMaintenanceSource {
            captured: Arc::new(unit),
        };
        let plan = crate::namespace_counts::contributions::plan_shard_namespace_count_maintenance(
            &catalog,
            &counted,
            &contributions,
            limits(),
        )
        .unwrap();
        let counted_doc = plan
            .counted_objects(&counted, limits())
            .unwrap()
            .into_iter()
            .find(|object| object.object_id == doc.object_id)
            .unwrap();
        assert_eq!(
            counted_doc.namespace_link_count,
            Some(if applied { 2 } else { 1 }),
            "{phase}"
        );
    }
}

/// A creation job whose only step left is the fence commit, with its fence
/// and canonical target in this shard, settles forward: the committed target
/// is in the image, the fence is dropped, and the job and its request are
/// Completed.
#[test]
fn a_local_fence_commit_settles_forward_in_the_image() {
    let root = sinomo_testdir::TestDir::new("pairs-local-commit");
    let catalog = catalog();
    let doc = object("pairs-doc", "/users/doc", DfsObjectKind::File);
    let (held, committed) = fence("pairs-creation-fence", "/services/doc-alias", &doc);
    let creation = job(
        "pairs-creation",
        DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        "/users/doc",
        Some("/services/doc-alias"),
        vec![fence_commit(
            "pairs-creation-fence",
            "/services/doc-alias",
            &doc,
            &committed,
        )],
    );
    let captured = capture(
        root.path(),
        &catalog,
        "users",
        vec![
            object("pairs-users", "/users", DfsObjectKind::Directory),
            doc.clone(),
        ],
        vec![held.clone(), creation.clone()],
        vec![request(
            &creation,
            hardlink("/users/doc", "/services/doc-alias"),
        )],
    );
    let (settled, questions) = questions_of(&captured, &catalog, "users");
    assert!(settled.pending_pairs.is_empty());
    assert_eq!(
        settled.settled,
        vec![(creation.job_id.clone(), "completed".to_owned())]
    );
    let unit = with_pair_settlement(
        &captured,
        &settled,
        &questions,
        &round(&questions, &[], CURRENT),
    )
    .unwrap();
    let rows = &unit.projected_rows;
    assert_eq!(
        rows.objects
            .iter()
            .find(|object| object.object_id == doc.object_id),
        Some(&committed)
    );
    let [job] = rows.namespace_jobs.as_slice() else {
        panic!("{:?}", rows.namespace_jobs)
    };
    assert_eq!(
        (job.job_id.as_str(), job.status.clone()),
        (creation.job_id.as_str(), DfsNamespaceJobStatus::Completed)
    );
    assert!(job.pending_frames.is_empty());
    assert!(matches!(
        rows.namespace_requests[0].result,
        Some(DfsNamespaceResult::Applied)
    ));
    // A canonical target that changed while its fence was held settles in
    // neither direction.
    let mut changed = doc.clone();
    changed.version = DfsVersion::from_legacy_named("pairs-doc-changed").unwrap();
    let moved = capture(
        root.path().join("changed").as_path(),
        &catalog,
        "users",
        vec![
            object("pairs-users", "/users", DfsObjectKind::Directory),
            changed,
        ],
        vec![held, creation],
        Vec::new(),
    );
    let error = settle_own_jobs(&catalog, users(&catalog), &moved)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("changed while its fence was held"),
        "{error}"
    );
}

/// An alias job whose only step left commits a fence another shard holds
/// settles forward once that shard no longer holds the fence; while a closed
/// shard holds it the round waits, and while a current shard holds it the
/// conversion stops, since releasing it needs a fence-release step.
#[test]
fn a_remote_fence_commit_settles_when_its_fence_is_released() {
    let root = sinomo_testdir::TestDir::new("pairs-remote-commit");
    let catalog = catalog();
    let canonical = object("pairs-services-file", "/services/file", DfsObjectKind::File);
    let link = alias(
        "pairs-renamed-alias",
        "/users/renamed",
        &canonical.object_id,
    );
    let (_, committed) = fence("pairs-services-fence", "/users/alias", &canonical);
    let mutation = job(
        "pairs-mutation",
        DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
        "/users/alias",
        Some("/users/renamed"),
        vec![fence_commit(
            "pairs-services-fence",
            "/users/alias",
            &canonical,
            &committed,
        )],
    );
    let captured = capture(
        root.path(),
        &catalog,
        "users",
        vec![
            object("pairs-users", "/users", DfsObjectKind::Directory),
            link,
        ],
        vec![mutation.clone()],
        vec![request(
            &mutation,
            hardlink("/users/alias", "/users/renamed"),
        )],
    );
    let (settled, questions) = questions_of(&captured, &catalog, "users");
    assert_eq!(settled.pending_pairs, vec![mutation.job_id.clone()]);
    assert_eq!(
        questions.for_source("services").jobs,
        vec!["pairs-services-fence".to_owned()]
    );
    let holding = [
        ("services", "pairs-services-fence"),
        ("services", canonical.object_id.as_str()),
    ];
    let error = settle_pairs(&questions, &round(&questions, &holding, CURRENT))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("current shard services still holds"),
        "{error}"
    );
    assert!(matches!(
        settle_pairs(&questions, &round(&questions, &holding, OLD)).unwrap(),
        PairDecision::Wait(evidence) if evidence.contains("settles when shard services converts")
    ));
    let released = round(
        &questions,
        &[("services", canonical.object_id.as_str())],
        CURRENT,
    );
    let unit = with_pair_settlement(&captured, &settled, &questions, &released).unwrap();
    let rows = &unit.projected_rows;
    assert_eq!(
        rows.namespace_jobs[0].status,
        DfsNamespaceJobStatus::Completed
    );
    assert!(matches!(
        rows.namespace_requests[0].result,
        Some(DfsNamespaceResult::Applied)
    ));
    let counted = crate::namespace_counts::NamespaceCountMaintenanceSource {
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

/// A source job whose recorded phase names a fence this shard holds is
/// settled per pair from that phase, so it does not hold the rounds; a job
/// touching the shard without naming one of its fences still does.
#[test]
fn a_job_naming_a_fence_this_shard_holds_is_settled_per_pair() {
    let root = sinomo_testdir::TestDir::new("pairs-fence-per-pair");
    let catalog = catalog();
    let doc = object("pairs-doc", "/users/doc", DfsObjectKind::File);
    let (held, _) = fence("pairs-fence", "/services/alias", &doc);
    let captured = capture(
        root.path(),
        &catalog,
        "users",
        vec![
            object("pairs-users", "/users", DfsObjectKind::Directory),
            doc.clone(),
        ],
        vec![held],
        Vec::new(),
    );
    let (_, questions) = questions_of(&captured, &catalog, "users");
    let job = |phase: &str| SourceJobTouchingTarget {
        job_id: "services-alias-job".into(),
        family: "hardlink-alias-mutation-across-shard-primaries".into(),
        phase: phase.into(),
    };
    assert!(questions.settles_per_pair(&job("cross-shard-hardlink-alias-commit;fence=pairs-fence")));
    assert!(questions.settles_per_pair(&job("cross-shard-hardlink-target-fence-commit;fence=pairs-fence")));
    assert!(!questions.settles_per_pair(&job("cross-shard-hardlink-alias-commit;fence=another-fence")));
    assert!(!questions.settles_per_pair(&job("cross-shard-dir-scan")));
}

/// The catalog with a `projects` shard nested under `/users`: its shard root
/// `/users/projects` has its parent in `users`.
fn nested_catalog() -> Vec<DfsMetadataShardConfig> {
    let mut catalog = catalog();
    catalog.push(config(
        "projects",
        DfsMetadataShardAssignment::PathPrefix {
            prefix: "/users/projects".into(),
        },
    ));
    catalog
}

/// The users shard holds, Reserved, the participant of a projects move of
/// `/users/projects` to `/services/old-projects`: the move takes one child
/// directory away from `/users`, which counts it (4 = 2 + `/users/sub` +
/// `/users/projects`) until the participant applies.
struct HeldParticipantCase {
    catalog: Vec<DfsMetadataShardConfig>,
    captured: CapturedMetadataUnit,
    participant: sinomo_dfs::DfsParentCountParticipant,
}

fn held_participant_case(root: &Path) -> HeldParticipantCase {
    let catalog = nested_catalog();
    let shard = |id: &str| catalog.iter().find(|shard| shard.shard_id == id).unwrap().clone();
    let mut parent = object("pairs-users", "/users", DfsObjectKind::Directory);
    parent.namespace_link_count = Some(4);
    let mut successor = parent.clone();
    successor.namespace_link_count = Some(3);
    successor.version = DfsVersion::from_legacy_named("pairs-users-after-move").unwrap();
    successor.modified_at_unix_nanos = 7;
    successor.changed_at_unix_nanos = 7;
    let participant = sinomo_dfs::DfsParentCountParticipant {
        participant_id: String::new(),
        binding: sinomo_dfs::DfsParentCountBinding {
            job_id: "projects-move".into(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            root_path: "/users/projects".into(),
            target_path: "/services/old-projects".into(),
            coordinator_scope: shard("projects"),
            source: object("pairs-projects", "/users/projects", DfsObjectKind::Directory),
            expected_destination: sinomo_dfs::DfsNamespaceDestinationPrecondition::Absent,
            origin: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
        },
        parent_scope: shard("users"),
        expected: parent.clone(),
        successor,
        delta: -1,
    }
    .with_derived_id_for_test()
    .unwrap();
    participant.validate_declaration().unwrap();
    let held = participant.recorded_job(
        sinomo_dfs::DfsParentCountState::Reserved,
        OffsetDateTime::UNIX_EPOCH,
    );
    let captured = capture(
        root,
        &catalog,
        "users",
        vec![
            parent,
            object("pairs-users-sub", "/users/sub", DfsObjectKind::Directory),
        ],
        vec![held],
        Vec::new(),
    );
    HeldParticipantCase {
        catalog,
        captured,
        participant,
    }
}

/// The coordinating move as the projects shard reports it, with `phase`.
fn coordinating_move(phase: &str) -> SourceJobTouchingTarget {
    SourceJobTouchingTarget {
        job_id: "projects-move".into(),
        family: "rename-directory-across-shard-primaries".into(),
        phase: phase.into(),
    }
}

/// The projects shard's contribution to users: `/users/projects` while it
/// is still there.
fn projects_contribution(moved: bool) -> ShardCountContributions {
    ShardCountContributions {
        source_shard: DfsMetadataShardId::new("projects"),
        target_shard: DfsMetadataShardId::new("users"),
        aliases: Vec::new(),
        child_directories: (!moved)
            .then(|| crate::namespace_counts::NamespaceCountChildDirectoryContribution {
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                parent_path: "/users".into(),
                child_object_id: id("pairs-projects"),
                child_path: "/users/projects".into(),
            })
            .into_iter()
            .collect(),
    }
}

/// Each recorded plan phase decides the participant: Issued or Settling
/// apply its successor, unless an Issued move's frames still hold its root's
/// move out of the parent; Preparing, Aborting and no reported phase keep it.
#[test]
fn a_held_parent_participant_settles_from_its_coordinators_plan_phase() {
    use sinomo_dfs::DfsParentCountPhase as Phase;
    let reported = |plan, effect_pending| ParticipantCoordinator::Reported {
        plan: Some(plan),
        effect_pending,
    };
    for (coordinator, outcome) in [
        (reported(Phase::Settling, false), ParticipantOutcome::Apply),
        (reported(Phase::Issued, false), ParticipantOutcome::Apply),
        (reported(Phase::Issued, true), ParticipantOutcome::Keep),
        (reported(Phase::Preparing, false), ParticipantOutcome::Keep),
        (reported(Phase::Preparing, true), ParticipantOutcome::Keep),
        (reported(Phase::Aborting, false), ParticipantOutcome::Keep),
        (
            ParticipantCoordinator::Reported {
                plan: None,
                effect_pending: false,
            },
            ParticipantOutcome::Keep,
        ),
        (ParticipantCoordinator::Unknown, ParticipantOutcome::Keep),
    ] {
        assert_eq!(
            settle_parent_participant(coordinator.clone()),
            outcome,
            "{coordinator:?}"
        );
    }
}

/// A participant the shard holds Reserved for another shard's move waits
/// for the rounds, then settles from the plan phase the move reports: an
/// applied successor is recorded Applied as the participant's own apply
/// records it, with its version row, and a kept participant leaves the row
/// and the reservation. Either way the shard's counts agree with what the
/// other shards contribute.
#[test]
fn a_held_parent_participant_applies_or_keeps_its_successor_per_phase() {
    let root = sinomo_testdir::TestDir::new("pairs-held-participant");
    let case = held_participant_case(root.path());
    let (settled, questions) = questions_of(&case.captured, &case.catalog, "users");
    let id = case.participant.participant_id.clone();
    assert_eq!(
        settled.pending_pairs,
        vec![id.clone()],
        "the participant waits for its pair"
    );
    let named = format!(";participant={id}");
    let pending = format!(";participant-effect-pending={id}");
    for (phase, applied) in [
        (
            format!("cross-shard-dir-finish;parent-counts=settling{named};parked-on-target"),
            true,
        ),
        (format!("cross-shard-dir-scan;parent-counts=issued{named}"), true),
        (
            format!("cross-shard-adopt;parent-counts=issued{named}{pending};parked-on-target"),
            false,
        ),
        (
            format!("cross-shard-dir-start;parent-counts=preparing{named}{pending};parked-on-target"),
            false,
        ),
        (
            format!("cross-shard-dir-finish;parent-counts=aborting{named};parked-on-target"),
            false,
        ),
        // A move that names another participant reports no phase for this one.
        (
            "cross-shard-dir-finish;parent-counts=settling;participant=another".into(),
            false,
        ),
    ] {
        let mut answers = round(&questions, &[], CURRENT);
        answers
            .get_mut("projects")
            .unwrap()
            .running_jobs_touching_target
            .push(coordinating_move(&phase));
        assert!(matches!(
            settle_pairs(&questions, &answers).unwrap(),
            PairDecision::Settled(_)
        ));
        let unit = with_pair_settlement(&case.captured, &settled, &questions, &answers).unwrap();
        let rows = &unit.projected_rows;
        let parent = rows
            .objects
            .iter()
            .find(|object| object.object_id == case.participant.expected.object_id)
            .unwrap();
        let job = rows
            .namespace_jobs
            .iter()
            .find(|job| job.job_id == id)
            .unwrap();
        if applied {
            assert_eq!(*parent, case.participant.successor, "{phase}");
            let recorded = case.participant.recorded_job(
                sinomo_dfs::DfsParentCountState::Applied,
                settled.settled_at.unwrap(),
            );
            assert_eq!(*job, recorded, "{phase}");
            assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
            assert!(
                rows.versions.contains(&case.participant.applied_version().unwrap()),
                "{phase}"
            );
        } else {
            assert_eq!(*parent, case.participant.expected, "{phase}");
            assert!(
                crate::namespace_counts::contributions::is_kept_parent_participant(job),
                "{phase}"
            );
        }
        // The applied move has taken `/users/projects` away; a kept one has
        // not, and projects still contributes it.
        let counted = crate::namespace_counts::NamespaceCountMaintenanceSource {
            captured: Arc::new(unit),
        };
        let plan = crate::namespace_counts::contributions::plan_shard_namespace_count_maintenance(
            &case.catalog,
            &counted,
            &[projects_contribution(applied)],
            limits(),
        )
        .unwrap_or_else(|failure| panic!("{phase}: {failure}"));
        let counted_parent = plan
            .counted_objects(&counted, limits())
            .unwrap()
            .into_iter()
            .find(|object| object.object_id == case.participant.expected.object_id)
            .unwrap();
        assert_eq!(
            counted_parent.namespace_link_count,
            Some(if applied { 3 } else { 4 }),
            "{phase}"
        );
    }
}
