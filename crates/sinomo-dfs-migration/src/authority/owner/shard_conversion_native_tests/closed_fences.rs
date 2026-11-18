//! Two closed shards whose alias unlinks cross convert in either order.
//!
//! Each shard coordinates the unlink of an alias naming the other's file,
//! and each job's only step left commits the fence the other shard holds on
//! that file. The shard that converts first keeps its job Running in its
//! image; once it serves, the job's commit parks on the other shard's closed
//! storage. The other shard converts next: it applies its fence from the
//! parked job's phase, read from the first shard's current tables, and
//! completes its own job forward, since the first shard no longer holds the
//! fence. Once it serves, the parked commit finds its fence committed.
//!
//! The fixture records each unlink's job as its coordinator holds it once
//! the alias is gone, before the fence commit, with the fence the canonical
//! shard holds for it. Each canonical row already is the generation its
//! fence commits, which a held fence admits, so every count stands as the
//! live aliases give it.
use super::*;
use sinomo_dfs::{
    DfsMetadataQueryResult, DfsMetadataSelection, DfsNamespaceJobFrame, DfsNamespaceJobKind,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsNamespaceObjectPrecondition,
    DfsObjectMetadata,
};

/// One alias unlink as recorded: the job its alias's shard coordinates and
/// the fence its canonical target's shard holds for it.
struct Unlink {
    job: DfsNamespaceJobRecord,
    fence: DfsNamespaceJobRecord,
}

fn query(
    cluster: &Cluster,
    config: &DfsMetadataShardConfig,
    selection: DfsMetadataSelection,
) -> DfsMetadataQueryResult {
    let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
    client.query_metadata_authority(config, selection).unwrap()
}

fn stat(cluster: &Cluster, config: &DfsMetadataShardConfig, path: &str) -> DfsObjectMetadata {
    match query(
        cluster,
        config,
        DfsMetadataSelection::StatAtPathV2 { path: path.into() },
    ) {
        DfsMetadataQueryResult::StatAtPathV2 {
            object: Some(object),
        } => *object,
        other => panic!("{path} is not live in {}: {other:?}", config.shard_id),
    }
}

fn namespace_job(
    cluster: &Cluster,
    config: &DfsMetadataShardConfig,
    job_id: &str,
) -> Option<DfsNamespaceJobRecord> {
    match query(
        cluster,
        config,
        DfsMetadataSelection::NamespaceJob {
            job_id: job_id.into(),
        },
    ) {
        DfsMetadataQueryResult::NamespaceJob { job, .. } => job.map(|job| *job),
        other => panic!("namespace job query returned {other:?}"),
    }
}

/// The unlink of `alias_path`, an alias of `after`'s object, whose alias
/// change is durable and whose fence committing `after` is still held.
fn unlink(alias_path: &str, after: &DfsObjectMetadata) -> Unlink {
    let now = time::OffsetDateTime::now_utc();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: after.object_id.clone(),
        version: sinomo_dfs::DfsVersion::generate(),
    };
    let record =
        |job_id: String, kind, root_path: &str, target_path, frame| DfsNamespaceJobRecord {
            job_id,
            file_system_id: after.file_system_id.clone(),
            origin: None,
            kind,
            status: DfsNamespaceJobStatus::Running,
            root_path: root_path.into(),
            target_path,
            pending_frames: vec![frame],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        };
    let name = alias_path.replace('/', "-");
    let fence_job_id = format!("closed-fences-fence{name}");
    Unlink {
        job: record(
            format!("closed-fences-unlink{name}"),
            DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
            alias_path,
            Some(alias_path.to_owned()),
            DfsNamespaceJobFrame::CrossShardHardlinkTargetFenceCommit {
                fence_job_id: fence_job_id.clone(),
                source_path: alias_path.into(),
                expected_target: expected.clone(),
                committed_target: after.clone(),
            },
        ),
        fence: record(
            fence_job_id,
            DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
            &after.path,
            None,
            DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
                source_path: alias_path.into(),
                expected_target: expected,
                committed_target: after.clone(),
            },
        ),
    }
}

/// Record `jobs` in `config`'s shard through its primary at `primary`.
fn put_jobs(
    cluster: &Cluster,
    primary: usize,
    config: &DfsMetadataShardConfig,
    jobs: [&DfsNamespaceJobRecord; 2],
) {
    let payload = sinomo_dfs::DfsPreparedOperationPayload::for_metadata_effects(
        sinomo_dfs::DfsPreparedOperationScope {
            file_system_id: cluster.fs.clone(),
            shard_id: sinomo_dfs::DfsMetadataShardId::new(&config.shard_id),
            owner_epoch: config.epoch,
            operation_id: format!("closed-fences-{}", config.shard_id),
        },
        Vec::new(),
        Vec::new(),
        jobs.into_iter()
            .map(|job| sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceJob { job: job.clone() })
            .collect(),
        time::OffsetDateTime::now_utc(),
    )
    .unwrap();
    cluster
        .native
        .with_main_maintenance_service(&cluster.nodes[primary], |service| {
            sinomo_dfs::test_support::publish_current_payload(service, payload).unwrap();
        })
        .unwrap();
}

fn resumed<E: std::fmt::Display>(resumed: std::result::Result<DfsNamespaceJobRecord, E>) -> String {
    match resumed {
        Ok(job) => format!("{:?}", job.status),
        Err(error) => error.to_string(),
    }
}

/// Run the next step of the job `job_id` with the service of its shard,
/// the owner of `path`, on that shard's primary, as a maintenance pass does.
fn step_job(cluster: &Cluster, path: &str, job_id: &str) -> String {
    let mut service = cluster.native.metadata_primary_service(path).unwrap();
    resumed(service.posix_resume_namespace_job_bounded(job_id, 64))
}

/// Resume the job `job_id` with the node service of its coordinator's
/// primary `primary`, which holds every shard copy on that node and so takes
/// in the other shard's reply.
fn resume_job(cluster: &Cluster, primary: usize, job_id: &str) -> String {
    let mut service = cluster.native.client(&cluster.nodes[primary]).unwrap();
    resumed(service.posix_resume_namespace_job_bounded(job_id, 64))
}

/// The shards `config`'s primary reports `job_id` waiting on, in its
/// contributions to `target`.
fn waits_of(
    cluster: &Cluster,
    config: &DfsMetadataShardConfig,
    target: &str,
    job_id: &str,
) -> Vec<String> {
    let mut after = None;
    loop {
        let DfsMetadataQueryResult::CountContributionsV1 { page } = query(
            cluster,
            config,
            DfsMetadataSelection::CountContributionsV1 {
                target_shard: target.into(),
                expected_prefix: None,
                after,
                question: Default::default(),
            },
        ) else {
            panic!("{} did not answer its contributions", config.shard_id);
        };
        if let Some(job) = page.running_jobs.iter().find(|job| job.job_id == job_id) {
            return job
                .waiting_for_storage_of
                .iter()
                .map(|shard| shard.0.clone())
                .collect();
        }
        if page.complete {
            return Vec::new();
        }
        after = page.after;
    }
}

fn await_completed(
    cluster: &Cluster,
    config: &DfsMetadataShardConfig,
    primary: usize,
    job_id: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let job = namespace_job(cluster, config, job_id);
        if job
            .as_ref()
            .is_some_and(|job| job.status == DfsNamespaceJobStatus::Completed)
        {
            return;
        }
        let resumed = resume_job(cluster, primary, job_id);
        assert!(
            Instant::now() < deadline,
            "{job_id} did not complete: {job:?} after {resumed}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Convert `first`, then `second`; each names a shard, its primary and the
/// alias path of the unlink it coordinates.
fn crossing_fences_convert(name: &str, first: (&str, usize, &str), second: (&str, usize, &str)) {
    let mut cluster = start(
        name,
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    write_namespace(&cluster);
    let (services, users) = {
        let reader = canonical_migration_runtime(
            &cluster.native.component_context(&cluster.nodes[0]).unwrap(),
        );
        (
            shard_config(&reader, &cluster.fs, "services"),
            shard_config(&reader, &cluster.fs, "users"),
        )
    };
    let users_unlink = unlink(
        "/users/gone-link",
        &stat(&cluster, &services, "/services/file"),
    );
    let services_unlink = unlink("/services/gone-link", &stat(&cluster, &users, "/users/doc"));
    put_jobs(
        &cluster,
        1,
        &services,
        [&services_unlink.job, &users_unlink.fence],
    );
    put_jobs(
        &cluster,
        2,
        &users,
        [&users_unlink.job, &services_unlink.fence],
    );
    let job_of = |path: &str| {
        [&users_unlink, &services_unlink]
            .into_iter()
            .find(|unlink| unlink.job.root_path == path)
            .unwrap()
            .job
            .job_id
            .clone()
    };
    hold(&mut cluster, &["root", "services", "users"]);
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    for shard in ["root", "services", "users"] {
        close(&owners[0], &fs, shard);
    }
    for (shard, primary) in [("services", 1), ("users", 2)] {
        let parked = drive(&owners[primary], &fs, shard);
        assert!(
            matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("no member that can answer")),
            "{shard}: {parked:?}"
        );
    }
    assert_eq!(drive(&owners[0], &fs, "root"), ConversionProgress::Ready);
    let root = shard_config(&owners[0], &fs, "root");
    await_activated(&cluster, &owners, "root", &configured(&root));
    release(&cluster, &owners, "root");

    // The first shard converts while the second is closed, keeping its job.
    let (shard, primary, alias) = first;
    assert_eq!(
        drive(&owners[primary], &fs, shard),
        ConversionProgress::Ready,
        "{shard}"
    );
    let first_config = shard_config(&owners[0], &fs, shard);
    await_activated(&cluster, &owners, shard, &configured(&first_config));
    release(&cluster, &owners, shard);
    assert!(
        storage(&owners[0], &fs, second.0).is_some_and(|record| !record.is_active()),
        "{} is still closed when {shard} has converted",
        second.0
    );
    let kept = job_of(alias);
    assert_eq!(
        namespace_job(&cluster, &first_config, &kept).map(|job| job.status),
        Some(DfsNamespaceJobStatus::Running),
        "{shard} keeps its fence commit"
    );
    // Its commit parks on the second shard's closed storage.
    let deadline = Instant::now() + Duration::from_secs(60);
    while waits_of(&cluster, &first_config, second.0, &kept) != [second.0] {
        assert!(
            Instant::now() < deadline,
            "{kept} does not wait on {}",
            second.0
        );
        step_job(&cluster, first.2, &kept);
        std::thread::sleep(Duration::from_millis(250));
    }

    // The second converts, applying its fence and completing its own job.
    let (shard, primary, alias) = second;
    assert_eq!(
        drive(&owners[primary], &fs, shard),
        ConversionProgress::Ready,
        "{shard}"
    );
    let second_config = shard_config(&owners[0], &fs, shard);
    await_activated(&cluster, &owners, shard, &configured(&second_config));
    release(&cluster, &owners, shard);
    assert_eq!(
        namespace_job(&cluster, &second_config, &job_of(alias)).map(|job| job.status),
        Some(DfsNamespaceJobStatus::Completed)
    );
    await_completed(&cluster, &first_config, first.1, &kept);

    cluster.native.wait_startup_completion().unwrap();
    for (config, fence) in [
        (&services, &users_unlink.fence),
        (&users, &services_unlink.fence),
    ] {
        assert_eq!(
            namespace_job(&cluster, config, &fence.job_id).map(|job| job.status),
            Some(DfsNamespaceJobStatus::Completed),
            "{}",
            fence.job_id
        );
    }
    for node in 0..cluster.nodes.len() {
        let mut client = cluster.native.client(&cluster.nodes[node]).unwrap();
        assert_eq!(
            client
                .posix_stat_distributed("/services/file")
                .unwrap()
                .link_count,
            2
        );
        assert_eq!(
            client
                .posix_stat_distributed("/users/doc")
                .unwrap()
                .link_count,
            2
        );
        assert!(client.posix_stat_distributed("/users/gone-link").is_err());
        assert!(
            client
                .posix_stat_distributed("/services/gone-link")
                .is_err()
        );
    }
    drop(owners);
    cluster.native.close().unwrap();
}

#[test]
fn native_closed_shards_with_crossing_fence_commits_convert_services_first() {
    crossing_fences_convert(
        "closed-fences-services-first",
        ("services", 1, "/services/gone-link"),
        ("users", 2, "/users/gone-link"),
    );
}

#[test]
fn native_closed_shards_with_crossing_fence_commits_convert_users_first() {
    crossing_fences_convert(
        "closed-fences-users-first",
        ("users", 2, "/users/gone-link"),
        ("services", 1, "/services/gone-link"),
    );
}
