//! Two closed shards, each coordinating a legacy cross-shard hardlink
//! creation whose alias path the other owns, convert in either order. Each
//! settles its own creation from the other's live aliases, read from the
//! other's capture while it is closed and from its current tables once it
//! has converted, and settles the other's creation per pair, so neither waits
//! for the other's conversion.
//!
//! The services creation of `/users/svc-link` was adopted: the alias is live
//! in users, as an ordinary cross-shard hardlink left it, and the job settles
//! Completed. The users creation of `/services/never-adopted` was not: no row
//! of services carries its alias, and the job settles Aborted. Each canonical
//! target counts exactly its live aliases.
use super::*;
use sinomo_dfs::{
    DfsMetadataQueryResult, DfsMetadataSelection, DfsNamespaceJobFrame, DfsNamespaceJobKind,
    DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsObjectKind, DfsObjectMetadata,
};

const ADOPTED: &str = "legacy-creation-users-svc-link";
const NEVER_ADOPTED: &str = "legacy-creation-services-never-adopted";

fn stat(cluster: &Cluster, config: &DfsMetadataShardConfig, path: &str) -> DfsObjectMetadata {
    let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
    match client
        .query_metadata_authority(
            config,
            DfsMetadataSelection::StatAtPathV2 { path: path.into() },
        )
        .unwrap()
    {
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
    let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
    match client
        .query_metadata_authority(
            config,
            DfsMetadataSelection::NamespaceJob {
                job_id: job_id.into(),
            },
        )
        .unwrap()
    {
        DfsMetadataQueryResult::NamespaceJob { job, .. } => job.map(|job| *job),
        other => panic!("namespace job query returned {other:?}"),
    }
}

/// A Running legacy creation of `canonical` at `alias.path`, its adopt
/// frame still pending, as the coordinator recorded it before the frame
/// that only advanced the canonical target's ctime.
fn legacy_creation(
    job_id: &str,
    canonical: &DfsObjectMetadata,
    alias: DfsObjectMetadata,
    destination: &NodeId,
) -> DfsNamespaceJobRecord {
    let now = time::OffsetDateTime::now_utc();
    DfsNamespaceJobRecord {
        job_id: job_id.into(),
        file_system_id: canonical.file_system_id.clone(),
        origin: None,
        kind: DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
        status: DfsNamespaceJobStatus::Running,
        root_path: canonical.path.clone(),
        target_path: Some(alias.path.clone()),
        pending_frames: vec![
            DfsNamespaceJobFrame::CrossShardHardlinkComplete {
                target_object_id: canonical.object_id.clone(),
            },
            DfsNamespaceJobFrame::CrossShardHardlinkAdopt {
                alias,
                target_object_id: canonical.object_id.clone(),
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

/// Record `job` in `config`'s shard through its primary at `primary`.
fn put_job(
    cluster: &Cluster,
    primary: usize,
    config: &DfsMetadataShardConfig,
    job: DfsNamespaceJobRecord,
) {
    let payload = sinomo_dfs::DfsPreparedOperationPayload::for_metadata_effects(
        sinomo_dfs::DfsPreparedOperationScope {
            file_system_id: cluster.fs.clone(),
            shard_id: sinomo_dfs::DfsMetadataShardId::new(&config.shard_id),
            owner_epoch: config.epoch,
            operation_id: format!("closed-pair-{}", job.job_id),
        },
        Vec::new(),
        Vec::new(),
        vec![sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceJob { job }],
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

/// Close all three shards with the two creations recorded, let services and
/// users capture, convert root, then convert `first` and `second` in that
/// order, each while the other is closed.
fn closed_pair_converts(name: &str, first: (&str, usize), second: (&str, usize)) {
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
    cluster
        .native
        .client(&cluster.nodes[0])
        .unwrap()
        .posix_hardlink("/services/file", "/users/svc-link")
        .unwrap();
    let (services, users) = {
        let reader = canonical_migration_runtime(
            &cluster.native.component_context(&cluster.nodes[0]).unwrap(),
        );
        (
            shard_config(&reader, &cluster.fs, "services"),
            shard_config(&reader, &cluster.fs, "users"),
        )
    };
    let file = stat(&cluster, &services, "/services/file");
    let doc = stat(&cluster, &users, "/users/doc");
    let link = stat(&cluster, &users, "/users/svc-link");
    assert_eq!(
        (link.kind.clone(), link.link_target_object_id.as_ref()),
        (DfsObjectKind::Hardlink, Some(&file.object_id))
    );
    let mut never = link.clone();
    never.object_id = sinomo_dfs::DfsObjectId::generate();
    never.path = "/services/never-adopted".into();
    never.link_target_object_id = Some(doc.object_id.clone());
    never.version = sinomo_dfs::DfsVersion::generate();
    put_job(
        &cluster,
        1,
        &services,
        legacy_creation(ADOPTED, &file, link, &cluster.nodes[2]),
    );
    put_job(
        &cluster,
        2,
        &users,
        legacy_creation(NEVER_ADOPTED, &doc, never, &cluster.nodes[1]),
    );
    hold(&mut cluster, &["root", "services", "users"]);
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    for shard in ["root", "services", "users"] {
        close(&owners[0], &fs, shard);
    }
    // Each records its attempt and capture, then waits for root's capture.
    for (shard, primary) in [("services", 1), ("users", 2)] {
        let parked = drive(&owners[primary], &fs, shard);
        assert!(
            matches!(&parked, ConversionProgress::Parked { evidence } if evidence.contains("no member that can answer")),
            "{shard}: {parked:?}"
        );
    }
    assert_eq!(drive(&owners[0], &fs, "root"), ConversionProgress::Ready);
    await_activated(
        &cluster,
        &owners,
        "root",
        &configured(&shard_config(&owners[0], &fs, "root")),
    );
    release(&cluster, &owners, "root");
    for (shard, primary) in [first, second] {
        assert_eq!(
            drive(&owners[primary], &fs, shard),
            ConversionProgress::Ready,
            "{shard}"
        );
        let config = shard_config(&owners[0], &fs, shard);
        await_activated(&cluster, &owners, shard, &configured(&config));
        release(&cluster, &owners, shard);
        if shard == first.0 {
            assert!(
                storage(&owners[0], &fs, second.0).is_some_and(|record| !record.is_active()),
                "{} is still closed when {shard} has converted",
                second.0
            );
        }
    }
    cluster.native.wait_startup_completion().unwrap();
    let services = shard_config(&owners[0], &fs, "services");
    let users = shard_config(&owners[0], &fs, "users");
    let adopted =
        namespace_job(&cluster, &services, ADOPTED).expect("the adopted creation is kept");
    assert_eq!(adopted.status, DfsNamespaceJobStatus::Completed);
    assert!(adopted.pending_frames.is_empty(), "{adopted:?}");
    assert_eq!(
        namespace_job(&cluster, &users, NEVER_ADOPTED),
        None,
        "the creation never adopted is aborted"
    );
    for node in 0..cluster.nodes.len() {
        let mut client = cluster.native.client(&cluster.nodes[node]).unwrap();
        assert_eq!(
            client
                .posix_stat_distributed("/services/file")
                .unwrap()
                .link_count,
            3
        );
        assert_eq!(
            client
                .posix_stat_distributed("/users/doc")
                .unwrap()
                .link_count,
            2
        );
        assert_eq!(
            client
                .posix_read_file_distributed("/users/svc-link")
                .unwrap(),
            b"services bytes"
        );
        assert!(
            client
                .posix_stat_distributed("/services/never-adopted")
                .is_err()
        );
    }
    drop(owners);
    cluster.native.close().unwrap();
}

#[test]
fn native_closed_shards_with_creations_toward_each_other_convert_services_first() {
    closed_pair_converts("closed-pair-services-first", ("services", 1), ("users", 2));
}

#[test]
fn native_closed_shards_with_creations_toward_each_other_convert_users_first() {
    closed_pair_converts("closed-pair-users-first", ("users", 2), ("services", 1));
}
