//! A directory move parked on a converting shard, as its coordinator reports
//! and announces it: the wait in its contribution pages, the rows of the page
//! it keeps, and the events of its park and its end.
use super::namespace_counts::{
    close_shard_for_conversion, directory_move_job, disjoint_shard_cluster,
};
use super::*;
use crate::{DfsNamespaceJobEvent, DfsNamespaceJobEventKind};

#[test]
fn a_directory_move_parked_on_a_converting_shard_reports_and_announces_its_wait() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-parked-move-announces");
    let root_node = cluster.replica_node.clone();
    let canonical = "/users/announced-target";
    let from = "/announced-tree";
    let to = "/services/announced-tree";
    let alias = format!("{from}/alias");
    let options = || DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]);
    cluster.replica.posix_mkdir_all(from).unwrap();
    cluster
        .replica
        .posix_write_file(canonical, b"announced target bytes", options())
        .unwrap();
    cluster.replica.posix_hardlink(canonical, &alias).unwrap();
    let linked = cluster
        .replica
        .posix_namespace_lstat_distributed(&alias)
        .unwrap();
    assert_eq!(linked.kind, DfsObjectKind::Hardlink);

    let events = Arc::new(Mutex::new(Vec::<DfsNamespaceJobEvent>::new()));
    let listener = {
        let events = Arc::clone(&events);
        crate::listen_for_namespace_job_events(&root_node, move |event| {
            events.lock().unwrap().push(event.clone());
        })
    };
    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let _records = close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    let parked = cluster.replica.posix_rename(from, to);
    assert!(
        matches!(&parked, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "the move waits on its carried alias's target shard: {parked:?}"
    );
    let job = directory_move_job(&cluster._runtime, &root_node, from);
    assert_eq!(job.status, DfsNamespaceJobStatus::Running);
    let users = DfsMetadataShardId::new("users");
    let announced = |kind, status| DfsNamespaceJobEvent {
        kind,
        file_system_id: sifs_file_system_id(),
        coordinator_shard_id: DfsMetadataShardId::new("root"),
        job_id: job.job_id.clone(),
        status,
        touched_shard_ids: vec![users.clone()],
    };
    assert!(
        events.lock().unwrap().contains(&announced(
            DfsNamespaceJobEventKind::Parked,
            DfsNamespaceJobStatus::Running
        )),
        "the park is announced to the shard it waits on: {:?}",
        events.lock().unwrap()
    );

    // The coordinator's page toward users names the wait, and the alias the
    // parked page carries is a live row at its old path, reported once and
    // carried by no frame yet.
    let root = cluster
        ._runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    let DfsMetadataQueryResult::CountContributionsV1 { page } = cluster
        .replica
        .query_metadata_authority_impl(
            &root,
            DfsMetadataSelection::CountContributionsV1 {
                target_shard: "users".into(),
                expected_prefix: None,
                after: None,
                question: Default::default(),
            },
        )
        .unwrap()
    else {
        panic!("wrong typed contribution response");
    };
    assert!(page.complete);
    let reported = page
        .running_jobs
        .iter()
        .find(|reported| reported.job_id == job.job_id)
        .expect("the parked move is reported Running");
    assert_eq!(reported.waiting_for_storage_of, vec![users.clone()]);
    assert_eq!(
        page.aliases
            .iter()
            .filter(|reported| reported.alias_object_id == linked.object_id)
            .map(|reported| reported.alias_path.clone())
            .collect::<Vec<_>>(),
        vec![alias.clone()]
    );
    assert!(
        page.running_jobs.iter().all(|reported| reported
            .carried
            .iter()
            .all(|object| object.object_id != linked.object_id)),
        "{page:?}"
    );

    // The job ends, here by failing, while users still converts: its end
    // is announced to users.
    let mut failed = directory_move_job(&cluster._runtime, &root_node, from);
    failed.status = DfsNamespaceJobStatus::Failed;
    failed.failure_reason = Some("ended while its target shard converts".into());
    failed.updated_at = time::OffsetDateTime::now_utc();
    // The node's resident service holds the coordinator's durable job.
    cluster
        ._runtime
        .with_main_maintenance_service(&root_node, |service| service.persist_namespace_job(&failed))
        .unwrap()
        .unwrap();
    assert!(
        events.lock().unwrap().contains(&announced(
            DfsNamespaceJobEventKind::Completed,
            DfsNamespaceJobStatus::Failed
        )),
        "{:?}",
        events.lock().unwrap()
    );
    drop(listener);
    drop(kv);
    cluster.close();
}
