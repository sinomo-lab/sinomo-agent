//! Compare individual proposal persistence with bounded groups on the same disk.
//! cargo run -p sinomo-kv --example group_commit_bench -- benchmark-results/group-commit
//! This excludes networking, quorum/apply, and the async queue's collection delay.
use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use sinomo_base::{CapabilityName, ClusterBootstrapState, Id};
use sinomo_kv::*;
use sinomo_node::NodeId;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use time::OffsetDateTime;
#[derive(Clone, Default)]
struct Observations(Arc<Mutex<BTreeMap<String, Vec<f64>>>>);
struct Samples {
    name: String,
    observations: Observations,
}
impl HistogramFn for Samples {
    fn record(&self, value: f64) {
        self.observations
            .0
            .lock()
            .unwrap()
            .entry(self.name.clone())
            .or_default()
            .push(value);
    }
}
impl Recorder for Observations {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(Samples {
            name: key.name().into(),
            observations: self.clone(),
        }))
    }
}
fn main() {
    let destination = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .expect("output directory on the disk to benchmark"),
    );
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    println!("round,group_size,requests,elapsed_ms,requests_per_second,wal_flushes,wal_ms");
    for round in 1..=3 {
        let mut sizes = vec![0, 1, 8, 32, 64];
        if round % 2 == 0 {
            sizes.reverse();
        }
        for size in sizes {
            let root = destination.join(format!("{nonce}-r{round}-g{size}"));
            std::fs::create_dir_all(&root).unwrap();
            let paths = ClusterKvPaths::new(&root);
            let local = NodeId::from_legacy_named("group_benchmark").unwrap();
            let mut state = ClusterBootstrapState::initialized_locally(
                Id::from_legacy_named("group_benchmark_cluster").unwrap(),
                "benchmark",
                local.clone(),
                "local",
                "pk",
                OffsetDateTime::now_utc(),
            );
            state.term = 7;
            state.members[0].capabilities =
                vec![CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY).unwrap()];
            state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
            paths
                .raft_store()
                .open_node(&local)
                .unwrap()
                .become_leader(7)
                .unwrap();
            let requests: Vec<_> = (0..128)
                .map(|i| {
                    KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                        key: format!("/bench/{i}"),
                        value: vec![b'x'; 256],
                        lease_id: None,
                    }))
                })
                .collect();
            let observations = Observations::default();
            let elapsed = metrics::with_local_recorder(&observations, || {
                let start = Instant::now();
                if size == 0 {
                    for request in &requests {
                        let _sequence =
                            lock_cluster_kv_commit_sequence(&paths, Duration::from_secs(30))
                                .unwrap();
                        let mut storage = paths.local_raft_command_storage(
                            local.clone(),
                            cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
                        );
                        storage.append_uncommitted(request.clone()).unwrap();
                        storage.release_session_for_quorum();
                    }
                } else {
                    for group in requests.chunks(size) {
                        let proposals: Vec<_> = group
                            .iter()
                            .map(|request| {
                                ClusterKvRaftProposal::new(
                                    state.clone(),
                                    request.clone(),
                                    Instant::now() + Duration::from_secs(30),
                                )
                            })
                            .collect();
                        for result in prepare_cluster_kv_raft_proposals(&paths, &local, &proposals)
                        {
                            assert!(matches!(
                                result.unwrap(),
                                ClusterKvRaftPreparedProposal::Appended(_)
                            ));
                        }
                    }
                }
                start.elapsed()
            });
            let mut node = paths.raft_store().open_node(&local).unwrap();
            assert_eq!(node.last_log_index(), 128);
            node.commit_to(128).unwrap();
            for request in &requests {
                assert!(
                    node.store()
                        .committed_request(&request.request_id)
                        .is_some()
                );
            }
            let values = observations.0.lock().unwrap();
            let wal = values.get("sinomo.kv.wal.append.us").unwrap();
            assert_eq!(
                wal.len(),
                if size == 0 {
                    128
                } else {
                    128usize.div_ceil(size)
                }
            );
            println!(
                "{round},{size},128,{:.3},{:.3},{},{:.3}",
                elapsed.as_secs_f64() * 1000.0,
                128.0 / elapsed.as_secs_f64(),
                wal.len(),
                wal.iter().sum::<f64>() / 1000.0
            );
        }
    }
}
