//! Native Raft reservation DATA only. No promise, fence or activation authority.
use super::*;
use std::sync::{
    Condvar,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

type Request = DfsMetadataShardRecoveryBallotRequest;
type Begin = DfsRecoveryBallotBegin;
type Refusal = DfsRecoveryBallotRefusal;
type Verify = DfsRecoveryBallotVerify;

fn deadline() -> Instant {
    Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
}

fn with_native(name: &str, check: impl FnOnce(&mut NativeFloorFixture)) {
    let mut fixture = NativeFloorFixture::new(name);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&mut fixture)));
    drop(fixture.service);
    drop(fixture.control);
    fixture.runtime.clear_faults();
    let closed = fixture.runtime.close();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    closed.unwrap();
}

fn peer_client(fixture: &NativeFloorFixture, node: &NodeId) -> Box<dyn DfsControlPlaneKvClient> {
    let mut peer = fixture.runtime.client(node).unwrap();
    peer.control_plane
        .take()
        .expect("actual mounted peer control-plane client")
}

fn key(shard: &DfsMetadataShardConfig) -> String {
    metadata_shard_recovery_ballot_key(&shard.file_system_id, &shard.shard_id)
}

fn read_one(client: &mut dyn DfsControlPlaneKvClient, name: &str) -> Option<KvEntry> {
    let rows = client
        .get_authoritative_consistent_before(&[name.to_owned()], deadline())
        .unwrap();
    assert_eq!(rows.len(), 1);
    if let Some(row) = &rows[0] {
        assert_eq!(row.key, name);
    }
    rows.into_iter().next().unwrap()
}

fn serving_cells(fixture: &mut NativeFloorFixture) -> Vec<Option<KvEntry>> {
    let names = vec![
        metadata_shard_key(&fixture.shard.file_system_id, &fixture.shard.shard_id),
        metadata_shard_membership_handoff_key(
            &fixture.shard.file_system_id,
            &fixture.shard.shard_id,
        ),
        metadata_shard_operation_floor_key(&fixture.shard.file_system_id, &fixture.shard.shard_id),
        metadata_shard_head_watermark_key(&fixture.shard.file_system_id, &fixture.shard.shard_id),
    ];
    let rows = fixture
        .control
        .get_authoritative_consistent_before(&names, deadline())
        .unwrap();
    assert_eq!(rows.len(), names.len());
    for (name, row) in names.iter().zip(&rows) {
        if let Some(row) = row {
            assert_eq!(&row.key, name);
        }
    }
    rows
}

fn request(fixture: &NativeFloorFixture, candidate: usize, floor: u64) -> Request {
    Request::prepare(
        &fixture.shard,
        &fixture.shard.replica_node_ids[candidate],
        floor,
    )
    .unwrap()
}

fn reserved(result: Begin, expected: &Request) {
    match result {
        Begin::Reserved(actual) => assert_eq!(&actual, expected),
        other => panic!("expected actual Reserved: {other:?}"),
    }
}
fn refused(result: Begin, expected: Refusal) {
    match result {
        Begin::Refused(actual) => assert_eq!(actual, expected),
        other => panic!("expected refusal {expected:?}: {other:?}"),
    }
}

/// One finite negative scheduling cut, before each actual peer's first CAS.
/// Reads and writes still use the genuine mounted Raft client. Timeout refuses
/// instead of leaving a test thread trapped at an unbounded Barrier.
#[derive(Debug)]
struct CasGate {
    arrived: Mutex<usize>,
    changed: Condvar,
    deadline: Instant,
}
impl CasGate {
    fn arrive(&self) -> Result<(), KvError> {
        let error = || {
            KvError::AuthorityUnavailable { detail: "native reservation CAS test peers did not reach their first transaction before deadline".into() }
        };
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        self.changed.notify_all();
        while *arrived < 2 {
            let Some(left) = self.deadline.checked_duration_since(Instant::now()) else {
                return Err(error());
            };
            let (next, _) = self.changed.wait_timeout(arrived, left).unwrap();
            arrived = next;
        }
        if Instant::now() >= self.deadline {
            return Err(error());
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Counts {
    calls: AtomicUsize,
    transactions: AtomicUsize,
    compare_misses: AtomicUsize,
}

#[derive(Debug)]
struct NativeCalls {
    inner: Box<dyn DfsControlPlaneKvClient>,
    counts: Arc<Counts>,
    gate: Option<Arc<CasGate>>,
}
impl NativeCalls {
    fn new(inner: Box<dyn DfsControlPlaneKvClient>, gate: Option<Arc<CasGate>>) -> Self {
        Self {
            inner,
            counts: Arc::new(Counts::default()),
            gate,
        }
    }
    fn call(&self) {
        self.counts.calls.fetch_add(1, Ordering::SeqCst);
    }
}
impl DfsControlPlaneKvClient for NativeCalls {
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.call();
        self.inner.refresh_latest()
    }
    fn get(&self, name: &str) -> Result<Option<KvEntry>, KvError> {
        self.call();
        self.inner.get(name)
    }
    fn get_consistent(&self, names: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.call();
        self.inner.get_consistent(names)
    }
    fn get_authoritative_consistent(
        &mut self,
        names: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.call();
        self.inner.get_authoritative_consistent(names)
    }
    fn get_authoritative_consistent_before(
        &mut self,
        names: &[String],
        at: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.call();
        self.inner.get_authoritative_consistent_before(names, at)
    }
    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.call();
        self.inner.range_prefix(prefix)
    }
    fn put(&mut self, name: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.call();
        self.inner.put(name, value)
    }
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.call();
        if let Some(gate) = self.gate.take() {
            gate.arrive()?;
        }
        self.counts.transactions.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.txn(txn)?;
        self.counts
            .compare_misses
            .fetch_add(usize::from(!result.succeeded), Ordering::SeqCst);
        Ok(result)
    }
    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<DfsNativeTxnCommit>), KvError> {
        self.call();
        if let Some(gate) = self.gate.take() {
            gate.arrive()?;
        }
        self.counts.transactions.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.txn_with_native_authority(txn)?;
        self.counts
            .compare_misses
            .fetch_add(usize::from(!result.0.succeeded), Ordering::SeqCst);
        Ok(result)
    }
}

#[test]
fn native_distinct_peer_cas_mints_once_and_delayed_loser_is_spent() {
    with_native("native-reservation-distinct-cas", |fixture| {
        let before = serving_cells(fixture);
        let requests = [request(fixture, 0, 0), request(fixture, 1, 0)];
        let until = deadline();
        let gate = Arc::new(CasGate {
            arrived: Mutex::new(0),
            changed: Condvar::new(),
            deadline: until,
        });
        let clients = [
            peer_client(fixture, &fixture.shard.replica_node_ids[0]),
            peer_client(fixture, &fixture.shard.replica_node_ids[1]),
        ];
        let mut clients = clients.map(|client| NativeCalls::new(client, Some(Arc::clone(&gate))));
        let results = std::thread::scope(|scope| {
            let [a, b] = &mut clients;
            let left_request = &requests[0];
            let right_request = &requests[1];
            let left = scope.spawn(move || {
                DfsControlPlaneStore::new(a)
                    .begin_metadata_shard_recovery_ballot_reservation(left_request, until)
                    .unwrap()
            });
            let right = scope.spawn(move || {
                DfsControlPlaneStore::new(b)
                    .begin_metadata_shard_recovery_ballot_reservation(right_request, until)
                    .unwrap()
            });
            [left.join().unwrap(), right.join().unwrap()]
        });
        assert_eq!(*gate.arrived.lock().unwrap(), 2);
        assert_eq!(
            clients
                .iter()
                .map(|x| x.counts.transactions.load(Ordering::SeqCst))
                .sum::<usize>(),
            2
        );
        assert_eq!(
            clients
                .iter()
                .map(|x| x.counts.compare_misses.load(Ordering::SeqCst))
                .sum::<usize>(),
            1
        );
        let winner = results
            .iter()
            .position(|r| matches!(r, Begin::Reserved(_)))
            .expect("one actual CAS wins");
        let loser = 1 - winner;
        assert!(
            matches!(&results[loser], Begin::Refused(Refusal::Spent)),
            "ordered expectedfloor rule: {results:?}"
        );
        let view = DfsControlPlaneStore::new(fixture.control.as_mut())
            .metadata_shard_recovery_ballot_floor_before(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                deadline(),
            )
            .unwrap();
        assert_eq!(view.attempt_floor, 1);
        assert_eq!(view.pending, Some(requests[winner].clone()));
        assert_eq!(view.serving_shard, Some(fixture.shard.clone()));
        assert!(!view.handoff_pending);
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&requests[winner], deadline())
                .unwrap()
        );
        refused(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&requests[loser], deadline())
                .unwrap(),
            Refusal::Spent,
        );
        let next =
            Request::prepare(&fixture.shard, &requests[loser].ballot.candidate_node_id, 1).unwrap();
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&next, deadline())
                .unwrap(),
            &next,
        );
        assert_eq!(next.ballot.attempt, 2);
        assert_eq!(serving_cells(fixture), before);
    });
}

#[test]
fn native_identical_peer_cas_converges_without_a_second_attempt() {
    with_native("native-reservation-identical-cas", |fixture| {
        let requested = request(fixture, 0, 0);
        let before = serving_cells(fixture);
        let until = deadline();
        let gate = Arc::new(CasGate {
            arrived: Mutex::new(0),
            changed: Condvar::new(),
            deadline: until,
        });
        let mut a = NativeCalls::new(
            peer_client(fixture, &fixture.shard.replica_node_ids[0]),
            Some(Arc::clone(&gate)),
        );
        let mut b = NativeCalls::new(
            peer_client(fixture, &fixture.shard.replica_node_ids[1]),
            Some(Arc::clone(&gate)),
        );
        let (left, right) = std::thread::scope(|scope| {
            let requested = &requested;
            let a = &mut a;
            let b = &mut b;
            let left = scope.spawn(move || {
                DfsControlPlaneStore::new(a)
                    .begin_metadata_shard_recovery_ballot_reservation(requested, until)
                    .unwrap()
            });
            let right = scope.spawn(move || {
                DfsControlPlaneStore::new(b)
                    .begin_metadata_shard_recovery_ballot_reservation(requested, until)
                    .unwrap()
            });
            (left.join().unwrap(), right.join().unwrap())
        });
        assert!(
            matches!((&left,&right),(Begin::Reserved(x),Begin::Existing(y))|(Begin::Existing(x),Begin::Reserved(y)) if x==&requested&&y==&requested),
            "{left:?} {right:?}"
        );
        assert_eq!(
            a.counts.compare_misses.load(Ordering::SeqCst)
                + b.counts.compare_misses.load(Ordering::SeqCst),
            1
        );
        let value = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        assert!(
            matches!(DfsControlPlaneStore::new(fixture.control.as_mut()).begin_metadata_shard_recovery_ballot_reservation(&requested,deadline()).unwrap(),Begin::Existing(x) if x==requested)
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&fixture.shard)),
            Some(value)
        );
        assert_eq!(serving_cells(fixture), before);
    });
}

#[test]
fn native_cancelled_requests_stay_spent_after_multiple_floor_advances() {
    with_native("native-reservation-spent-history", |fixture| {
        let before = serving_cells(fixture);
        let first = request(fixture, 0, 0);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap(),
            &first,
        );
        let held = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        assert!(
            matches!(DfsControlPlaneStore::new(fixture.control.as_mut()).begin_metadata_shard_recovery_ballot_reservation(&first,deadline()).unwrap(),Begin::Existing(x) if x==first)
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&fixture.shard)),
            Some(held)
        );
        let held = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        let blocked = request(fixture, 1, 1);
        refused(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&blocked, deadline())
                .unwrap(),
            Refusal::PendingHeldByOther,
        );
        assert!(
            !DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&blocked, deadline())
                .unwrap()
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&fixture.shard)),
            Some(held)
        );
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap()
        );
        let second = request(fixture, 1, 1);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&second, deadline())
                .unwrap(),
            &second,
        );
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&second, deadline())
                .unwrap()
        );
        let cancelled = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        for old in [&first, &second] {
            refused(
                DfsControlPlaneStore::new(fixture.control.as_mut())
                    .begin_metadata_shard_recovery_ballot_reservation(old, deadline())
                    .unwrap(),
                Refusal::Spent,
            );
            assert!(matches!(
                DfsControlPlaneStore::new(fixture.control.as_mut())
                    .verify_metadata_shard_recovery_ballot_before(
                        old,
                        &fixture.shard.primary_node_id,
                        deadline()
                    )
                    .unwrap(),
                Verify::Refused(_)
            ));
            assert!(
                !DfsControlPlaneStore::new(fixture.control.as_mut())
                    .cancel_metadata_shard_recovery_ballot_reservation(old, deadline())
                    .unwrap()
            );
            assert_eq!(
                read_one(fixture.control.as_mut(), &key(&fixture.shard)),
                Some(cancelled.clone())
            );
        }
        let ahead = request(fixture, 0, 5);
        refused(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&ahead, deadline())
                .unwrap(),
            Refusal::FloorAhead,
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&fixture.shard)),
            Some(cancelled)
        );
        let third = request(fixture, 0, 2);
        assert_ne!(third.reservation_id, first.reservation_id);
        assert_ne!(third.reservation_id, second.reservation_id);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&third, deadline())
                .unwrap(),
            &third,
        );
        assert_eq!(third.ballot.attempt, 3);
        assert_eq!(third.ballot.epoch, fixture.shard.epoch + 1);
        let view = DfsControlPlaneStore::new(fixture.control.as_mut())
            .metadata_shard_recovery_ballot_floor_before(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                deadline(),
            )
            .unwrap();
        assert_eq!(view.attempt_floor, 3);
        assert_eq!(view.floor_epoch, Some(fixture.shard.epoch));
        assert_eq!(view.pending, Some(third));
        assert_eq!(serving_cells(fixture), before);
    });
}

#[test]
fn native_stale_and_concurrent_cancellation_never_erase_new_pending() {
    with_native("native-reservation-cancel-cas", |fixture| {
        let first = request(fixture, 0, 0);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap(),
            &first,
        );
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap()
        );
        let next = request(fixture, 1, 1);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&next, deadline())
                .unwrap(),
            &next,
        );
        let held = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        assert!(
            !DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap()
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&fixture.shard)),
            Some(held)
        );
        let until = deadline();
        let gate = Arc::new(CasGate {
            arrived: Mutex::new(0),
            changed: Condvar::new(),
            deadline: until,
        });
        let mut a = NativeCalls::new(
            peer_client(fixture, &fixture.shard.replica_node_ids[0]),
            Some(Arc::clone(&gate)),
        );
        let mut b = NativeCalls::new(
            peer_client(fixture, &fixture.shard.replica_node_ids[1]),
            Some(Arc::clone(&gate)),
        );
        let outcomes = std::thread::scope(|scope| {
            let next = &next;
            let a = &mut a;
            let b = &mut b;
            let a = scope.spawn(move || {
                DfsControlPlaneStore::new(a)
                    .cancel_metadata_shard_recovery_ballot_reservation(next, until)
                    .unwrap()
            });
            let b = scope.spawn(move || {
                DfsControlPlaneStore::new(b)
                    .cancel_metadata_shard_recovery_ballot_reservation(next, until)
                    .unwrap()
            });
            [a.join().unwrap(), b.join().unwrap()]
        });
        assert_eq!(outcomes.into_iter().filter(|x| *x).count(), 1);
        assert_eq!(
            a.counts.compare_misses.load(Ordering::SeqCst)
                + b.counts.compare_misses.load(Ordering::SeqCst),
            1
        );
        let view = DfsControlPlaneStore::new(fixture.control.as_mut())
            .metadata_shard_recovery_ballot_floor_before(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                deadline(),
            )
            .unwrap();
        assert_eq!(view.attempt_floor, 2);
        assert!(view.pending.is_none());
    });
}

#[test]
fn native_pending_reservation_survives_peer_restart_without_serving_changes() {
    with_native("native-reservation-peer-restart", |fixture| {
        let before = serving_cells(fixture);
        let requested = request(fixture, 0, 0);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap(),
            &requested,
        );
        let persisted = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        let restarted = fixture.shard.replica_node_ids[1].clone();
        fixture.runtime.stop_node(&restarted).unwrap();
        fixture.runtime.reopen_node(restarted.clone()).unwrap();
        let mut fresh = peer_client(fixture, &restarted);
        let view = DfsControlPlaneStore::new(fresh.as_mut())
            .metadata_shard_recovery_ballot_floor_before(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                deadline(),
            )
            .unwrap();
        assert_eq!(view.attempt_floor, 1);
        assert_eq!(view.floor_epoch, Some(fixture.shard.epoch));
        assert_eq!(view.pending, Some(requested.clone()));
        assert_eq!(
            read_one(fresh.as_mut(), &key(&fixture.shard)),
            Some(persisted)
        );
        assert!(
            matches!(DfsControlPlaneStore::new(fresh.as_mut()).verify_metadata_shard_recovery_ballot_before(&requested,&restarted,deadline()).unwrap(),Verify::Verified(actual) if actual==requested)
        );
        assert_eq!(serving_cells(fixture), before);
    });
}

#[test]
fn native_pending_reservation_does_not_freeze_create_or_maintenance() {
    with_native("native-reservation-no-serving-freeze", |fixture| {
        let shard_id = crate::DfsMetadataShardId::new(&fixture.shard.shard_id);
        let freeze = fixture
            .primary_store()
            .metadata_shard_write_freeze(&shard_id)
            .unwrap();
        let requested = request(fixture, 0, 0);
        let serving = fixture.shard.clone();
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap(),
            &requested,
        );
        let pending = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        assert_eq!(
            fixture
                .primary_store()
                .metadata_shard_write_freeze(&shard_id)
                .unwrap(),
            freeze
        );
        let operation = sinomo_core::Id::generate().to_string();
        let created = fixture
            .service
            .posix_create_file_from_reader_with_operation_id(
                "/native-floor-a.bin",
                std::io::Cursor::new(b"ordinary during reservation"),
                crate::DfsWriteOptions::replicated_eventual(vec![serving.primary_node_id.clone()]),
                &operation,
            )
            .unwrap();
        fixture
            .service
            .posix_sync_file("/native-floor-a.bin", crate::DfsPosixSyncKind::File)
            .unwrap();
        assert_eq!(
            fixture
                .service
                .posix_read_file_distributed("/native-floor-a.bin")
                .unwrap(),
            b"ordinary during reservation"
        );
        let result = fixture
            .runtime
            .maintenance_tick(
                &serving.primary_node_id,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        assert!(matches!(
            result.outcome.unwrap(),
            crate::DfsMaintenanceTickOutcome::Completed { .. }
        ));
        assert_eq!(
            created.object_id,
            fixture
                .service
                .posix_stat_distributed("/native-floor-a.bin")
                .unwrap()
                .object_id
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&serving)),
            Some(pending)
        );
        assert_eq!(
            read_one(
                fixture.control.as_mut(),
                &metadata_shard_key(&serving.file_system_id, &serving.shard_id)
            )
            .unwrap()
            .decode::<DfsMetadataShardConfig>()
            .unwrap(),
            serving
        );
        assert!(
            read_one(
                fixture.control.as_mut(),
                &metadata_shard_membership_handoff_key(&serving.file_system_id, &serving.shard_id)
            )
            .is_none()
        );
        assert_eq!(
            fixture
                .primary_store()
                .metadata_shard_write_freeze(&shard_id)
                .unwrap(),
            freeze
        );
        assert!(
            matches!(DfsControlPlaneStore::new(fixture.control.as_mut()).verify_metadata_shard_recovery_ballot_before(&requested,&serving.primary_node_id,deadline()).unwrap(),Verify::Verified(actual) if actual==requested)
        );
        let outsider = NodeId::generate();
        assert!(matches!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .verify_metadata_shard_recovery_ballot_before(&requested, &outsider, deadline())
                .unwrap(),
            Verify::Refused(Refusal::NotMember)
        ));
    });
}

#[test]
fn native_wrong_or_absent_authority_and_real_pending_handoff_refuse_verification() {
    with_native("native-reservation-authority-refusal", |fixture| {
        let before = serving_cells(fixture);
        let mut changed = fixture.shard.clone();
        changed.default_policy = if changed.default_policy == DfsObjectPolicy::ReplicatedConfirmed {
            DfsObjectPolicy::ReplicatedEventual
        } else {
            DfsObjectPolicy::ReplicatedConfirmed
        };
        let wrong = Request::prepare(&changed, &changed.replica_node_ids[0], 0).unwrap();
        refused(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&wrong, deadline())
                .unwrap(),
            Refusal::AuthorityChanged,
        );
        assert!(read_one(fixture.control.as_mut(), &key(&fixture.shard)).is_none());
        let mut absent = fixture.shard.clone();
        absent.file_system_id = DfsFileSystemId::generate();
        let missing = Request::prepare(&absent, &absent.replica_node_ids[0], 0).unwrap();
        refused(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&missing, deadline())
                .unwrap(),
            Refusal::NoAuthority,
        );
        assert!(read_one(fixture.control.as_mut(), &key(&absent)).is_none());
        assert_eq!(serving_cells(fixture), before);
        let requested = request(fixture, 0, 0);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap(),
            &requested,
        );
        let pending = read_one(fixture.control.as_mut(), &key(&fixture.shard)).unwrap();
        fixture.deny_last_metadata_participant();
        let promoted = fixture.shard.replica_node_ids.last().unwrap().clone();
        let handoff = DfsControlPlaneStore::new(fixture.control.as_mut())
            .request_metadata_shard_primary_handoff(&fixture.shard, &promoted)
            .unwrap()
            .unwrap();
        assert_eq!(handoff.retiring_shard, fixture.shard);
        assert!(matches!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .verify_metadata_shard_recovery_ballot_before(
                    &requested,
                    &fixture.shard.primary_node_id,
                    deadline()
                )
                .unwrap(),
            Verify::Refused(Refusal::HandoffPending)
        ));
        refused(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap(),
            Refusal::HandoffPending,
        );
        assert_eq!(
            read_one(fixture.control.as_mut(), &key(&fixture.shard)),
            Some(pending)
        );
        assert_eq!(
            read_one(
                fixture.control.as_mut(),
                &metadata_shard_key(&fixture.shard.file_system_id, &fixture.shard.shard_id)
            )
            .unwrap()
            .decode::<DfsMetadataShardConfig>()
            .unwrap(),
            fixture.shard
        );
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap(),
            "stale pending can be explicitly cancelled without granting serving authority"
        );
    });
}

#[test]
fn native_malformed_request_and_expired_deadline_perform_no_client_io() {
    with_native("native-reservation-input-preflight", |fixture| {
        let good = request(fixture, 0, 0);
        let before = serving_cells(fixture);
        let mut client = NativeCalls::new(
            peer_client(fixture, &fixture.shard.replica_node_ids[0]),
            None,
        );
        let mut mutations = Vec::new();
        let mut changed = good.clone();
        changed.reservation_id.push_str("-different");
        mutations.push(changed);
        let mut changed = good.clone();
        changed.ballot.candidate_node_id = fixture.shard.replica_node_ids[1].clone();
        mutations.push(changed);
        let mut changed = good.clone();
        changed.serving_shard.default_policy =
            if good.serving_shard.default_policy == DfsObjectPolicy::ReplicatedEventual {
                DfsObjectPolicy::ReplicatedConfirmed
            } else {
                DfsObjectPolicy::ReplicatedEventual
            };
        mutations.push(changed);
        let mut changed = good.clone();
        changed.expected_attempt_floor += 1;
        mutations.push(changed);
        for malformed in mutations {
            refused(
                DfsControlPlaneStore::new(&mut client)
                    .begin_metadata_shard_recovery_ballot_reservation(&malformed, deadline())
                    .unwrap(),
                Refusal::MalformedId,
            );
            assert!(matches!(
                DfsControlPlaneStore::new(&mut client)
                    .cancel_metadata_shard_recovery_ballot_reservation(&malformed, deadline()),
                Err(KvError::Serialization { .. })
            ));
            assert!(matches!(
                DfsControlPlaneStore::new(&mut client)
                    .verify_metadata_shard_recovery_ballot_before(
                        &malformed,
                        &fixture.shard.primary_node_id,
                        deadline()
                    )
                    .unwrap(),
                Verify::Refused(Refusal::MalformedId)
            ));
            assert_eq!(client.counts.calls.load(Ordering::SeqCst), 0);
        }
        let mut overflow = fixture.shard.clone();
        overflow.epoch = u64::MAX;
        assert!(matches!(
            Request::prepare(&overflow, &overflow.replica_node_ids[0], 0),
            Err(Refusal::EpochOverflow)
        ));
        assert!(matches!(
            Request::prepare(&fixture.shard, &fixture.shard.replica_node_ids[0], u64::MAX),
            Err(Refusal::AttemptOverflow)
        ));
        let mut wrong_group = fixture.shard.clone();
        wrong_group.replica_node_ids.pop();
        assert!(matches!(
            Request::prepare(&wrong_group, &fixture.shard.replica_node_ids[0], 0),
            Err(Refusal::NotFixedThreeMember)
        ));
        assert!(matches!(
            Request::prepare(&fixture.shard, &fixture.shard.primary_node_id, 0),
            Err(Refusal::CandidateNotReplica)
        ));
        assert_eq!(client.counts.calls.load(Ordering::SeqCst), 0);
        let expired = Instant::now() - Duration::from_millis(1);
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .metadata_shard_recovery_ballot_floor_before(
                    &fixture.shard.file_system_id,
                    &fixture.shard.shard_id,
                    expired
                )
                .is_err()
        );
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .begin_metadata_shard_recovery_ballot_reservation(&good, expired)
                .is_err()
        );
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .cancel_metadata_shard_recovery_ballot_reservation(&good, expired)
                .is_err()
        );
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .verify_metadata_shard_recovery_ballot_before(
                    &good,
                    &fixture.shard.primary_node_id,
                    expired
                )
                .is_err()
        );
        assert_eq!(client.counts.calls.load(Ordering::SeqCst), 0);
        assert!(read_one(fixture.control.as_mut(), &key(&fixture.shard)).is_none());
        assert_eq!(serving_cells(fixture), before);
    });
}

#[test]
fn native_corrupt_stored_reservation_never_writes_or_becomes_authority() {
    with_native("native-reservation-corrupt-store", |fixture| {
        let requested = request(fixture, 0, 0);
        reserved(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .begin_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap(),
            &requested,
        );
        let name = key(&fixture.shard);
        let original = read_one(fixture.control.as_mut(), &name).unwrap();
        let before = serving_cells(fixture);
        let valid = serde_json::json!({"attempt_floor":1,"floor_epoch":fixture.shard.epoch,"pending":requested});
        let mut cases = Vec::new();
        let mut bad = valid.clone();
        bad["attempt_floor"] = 0.into();
        cases.push(bad);
        let mut bad = valid.clone();
        bad["pending"]["ballot"]["attempt"] = 2.into();
        cases.push(bad);
        let mut bad = valid.clone();
        bad["pending"]["reservation_id"] = "changed-nonempty-id".into();
        cases.push(bad);
        let mut bad = valid.clone();
        bad["floor_epoch"] = (fixture.shard.epoch + 1).into();
        cases.push(bad);
        let mut bad = valid.clone();
        bad["pending"]["ballot"]["candidate_node_id"] = NodeId::generate().to_string().into();
        cases.push(bad);
        for bad in cases {
            fixture
                .control
                .put(
                    name.clone(),
                    sinomo_core::kv::encode_kv_value(&bad).unwrap(),
                )
                .unwrap();
            let corrupt = read_one(fixture.control.as_mut(), &name).unwrap();
            assert!(matches!(
                DfsControlPlaneStore::new(fixture.control.as_mut())
                    .begin_metadata_shard_recovery_ballot_reservation(&requested, deadline()),
                Err(KvError::Serialization { .. })
            ));
            assert!(matches!(
                DfsControlPlaneStore::new(fixture.control.as_mut())
                    .cancel_metadata_shard_recovery_ballot_reservation(&requested, deadline()),
                Err(KvError::Serialization { .. })
            ));
            assert!(matches!(
                DfsControlPlaneStore::new(fixture.control.as_mut())
                    .verify_metadata_shard_recovery_ballot_before(
                        &requested,
                        &fixture.shard.primary_node_id,
                        deadline()
                    ),
                Err(KvError::Serialization { .. })
            ));
            assert_eq!(read_one(fixture.control.as_mut(), &name), Some(corrupt));
            assert_eq!(serving_cells(fixture), before);
        }
        fixture.control.put(name, original.value).unwrap();
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .cancel_metadata_shard_recovery_ballot_reservation(&requested, deadline())
                .unwrap()
        );
    });
}
