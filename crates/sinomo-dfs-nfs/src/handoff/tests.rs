use super::*;
use crate::adapter::SifsNfsFileSystem;
use nfs3_server::nfs3_types::nfs3::{filename3, nfsstat3};
use nfs3_server::vfs::{NfsFileSystem, NfsReadFileSystem};
use sinomo_vfs_sifs::test_support::{NativeMountedOwner, open_registered};
use std::time::Duration;

/// The daemon's end of the handoff over the real outbox: it applies the
/// daemon's own transitions, and a test makes it silent or makes it refuse.
type TestDaemon = dfs::MountedSifsHandoffTestDaemon;

/// An export over a real native owner whose handoff talks to a [`TestDaemon`].
struct TestExport {
    fs: SifsNfsFileSystem,
    handoff: Arc<NfsMountedMutationHandoff>,
    daemon: Arc<TestDaemon>,
    store: dfs::MountedSifsMutationOutboxStore,
    // Dropped last: the export's clients must be gone before their owner.
    _outbox: sinomo_testdir::TestDir,
    owner: NativeMountedOwner,
}

impl TestExport {
    fn open(name: &str) -> Self {
        let owner = NativeMountedOwner::start(name).expect("start native SIFS owner");
        let outbox = sinomo_testdir::TestDir::new(&format!("{name}-outbox"));
        let store = dfs::MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(outbox.path()))
            .expect("open the export's outbox");
        let session = store
            .open_host_mount_session(dfs::MountedSifsMutationAdapter::Nfs, "nfs:test")
            .expect("open the export's host-mount session");
        let daemon = TestDaemon::new(store.clone());
        let closing = dfs::DfsCancellationFlag::new();
        let handoff = Arc::new(
            NfsMountedMutationHandoff::start(store.clone(), session, daemon.clone(), &closing)
                .expect("start the export's handoff"),
        );
        let mut opened = open_registered(owner.data_dir(), Some(closing.clone()))
            .expect("registered native SIFS owner")
            .expect("open the export's service");
        let root_stat = opened
            .service
            .posix_lstat_distributed("/")
            .expect("stat the export root");
        let fs = SifsNfsFileSystem::from_opened(
            opened,
            root_stat,
            "/".into(),
            false,
            Some(Arc::clone(&handoff)),
            closing,
        )
        .expect("open the export");
        Self {
            fs,
            handoff,
            daemon,
            store,
            _outbox: outbox,
            owner,
        }
    }

    /// A second client of the same owner, for driving one handoff directly.
    fn service(&self) -> ServiceLock {
        dfs::MountedSifsLock::new(
            open_registered(self.owner.data_dir(), None)
                .expect("registered native SIFS owner")
                .expect("open a second service")
                .service,
        )
    }

    fn status(&self) -> dfs::MountedSifsMutationOutboxStatus {
        self.store.status().expect("outbox status")
    }
}

fn filename(value: &str) -> filename3<'static> {
    filename3::from(value.as_bytes().to_vec())
}

/// Wait for `reached`, which an event the test already caused will make true.
async fn eventually(what: &str, reached: impl Fn() -> bool) {
    let until = Instant::now() + Duration::from_secs(20);
    while !reached() {
        assert!(Instant::now() < until, "{what} did not happen");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_daemon_is_sent_the_step_again_and_the_export_serves_reads_meanwhile() {
    let export = TestExport::open("nfs-handoff-resend");
    let root = export.fs.root_dir();
    export.daemon.set_silent(true);
    let mkdir = tokio::spawn({
        let fs = export.fs.clone();
        let root = root.clone();
        async move { fs.mkdir(&root, &filename("made")).await }
    });
    // No budget: the request keeps sending Arm for as long as it takes.
    eventually("Arm sent again", || {
        export.daemon.attempts_of(TestDaemon::ARM) >= 3
    })
    .await;
    assert!(!mkdir.is_finished(), "the request waits for the daemon");
    // The request waits for the daemon without the service lock: reads on
    // the same export are answered meanwhile.
    assert!(export.fs.getattr(&root).await.is_ok());
    assert!(matches!(
        export.fs.lookup(&root, &filename("made")).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
    assert_eq!(
        export.handoff.handoff.unfinished_count(),
        0,
        "the request itself re-sends within its own call"
    );

    export.daemon.set_silent(false);
    let (made, _) = mkdir
        .await
        .unwrap()
        .expect("the mutation runs once the daemon acknowledges");
    assert!(export.fs.getattr(&made).await.is_ok());
    assert_eq!(export.daemon.attempts_of(TestDaemon::COMMIT_APPLIED), 1);
    assert_eq!(export.status().pending_marker_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_past_its_budget_ends_with_an_io_error_and_its_step_is_sent_until_acknowledged() {
    let export = TestExport::open("nfs-handoff-budget");
    let root = export.fs.root_dir();
    let budgeted = export
        .fs
        .clone()
        .with_operation_budget(Some(Duration::from_millis(150)));
    export.daemon.set_silent(true);
    let started = Instant::now();
    assert!(matches!(
        budgeted.mkdir(&root, &filename("late")).await,
        Err(nfsstat3::NFS3ERR_IO)
    ));
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "the request waited for its whole budget"
    );
    // The export is not fenced: it serves, and the step the request left
    // behind is with the re-sender.
    assert!(budgeted.getattr(&root).await.is_ok());
    eventually("the step reached the re-sender", || {
        export.handoff.handoff.unfinished_count() == 1
    })
    .await;
    let sent = export.daemon.attempts_of(TestDaemon::ARM);
    eventually("the re-sender sent Arm again", || {
        export.daemon.attempts_of(TestDaemon::ARM) > sent
    })
    .await;
    assert_eq!(export.status().live_marker_count, 1);

    export.daemon.set_silent(false);
    eventually("the daemon acknowledged the step", || {
        export.handoff.handoff.unfinished_count() == 0 && export.status().pending_marker_count == 0
    })
    .await;
    // The mutation never entered the service: its marker closed as no-write.
    assert_eq!(export.daemon.attempts_of(TestDaemon::PROVE_NO_WRITE), 1);
    assert!(matches!(
        budgeted.lookup(&root, &filename("late")).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
    assert!(budgeted.mkdir(&root, &filename("after")).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_outbox_holds_a_mutation_until_a_marker_is_released() {
    let export = TestExport::open("nfs-handoff-outbox-full");
    let root = export.fs.root_dir();
    let mut fillers = Vec::new();
    for _ in 0..dfs::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
        fillers.push(
            export
                .handoff
                .handoff
                .prepare("nfs.test-filler", None)
                .expect("the outbox has room")
                .into_owned(),
        );
    }
    assert!(matches!(
        export.handoff.handoff.prepare("nfs.test-overflow", None),
        Err(dfs::MountedSifsHandoffStepError::Unacknowledged(_))
    ));

    let mkdir = tokio::spawn({
        let fs = export.fs.clone();
        let root = root.clone();
        async move { fs.mkdir(&root, &filename("queued")).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !mkdir.is_finished(),
        "a full outbox is waited on, not refused"
    );
    assert!(export.fs.getattr(&root).await.is_ok());
    assert_eq!(export.daemon.attempts_of(TestDaemon::ARM), 0);

    // Close one marker as no-write: its release makes room.
    let mut released = fillers.pop().unwrap();
    let handoff = &export.handoff.handoff;
    let arm_request_id = dfs::MountedSifsMutationArmRequestId::generate();
    handoff.arm(&released, &arm_request_id).unwrap();
    handoff
        .persist_proven_no_write(&mut released, &arm_request_id, "nfs-test-filler-released")
        .unwrap();
    handoff
        .commit_proven_no_write(&released, &arm_request_id, "nfs-test-filler-released")
        .unwrap();
    handoff.release_after_daemon_ack(released).unwrap();

    assert!(mkdir.await.unwrap().is_ok());
    drop(fillers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_arm_ends_the_request_and_releases_its_marker_and_the_lane() {
    let export = TestExport::open("nfs-handoff-refused-arm");
    let root = export.fs.root_dir();
    export.daemon.refuse(TestDaemon::ARM);
    // No budget: only the refusal can end this request.
    assert!(matches!(
        export.fs.mkdir(&root, &filename("refused")).await,
        Err(nfsstat3::NFS3ERR_IO)
    ));
    assert_eq!(
        export.daemon.attempts_of(TestDaemon::ARM),
        1,
        "a refusal is not re-sent"
    );
    assert_eq!(export.daemon.attempts_of(TestDaemon::ABORT_UNSTARTED), 1);
    assert_eq!(
        export.status().pending_marker_count,
        0,
        "the marker was aborted as unstarted"
    );
    assert!(matches!(
        export.fs.lookup(&root, &filename("refused")).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));

    export.daemon.refuse_nothing();
    assert!(
        export.fs.mkdir(&root, &filename("next")).await.is_ok(),
        "the mutation lane was released"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_commit_ends_the_request_and_leaves_its_exact_work_to_recovery() {
    let export = TestExport::open("nfs-handoff-refused-commit");
    let root = export.fs.root_dir();
    export.daemon.refuse(TestDaemon::COMMIT_APPLIED);
    assert!(matches!(
        export.fs.mkdir(&root, &filename("applied")).await,
        Err(nfsstat3::NFS3ERR_IO)
    ));
    assert_eq!(export.daemon.attempts_of(TestDaemon::COMMIT_APPLIED), 1);
    // The change is made; the daemon's recovery reads its exact captured
    // work from the marker now that the request's lease has ended.
    assert!(export.fs.lookup(&root, &filename("applied")).await.is_ok());
    let status = export.status();
    assert_eq!(status.exact_recovery_marker_count, 1);
    assert_eq!(status.live_marker_count, 0);
    eventually(
        "the daemon was asked to look for the recoverable marker",
        || export.daemon.answered_recovery_wakes() >= 1,
    )
    .await;

    export.daemon.refuse_nothing();
    assert!(export.fs.mkdir(&root, &filename("next")).await.is_ok());
}

#[test]
fn a_refused_error_record_goes_to_recovery_while_the_export_serves() {
    let export = TestExport::open("nfs-handoff-refused-unknown");
    let service = export.service();
    export.daemon.refuse(TestDaemon::COMMIT_UNKNOWN);
    let result: std::result::Result<(), dfs::DfsPosixError> =
        export
            .handoff
            .run_operation("nfs.test-error", None, &service, |_service| {
                Err(dfs::DfsPosixError::Conflict {
                    path: "/refused".into(),
                    reason: "an outcome the adapter cannot prove wrote nothing".into(),
                })
            });
    assert!(
        matches!(result, Err(dfs::DfsPosixError::Dfs(_))),
        "the refusal is this request's answer: {result:?}"
    );
    assert_eq!(export.daemon.attempts_of(TestDaemon::COMMIT_UNKNOWN), 1);
    // The released lease hands the record to recovery while the export's
    // session lives, and the daemon is asked to look for it.
    let status = export.status();
    assert_eq!(status.full_recovery_marker_count, 1);
    assert_eq!(status.deferred_live_session_marker_count, 0);
    let until = Instant::now() + Duration::from_secs(20);
    while export.daemon.answered_recovery_wakes() == 0 {
        assert!(Instant::now() < until, "the daemon was not asked to look");
        std::thread::sleep(Duration::from_millis(2));
    }

    export.daemon.refuse_nothing();
    let next: std::result::Result<(), dfs::DfsPosixError> =
        export
            .handoff
            .run_operation("nfs.test-next", None, &service, |_service| Ok(()));
    assert!(next.is_ok(), "the export keeps serving mutations");
}
