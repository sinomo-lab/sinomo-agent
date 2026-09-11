//! Locks regression tests.

use super::*;

#[test]
fn sifs_cluster_routes_advisory_locks_and_promotes_fifo_waiters() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-routed-advisory-lock");
    let path = "/users/distributed-lock.txt";
    sifs_cluster_routed_posix_write(&mut cluster, path, b"locked bytes");

    let holder_session = DfsAdvisoryLockSessionId::new("holder-session");
    let holder_command = DfsAdvisoryLockCommand::Acquire {
        request_id: DfsAdvisoryLockRequestId::new("holder-acquire"),
        session_id: holder_session.clone(),
        mode: DfsAdvisoryLockMode::Exclusive,
        blocking: false,
        process_id: Some(101),
    };
    let path_for_call = path.to_string();
    let holder_outcome = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "exclusive advisory-lock grant",
        move |writer| writer.posix_advisory_lock(&path_for_call, holder_command),
    )
    .expect("holder lock request");
    let holder_grant = match holder_outcome {
        DfsAdvisoryLockOutcome::Acquired { grant } => grant,
        outcome => panic!("expected holder grant, got {outcome:?}"),
    };

    let waiter_session = DfsAdvisoryLockSessionId::new("waiter-session");
    let waiter_command = DfsAdvisoryLockCommand::Acquire {
        request_id: DfsAdvisoryLockRequestId::new("waiter-acquire"),
        session_id: waiter_session.clone(),
        mode: DfsAdvisoryLockMode::Shared,
        blocking: true,
        process_id: Some(202),
    };
    let path_for_call = path.to_string();
    let first_waiter_outcome =
        sifs_cluster_writer_call_with_pump(&mut cluster, "blocking advisory-lock enqueue", {
            let waiter_command = waiter_command.clone();
            move |writer| writer.posix_advisory_lock(&path_for_call, waiter_command)
        })
        .expect("waiter lock request");
    assert!(matches!(
        first_waiter_outcome,
        DfsAdvisoryLockOutcome::Waiting { position: 1, .. }
    ));

    let path_for_call = path.to_string();
    let unlock_outcome = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "exact advisory-lock unlock",
        move |writer| {
            writer.posix_advisory_lock(
                &path_for_call,
                DfsAdvisoryLockCommand::Unlock {
                    request_id: DfsAdvisoryLockRequestId::new("holder-unlock"),
                    token: holder_grant.token,
                },
            )
        },
    )
    .expect("holder unlock request");
    assert_eq!(unlock_outcome, DfsAdvisoryLockOutcome::Unlocked);

    let path_for_call = path.to_string();
    let promoted_outcome = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "FIFO advisory-lock promotion",
        move |writer| writer.posix_advisory_lock(&path_for_call, waiter_command),
    )
    .expect("promoted waiter request");
    let promoted_grant = match promoted_outcome {
        DfsAdvisoryLockOutcome::Acquired { grant } => grant,
        outcome => panic!("expected promoted grant, got {outcome:?}"),
    };
    assert!(
        promoted_grant
            .token
            .session_id
            .0
            .ends_with(&waiter_session.0)
    );

    let path_for_call = path.to_string();
    let close_outcome = sifs_cluster_writer_call_with_pump(
        &mut cluster,
        "advisory-lock close cleanup",
        move |writer| {
            writer.posix_advisory_lock(
                &path_for_call,
                DfsAdvisoryLockCommand::Close {
                    request_id: DfsAdvisoryLockRequestId::new("waiter-close"),
                    session_id: waiter_session,
                },
            )
        },
    )
    .expect("waiter close request");
    assert!(matches!(
        close_outcome,
        DfsAdvisoryLockOutcome::Closed {
            released_grant: true,
            ..
        }
    ));

    assert!(cluster.rejections.is_empty(), "{:?}", cluster.rejections);
    std::fs::remove_dir_all(&cluster.root).ok();
}
