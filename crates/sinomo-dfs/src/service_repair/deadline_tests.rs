//! Clock-boundary DATA controls; these supply no native receipt or authority.
use super::*;
use crate::transfer::RepairRetryState;

fn input() -> (DfsRepairAttemptRecord, RepairRetryState) {
    let row = DfsRepairAttemptRecord {
        intent_id: "deadline-projection-data".into(),
        object_id: DfsObjectId::generate(),
        version: DfsVersion::generate(),
        content_hash: "blake3:deadline-projection-data".into(),
        missing_replica_node_id: NodeId::generate(),
        attempts: 1,
        in_flight: false,
        last_attempt_unix_nanos: 10,
        next_retry_unix_nanos: 15,
        transfer_request_id: None,
        last_error: Some("typed carrier was definitely not sent".into()),
        transfer_delivery_state: Some(
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
        ),
        completed_transfer_request_id: None,
        transfer_kind: Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair),
    };
    let state = RepairRetryState {
        attempts: 1,
        next_retry_tick: 9,
        transfer_delivery_state: row.transfer_delivery_state,
        ..Default::default()
    };
    (row, state)
}

#[test]
fn elapsed_absolute_deadline_projects_to_the_current_tick_without_another_attempt() {
    let (row, original) = input();
    let unchanged_row = row.clone();
    let mut before_due = original.clone();
    restore_settled_due_repair_tick(&row, &mut before_due, 3, 14);
    assert_eq!(format!("{before_due:?}"), format!("{original:?}"));
    for now in [15, 16, 40] {
        let mut state = original.clone();
        restore_settled_due_repair_tick(&row, &mut state, 3, now);
        let mut expected = original.clone();
        expected.next_retry_tick = 3;
        assert_eq!(format!("{state:?}"), format!("{expected:?}"));
        assert_eq!(
            row, unchanged_row,
            "projection never rewrites durable attempt DATA"
        );
    }
    let mut already_due = original;
    already_due.next_retry_tick = 2;
    restore_settled_due_repair_tick(&row, &mut already_due, 3, 40);
    assert_eq!(already_due.next_retry_tick, 2);
}

#[test]
fn future_parked_uncertain_and_newer_deferrals_keep_their_existing_ticks() {
    for case in 0..18 {
        let (mut row, mut state) = input();
        match case {
            0 => row.next_retry_unix_nanos = 41,
            1 => row.next_retry_unix_nanos = u64::MAX,
            2 => row.in_flight = true,
            3 => row.transfer_request_id = Some("named-native-carrier-data".into()),
            4 => row.completed_transfer_request_id = Some("completed-native-carrier-data".into()),
            5 => row.transfer_delivery_state = None,
            6 => row.transfer_kind = None,
            7 => {
                row.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Stage)
            }
            8 => {
                row.transfer_delivery_state =
                    Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
            }
            9 => row.attempts = 0,
            10 => row.last_attempt_unix_nanos = 0,
            11 => state.next_retry_tick = DFS_REPAIR_RETRY_SUPPRESSED_TICK,
            12 => state.inflight = true,
            13 => state.unresolved_custody_without_request = true,
            14 => state.attempts = 2,
            15 => state.transfer_delivery_state = None,
            16 => state.retrying_unresolved_transfer = true,
            17 => {
                state.completed_transfer_request_id = Some("completed-native-carrier-data".into())
            }
            _ => unreachable!(),
        }
        let original = format!("{state:?}");
        let unchanged_row = row.clone();
        restore_settled_due_repair_tick(&row, &mut state, 3, 40);
        assert_eq!(format!("{state:?}"), original, "negative DATA case {case}");
        assert_eq!(row, unchanged_row);
    }
}
