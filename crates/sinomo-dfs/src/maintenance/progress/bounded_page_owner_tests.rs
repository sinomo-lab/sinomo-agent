//! Private owner/checkpoint DATA controls. These do not claim native authority.
//! Native producer and new-input controls are in native_page_tests.
use super::*;

fn scope() -> Scope {
    Scope {
        file_system_id: crate::sifs_file_system_id(),
        node: sinomo_core::node::NodeId::generate(),
        shards: Vec::new(),
        options: crate::dfs_resident_routine_maintenance_tick_options(),
        projection_revision: None,
    }
}
fn owner() -> Arc<DfsMaintenanceProgressState> {
    let owner = Arc::new(DfsMaintenanceProgressState::default());
    owner.bind_native_generation(3, 4).unwrap();
    owner
}
fn intent_page(turn: &mut Turn) {
    turn.report().write_intents.scan_has_more = true;
    turn.report().write_intents.visited_intent_shard_count = 1;
    turn.report().write_intents.visited_intent_record_count = 2;
}
fn repair_page(turn: &mut Turn) {
    turn.report().write_intents.scan_has_more = false;
    turn.report().repair_budget_exhausted = true;
    turn.report().repair_page_checkpoint.has_more = true;
    turn.report().repair_page_checkpoint.completed_record_count = 2;
}

#[test]
fn bounded_intent_page_retains_owner_and_cannot_finish_recovery() {
    let owner = owner();
    let scope = scope();
    let before = owner.epoch().unwrap();
    let mut turn = owner.begin(scope.clone()).unwrap();
    turn.advance(Phase::WriteIntents);
    intent_page(&mut turn);
    let report = turn.write_intent_page_report().unwrap();
    assert!(super::super::dfs_maintenance_report_reached_limit(&report));
    assert!(owner.has_unfinished_turn().unwrap());
    assert!(owner.epoch().unwrap() > before);
    let resumed = owner.begin(scope).unwrap();
    assert_eq!(resumed.phase(), Phase::WriteIntents);
    assert!(!resumed.continues_repair_sweep());
    assert!(
        resumed.finish().is_err(),
        "bounded DATA must not relax the existing full-turn finish guard"
    );
}

#[test]
fn partial_page_never_borrows_another_phase_or_zero_progress() {
    for wrong_phase in [false, true] {
        let owner = owner();
        let mut turn = owner.begin(scope()).unwrap();
        turn.advance(if wrong_phase {
            Phase::Repair
        } else {
            Phase::WriteIntents
        });
        intent_page(&mut turn);
        if !wrong_phase {
            turn.report().write_intents.visited_intent_shard_count = 0;
        }
        repair_page(&mut turn); // Another positive report cannot supply an intent page.
        turn.report().write_intents.scan_has_more = true;
        assert!(turn.write_intent_page_report().is_err());
    }
    for missing in [0, 1, 2] {
        let owner = owner();
        let mut turn = owner.begin(scope()).unwrap();
        turn.advance(Phase::Publish);
        repair_page(&mut turn);
        match missing {
            0 => turn.report().repair_page_checkpoint.completed_record_count = 0,
            1 => turn.report().repair_page_checkpoint.has_more = false,
            _ => turn.report().write_intents.scan_has_more = true,
        }
        assert!(turn.repair_sweep_report_after_publish().is_err());
    }
}

#[test]
fn repair_sweep_revisits_prior_owners_and_survives_prelude_cancellation() {
    let owner = owner();
    let scope = scope();
    let mut turn = owner.begin(scope.clone()).unwrap();
    turn.advance(Phase::Publish);
    repair_page(&mut turn);
    let report = turn.repair_sweep_report_after_publish().unwrap();
    assert!(super::super::dfs_maintenance_report_reached_limit(&report));
    for phase in [
        Phase::Handoff,
        Phase::NamespaceRequests,
        Phase::NamespaceJobs,
    ] {
        let mut resumed = owner.begin(scope.clone()).unwrap();
        assert!(resumed.continues_repair_sweep());
        if phase != Phase::Handoff {
            resumed.advance(phase);
        }
        drop(resumed); // Same cancellation/drop owner as the actual future.
        assert!(owner.has_unfinished_turn().unwrap());
    }
    let mut resumed = owner.begin(scope).unwrap();
    assert_eq!(resumed.phase(), Phase::NamespaceJobs);
    assert!(resumed.continues_repair_sweep());
    resumed.advance(Phase::Placement);
    resumed.advance(Phase::Publish);
    resumed.report().repair_budget_exhausted = false;
    resumed.report().repair_page_checkpoint.has_more = false;
    resumed.advance(Phase::Complete);
    let final_report = resumed.finish().unwrap();
    assert!(!super::super::dfs_maintenance_report_reached_limit(
        &final_report
    ));
    assert!(!owner.has_unfinished_turn().unwrap());
}

#[test]
fn old_native_generation_and_changed_budget_cannot_reuse_partial_report() {
    let owner = owner();
    let scope = scope();
    let mut old = owner.begin(scope.clone()).unwrap();
    old.advance(Phase::WriteIntents);
    intent_page(&mut old);
    owner.bind_native_generation(3, 5).unwrap();
    assert!(old.write_intent_page_report().is_err());
    assert!(!owner.has_unfinished_turn().unwrap());
    let mut current = owner.begin(scope.clone()).unwrap();
    assert_eq!(current.phase(), Phase::Handoff);
    current.advance(Phase::Publish);
    repair_page(&mut current);
    current.repair_sweep_report_after_publish().unwrap();
    let mut changed = scope;
    changed.options.repair_max_jobs = 0;
    let reset = owner.begin(changed).unwrap();
    assert_eq!(reset.phase(), Phase::Handoff);
    assert!(!reset.continues_repair_sweep());
}

#[test]
fn explicitly_disabled_intent_scan_is_distinct_from_unfinished_enabled_scan() {
    let owner = owner();
    let mut disabled = scope();
    disabled.options.write_intent_max_records = 0;
    let mut turn = owner.begin(disabled.clone()).unwrap();
    turn.advance(Phase::Publish);
    repair_page(&mut turn);
    turn.report().write_intents.scan_has_more = true;
    assert!(turn.continues_repair_sweep());
    turn.repair_sweep_report_after_publish().unwrap();
    let resumed = owner.begin(disabled).unwrap();
    assert_eq!(resumed.phase(), Phase::Handoff);
    assert!(resumed.continues_repair_sweep());
}
