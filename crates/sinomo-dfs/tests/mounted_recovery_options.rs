use sinomo_dfs::dfs_mounted_mutation_recovery_tick_options;

#[test]
fn mounted_recovery_finishes_replay_without_waiting_for_adaptive_work() {
    let options = dfs_mounted_mutation_recovery_tick_options();
    assert!(options.namespace_job_max_jobs > 0);
    assert!(options.namespace_job_max_steps_per_job > 0);
    assert!(options.write_intent_max_records > 0);
    assert_eq!(options.placement_reconciliation_max_objects, 0);
    assert_eq!(options.repair_max_jobs, 0);
    assert!(!options.publish_metadata);
    assert!(!options.lifecycle.collect_metadata_history);
    assert!(!options.lifecycle.collect_content_versions);
    assert!(!options.lifecycle.collect_orphan_content);
}
