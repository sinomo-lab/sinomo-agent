//! Advisory turn checkpoints. Durable queues, journals and native authority
//! remain the only effect/recovery evidence; this state only avoids restarting
//! completed bounded phases when the owning future yields to foreground work.
use super::*;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum Phase {
    #[default]
    Handoff,
    NamespaceRequests,
    NamespaceJobs,
    WriteIntents,
    Placement,
    MetadataLifecycle,
    ContentLifecycle,
    Repair,
    Publish,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Scope {
    pub file_system_id: DfsFileSystemId,
    pub node: NodeId,
    pub shards: Vec<DfsMetadataShardConfig>,
    pub options: DfsMaintenanceTickOptions,
    pub projection_revision: Option<sinomo_core::kv::KvRevision>,
}

#[derive(Debug)]
struct Checkpoint {
    scope: Scope,
    phase: Phase,
    repair_before_placement: bool,
    report: DfsMaintenanceTickReport,
}

#[derive(Debug, Default)]
struct State {
    native_generation: Option<(u64, u64)>,
    active: bool,
    progress_epoch: u64,
    checkpoint: Option<Checkpoint>,
    uncheckpointed_births: Vec<DfsRepairAttemptRecord>,
}

#[derive(Debug, Default)]
pub(crate) struct DfsMaintenanceProgressState(Mutex<State>);

impl DfsMaintenanceProgressState {
    pub(crate) fn bind_native_generation(
        &self,
        authority: u64,
        storage: u64,
    ) -> Result<(), DfsError> {
        let mut state = self.0.lock().map_err(|_| poisoned())?;
        let generation = Some((authority, storage));
        if state.native_generation != generation {
            state.native_generation = generation;
            state.checkpoint = None;
        }
        Ok(())
    }

    pub(super) fn checkpoint_births<T: DfsMessageTransport>(
        &self,
        service: &mut NetworkedDfsService<T>,
        host: &mut dyn DfsMaintenanceHost,
    ) -> Result<(), DfsError> {
        {
            let mut state = self.0.lock().map_err(|_| poisoned())?;
            state
                .uncheckpointed_births
                .extend(service.take_message_committed_repair_queue_records());
            service.coordinator.ensure_message_repair_job_capture();
        }
        self.retry_births(host)
    }

    pub(super) fn retry_births(&self, host: &mut dyn DfsMaintenanceHost) -> Result<(), DfsError> {
        let mut state = self.0.lock().map_err(|_| poisoned())?;
        if !state.uncheckpointed_births.is_empty() {
            let page = host.load_repair_maintenance_page(0)?;
            let page =
                host.enroll_repair_maintenance_page(&page, 0, &state.uncheckpointed_births)?;
            host.checkpoint_repair_maintenance_page(&page, &BTreeSet::new(), &[], &[])?;
            state.uncheckpointed_births.clear();
        }
        Ok(())
    }

    pub(crate) fn epoch(&self) -> Result<u64, DfsError> {
        Ok(self.0.lock().map_err(|_| poisoned())?.progress_epoch)
    }
    pub(super) fn restart_after_error(&self) -> Result<(), DfsError> {
        let mut state = self.0.lock().map_err(|_| poisoned())?;
        if state.active {
            return Err(DfsError::Metadata {
                message: "cannot restart phases while a maintenance turn is active".into(),
            });
        }
        // A returned error can require an earlier phase to advance a dependency
        // (for example a placement scan before its repair permit). Retain all
        // durable work and unacknowledged births, but re-evaluate phase ordering.
        // Cancellation does not call this: ordinary reads retain their progress.
        state.checkpoint = None;
        Ok(())
    }
    pub(crate) fn unfinished_progress_since(&self, epoch: u64) -> Result<bool, DfsError> {
        let state = self.0.lock().map_err(|_| poisoned())?;
        Ok(state.progress_epoch != epoch && state.checkpoint.is_some())
    }

    /// A foreground waiter can interrupt reentry before this turn advances.
    /// Retain that unfinished continuation without inventing a progress epoch.
    pub(crate) fn has_unfinished_turn(&self) -> Result<bool, DfsError> {
        let state = self.0.lock().map_err(|_| poisoned())?;
        Ok(state.checkpoint.is_some())
    }

    pub(super) fn begin(self: &Arc<Self>, mut scope: Scope) -> Result<Turn, DfsError> {
        let mut state = self.0.lock().map_err(|_| poisoned())?;
        if state.active {
            return Err(DfsError::Metadata {
                message: "maintenance turn already has an active owner".into(),
            });
        }
        // Main binds its actual retained authority/storage generations. Other
        // callers conservatively restart phases when their supplied cut changes.
        if state.native_generation.is_some() {
            scope.projection_revision = None;
        }
        let checkpoint = state
            .checkpoint
            .take()
            .filter(|saved| saved.scope == scope)
            .unwrap_or_else(|| Checkpoint {
                scope,
                phase: Phase::Handoff,
                repair_before_placement: false,
                report: empty_report(),
            });
        state.active = true;
        Ok(Turn {
            owner: Arc::clone(self),
            native_generation: state.native_generation,
            started_phase: checkpoint.phase,
            started_repair_turn: checkpoint.repair_before_placement,
            checkpoint: Some(checkpoint),
            yielded_after_repair: false,
        })
    }
}

fn poisoned() -> DfsError {
    DfsError::Metadata {
        message: "maintenance progress owner is poisoned".into(),
    }
}

fn empty_report() -> DfsMaintenanceTickReport {
    DfsMaintenanceTickReport {
        namespace_jobs: Default::default(),
        write_intents: Default::default(),
        placement_reconciliation: None,
        lifecycle_reports: Vec::new(),
        scheduled_repairs: Vec::new(),
        followup_events: Vec::new(),
        repair_attempts_before: Vec::new(),
        repair_attempts_after: Vec::new(),
        repair_budget_exhausted: false,
        repair_page_checkpoint: Default::default(),
        metadata_replication: DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        },
    }
}

pub(super) struct Turn {
    owner: Arc<DfsMaintenanceProgressState>,
    native_generation: Option<(u64, u64)>,
    checkpoint: Option<Checkpoint>,
    started_phase: Phase,
    started_repair_turn: bool,
    yielded_after_repair: bool,
}
impl Turn {
    pub(super) fn phase(&self) -> Phase {
        self.checkpoint.as_ref().expect("active turn").phase
    }
    pub(super) fn needs_repair_turn(&self) -> bool {
        let checkpoint = self.checkpoint.as_ref().expect("active turn");
        checkpoint.phase == Phase::Placement && checkpoint.repair_before_placement
    }
    pub(super) fn defer_after_repair(mut self) {
        assert_eq!(self.phase(), Phase::Placement);
        self.checkpoint
            .as_mut()
            .expect("active turn")
            .repair_before_placement = false;
        self.yielded_after_repair = true;
    }
    pub(super) fn report(&mut self) -> &mut DfsMaintenanceTickReport {
        &mut self.checkpoint.as_mut().expect("active turn").report
    }
    pub(super) fn advance(&mut self, next: Phase) {
        let checkpoint = self.checkpoint.as_mut().expect("active turn");
        assert!(
            next > checkpoint.phase,
            "completed phases advance once per turn"
        );
        checkpoint.phase = next;
        checkpoint.repair_before_placement = false;
    }
    /// Per-page DATA for the existing bounded-tick handoff. This does not
    /// finish the turn or acknowledge a recovery generation. The existing
    /// reached-limit predicate must remain true, so normal runtime consumers
    /// cannot mistake this report for whole-pass completion.
    fn checkpointed_page_report(&self) -> Result<DfsMaintenanceTickReport, DfsError> {
        let checkpoint = self.checkpoint.as_ref().expect("active turn");
        if !super::dfs_maintenance_report_reached_limit(&checkpoint.report) {
            return Err(DfsError::Metadata {
                message: "unfinished maintenance page has no actual bounded progress".into(),
            });
        }
        let mut state = self.owner.0.lock().map_err(|_| poisoned())?;
        if state.native_generation != self.native_generation
            || !state.uncheckpointed_births.is_empty()
        {
            return Err(DfsError::Metadata {
                message: "bounded maintenance page lost its generation or durable births".into(),
            });
        }
        state.progress_epoch = state.progress_epoch.wrapping_add(1);
        Ok(checkpoint.report.clone())
    }

    pub(super) fn write_intent_page_report(self) -> Result<DfsMaintenanceTickReport, DfsError> {
        let checkpoint = self.checkpoint.as_ref().expect("active turn");
        if checkpoint.phase != Phase::WriteIntents
            || !checkpoint.report.write_intents.scan_has_more
            || checkpoint.report.write_intents.visited_intent_shard_count == 0
        {
            return Err(DfsError::Metadata {
                message: "maintenance intent continuation lacks its actual bounded page".into(),
            });
        }
        self.checkpointed_page_report()
    }

    /// Only this existing, acknowledged report identifies a repair sweep whose
    /// earlier intent inventory already reached EOF. It is never inferred from
    /// a queue count, a timer, or an ordinary metadata-input "generation".
    pub(super) fn continues_repair_sweep(&self) -> bool {
        let report = &self.checkpoint.as_ref().expect("active turn").report;
        (self
            .checkpoint
            .as_ref()
            .expect("active turn")
            .scope
            .options
            .write_intent_max_records
            == 0
            || !report.write_intents.scan_has_more)
            && report.repair_budget_exhausted
            && report.repair_page_checkpoint.has_more
            && (report.repair_page_checkpoint.completed_record_count > 0
                || report.repair_page_checkpoint.operation_count > 0)
    }

    pub(super) fn repair_sweep_report_after_publish(
        mut self,
    ) -> Result<DfsMaintenanceTickReport, DfsError> {
        let checkpoint = self.checkpoint.as_ref().expect("active turn");
        if checkpoint.phase != Phase::Publish || !self.continues_repair_sweep() {
            return Err(DfsError::Metadata {
                message: "maintenance repair continuation lacks its checkpointed published page"
                    .into(),
            });
        }
        let report = self.checkpointed_page_report()?;
        // This sole rewind follows the actual page's normal Publish phase.
        // Revisit normal handoff/namespace/placement/lifecycle work per page;
        // only the intent sweep's genuine EOF stays latched in this same report.
        self.checkpoint.as_mut().expect("active turn").phase = Phase::Handoff;
        Ok(report)
    }

    pub(super) fn finish(mut self) -> Result<DfsMaintenanceTickReport, DfsError> {
        if self.phase() != Phase::Complete {
            return Err(DfsError::Metadata {
                message: "unfinished maintenance cannot report completion".into(),
            });
        }
        let state = self.owner.0.lock().map_err(|_| poisoned())?;
        if state.native_generation != self.native_generation
            || !state.uncheckpointed_births.is_empty()
        {
            return Err(DfsError::Metadata {
                message:
                    "maintenance completion lost its generation or still has uncheckpointed work"
                        .into(),
            });
        }
        drop(state);
        Ok(self.checkpoint.take().expect("complete turn").report)
    }
}
impl Drop for Turn {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.active = false;
        // Cancellation preserves only completed phase reports. A changed native
        // generation cannot recover an old scheduling checkpoint. No admission,
        // backend borrow or publication capability is retained here.
        if state.native_generation == self.native_generation {
            if let Some(checkpoint) = self.checkpoint.as_mut()
                && checkpoint.phase == Phase::Placement
                && !self.yielded_after_repair
            {
                // An unfinished asynchronous placement page cannot stand in
                // front of already-durable repairs. Its own durable page cursor
                // stays pending; give the queue one bounded turn on reentry.
                checkpoint.repair_before_placement = true;
            }
            if self.checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.phase != self.started_phase
                    || checkpoint.repair_before_placement != self.started_repair_turn
            }) {
                state.progress_epoch = state.progress_epoch.wrapping_add(1);
            }
            state.checkpoint = self.checkpoint.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> Scope {
        Scope {
            file_system_id: crate::sifs_file_system_id(),
            node: NodeId::from_legacy_named("maintenance-progress-owner").unwrap(),
            shards: Vec::new(),
            options: dfs_resident_routine_maintenance_tick_options(),
            projection_revision: Some(sinomo_core::kv::KvRevision(7)),
        }
    }

    #[tokio::test]
    async fn cancellation_retains_completed_phase_and_report_without_retaining_admission() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        owner.bind_native_generation(3, 4).unwrap();
        let mut future = Box::pin(async {
            let mut turn = owner.begin(scope()).unwrap();
            turn.report().namespace_jobs.scanned_job_count = 2;
            turn.advance(Phase::NamespaceRequests);
            std::future::pending::<()>().await;
        });
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                future.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        assert!(
            owner.begin(scope()).is_err(),
            "one turn retains the checkpoint until cancellation drains it"
        );
        drop(future);
        let mut resumed = owner.begin(scope()).unwrap();
        assert_eq!(resumed.phase(), Phase::NamespaceRequests);
        assert_eq!(resumed.report().namespace_jobs.scanned_job_count, 2);
        assert_eq!(
            resumed.checkpoint.as_ref().unwrap().scope.options,
            scope().options
        );
    }

    #[test]
    fn repeated_preemption_retains_pending_turn_without_inventing_progress() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        owner.bind_native_generation(3, 4).unwrap();
        assert!(!owner.has_unfinished_turn().unwrap());
        let mut first = owner.begin(scope()).unwrap();
        first.advance(Phase::Repair);
        drop(first);
        let saved_epoch = owner.epoch().unwrap();
        for _ in 0..8 {
            let resumed = owner.begin(scope()).unwrap();
            assert_eq!(resumed.phase(), Phase::Repair);
            drop(resumed);
            assert!(owner.has_unfinished_turn().unwrap());
            assert!(!owner.unfinished_progress_since(saved_epoch).unwrap());
        }
        // The scheduling continuation grants neither authority nor completion.
        owner.bind_native_generation(4, 4).unwrap();
        assert!(!owner.has_unfinished_turn().unwrap());
        let mut current = owner.begin(scope()).unwrap();
        assert_eq!(current.phase(), Phase::Handoff);
        current.advance(Phase::Complete);
        current.finish().unwrap();
        assert!(!owner.has_unfinished_turn().unwrap());
    }

    #[test]
    fn incomplete_turn_never_reports_success() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        assert!(owner.begin(scope()).unwrap().finish().is_err());
        assert_eq!(owner.begin(scope()).unwrap().phase(), Phase::Handoff);
    }

    #[test]
    fn returned_error_restarts_phase_dependencies_after_the_turn_drains() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        let mut turn = owner.begin(scope()).unwrap();
        turn.advance(Phase::Repair);
        assert!(owner.restart_after_error().is_err());
        drop(turn);
        owner.restart_after_error().unwrap();
        assert_eq!(owner.begin(scope()).unwrap().phase(), Phase::Handoff);
    }

    #[test]
    fn interrupted_placement_gives_durable_repairs_a_turn_without_restarting_prior_phases() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        {
            let mut turn = owner.begin(scope()).unwrap();
            turn.report().write_intents.visited_intent_record_count = 4;
            turn.advance(Phase::Placement);
        }
        let mut resumed = owner.begin(scope()).unwrap();
        assert_eq!(resumed.phase(), Phase::Placement);
        assert!(resumed.needs_repair_turn());
        assert_eq!(
            resumed.report().write_intents.visited_intent_record_count,
            4
        );
        assert_eq!(
            resumed
                .checkpoint
                .as_ref()
                .unwrap()
                .scope
                .options
                .repair_max_jobs,
            dfs_resident_routine_maintenance_tick_options().repair_max_jobs
        );
    }

    #[test]
    fn repair_only_turn_yields_without_completing_or_repeating_placement() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        {
            let mut turn = owner.begin(scope()).unwrap();
            turn.advance(Phase::Placement);
        }
        let turn = owner.begin(scope()).unwrap();
        assert!(turn.needs_repair_turn());
        turn.defer_after_repair();
        let turn = owner.begin(scope()).unwrap();
        assert_eq!(turn.phase(), Phase::Placement);
        assert!(!turn.needs_repair_turn());
        assert!(
            turn.finish().is_err(),
            "a repair-only turn cannot finish the placement obligation"
        );
    }

    #[test]
    fn native_generation_rollover_rejects_old_completion_and_checkpoint() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        owner.bind_native_generation(3, 4).unwrap();
        let mut old = owner.begin(scope()).unwrap();
        old.advance(Phase::Complete);
        owner.bind_native_generation(4, 4).unwrap();
        assert!(
            old.finish().is_err(),
            "an old report cannot finish a new native generation"
        );
        assert_eq!(owner.begin(scope()).unwrap().phase(), Phase::Handoff);
        let mut old = owner.begin(scope()).unwrap();
        old.advance(Phase::Repair);
        owner.bind_native_generation(4, 5).unwrap();
        drop(old);
        assert_eq!(owner.begin(scope()).unwrap().phase(), Phase::Handoff);
    }

    #[test]
    fn changed_budget_or_scope_restarts_advisory_phases() {
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        {
            let mut old = owner.begin(scope()).unwrap();
            old.advance(Phase::Repair);
        }
        let mut changed = scope();
        changed.options.repair_max_jobs = changed.options.repair_max_jobs.saturating_sub(1);
        assert_eq!(owner.begin(changed).unwrap().phase(), Phase::Handoff);
        {
            let mut old = owner.begin(scope()).unwrap();
            old.advance(Phase::Repair);
        }
        let mut changed = scope();
        changed.projection_revision = Some(sinomo_core::kv::KvRevision(8));
        assert_eq!(owner.begin(changed).unwrap().phase(), Phase::Handoff);
    }
    #[test]
    fn native_repair_births_survive_failed_checkpoint_and_reopen_exactly() {
        use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule};
        let (native, nodes) =
            DfsTestRuntime::start_cluster("native-maintenance-birth-checkpoint", 3).unwrap();
        let path = "/services/birth-checkpoint.bin";
        let shard = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|row| row.shard_id == "services")
            .unwrap();
        let primary = shard.primary_node_id.clone();
        native
            .apply_placement_policy(
                &primary,
                path,
                crate::DfsPlacementScope::SelfOnly,
                crate::DfsPlacementPolicy::new(
                    sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                    1,
                    3,
                )
                .unwrap(),
            )
            .unwrap();
        native.install_fault(FaultRule {
            source: Some(primary.clone()),
            target: None,
            route: Some(crate::DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::BudgetExhausted,
        });
        let mut writer = native.metadata_primary_service(path).unwrap();
        let execution = writer
            .acquire_prepared_execution(&crate::DfsPreparedOperationScope {
                file_system_id: shard.file_system_id.clone(),
                shard_id: crate::DfsMetadataShardId::new(&shard.shard_id),
                owner_epoch: shard.epoch,
                operation_id: "native-birth-checkpoint-admission".into(),
            })
            .unwrap()
            .expect("real current producer shard execution admission");
        writer.coordinator.ensure_message_repair_job_capture();
        let written = writer
            .posix_write_file_with_operation_id(
                path,
                b"native checkpoint source",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 3).unwrap(),
                    replica_candidates: nodes,
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
                "native-birth-checkpoint-original",
            )
            .unwrap();
        assert_eq!(written.current_locations, vec![primary]);
        let root = sinomo_testdir::TestDir::new("native-maintenance-queue-reopen");
        let data = SinomoDataDir::new(root.path());
        data.ensure_root_layout().unwrap();
        let snapshot = default_dfs_repair_attempt_snapshot_path(&data);
        std::fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        let blocked_log = snapshot.with_file_name("repair-attempts.active.slog");
        // A negative filesystem fault in this test's new queue, never real DATA.
        std::fs::create_dir(&blocked_log).unwrap();
        let owner = Arc::new(DfsMaintenanceProgressState::default());
        let mut host = LocalDfsMaintenanceHost::new(data.clone());
        assert!(owner.checkpoint_births(&mut writer, &mut host).is_err());
        let retained = owner.0.lock().unwrap().uncheckpointed_births.clone();
        assert_eq!(
            retained.len(),
            2,
            "actual W1/N3 producer captured both missing replicas"
        );
        assert!(retained.iter().all(|row| row.object_id == written.object_id
            && row.version == written.version
            && Some(&row.content_hash) == written.content_hash.as_ref()));
        owner.restart_after_error().unwrap();
        assert_eq!(owner.0.lock().unwrap().uncheckpointed_births, retained);
        std::fs::remove_dir(&blocked_log).unwrap();
        owner.checkpoint_births(&mut writer, &mut host).unwrap();
        assert!(owner.0.lock().unwrap().uncheckpointed_births.is_empty());
        let durable = host.repair_queue_store.view().unwrap().records;
        assert_eq!(durable, retained);
        drop(host);
        drop(owner);
        let reopened = LocalDfsMaintenanceHost::new(data);
        assert_eq!(
            reopened.repair_queue_store.view().unwrap().records,
            durable,
            "reopening replays the exact durable captures, not a fabricated completion"
        );
        assert_eq!(
            writer
                .metadata_backend()
                .unwrap()
                .get_object(&written.object_id)
                .unwrap()
                .unwrap()
                .version,
            written.version
        );
        drop(reopened);
        drop(execution);
        drop(writer);
        native.close().unwrap();
    }
}

#[cfg(test)]
mod native_page_tests;
#[cfg(test)]
mod bounded_page_owner_tests;
