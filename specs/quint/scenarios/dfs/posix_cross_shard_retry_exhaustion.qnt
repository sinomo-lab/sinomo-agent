/// Deterministic regression for exhausted cross-shard destination retries.
///
/// Destination adoption succeeds before every simulated transport failure.
/// After the third failure the ordinary retry budget is exhausted, but durable
/// destination evidence still permits mandatory forward finalization. The
/// terminal state has one visible rename destination and no blocked paths.
module dfs_posix_cross_shard_retry_exhaustion_scenario {
  import dfs_posix_cross_shard_namespace from "../../models/dfs/posix_cross_shard_namespace"

  type Phase =
    | Start
    | Prepared
    | Adopted1
    | Failed1
    | Adopted2
    | Failed2
    | Adopted3
    | Exhausted
    | Completed

  var phase: Phase

  action init = all {
    dfs_posix_cross_shard_namespace::init,
    phase' = Start,
  }

  action step =
    if (phase == Start) {
      all {
        dfs_posix_cross_shard_namespace::startJob(
          dfs_posix_cross_shard_namespace::RenameMove,
          dfs_posix_cross_shard_namespace::LeftFile,
          dfs_posix_cross_shard_namespace::RightFile
        ),
        phase' = Prepared,
      }
    } else if (phase == Prepared) {
      all {
        dfs_posix_cross_shard_namespace::applyDestination,
        phase' = Adopted1,
      }
    } else if (phase == Adopted1) {
      all {
        dfs_posix_cross_shard_namespace::failAfterDestination,
        phase' = Failed1,
      }
    } else if (phase == Failed1) {
      all {
        dfs_posix_cross_shard_namespace::applyDestination,
        phase' = Adopted2,
      }
    } else if (phase == Adopted2) {
      all {
        dfs_posix_cross_shard_namespace::failAfterDestination,
        phase' = Failed2,
      }
    } else if (phase == Failed2) {
      all {
        dfs_posix_cross_shard_namespace::applyDestination,
        phase' = Adopted3,
      }
    } else if (phase == Adopted3) {
      all {
        dfs_posix_cross_shard_namespace::failAfterDestination,
        phase' = Exhausted,
      }
    } else if (phase == Exhausted) {
      all {
        dfs_posix_cross_shard_namespace::finishExhaustedRecoverable,
        phase' = Completed,
      }
    } else {
      all {
        phase == Completed,
        dfs_posix_cross_shard_namespace::stutterTerminal,
        phase' = phase,
      }
    }

  def safety: bool = all {
    dfs_posix_cross_shard_namespace::retryCount >= 0,
    dfs_posix_cross_shard_namespace::retryCount
      <= dfs_posix_cross_shard_namespace::MaxRetries,
    match phase {
      | Start => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::Idle,
          dfs_posix_cross_shard_namespace::blockedPaths == Set(),
        }
      | Prepared => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::Prepared,
          dfs_posix_cross_shard_namespace::retryCount == 0,
        }
      | Adopted1 => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::DestinationAdopted,
          dfs_posix_cross_shard_namespace::retryCount == 1,
        }
      | Failed1 => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::FailedRecoverable,
          dfs_posix_cross_shard_namespace::retryCount == 1,
        }
      | Adopted2 => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::DestinationAdopted,
          dfs_posix_cross_shard_namespace::retryCount == 2,
        }
      | Failed2 => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::FailedRecoverable,
          dfs_posix_cross_shard_namespace::retryCount == 2,
        }
      | Adopted3 => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::DestinationAdopted,
          dfs_posix_cross_shard_namespace::retryCount
            == dfs_posix_cross_shard_namespace::MaxRetries,
        }
      | Exhausted => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::FailedRecoverable,
          dfs_posix_cross_shard_namespace::retryCount
            == dfs_posix_cross_shard_namespace::MaxRetries,
          dfs_posix_cross_shard_namespace::blockedPaths.contains(
            dfs_posix_cross_shard_namespace::sourcePath
          ),
          dfs_posix_cross_shard_namespace::blockedPaths.contains(
            dfs_posix_cross_shard_namespace::destinationPath
          ),
        }
      | Completed => all {
          dfs_posix_cross_shard_namespace::jobPhase
            == dfs_posix_cross_shard_namespace::Completed,
          any {
            dfs_posix_cross_shard_namespace::lastDecision
              == dfs_posix_cross_shard_namespace::RetryBudgetExhaustedForwarded,
            dfs_posix_cross_shard_namespace::lastDecision
              == dfs_posix_cross_shard_namespace::TerminalStutter,
          },
          dfs_posix_cross_shard_namespace::blockedPaths == Set(),
          dfs_posix_cross_shard_namespace::kindAt.get(
            dfs_posix_cross_shard_namespace::sourcePath
          ) == dfs_posix_cross_shard_namespace::Tombstone,
          dfs_posix_cross_shard_namespace::isLive(
            dfs_posix_cross_shard_namespace::kindAt.get(
              dfs_posix_cross_shard_namespace::destinationPath
            )
          ),
          dfs_posix_cross_shard_namespace::objectAt.get(
            dfs_posix_cross_shard_namespace::destinationPath
          ) == dfs_posix_cross_shard_namespace::beforeObjectAt.get(
            dfs_posix_cross_shard_namespace::sourcePath
          ),
        }
    },
  }
}
