/// Invariant checks for the current DFS namespace-command replay cache.
module dfs_namespace_command_replay_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_command_replay.* from "../../models/dfs/namespace_command_replay"

  def cacheShapeIsFifoBounded: bool = all {
    cacheSize(replayCache) <= 2,
    isSome(replayCache.newer) implies isSome(replayCache.older),
  }


  def replayEntryHasMatchingResponseIdentity(entry: ReplayEntry): bool = all {
    entry.response.requestId == entry.key.command.requestId,
    entry.response.responseSerial > 0,
  }


  def cachedResponsesMatchTheirCompleteCommands: bool = all {
    match replayCache.older {
      | Some(entry) => replayEntryHasMatchingResponseIdentity(entry)
      | None => true
    },
    match replayCache.newer {
      | Some(entry) => replayEntryHasMatchingResponseIdentity(entry)
      | None => true
    },
    cacheKeys(replayCache).subseteq(executedKeys),
  }


  def cacheSlotsDoNotDuplicateKeys: bool =
    match replayCache.older {
      | Some(older) =>
          match replayCache.newer {
            | Some(newer) => older.key != newer.key
            | None => true
          }
      | None => true
    }


  def responseAuditsPreserveIdentityAndEffectBoundary: bool =
    match lastServe {
      | Some(audit) => all {
          audit.response.requestId == audit.key.command.requestId,
          audit.responseDelivered == audit.waitWasActive,
          if (isSome(audit.cachedResponseBefore)) {
            all {
              audit.response == unwrapOr(audit.cachedResponseBefore, defaultResponse),
              audit.effectCountAfter == audit.effectCountBefore,
            }
          } else {
            audit.effectCountAfter == audit.effectCountBefore + 1
          },
        }
      | None => true
    }


  def replayDecisionRequiresResidentExactResponse: bool =
    match lastLifecycle {
      | ResponseReplayed(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          all {
            lastServe != None,
            audit.key == key,
            audit.cachedResponseBefore == Some(audit.response),
            replayLookup(replayCache, key) == Some(audit.response),
            audit.effectCountAfter == audit.effectCountBefore,
          }
        }
      | _ => true
    }


  def executionDecisionRequiresCacheMissAndOneEffect: bool =
    match lastLifecycle {
      | CommandExecuted(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          all {
            lastServe != None,
            audit.key == key,
            audit.cachedResponseBefore == None,
            audit.effectCountAfter == audit.effectCountBefore + 1,
            executedKeys.contains(key),
          }
        }
      | _ => true
    }


  def changedCommandAuditReallyDiffersBeyondRequestId: bool =
    match lastServe {
      | Some(audit) =>
          match audit.sameRequestDifferentKeyBefore {
            | Some(previous) => all {
                previous.requester == audit.key.requester,
                previous.command.requestId == audit.key.command.requestId,
                previous.command != audit.key.command,
                audit.cachedResponseBefore == None,
              }
            | None => true
          }
      | None => true
    }


  def duplicateExecutionRequiresAReplayBoundary: bool =
    reexecutedKeys.forall(key => all {
      executedKeys.contains(key),
      any {
        evictedKeys.contains(key),
        restartClearedKeys.contains(key),
      },
    })


  def replayHistorySetsComeFromExecutedCommands: bool = all {
    evictedKeys.subseteq(executedKeys),
    restartClearedKeys.subseteq(executedKeys),
    reexecutedKeys.subseteq(executedKeys),
  }


  def activeWaitTracksTheDispatchedCommand: bool =
    match waitingFor {
      | Some(key) => inFlight == Some(key)
      | None => true
    }


  def cancellationDoesNotRetractInFlightCommand: bool =
    match lastLifecycle {
      | WaitCancelled(key) => all {
          waitingFor == None,
          inFlight == Some(key),
          cancelledKeys.contains(key),
        }
      | _ => true
    }


  def responsesAreClassifiedByWaitState: bool = all {
    deliveredResponses.forall(entry =>
      replayEntryHasMatchingResponseIdentity(entry)
    ),
    discardedResponses.forall(entry =>
      replayEntryHasMatchingResponseIdentity(entry)
    ),
  }


  def countersStayInsideModelBounds: bool = all {
    effectCount >= 0,
    effectCount <= MaxEffectExecutions,
    restartCount >= 0,
    restartCount <= MaxRestarts,
  }


  /// Exact full-command retry while resident returns the original response and
  /// adds no namespace effect.
  def witnessExactResidentRetryReplays: bool =
    match lastLifecycle {
      | ResponseReplayed(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          all {
            audit.key == key,
            audit.cachedResponseBefore == Some(audit.response),
            audit.effectCountAfter == audit.effectCountBefore,
          }
        }
      | _ => false
    }


  /// Request id alone is not the key: a changed operation payload executes.
  def witnessSameRequestIdChangedPayloadExecutes: bool =
    match lastLifecycle {
      | CommandExecuted(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          match audit.sameRequestDifferentKeyBefore {
            | Some(previous) => all {
                previous.requester == key.requester,
                previous.command.requestId == key.command.requestId,
                previous.command.mutation != key.command.mutation,
                audit.effectCountAfter == audit.effectCountBefore + 1,
              }
            | None => false
          }
        }
      | _ => false
    }


  /// Cancellation stops the requester wait, not primary execution.
  def witnessCancelledCommandStillExecutes: bool =
    match lastLifecycle {
      | CommandExecuted(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          all {
            cancelledKeys.contains(key),
            not(audit.waitWasActive),
            not(audit.responseDelivered),
            audit.effectCountAfter == audit.effectCountBefore + 1,
          }
        }
      | _ => false
    }


  /// Once FIFO eviction removes a key, an exact delayed retry can execute the
  /// mutation again. This is deliberately a limitation witness.
  def witnessEvictedExactRetryReexecutes: bool =
    match lastLifecycle {
      | CommandExecuted(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          all {
            audit.key == key,
            audit.executedBefore,
            audit.evictedBefore,
            reexecutedKeys.contains(key),
          }
        }
      | _ => false
    }


  /// The cache is not durable: restart-cleared exact retries can execute again.
  def witnessRestartClearedRetryReexecutes: bool =
    match lastLifecycle {
      | CommandExecuted(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          all {
            audit.key == key,
            audit.executedBefore,
            audit.restartClearedBefore,
            reexecutedKeys.contains(key),
          }
        }
      | _ => false
    }


  /// A retry of a cancelled top-level call with a fresh request id is not an
  /// exact cache retry, even if the mutation payload is unchanged.
  def witnessCancelledFreshIdRetryExecutes: bool =
    match lastLifecycle {
      | CommandExecuted(key) => {
          val audit = unwrapOr(lastServe, defaultAudit)
          val cancelled = unwrapOr(mostRecentCancelled, defaultKey)
          all {
            mostRecentCancelled != None,
            key.requester == cancelled.requester,
            key.command.requestId != cancelled.command.requestId,
            key.command.mutation == cancelled.command.mutation,
            audit.cachedResponseBefore == None,
            audit.effectCountAfter == audit.effectCountBefore + 1,
          }
        }
      | _ => false
    }


  def safety: bool = all {
    cacheShapeIsFifoBounded,
    cachedResponsesMatchTheirCompleteCommands,
    cacheSlotsDoNotDuplicateKeys,
    responseAuditsPreserveIdentityAndEffectBoundary,
    replayDecisionRequiresResidentExactResponse,
    executionDecisionRequiresCacheMissAndOneEffect,
    changedCommandAuditReallyDiffersBeyondRequestId,
    duplicateExecutionRequiresAReplayBoundary,
    replayHistorySetsComeFromExecutedCommands,
    activeWaitTracksTheDispatchedCommand,
    cancellationDoesNotRetractInFlightCommand,
    responsesAreClassifiedByWaitState,
    countersStayInsideModelBounds,
  }
}
