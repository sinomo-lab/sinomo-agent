/// Invariant checks for durable DFS namespace-request admission.
module dfs_namespace_command_replay_safety {
  import common.* from "../../lib/common"
  import dfs_namespace_command_replay.* from "../../models/dfs/namespace_command_replay"

  def admissionRecordIsWellFormed(key: RequestKey): bool =
    match replay.ledger.get(key) {
      | Some(record) => all {
          record.command.requestId == key.requestId,
          if (record.status == AdmissionPending) {
            record.response == None
          } else {
            match record.response {
              | Some(response) => all {
                  response.requestId == key.requestId,
                  response.responseSerial > 0,
                }
              | None => false
            }
          },
        }
      | None => true
    }

  def ledgerRecordsAreWellFormed: bool =
    RequestKeys.forall(admissionRecordIsWellFormed)

  def completedRecordsNameExactlyExecutedIdentities: bool =
    completedKeys(replay.ledger) == replay.executedKeys

  def effectsAreAtMostOncePerIdentity: bool = all {
    replay.effectCount == replay.executedKeys.size(),
    replay.effectCount >= 0,
    replay.effectCount <= MaxEffectExecutions,
  }

  def executionPermitNamesExactPendingAdmission: bool =
    match replay.executionPermit {
      | Some(request) => all {
          replay.inFlight == Some(request),
          isExactPending(recordFor(replay.ledger, request), request.command),
          not(replay.executedKeys.contains(keyFor(request))),
        }
      | None => true
    }

  def activeWaitTracksTheDispatchedCommand: bool =
    match replay.waitingFor {
      | Some(request) => replay.inFlight == Some(request)
      | None => true
    }

  def cancellationDoesNotRetractAdmittedExecution: bool =
    match replay.lastLifecycle {
      | WaitCancelled(request) => all {
          replay.waitingFor == None,
          replay.inFlight == Some(request),
          replay.cancelledKeys.contains(keyFor(request)),
        }
      | _ => true
    }

  def completionRequiresPendingAndAddsOneEffect: bool =
    match replay.lastLifecycle {
      | CommandCompleted(request) => {
          val audit = unwrapOr(replay.lastServe, {
            request: defaultRequest,
            recordBefore: None,
            response: None,
            waitWasActive: false,
            responseDelivered: false,
            effectCountBefore: 0,
            effectCountAfter: 0,
          })
          all {
            audit.request == request,
            isExactPending(audit.recordBefore, request.command),
            audit.effectCountAfter == audit.effectCountBefore + 1,
            replay.executedKeys.contains(keyFor(request)),
            isExactCompleted(recordFor(replay.ledger, request), request.command),
          }
        }
      | _ => true
    }

  def replayRequiresCompletedAndAddsNoEffect: bool =
    match replay.lastLifecycle {
      | ResponseReplayed(request) => {
          val audit = unwrapOr(replay.lastServe, {
            request: defaultRequest,
            recordBefore: None,
            response: None,
            waitWasActive: false,
            responseDelivered: false,
            effectCountBefore: 0,
            effectCountAfter: 0,
          })
          all {
            audit.request == request,
            isExactCompleted(audit.recordBefore, request.command),
            audit.response == Some(responseFor(audit.recordBefore)),
            audit.effectCountAfter == audit.effectCountBefore,
          }
        }
      | _ => true
    }

  def rejectionPreservesEffectCount: bool =
    match replay.lastLifecycle {
      | ChangedPayloadRejected(request) => {
          val audit = unwrapOr(replay.lastServe, {
            request: defaultRequest,
            recordBefore: None,
            response: None,
            waitWasActive: false,
            responseDelivered: false,
            effectCountBefore: 0,
            effectCountAfter: 0,
          })
          all {
            audit.request == request,
            isSome(audit.recordBefore),
            unwrapOr(audit.recordBefore, {
              command: defaultCommand,
              status: AdmissionPending,
              response: None,
            }).command != request.command,
            audit.effectCountAfter == audit.effectCountBefore,
          }
        }
      | PendingOutcomeRejected(request) => {
          val audit = unwrapOr(replay.lastServe, {
            request: defaultRequest,
            recordBefore: None,
            response: None,
            waitWasActive: false,
            responseDelivered: false,
            effectCountBefore: 0,
            effectCountAfter: 0,
          })
          all {
            audit.request == request,
            isExactPending(audit.recordBefore, request.command),
            audit.effectCountAfter == audit.effectCountBefore,
          }
        }
      | _ => true
    }

  def interruptedPendingNeverExecuted: bool =
    match replay.interruptedPending {
      | Some(request) => all {
          isExactPending(recordFor(replay.ledger, request), request.command),
          not(replay.executedKeys.contains(keyFor(request))),
        }
      | None => true
    }

  def completedBeforeRestartRemainsCompleted: bool =
    replay.completedBeforeRestart.forall(key =>
      match replay.ledger.get(key) {
        | Some(record) => record.status == AdmissionCompleted
        | None => false
      }
    )

  def countersStayInsideModelBounds: bool = all {
    replay.restartCount >= 0,
    replay.restartCount <= MaxRestarts,
  }

  def witnessExactCompletedRetryReplays: bool =
    match replay.lastLifecycle {
      | ResponseReplayed(request) => all {
          replay.executedKeys.contains(keyFor(request)),
          unwrapOr(replay.lastServe, {
            request: defaultRequest,
            recordBefore: None,
            response: None,
            waitWasActive: false,
            responseDelivered: false,
            effectCountBefore: 0,
            effectCountAfter: 0,
          }).effectCountAfter == replay.effectCount,
        }
      | _ => false
    }

  def witnessChangedPayloadSameIdRejected: bool =
    match replay.lastLifecycle {
      | ChangedPayloadRejected(request) => all {
          isSome(recordFor(replay.ledger, request)),
          unwrapOr(recordFor(replay.ledger, request), {
            command: defaultCommand,
            status: AdmissionPending,
            response: None,
          }).command.mutation != request.command.mutation,
        }
      | _ => false
    }

  def witnessCancelledCommandStillCompletes: bool =
    match replay.lastLifecycle {
      | CommandCompleted(request) => {
          val audit = unwrapOr(replay.lastServe, {
            request: defaultRequest,
            recordBefore: None,
            response: None,
            waitWasActive: false,
            responseDelivered: false,
            effectCountBefore: 0,
            effectCountAfter: 0,
          })
          all {
            replay.cancelledKeys.contains(keyFor(request)),
            not(audit.waitWasActive),
            not(audit.responseDelivered),
          }
        }
      | _ => false
    }

  def witnessRestartedPendingRetryRejected: bool =
    match replay.lastLifecycle {
      | PendingOutcomeRejected(request) => all {
          replay.interruptedPending == Some(request),
          not(replay.executedKeys.contains(keyFor(request))),
        }
      | _ => false
    }

  def witnessCompletedRetryAfterRestartReplays: bool =
    match replay.lastLifecycle {
      | ResponseReplayed(request) =>
          replay.completedBeforeRestart.contains(keyFor(request))
      | _ => false
    }

  def witnessCancelledFreshIdExecutes: bool =
    match replay.lastLifecycle {
      | CommandCompleted(request) => {
          val cancelled = unwrapOr(replay.lastCancelled, defaultRequest)
          all {
            replay.lastCancelled != None,
            request.requester == cancelled.requester,
            request.command.requestId != cancelled.command.requestId,
            request.command.mutation == cancelled.command.mutation,
          }
        }
      | _ => false
    }

  def safety: bool = all {
    ledgerRecordsAreWellFormed,
    completedRecordsNameExactlyExecutedIdentities,
    effectsAreAtMostOncePerIdentity,
    executionPermitNamesExactPendingAdmission,
    activeWaitTracksTheDispatchedCommand,
    cancellationDoesNotRetractAdmittedExecution,
    completionRequiresPendingAndAddsOneEffect,
    replayRequiresCompletedAndAddsNoEffect,
    rejectionPreservesEffectCount,
    interruptedPendingNeverExecuted,
    completedBeforeRestartRemainsCompleted,
    countersStayInsideModelBounds,
  }
}
