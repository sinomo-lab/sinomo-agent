/// DFS durable namespace-request admission and cancellation boundary.
///
/// Evidence status: current-behavior refinement for
/// `NetworkedDfsService::handle_namespace_request`,
/// `admit_durable_namespace_request`, and
/// `complete_durable_namespace_request`.
///
/// Mutating namespace commands are admitted through a durable record keyed by
/// `(transport requester, request id)`. The record stores the semantic mutation
/// independently of response journal options. A fresh identity is first
/// persisted as `Pending`; the namespace effect may run only under an execution
/// permit created by admission or autonomous recovery. Transaction-backed
/// effects atomically publish their terminal request result, while durable jobs
/// and sagas resume until they can do the same. Exact completed retries replay
/// that result and reuse of the identity with a changed mutation is rejected.
///
/// A process restart clears only transient delivery, wait, and execution state.
/// The admission ledger remains. Recovery recreates the permit for the exact
/// durable mutation without requiring a client retry. Cancellation is
/// requester-local: it removes the pending wait but cannot retract an admitted
/// command. A retry with a new request id is a new operation identity.
module dfs_namespace_command_replay {
  import common.* from "../../lib/common"

  type RequestId = Req1 | Req2 | Req3
  type NamespaceMutation = MutationA | MutationB
  type JournalCursor = Cursor0 | Cursor1

  type NamespaceCommand = {
    requestId: RequestId,
    mutation: NamespaceMutation,
    includeJournalBatches: bool,
    cursor: JournalCursor,
  }

  type RoutedCommand = {
    requester: Node,
    command: NamespaceCommand,
  }

  type RequestKey = {
    requester: Node,
    requestId: RequestId,
  }

  type NamespaceResponse = {
    requestId: RequestId,
    responseSerial: int,
  }

  type AdmissionStatus = AdmissionPending | AdmissionCompleted

  type AdmissionRecord = {
    command: NamespaceCommand,
    status: AdmissionStatus,
    response: Option[NamespaceResponse],
  }

  type ServeAudit = {
    request: RoutedCommand,
    recordBefore: Option[AdmissionRecord],
    response: Option[NamespaceResponse],
    waitWasActive: bool,
    responseDelivered: bool,
    effectCountBefore: int,
    effectCountAfter: int,
  }

  type LifecycleDecision =
    | LifecycleIdle
    | CommandDispatched(RoutedCommand)
    | RequestAdmitted(RoutedCommand)
    | CommandCompleted(RoutedCommand)
    | ResponseReplayed(RoutedCommand)
    | ChangedPayloadRejected(RoutedCommand)
    | PendingRecoveryStarted(RoutedCommand)
    | WaitCancelled(RoutedCommand)
    | ServerRestarted

  type ReplayState = {
    ledger: RequestKey -> Option[AdmissionRecord],
    inFlight: Option[RoutedCommand],
    waitingFor: Option[RoutedCommand],
    executionPermit: Option[RoutedCommand],
    executedKeys: Set[RequestKey],
    cancelledKeys: Set[RequestKey],
    completedBeforeRestart: Set[RequestKey],
    interruptedPending: Option[RoutedCommand],
    lastCompleted: Option[RoutedCommand],
    lastCancelled: Option[RoutedCommand],
    effectCount: int,
    restartCount: int,
    lastServe: Option[ServeAudit],
    lastLifecycle: LifecycleDecision,
  }

  pure val RequestIds: Set[RequestId] = Set(Req1, Req2, Req3)
  pure val Mutations: Set[NamespaceMutation] = Set(MutationA, MutationB)
  pure val JournalCursors: Set[JournalCursor] = Set(Cursor0, Cursor1)
  pure val Bools: Set[bool] = Set(true, false)
  pure val MaxEffectExecutions: int = 9
  pure val MaxRestarts: int = 2

  pure val RequestKeys: Set[RequestKey] = Set(
    { requester: N1, requestId: Req1 },
    { requester: N1, requestId: Req2 },
    { requester: N1, requestId: Req3 },
    { requester: N2, requestId: Req1 },
    { requester: N2, requestId: Req2 },
    { requester: N2, requestId: Req3 },
    { requester: N3, requestId: Req1 },
    { requester: N3, requestId: Req2 },
    { requester: N3, requestId: Req3 },
  )

  pure val defaultCommand: NamespaceCommand = {
    requestId: Req1,
    mutation: MutationA,
    includeJournalBatches: true,
    cursor: Cursor0,
  }

  pure val defaultRequest: RoutedCommand = {
    requester: N1,
    command: defaultCommand,
  }

  pure val defaultResponse: NamespaceResponse = {
    requestId: Req1,
    responseSerial: 0,
  }

  pure val emptyLedger: RequestKey -> Option[AdmissionRecord] =
    RequestKeys.mapBy(_ => None)

  pure def keyFor(request: RoutedCommand): RequestKey = {
    requester: request.requester,
    requestId: request.command.requestId,
  }

  pure def recordFor(
    ledger: RequestKey -> Option[AdmissionRecord],
    request: RoutedCommand,
  ): Option[AdmissionRecord] =
    ledger.get(keyFor(request))

  pure def sameSemanticCommand(
    left: NamespaceCommand,
    right: NamespaceCommand,
  ): bool = all {
    left.requestId == right.requestId,
    left.mutation == right.mutation,
  }

  pure def isExactPending(
    record: Option[AdmissionRecord],
    command: NamespaceCommand,
  ): bool =
    match record {
      | Some(value) => all {
          sameSemanticCommand(value.command, command),
          value.status == AdmissionPending,
          value.response == None,
        }
      | None => false
    }

  pure def isExactCompleted(
    record: Option[AdmissionRecord],
    command: NamespaceCommand,
  ): bool =
    match record {
      | Some(value) => all {
          sameSemanticCommand(value.command, command),
          value.status == AdmissionCompleted,
          isSome(value.response),
        }
      | None => false
    }

  pure def responseFor(record: Option[AdmissionRecord]): NamespaceResponse =
    match record {
      | Some(value) => unwrapOr(value.response, defaultResponse)
      | None => defaultResponse
    }

  pure def completedKeys(
    ledger: RequestKey -> Option[AdmissionRecord]
  ): Set[RequestKey] =
    RequestKeys.filter(key =>
      match ledger.get(key) {
        | Some(value) => value.status == AdmissionCompleted
        | None => false
      }
    )

  pure def otherMutation(mutation: NamespaceMutation): NamespaceMutation =
    if (mutation == MutationA) { MutationB } else { MutationA }

  var replay: ReplayState

  action init =
    replay' = {
      ledger: emptyLedger,
      inFlight: None,
      waitingFor: None,
      executionPermit: None,
      executedKeys: Set(),
      cancelledKeys: Set(),
      completedBeforeRestart: Set(),
      interruptedPending: None,
      lastCompleted: None,
      lastCancelled: None,
      effectCount: 0,
      restartCount: 0,
      lastServe: None,
      lastLifecycle: LifecycleIdle,
    }

  action dispatch(request: RoutedCommand): bool = all {
    replay.inFlight == None,
    replay.waitingFor == None,
    replay.executionPermit == None,
    replay' = replay
      .with("inFlight", Some(request))
      .with("waitingFor", Some(request))
      .with("lastLifecycle", CommandDispatched(request)),
  }

  /// Persist `Pending` before granting the one transient execution permit.
  action admitInFlight: bool = {
    val request = unwrapOr(replay.inFlight, defaultRequest)
    val key = keyFor(request)
    val pending = {
      command: request.command,
      status: AdmissionPending,
      response: None,
    }
    all {
      replay.inFlight != None,
      recordFor(replay.ledger, request) == None,
      replay.executionPermit == None,
      replay' = replay
        .with("ledger", replay.ledger.set(key, Some(pending)))
        .with("executionPermit", Some(request))
        .with("lastLifecycle", RequestAdmitted(request)),
    }
  }

  /// Namespace execution and durable terminal-result publication are one
  /// abstract completion step. Concrete transaction-backed operations commit
  /// the effect and terminal row in one durable prepare batch; resumable jobs
  /// refine the same step through their own durable progress records.
  action completeAdmitted: bool = {
    val request = unwrapOr(replay.executionPermit, defaultRequest)
    val key = keyFor(request)
    val before = recordFor(replay.ledger, request)
    val response = {
      requestId: request.command.requestId,
      responseSerial: replay.effectCount + 1,
    }
    val completed = {
      command: request.command,
      status: AdmissionCompleted,
      response: Some(response),
    }
    val waitActive = replay.waitingFor == Some(request)
    all {
      replay.executionPermit != None,
      replay.inFlight == Some(request),
      isExactPending(before, request.command),
      replay.effectCount < MaxEffectExecutions,
      replay' = replay
        .with("ledger", replay.ledger.set(key, Some(completed)))
        .with("inFlight", None)
        .with("waitingFor", None)
        .with("executionPermit", None)
        .with("executedKeys", replay.executedKeys.union(Set(key)))
        .with("lastCompleted", Some(request))
        .with("effectCount", replay.effectCount + 1)
        .with("lastServe", Some({
          request: request,
          recordBefore: before,
          response: Some(response),
          waitWasActive: waitActive,
          responseDelivered: waitActive,
          effectCountBefore: replay.effectCount,
          effectCountAfter: replay.effectCount + 1,
        }))
        .with("lastLifecycle", CommandCompleted(request)),
    }
  }

  /// Exact completed retries return the durable result without another effect.
  action replayCompleted: bool = {
    val request = unwrapOr(replay.inFlight, defaultRequest)
    val before = recordFor(replay.ledger, request)
    val response = responseFor(before)
    val waitActive = replay.waitingFor == Some(request)
    all {
      replay.inFlight != None,
      replay.executionPermit == None,
      isExactCompleted(before, request.command),
      replay' = replay
        .with("inFlight", None)
        .with("waitingFor", None)
        .with("lastServe", Some({
          request: request,
          recordBefore: before,
          response: Some(response),
          waitWasActive: waitActive,
          responseDelivered: waitActive,
          effectCountBefore: replay.effectCount,
          effectCountAfter: replay.effectCount,
        }))
        .with("lastLifecycle", ResponseReplayed(request)),
    }
  }

  /// The identity is immutable: any command change is rejected.
  action rejectChangedPayload: bool = {
    val request = unwrapOr(replay.inFlight, defaultRequest)
    val before = recordFor(replay.ledger, request)
    all {
      replay.inFlight != None,
      replay.executionPermit == None,
      isSome(before),
      not(sameSemanticCommand(unwrapOr(before, {
        command: defaultCommand,
        status: AdmissionPending,
        response: None,
      }).command, request.command)),
      replay' = replay
        .with("inFlight", None)
        .with("waitingFor", None)
        .with("lastServe", Some({
          request: request,
          recordBefore: before,
          response: None,
          waitWasActive: true,
          responseDelivered: true,
          effectCountBefore: replay.effectCount,
          effectCountAfter: replay.effectCount,
        }))
        .with("lastLifecycle", ChangedPayloadRejected(request)),
    }
  }

  /// An exact retry of a surviving `Pending` record resumes the same durable
  /// operation identity rather than creating new work.
  action recoverPendingInFlight: bool = {
    val request = unwrapOr(replay.inFlight, defaultRequest)
    all {
      replay.inFlight != None,
      replay.executionPermit == None,
      isExactPending(recordFor(replay.ledger, request), request.command),
      replay' = replay
        .with("executionPermit", Some(request))
        .with("lastLifecycle", PendingRecoveryStarted(request)),
    }
  }

  /// Maintenance resumes an interrupted admission even when no requester is
  /// waiting for the result.
  action recoverInterruptedPending: bool = {
    val request = unwrapOr(replay.interruptedPending, defaultRequest)
    all {
      replay.interruptedPending != None,
      replay.inFlight == None,
      replay.waitingFor == None,
      replay.executionPermit == None,
      isExactPending(recordFor(replay.ledger, request), request.command),
      replay' = replay
        .with("inFlight", Some(request))
        .with("executionPermit", Some(request))
        .with("lastLifecycle", PendingRecoveryStarted(request)),
    }
  }

  /// Caller cancellation removes only the wait. An admitted command retains
  /// its execution permit and may still complete.
  action cancelWait: bool = {
    val request = unwrapOr(replay.waitingFor, defaultRequest)
    all {
      replay.waitingFor != None,
      replay.inFlight == Some(request),
      replay' = replay
        .with("waitingFor", None)
        .with("cancelledKeys", replay.cancelledKeys.union(Set(keyFor(request))))
        .with("lastCancelled", Some(request))
        .with("lastLifecycle", WaitCancelled(request)),
    }
  }

  /// Durable admission survives restart; only the transient permit is lost.
  action restartServer: bool = {
    val interrupted = replay.executionPermit
    all {
      replay.restartCount < MaxRestarts,
      replay' = replay
        .with("inFlight", None)
        .with("waitingFor", None)
        .with("executionPermit", None)
        .with("completedBeforeRestart",
          replay.completedBeforeRestart.union(completedKeys(replay.ledger)))
        .with("interruptedPending",
          if (isSome(interrupted)) { interrupted } else { replay.interruptedPending })
        .with("restartCount", replay.restartCount + 1)
        .with("lastLifecycle", ServerRestarted),
    }
  }

  action retryLastCompletedExact: bool = {
    val request = unwrapOr(replay.lastCompleted, defaultRequest)
    all {
      replay.lastCompleted != None,
      dispatch(request),
    }
  }

  action dispatchChangedMutationFromLastCompleted: bool = {
    val previous = unwrapOr(replay.lastCompleted, defaultRequest)
    val changed = {
      requester: previous.requester,
      command: {
        requestId: previous.command.requestId,
        mutation: otherMutation(previous.command.mutation),
        includeJournalBatches: previous.command.includeJournalBatches,
        cursor: previous.command.cursor,
      },
    }
    all {
      replay.lastCompleted != None,
      dispatch(changed),
    }
  }

  action retryInterruptedPending: bool = {
    val request = unwrapOr(replay.interruptedPending, defaultRequest)
    all {
      replay.interruptedPending != None,
      dispatch(request),
    }
  }

  action retryCancelledWithFreshId(newRequestId: RequestId): bool = {
    val previous = unwrapOr(replay.lastCancelled, defaultRequest)
    val fresh = {
      requester: previous.requester,
      command: {
        requestId: newRequestId,
        mutation: previous.command.mutation,
        includeJournalBatches: previous.command.includeJournalBatches,
        cursor: previous.command.cursor,
      },
    }
    all {
      replay.lastCancelled != None,
      newRequestId != previous.command.requestId,
      dispatch(fresh),
    }
  }

  action stutter: bool = replay' = replay

  action step = any {
    {
      nondet requester = oneOf(Nodes)
      nondet requestId = oneOf(RequestIds)
      nondet mutation = oneOf(Mutations)
      nondet includeJournalBatches = oneOf(Bools)
      nondet cursor = oneOf(JournalCursors)
      dispatch({
        requester: requester,
        command: {
          requestId: requestId,
          mutation: mutation,
          includeJournalBatches: includeJournalBatches,
          cursor: cursor,
        },
      })
    },
    admitInFlight,
    completeAdmitted,
    replayCompleted,
    rejectChangedPayload,
    recoverPendingInFlight,
    recoverInterruptedPending,
    cancelWait,
    restartServer,
    retryLastCompletedExact,
    dispatchChangedMutationFromLastCompleted,
    retryInterruptedPending,
    {
      nondet newRequestId = oneOf(RequestIds)
      retryCancelledWithFreshId(newRequestId)
    },
    stutter,
  }
}
