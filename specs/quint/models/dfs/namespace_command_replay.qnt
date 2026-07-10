/// DFS namespace-command replay cache and cancellation boundary.
///
/// Evidence status: current-behavior refinement for
/// `NetworkedDfsService::handle_namespace_request`,
/// `served_namespace_replays`, and `settle_routed_namespace_op_inner`.
/// The production cache is a process-local `VecDeque` with 32 entries. This
/// finite model reduces that capacity to two while preserving its lookup key,
/// FIFO eviction, exact-response replay, and restart lifetime.
///
/// The replay key is `(transport requester, complete DfsNamespaceCommand)`,
/// not request id alone. Consequently an exact duplicate that is still cached
/// does not execute the namespace mutation again, while the same request id
/// with a changed operation/cursor/response option is a new command. Eviction
/// or service restart also removes the at-most-once protection.
///
/// Cancellation is requester-local: it removes the pending wait but cannot
/// retract a command already sent to the primary. The primary may therefore
/// execute after cancellation. A later retry is protected only if it preserves
/// the complete command and its entry is still resident. This model restricts
/// execution to authorized, successful mutating commands; POSIX operation
/// semantics, authority rejection, journal contents, and response transport
/// delay are covered by other DFS slices.
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

  type ReplayKey = {
    requester: Node,
    command: NamespaceCommand,
  }

  /// `responseSerial` represents the full response captured after execution,
  /// including its result, synced objects, and journal batches. Replays return
  /// the same serial instead of rebuilding a response from newer state.
  type NamespaceResponse = {
    requestId: RequestId,
    responseSerial: int,
  }

  type ReplayEntry = {
    key: ReplayKey,
    response: NamespaceResponse,
  }

  /// Two slots are the bounded analogue of the Rust `VecDeque`: `older` is
  /// popped when both slots are occupied and a cache miss is appended.
  type ReplayCache = {
    older: Option[ReplayEntry],
    newer: Option[ReplayEntry],
  }

  type ServeAudit = {
    key: ReplayKey,
    response: NamespaceResponse,
    cachedResponseBefore: Option[NamespaceResponse],
    sameRequestDifferentKeyBefore: Option[ReplayKey],
    executedBefore: bool,
    evictedBefore: bool,
    restartClearedBefore: bool,
    waitWasActive: bool,
    responseDelivered: bool,
    effectCountBefore: int,
    effectCountAfter: int,
  }

  type LifecycleDecision =
    | LifecycleIdle
    | CommandDispatched(ReplayKey)
    | WaitCancelled(ReplayKey)
    | CommandExecuted(ReplayKey)
    | ResponseReplayed(ReplayKey)
    | ServerRestarted

  pure val RequestIds: Set[RequestId] = Set(Req1, Req2, Req3)
  pure val Mutations: Set[NamespaceMutation] = Set(MutationA, MutationB)
  pure val JournalCursors: Set[JournalCursor] = Set(Cursor0, Cursor1)
  pure val Bools: Set[bool] = Set(true, false)
  pure val MaxEffectExecutions: int = 8
  pure val MaxRestarts: int = 2

  pure val defaultCommand: NamespaceCommand = {
    requestId: Req1,
    mutation: MutationA,
    includeJournalBatches: true,
    cursor: Cursor0,
  }

  pure val defaultKey: ReplayKey = {
    requester: N1,
    command: defaultCommand,
  }

  pure val defaultResponse: NamespaceResponse = {
    requestId: Req1,
    responseSerial: 0,
  }

  pure val defaultAudit: ServeAudit = {
    key: defaultKey,
    response: defaultResponse,
    cachedResponseBefore: None,
    sameRequestDifferentKeyBefore: None,
    executedBefore: false,
    evictedBefore: false,
    restartClearedBefore: false,
    waitWasActive: false,
    responseDelivered: false,
    effectCountBefore: 0,
    effectCountAfter: 0,
  }

  pure val emptyReplayCache: ReplayCache = {
    older: None,
    newer: None,
  }

  var replayCache: ReplayCache
  var inFlight: Option[ReplayKey]
  var waitingFor: Option[ReplayKey]
  var deliveredResponses: Set[ReplayEntry]
  var discardedResponses: Set[ReplayEntry]
  /// Durable audit abstractions. Unlike the process-local cache, these survive
  /// `restartServer` so the model can expose duplicate execution boundaries.
  var executedKeys: Set[ReplayKey]
  var reexecutedKeys: Set[ReplayKey]
  var evictedKeys: Set[ReplayKey]
  var restartClearedKeys: Set[ReplayKey]
  var cancelledKeys: Set[ReplayKey]
  var mostRecentCancelled: Option[ReplayKey]
  var mostRecentEvicted: Option[ReplayKey]
  var mostRecentRestartCleared: Option[ReplayKey]
  var effectCount: int
  var restartCount: int
  var lastServe: Option[ServeAudit]
  var lastLifecycle: LifecycleDecision

  pure def entryKeys(entry: Option[ReplayEntry]): Set[ReplayKey] =
    match entry {
      | Some(value) => Set(value.key)
      | None => Set()
    }

  pure def cacheKeys(cache: ReplayCache): Set[ReplayKey] =
    entryKeys(cache.older).union(entryKeys(cache.newer))

  pure def cacheSize(cache: ReplayCache): int =
    cacheKeys(cache).size()

  pure def replayLookup(cache: ReplayCache, key: ReplayKey): Option[NamespaceResponse] =
    match cache.older {
      | Some(entry) =>
          if (entry.key == key) {
            Some(entry.response)
          } else {
            match cache.newer {
              | Some(newer) =>
                  if (newer.key == key) { Some(newer.response) } else { None }
              | None => None
            }
          }
      | None => None
    }

  pure def sameRequestDifferentKey(
    cache: ReplayCache,
    key: ReplayKey
  ): Option[ReplayKey] = {
    val olderMatch = match cache.older {
      | Some(entry) => all {
          entry.key.requester == key.requester,
          entry.key.command.requestId == key.command.requestId,
          entry.key.command != key.command,
        }
      | None => false
    }
    if (olderMatch) {
      match cache.older {
        | Some(entry) => Some(entry.key)
        | None => None
      }
    } else {
      match cache.newer {
        | Some(entry) =>
            if (all {
              entry.key.requester == key.requester,
              entry.key.command.requestId == key.command.requestId,
              entry.key.command != key.command,
            }) { Some(entry.key) } else { None }
        | None => None
      }
    }
  }

  pure def evictedByInsert(cache: ReplayCache): Option[ReplayKey] =
    match cache.older {
      | Some(older) =>
          match cache.newer {
            | Some(_) => Some(older.key)
            | None => None
          }
      | None => None
    }

  pure def insertReplay(cache: ReplayCache, entry: ReplayEntry): ReplayCache =
    match cache.older {
      | None => {
          older: Some(entry),
          newer: None,
        }
      | Some(_) =>
          match cache.newer {
            | None => {
                older: cache.older,
                newer: Some(entry),
              }
            | Some(newer) => {
                older: Some(newer),
                newer: Some(entry),
              }
          }
    }

  pure def mostRecentCacheKey(cache: ReplayCache): Option[ReplayKey] =
    match cache.newer {
      | Some(entry) => Some(entry.key)
      | None =>
          match cache.older {
            | Some(entry) => Some(entry.key)
            | None => None
          }
    }

  pure def otherMutation(mutation: NamespaceMutation): NamespaceMutation =
    if (mutation == MutationA) { MutationB } else { MutationA }

  action init = all {
    replayCache' = emptyReplayCache,
    inFlight' = None,
    waitingFor' = None,
    deliveredResponses' = Set(),
    discardedResponses' = Set(),
    executedKeys' = Set(),
    reexecutedKeys' = Set(),
    evictedKeys' = Set(),
    restartClearedKeys' = Set(),
    cancelledKeys' = Set(),
    mostRecentCancelled' = None,
    mostRecentEvicted' = None,
    mostRecentRestartCleared' = None,
    effectCount' = 0,
    restartCount' = 0,
    lastServe' = None,
    lastLifecycle' = LifecycleIdle,
  }

  /// Send one complete command and register its requester-side waiter. Network
  /// endpoint retries use this same key; a new POSIX call may choose a new id.
  action dispatch(key: ReplayKey): bool = all {
    inFlight == None,
    waitingFor == None,
    inFlight' = Some(key),
    waitingFor' = Some(key),
    replayCache' = replayCache,
    deliveredResponses' = deliveredResponses,
    discardedResponses' = discardedResponses,
    executedKeys' = executedKeys,
    reexecutedKeys' = reexecutedKeys,
    evictedKeys' = evictedKeys,
    restartClearedKeys' = restartClearedKeys,
    cancelledKeys' = cancelledKeys,
    mostRecentCancelled' = mostRecentCancelled,
    mostRecentEvicted' = mostRecentEvicted,
    mostRecentRestartCleared' = mostRecentRestartCleared,
    effectCount' = effectCount,
    restartCount' = restartCount,
    lastServe' = lastServe,
    lastLifecycle' = CommandDispatched(key),
  }

  /// Caller cancellation removes only the pending result. The already-sent
  /// command remains in flight and may still mutate the primary.
  action cancelWait: bool = {
    val key = unwrapOr(waitingFor, defaultKey)
    all {
      waitingFor != None,
      inFlight == waitingFor,
      waitingFor' = None,
      inFlight' = inFlight,
      replayCache' = replayCache,
      deliveredResponses' = deliveredResponses,
      discardedResponses' = discardedResponses,
      executedKeys' = executedKeys,
      reexecutedKeys' = reexecutedKeys,
      evictedKeys' = evictedKeys,
      restartClearedKeys' = restartClearedKeys,
      cancelledKeys' = cancelledKeys.union(Set(key)),
      mostRecentCancelled' = Some(key),
      mostRecentEvicted' = mostRecentEvicted,
      mostRecentRestartCleared' = mostRecentRestartCleared,
      effectCount' = effectCount,
      restartCount' = restartCount,
      lastServe' = lastServe,
      lastLifecycle' = WaitCancelled(key),
    }
  }

  /// Handle the in-flight command at the primary. Cache hits replay the exact
  /// response and keep FIFO order unchanged. Misses execute once and append.
  action serveInFlight: bool = {
    val key = unwrapOr(inFlight, defaultKey)
    val cached = replayLookup(replayCache, key)
    val waitActive = waitingFor == Some(key)
    if (isSome(cached)) {
      val response = unwrapOr(cached, defaultResponse)
      val served = { key: key, response: response }
      all {
        inFlight != None,
        inFlight' = None,
        waitingFor' = None,
        replayCache' = replayCache,
        deliveredResponses' =
          if (waitActive) { deliveredResponses.union(Set(served)) } else { deliveredResponses },
        discardedResponses' =
          if (waitActive) { discardedResponses } else { discardedResponses.union(Set(served)) },
        executedKeys' = executedKeys,
        reexecutedKeys' = reexecutedKeys,
        evictedKeys' = evictedKeys,
        restartClearedKeys' = restartClearedKeys,
        cancelledKeys' = cancelledKeys,
        mostRecentCancelled' = mostRecentCancelled,
        mostRecentEvicted' = mostRecentEvicted,
        mostRecentRestartCleared' = mostRecentRestartCleared,
        effectCount' = effectCount,
        restartCount' = restartCount,
        lastServe' = Some({
          key: key,
          response: response,
          cachedResponseBefore: cached,
          sameRequestDifferentKeyBefore: None,
          executedBefore: executedKeys.contains(key),
          evictedBefore: evictedKeys.contains(key),
          restartClearedBefore: restartClearedKeys.contains(key),
          waitWasActive: waitActive,
          responseDelivered: waitActive,
          effectCountBefore: effectCount,
          effectCountAfter: effectCount,
        }),
        lastLifecycle' = ResponseReplayed(key),
      }
    } else {
      val prior = sameRequestDifferentKey(replayCache, key)
      val evicted = evictedByInsert(replayCache)
      val response = {
        requestId: key.command.requestId,
        responseSerial: effectCount + 1,
      }
      val entry = { key: key, response: response }
      all {
        inFlight != None,
        effectCount < MaxEffectExecutions,
        inFlight' = None,
        waitingFor' = None,
        replayCache' = insertReplay(replayCache, entry),
        deliveredResponses' =
          if (waitActive) { deliveredResponses.union(Set(entry)) } else { deliveredResponses },
        discardedResponses' =
          if (waitActive) { discardedResponses } else { discardedResponses.union(Set(entry)) },
        executedKeys' = executedKeys.union(Set(key)),
        reexecutedKeys' =
          if (executedKeys.contains(key)) { reexecutedKeys.union(Set(key)) } else { reexecutedKeys },
        evictedKeys' = match evicted {
          | Some(evictedKey) => evictedKeys.union(Set(evictedKey))
          | None => evictedKeys
        },
        restartClearedKeys' = restartClearedKeys,
        cancelledKeys' = cancelledKeys,
        mostRecentCancelled' = mostRecentCancelled,
        mostRecentEvicted' = evicted,
        mostRecentRestartCleared' = mostRecentRestartCleared,
        effectCount' = effectCount + 1,
        restartCount' = restartCount,
        lastServe' = Some({
          key: key,
          response: response,
          cachedResponseBefore: None,
          sameRequestDifferentKeyBefore: prior,
          executedBefore: executedKeys.contains(key),
          evictedBefore: evictedKeys.contains(key),
          restartClearedBefore: restartClearedKeys.contains(key),
          waitWasActive: waitActive,
          responseDelivered: waitActive,
          effectCountBefore: effectCount,
          effectCountAfter: effectCount + 1,
        }),
        lastLifecycle' = CommandExecuted(key),
      }
    }
  }

  /// A process restart keeps durable namespace effects but loses the replay
  /// `VecDeque`. This is a current limitation, not a durable-idempotency claim.
  action restartServer: bool = all {
    inFlight == None,
    waitingFor == None,
    restartCount < MaxRestarts,
    replayCache' = emptyReplayCache,
    inFlight' = None,
    waitingFor' = None,
    deliveredResponses' = deliveredResponses,
    discardedResponses' = discardedResponses,
    executedKeys' = executedKeys,
    reexecutedKeys' = reexecutedKeys,
    evictedKeys' = evictedKeys,
    restartClearedKeys' = restartClearedKeys.union(cacheKeys(replayCache)),
    cancelledKeys' = cancelledKeys,
    mostRecentCancelled' = mostRecentCancelled,
    mostRecentEvicted' = mostRecentEvicted,
    mostRecentRestartCleared' = mostRecentCacheKey(replayCache),
    effectCount' = effectCount,
    restartCount' = restartCount + 1,
    lastServe' = lastServe,
    lastLifecycle' = ServerRestarted,
  }

  /// Real retry lane used to make stable-command replay explicit in bounded
  /// exploration without relying on a random reconstruction of every field.
  action retryLastServedExact: bool = {
    val audit = unwrapOr(lastServe, defaultAudit)
    all {
      lastServe != None,
      dispatch(audit.key),
    }
  }

  /// Same request id, requester, cursor, and response options, but a changed
  /// operation payload. Full-command equality makes this a cache miss.
  action dispatchChangedMutationFromLast: bool = {
    val audit = unwrapOr(lastServe, defaultAudit)
    val oldKey = audit.key
    val changedKey = {
      requester: oldKey.requester,
      command: {
        requestId: oldKey.command.requestId,
        mutation: otherMutation(oldKey.command.mutation),
        includeJournalBatches: oldKey.command.includeJournalBatches,
        cursor: oldKey.command.cursor,
      },
    }
    all {
      lastServe != None,
      dispatch(changedKey),
    }
  }

  /// Retrying a cancelled top-level POSIX call normally allocates a fresh
  /// request id. Even an otherwise identical payload is then a new cache key.
  action retryCancelledWithFreshId(newRequestId: RequestId): bool = {
    val oldKey = unwrapOr(mostRecentCancelled, defaultKey)
    val freshKey = {
      requester: oldKey.requester,
      command: {
        requestId: newRequestId,
        mutation: oldKey.command.mutation,
        includeJournalBatches: oldKey.command.includeJournalBatches,
        cursor: oldKey.command.cursor,
      },
    }
    all {
      mostRecentCancelled != None,
      newRequestId != oldKey.command.requestId,
      dispatch(freshKey),
    }
  }

  action retryMostRecentEvicted: bool = {
    val key = unwrapOr(mostRecentEvicted, defaultKey)
    all {
      mostRecentEvicted != None,
      dispatch(key),
    }
  }

  action retryMostRecentRestartCleared: bool = {
    val key = unwrapOr(mostRecentRestartCleared, defaultKey)
    all {
      mostRecentRestartCleared != None,
      dispatch(key),
    }
  }

  action stutter: bool = all {
    replayCache' = replayCache,
    inFlight' = inFlight,
    waitingFor' = waitingFor,
    deliveredResponses' = deliveredResponses,
    discardedResponses' = discardedResponses,
    executedKeys' = executedKeys,
    reexecutedKeys' = reexecutedKeys,
    evictedKeys' = evictedKeys,
    restartClearedKeys' = restartClearedKeys,
    cancelledKeys' = cancelledKeys,
    mostRecentCancelled' = mostRecentCancelled,
    mostRecentEvicted' = mostRecentEvicted,
    mostRecentRestartCleared' = mostRecentRestartCleared,
    effectCount' = effectCount,
    restartCount' = restartCount,
    lastServe' = lastServe,
    lastLifecycle' = lastLifecycle,
  }

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
    serveInFlight,
    cancelWait,
    restartServer,
    retryLastServedExact,
    dispatchChangedMutationFromLast,
    {
      nondet newRequestId = oneOf(RequestIds)
      retryCancelledWithFreshId(newRequestId)
    },
    retryMostRecentEvicted,
    retryMostRecentRestartCleared,
    stutter,
  }
}
