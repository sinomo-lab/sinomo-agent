/// DFS pending request lifecycle safety.
///
/// DFS has several request maps with different ownership rules:
/// - remote reads and routed namespace ops are keyed by request id and removed
///   when they complete or time out;
/// - routed writes are keyed by request id, but a completed result may remain
///   as a terminal cached outcome for `write_outcome`;
/// - primary-side remote write responses are keyed by intent id and carry the
///   original request id to respond to once the local write settles;
/// - stage and repair transfers store request ids under the target replica.
///
/// This model verifies the shared safety contract: request ids are not reused
/// while active, a response can settle only the matching tracked request, late
/// or mismatched responses are discarded, and terminal/timeout transitions stop
/// being active.
module dfs_pending_request_lifecycle {
  import common.* from "../../lib/common"

  type RequestId = Req1 | Req2 | Req3
  type DfsObject = ObjA | ObjB
  type IntentId = IntentA | IntentB
  type RoutedState = RoutedActive | RoutedTerminal
  type Observation = NoObservation | Settled(RequestId) | Discarded(RequestId) | TimedOut(RequestId)

  type PendingEndpoint = {
    source: Node,
    object: DfsObject,
  }

  type PendingRoutedWrite = {
    primary: Node,
    object: DfsObject,
    state: RoutedState,
  }

  type PendingRemoteWriteResponse = {
    requester: Node,
    requestId: RequestId,
    object: DfsObject,
  }

  pure val RequestIds: Set[RequestId] = Set(Req1, Req2, Req3)
  pure val Objects: Set[DfsObject] = Set(ObjA, ObjB)
  pure val Intents: Set[IntentId] = Set(IntentA, IntentB)

  var issued: Set[RequestId]
  var pendingReads: RequestId -> Option[PendingEndpoint]
  var pendingRoutedWrites: RequestId -> Option[PendingRoutedWrite]
  var pendingNamespaces: RequestId -> Option[PendingEndpoint]
  var pendingRemoteWriteResponses: IntentId -> Option[PendingRemoteWriteResponse]
  var stageInflight: Node -> Option[RequestId]
  var repairInflight: Node -> Option[RequestId]
  var settled: Set[RequestId]
  var discarded: Set[RequestId]
  var wrongSettles: Set[RequestId]
  var lastObservation: Observation

  pure val emptyEndpointMap: RequestId -> Option[PendingEndpoint] =
    RequestIds.mapBy(_ => None)

  pure val emptyRoutedWriteMap: RequestId -> Option[PendingRoutedWrite] =
    RequestIds.mapBy(_ => None)

  pure val emptyRemoteWriteResponseMap: IntentId -> Option[PendingRemoteWriteResponse] =
    Intents.mapBy(_ => None)

  pure val emptyNodeRequestMap: Node -> Option[RequestId] =
    Nodes.mapBy(_ => None)

  pure def boolToInt(value: bool): int =
    if (value) { 1 } else { 0 }

  def routedWriteIsActive(requestId: RequestId): bool =
    match pendingRoutedWrites.get(requestId) {
      | Some(pending) => pending.state == RoutedActive
      | None => false
    }

  def routedWriteIsTerminal(requestId: RequestId): bool =
    match pendingRoutedWrites.get(requestId) {
      | Some(pending) => pending.state == RoutedTerminal
      | None => false
    }

  def stageHasRequest(requestId: RequestId): bool =
    Nodes.filter(node => stageInflight.get(node) == Some(requestId)).size() > 0

  def repairHasRequest(requestId: RequestId): bool =
    Nodes.filter(node => repairInflight.get(node) == Some(requestId)).size() > 0

  def remoteWriteResponseHasRequest(requestId: RequestId): bool =
    Intents.filter(intent =>
      match pendingRemoteWriteResponses.get(intent) {
        | Some(pending) => pending.requestId == requestId
        | None => false
      }
    ).size() > 0

  def activeRequestMapCount(requestId: RequestId): int =
    boolToInt(isSome(pendingReads.get(requestId)))
      + boolToInt(routedWriteIsActive(requestId))
      + boolToInt(isSome(pendingNamespaces.get(requestId)))
      + boolToInt(remoteWriteResponseHasRequest(requestId))
      + boolToInt(stageHasRequest(requestId))
      + boolToInt(repairHasRequest(requestId))

  def requestIsActive(requestId: RequestId): bool =
    activeRequestMapCount(requestId) > 0

  action init = all {
    issued' = Set(),
    pendingReads' = emptyEndpointMap,
    pendingRoutedWrites' = emptyRoutedWriteMap,
    pendingNamespaces' = emptyEndpointMap,
    pendingRemoteWriteResponses' = emptyRemoteWriteResponseMap,
    stageInflight' = emptyNodeRequestMap,
    repairInflight' = emptyNodeRequestMap,
    settled' = Set(),
    discarded' = Set(),
    wrongSettles' = Set(),
    lastObservation' = NoObservation,
  }

  action beginRead(requestId: RequestId, source: Node, object: DfsObject): bool = all {
    not(issued.contains(requestId)),
    issued' = issued.union(Set(requestId)),
    pendingReads' = pendingReads.set(requestId, Some({
      source: source,
      object: object,
    })),
    pendingRoutedWrites' = pendingRoutedWrites,
    pendingNamespaces' = pendingNamespaces,
    pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
    stageInflight' = stageInflight,
    repairInflight' = repairInflight,
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    lastObservation' = NoObservation,
  }

  action beginRoutedWrite(requestId: RequestId, primary: Node, object: DfsObject): bool = all {
    not(issued.contains(requestId)),
    issued' = issued.union(Set(requestId)),
    pendingReads' = pendingReads,
    pendingRoutedWrites' = pendingRoutedWrites.set(requestId, Some({
      primary: primary,
      object: object,
      state: RoutedActive,
    })),
    pendingNamespaces' = pendingNamespaces,
    pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
    stageInflight' = stageInflight,
    repairInflight' = repairInflight,
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    lastObservation' = NoObservation,
  }

  action beginNamespace(requestId: RequestId, primary: Node, object: DfsObject): bool = all {
    not(issued.contains(requestId)),
    issued' = issued.union(Set(requestId)),
    pendingReads' = pendingReads,
    pendingRoutedWrites' = pendingRoutedWrites,
    pendingNamespaces' = pendingNamespaces.set(requestId, Some({
      source: primary,
      object: object,
    })),
    pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
    stageInflight' = stageInflight,
    repairInflight' = repairInflight,
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    lastObservation' = NoObservation,
  }

  action beginRemoteWriteResponse(
    intentId: IntentId,
    requestId: RequestId,
    requester: Node,
    object: DfsObject
  ): bool = all {
    not(issued.contains(requestId)),
    pendingRemoteWriteResponses.get(intentId) == None,
    issued' = issued.union(Set(requestId)),
    pendingReads' = pendingReads,
    pendingRoutedWrites' = pendingRoutedWrites,
    pendingNamespaces' = pendingNamespaces,
    pendingRemoteWriteResponses' = pendingRemoteWriteResponses.set(intentId, Some({
      requester: requester,
      requestId: requestId,
      object: object,
    })),
    stageInflight' = stageInflight,
    repairInflight' = repairInflight,
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    lastObservation' = NoObservation,
  }

  action beginStage(requestId: RequestId, replica: Node): bool = all {
    not(issued.contains(requestId)),
    stageInflight.get(replica) == None,
    issued' = issued.union(Set(requestId)),
    pendingReads' = pendingReads,
    pendingRoutedWrites' = pendingRoutedWrites,
    pendingNamespaces' = pendingNamespaces,
    pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
    stageInflight' = stageInflight.set(replica, Some(requestId)),
    repairInflight' = repairInflight,
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    lastObservation' = NoObservation,
  }

  action beginRepair(requestId: RequestId, replica: Node): bool = all {
    not(issued.contains(requestId)),
    repairInflight.get(replica) == None,
    issued' = issued.union(Set(requestId)),
    pendingReads' = pendingReads,
    pendingRoutedWrites' = pendingRoutedWrites,
    pendingNamespaces' = pendingNamespaces,
    pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
    stageInflight' = stageInflight,
    repairInflight' = repairInflight.set(replica, Some(requestId)),
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    lastObservation' = NoObservation,
  }

  action receiveReadResponse(requestId: RequestId, source: Node, object: DfsObject): bool = {
    val pending = unwrapOr(pendingReads.get(requestId), {
      source: N1,
      object: ObjA,
    })
    if (isSome(pendingReads.get(requestId)) and source == pending.source) {
      all {
        pendingReads' = pendingReads.set(requestId, None),
        settled' = settled.union(Set(requestId)),
        lastObservation' = Settled(requestId),
        issued' = issued,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingNamespaces' = pendingNamespaces,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight,
        discarded' = discarded,
        wrongSettles' = wrongSettles,
      }
    } else {
      all {
        pendingReads' = pendingReads,
        discarded' = discarded.union(Set(requestId)),
        lastObservation' = Discarded(requestId),
        issued' = issued,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingNamespaces' = pendingNamespaces,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight,
        settled' = settled,
        wrongSettles' = wrongSettles,
      }
    }
  }

  action receiveRoutedWriteResponse(requestId: RequestId, source: Node, object: DfsObject): bool =
    match pendingRoutedWrites.get(requestId) {
      | Some(pending) =>
          if (pending.primary == source and pending.object == object) {
            all {
              pendingRoutedWrites' = pendingRoutedWrites.set(requestId, Some({
                primary: pending.primary,
                object: pending.object,
                state: RoutedTerminal,
              })),
              settled' = settled.union(Set(requestId)),
              lastObservation' = Settled(requestId),
              issued' = issued,
              pendingReads' = pendingReads,
              pendingNamespaces' = pendingNamespaces,
              pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
              stageInflight' = stageInflight,
              repairInflight' = repairInflight,
              discarded' = discarded,
              wrongSettles' = wrongSettles,
            }
          } else {
            all {
              pendingRoutedWrites' = pendingRoutedWrites,
              discarded' = discarded.union(Set(requestId)),
              lastObservation' = Discarded(requestId),
              issued' = issued,
              pendingReads' = pendingReads,
              pendingNamespaces' = pendingNamespaces,
              pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
              stageInflight' = stageInflight,
              repairInflight' = repairInflight,
              settled' = settled,
              wrongSettles' = wrongSettles,
            }
          }
      | None => all {
          pendingRoutedWrites' = pendingRoutedWrites,
          discarded' = discarded.union(Set(requestId)),
          lastObservation' = Discarded(requestId),
          issued' = issued,
          pendingReads' = pendingReads,
          pendingNamespaces' = pendingNamespaces,
          pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
          stageInflight' = stageInflight,
          repairInflight' = repairInflight,
          settled' = settled,
          wrongSettles' = wrongSettles,
        }
    }

  action receiveNamespaceResponse(requestId: RequestId, source: Node, object: DfsObject): bool = {
    val pending = unwrapOr(pendingNamespaces.get(requestId), {
      source: N1,
      object: ObjA,
    })
    if (isSome(pendingNamespaces.get(requestId))
      and source == pending.source
      and object == pending.object) {
      all {
        pendingNamespaces' = pendingNamespaces.set(requestId, None),
        settled' = settled.union(Set(requestId)),
        lastObservation' = Settled(requestId),
        issued' = issued,
        pendingReads' = pendingReads,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight,
        discarded' = discarded,
        wrongSettles' = wrongSettles,
      }
    } else {
      all {
        pendingNamespaces' = pendingNamespaces,
        discarded' = discarded.union(Set(requestId)),
        lastObservation' = Discarded(requestId),
        issued' = issued,
        pendingReads' = pendingReads,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        stageInflight' = stageInflight,
        repairInflight' = repairInflight,
        settled' = settled,
        wrongSettles' = wrongSettles,
      }
    }
  }

  action flushRemoteWriteResponse(intentId: IntentId): bool =
    match pendingRemoteWriteResponses.get(intentId) {
      | Some(pending) => all {
          pendingRemoteWriteResponses' = pendingRemoteWriteResponses.set(intentId, None),
          settled' = settled.union(Set(pending.requestId)),
          lastObservation' = Settled(pending.requestId),
          issued' = issued,
          pendingReads' = pendingReads,
          pendingRoutedWrites' = pendingRoutedWrites,
          pendingNamespaces' = pendingNamespaces,
          stageInflight' = stageInflight,
          repairInflight' = repairInflight,
          discarded' = discarded,
          wrongSettles' = wrongSettles,
        }
      | None => all {
          pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
          lastObservation' = NoObservation,
          issued' = issued,
          pendingReads' = pendingReads,
          pendingRoutedWrites' = pendingRoutedWrites,
          pendingNamespaces' = pendingNamespaces,
          stageInflight' = stageInflight,
          repairInflight' = repairInflight,
          settled' = settled,
          discarded' = discarded,
          wrongSettles' = wrongSettles,
        }
    }

  action receiveStageResponse(requestId: RequestId, replica: Node): bool =
    if (stageInflight.get(replica) == Some(requestId)) {
      all {
        stageInflight' = stageInflight.set(replica, None),
        settled' = settled.union(Set(requestId)),
        lastObservation' = Settled(requestId),
        issued' = issued,
        pendingReads' = pendingReads,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingNamespaces' = pendingNamespaces,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        repairInflight' = repairInflight,
        discarded' = discarded,
        wrongSettles' = wrongSettles,
      }
    } else {
      all {
        stageInflight' = stageInflight,
        discarded' = discarded.union(Set(requestId)),
        lastObservation' = Discarded(requestId),
        issued' = issued,
        pendingReads' = pendingReads,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingNamespaces' = pendingNamespaces,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        repairInflight' = repairInflight,
        settled' = settled,
        wrongSettles' = wrongSettles,
      }
    }

  action receiveRepairResponse(requestId: RequestId, replica: Node): bool =
    if (repairInflight.get(replica) == Some(requestId)) {
      all {
        repairInflight' = repairInflight.set(replica, None),
        settled' = settled.union(Set(requestId)),
        lastObservation' = Settled(requestId),
        issued' = issued,
        pendingReads' = pendingReads,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingNamespaces' = pendingNamespaces,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        stageInflight' = stageInflight,
        discarded' = discarded,
        wrongSettles' = wrongSettles,
      }
    } else {
      all {
        repairInflight' = repairInflight,
        discarded' = discarded.union(Set(requestId)),
        lastObservation' = Discarded(requestId),
        issued' = issued,
        pendingReads' = pendingReads,
        pendingRoutedWrites' = pendingRoutedWrites,
        pendingNamespaces' = pendingNamespaces,
        pendingRemoteWriteResponses' = pendingRemoteWriteResponses,
        stageInflight' = stageInflight,
        settled' = settled,
        wrongSettles' = wrongSettles,
      }
    }

  action timeoutRequest(requestId: RequestId): bool = all {
    requestIsActive(requestId),
    pendingReads' = pendingReads.set(requestId, None),
    pendingRoutedWrites' =
      if (routedWriteIsActive(requestId)) {
        pendingRoutedWrites.set(requestId, None)
      } else {
        pendingRoutedWrites
      },
    pendingNamespaces' = pendingNamespaces.set(requestId, None),
    pendingRemoteWriteResponses' =
      Intents.mapBy(intent =>
        match pendingRemoteWriteResponses.get(intent) {
          | Some(pending) =>
              if (pending.requestId == requestId) {
                None
              } else {
                Some(pending)
              }
          | None => None
        }
      ),
    stageInflight' = Nodes.mapBy(node =>
      if (stageInflight.get(node) == Some(requestId)) {
        None
      } else {
        stageInflight.get(node)
      }
    ),
    repairInflight' = Nodes.mapBy(node =>
      if (repairInflight.get(node) == Some(requestId)) {
        None
      } else {
        repairInflight.get(node)
      }
    ),
    settled' = settled,
    discarded' = discarded,
    wrongSettles' = wrongSettles,
    issued' = issued,
    lastObservation' = TimedOut(requestId),
  }

  action step = any {
    {
      nondet requestId = oneOf(RequestIds)
      nondet source = oneOf(Nodes)
      nondet object = oneOf(Objects)
      any {
        beginRead(requestId, source, object),
        beginRoutedWrite(requestId, source, object),
        beginNamespace(requestId, source, object),
      }
    },
    {
      nondet requestId = oneOf(RequestIds)
      nondet replica = oneOf(Nodes)
      any {
        beginStage(requestId, replica),
        beginRepair(requestId, replica),
      }
    },
    {
      nondet intentId = oneOf(Intents)
      nondet requestId = oneOf(RequestIds)
      nondet requester = oneOf(Nodes)
      nondet object = oneOf(Objects)
      beginRemoteWriteResponse(intentId, requestId, requester, object)
    },
    {
      nondet requestId = oneOf(RequestIds)
      nondet source = oneOf(Nodes)
      nondet object = oneOf(Objects)
      any {
        receiveReadResponse(requestId, source, object),
        receiveRoutedWriteResponse(requestId, source, object),
        receiveNamespaceResponse(requestId, source, object),
      }
    },
    {
      nondet requestId = oneOf(RequestIds)
      nondet replica = oneOf(Nodes)
      any {
        receiveStageResponse(requestId, replica),
        receiveRepairResponse(requestId, replica),
      }
    },
    {
      nondet intentId = oneOf(Intents)
      flushRemoteWriteResponse(intentId)
    },
    {
      nondet requestId = oneOf(RequestIds)
      timeoutRequest(requestId)
    },
  }
}
