/// Timed adversarial transport model.
///
/// This is below route planning and retry. It only models packet copies moving
/// through a lossy, delaying, duplicating transport with a bounded logical
/// clock.
module network_timed_transport {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"

  type PacketCopy = OriginalCopy | DuplicateCopy
  type TransportDecision =
    | TransportNoDecision
    | TransportTicked
    | OriginalSent
    | DuplicateSent
    | PacketDelayed
    | PacketDelivered
    | PacketDropped
    | LinkPartitioned
    | LinkHealed
    | NodeCrashed
    | NodeRestarted
    | TransportStuttered

  pure val PacketCopies: Set[PacketCopy] = Set(OriginalCopy, DuplicateCopy)
  pure val SourceNode: Node = N1
  pure val TargetNode: Node = N2

  var transportNow: int
  var online: Node -> bool
  var linkUp: bool
  var sentCopies: Set[PacketCopy]
  var inFlightCopies: Set[PacketCopy]
  var droppedCopies: Set[PacketCopy]
  var deliveredCopies: Set[PacketCopy]
  var sendTime: PacketCopy -> int
  var deliverableTime: PacketCopy -> int
  var expiryTime: PacketCopy -> int
  var originalDroppedBeforeDelivery: bool
  var duplicateCreatedBeforeOriginalDrop: bool
  var lastCopy: Option[PacketCopy]
  var lastDeliveryWasLive: bool
  var transportLastDecision: TransportDecision

  pure val zeroCopyTime: PacketCopy -> int =
    PacketCopies.mapBy(_ => 0)

  pure val allOnline: Node -> bool =
    Nodes.mapBy(_ => true)

  def canDeliverCopy(copy: PacketCopy): bool =
    all {
      inFlightCopies.contains(copy),
      online.get(SourceNode),
      online.get(TargetNode),
      linkUp,
      transportNow >= deliverableTime.get(copy),
      packetAlive(transportNow, expiryTime.get(copy)),
    }

  action init = all {
    transportNow' = 0,
    online' = allOnline,
    linkUp' = true,
    sentCopies' = Set(),
    inFlightCopies' = Set(),
    droppedCopies' = Set(),
    deliveredCopies' = Set(),
    sendTime' = zeroCopyTime,
    deliverableTime' = zeroCopyTime,
    expiryTime' = zeroCopyTime,
    originalDroppedBeforeDelivery' = false,
    duplicateCreatedBeforeOriginalDrop' = false,
    lastCopy' = None,
    lastDeliveryWasLive' = false,
    transportLastDecision' = TransportNoDecision,
  }

  action tick: bool = all {
    transportNow < MaxNetworkTick,
    transportNow' = tickNext(transportNow),
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies,
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime,
    deliverableTime' = deliverableTime,
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = lastCopy,
    lastDeliveryWasLive' = lastDeliveryWasLive,
    transportLastDecision' = TransportTicked,
  }

  action sendOriginal(delay: int): bool = all {
    PacketDelayChoices.contains(delay),
    not(sentCopies.contains(OriginalCopy)),
    transportNow < MaxNetworkTick,
    transportNow' = transportNow,
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies.union(Set(OriginalCopy)),
    inFlightCopies' = inFlightCopies.union(Set(OriginalCopy)),
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime.set(OriginalCopy, transportNow),
    deliverableTime' = deliverableTime.set(OriginalCopy, boundedAdd(transportNow, delay)),
    expiryTime' = expiryTime.set(OriginalCopy, packetExpiresAt(transportNow)),
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = Some(OriginalCopy),
    lastDeliveryWasLive' = false,
    transportLastDecision' = OriginalSent,
  }

  action duplicateOriginal(delay: int): bool = all {
    PacketDelayChoices.contains(delay),
    inFlightCopies.contains(OriginalCopy),
    not(sentCopies.contains(DuplicateCopy)),
    transportNow < expiryTime.get(OriginalCopy),
    transportNow' = transportNow,
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies.union(Set(DuplicateCopy)),
    inFlightCopies' = inFlightCopies.union(Set(DuplicateCopy)),
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime.set(DuplicateCopy, transportNow),
    deliverableTime' = deliverableTime.set(DuplicateCopy, boundedAdd(transportNow, delay)),
    expiryTime' = expiryTime.set(DuplicateCopy, expiryTime.get(OriginalCopy)),
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = not(droppedCopies.contains(OriginalCopy)),
    lastCopy' = Some(DuplicateCopy),
    lastDeliveryWasLive' = false,
    transportLastDecision' = DuplicateSent,
  }

  action delayCopy(copy: PacketCopy): bool = all {
    inFlightCopies.contains(copy),
    deliverableTime.get(copy) < expiryTime.get(copy),
    transportNow' = transportNow,
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies,
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime,
    deliverableTime' = deliverableTime.set(copy, tickNext(deliverableTime.get(copy))),
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = Some(copy),
    lastDeliveryWasLive' = false,
    transportLastDecision' = PacketDelayed,
  }

  action deliverCopy(copy: PacketCopy): bool = all {
    canDeliverCopy(copy),
    transportNow' = transportNow,
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies,
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies.union(Set(copy)),
    sendTime' = sendTime,
    deliverableTime' = deliverableTime,
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = Some(copy),
    lastDeliveryWasLive' = true,
    transportLastDecision' = PacketDelivered,
  }

  action dropCopy(copy: PacketCopy): bool = all {
    inFlightCopies.contains(copy),
    transportNow' = transportNow,
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies.exclude(Set(copy)),
    droppedCopies' = droppedCopies.union(Set(copy)),
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime,
    deliverableTime' = deliverableTime,
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' =
      if (copy == OriginalCopy and deliveredCopies.size() == 0) {
        true
      } else {
        originalDroppedBeforeDelivery
      },
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = Some(copy),
    lastDeliveryWasLive' = false,
    transportLastDecision' = PacketDropped,
  }

  action setLink(isUp: bool): bool = all {
    transportNow' = transportNow,
    online' = online,
    linkUp' = isUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies,
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime,
    deliverableTime' = deliverableTime,
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = lastCopy,
    lastDeliveryWasLive' = false,
    transportLastDecision' = if (isUp) { LinkHealed } else { LinkPartitioned },
  }

  action setNodeOnline(node: Node, isOnline: bool): bool = all {
    transportNow' = transportNow,
    online' = online.set(node, isOnline),
    linkUp' = linkUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies,
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime,
    deliverableTime' = deliverableTime,
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = lastCopy,
    lastDeliveryWasLive' = false,
    transportLastDecision' = if (isOnline) { NodeRestarted } else { NodeCrashed },
  }

  action stutter: bool = all {
    transportNow' = transportNow,
    online' = online,
    linkUp' = linkUp,
    sentCopies' = sentCopies,
    inFlightCopies' = inFlightCopies,
    droppedCopies' = droppedCopies,
    deliveredCopies' = deliveredCopies,
    sendTime' = sendTime,
    deliverableTime' = deliverableTime,
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastCopy' = lastCopy,
    lastDeliveryWasLive' = lastDeliveryWasLive,
    transportLastDecision' = TransportStuttered,
  }

  action step = any {
    tick,
    {
      nondet delay = oneOf(PacketDelayChoices)
      any {
        sendOriginal(delay),
        duplicateOriginal(delay),
      }
    },
    {
      nondet copy = oneOf(PacketCopies)
      any {
        delayCopy(copy),
        deliverCopy(copy),
        dropCopy(copy),
      }
    },
    any {
      setLink(true),
      setLink(false),
    },
    {
      nondet node = oneOf(Nodes)
      any {
        setNodeOnline(node, true),
        setNodeOnline(node, false),
      }
    },
    stutter,
  }
}
