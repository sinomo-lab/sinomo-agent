/// Timed adversarial transport model.
///
/// This is below route planning and retry. It only models packet copies moving
/// through a lossy, delaying, duplicating transport with a bounded logical
/// clock.
///
/// Evidence class: abstract transport contract. It over-approximates packet
/// behavior and does not refine a concrete TCP, Iroh, or module transport.
module network_timed_transport {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"

  type PacketCopy = OriginalCopy | DuplicateCopy
  type TimedPacket = {
    endpoint: TimedEndpoint,
    operationId: TimedOperationId,
    attemptNonce: TimedAttemptNonce,
    copy: PacketCopy,
  }
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
  pure val TimedPackets: Set[TimedPacket] = Set(
    {
      endpoint: TimedPrimary,
      operationId: StableOperationId,
      attemptNonce: PrimaryAttemptNonce,
      copy: OriginalCopy,
    },
    {
      endpoint: TimedPrimary,
      operationId: StableOperationId,
      attemptNonce: PrimaryAttemptNonce,
      copy: DuplicateCopy,
    },
    {
      endpoint: TimedFallback,
      operationId: StableOperationId,
      attemptNonce: FallbackAttemptNonce,
      copy: OriginalCopy,
    },
    {
      endpoint: TimedFallback,
      operationId: StableOperationId,
      attemptNonce: FallbackAttemptNonce,
      copy: DuplicateCopy,
    }
  )
  pure val SourceNode: Node = N1
  pure val TargetNode: Node = N2

  pure def packetFor(endpoint: TimedEndpoint, copy: PacketCopy): TimedPacket = {
    endpoint: endpoint,
    operationId: StableOperationId,
    attemptNonce: nonceFor(endpoint),
    copy: copy,
  }

  var transportNow: int
  var online: Node -> bool
  var linkUp: bool
  var sentCopies: Set[TimedPacket]
  var inFlightCopies: Set[TimedPacket]
  var droppedCopies: Set[TimedPacket]
  var deliveredCopies: Set[TimedPacket]
  var sendTime: TimedPacket -> int
  var deliverableTime: TimedPacket -> int
  var expiryTime: TimedPacket -> int
  var originalDroppedBeforeDelivery: TimedEndpoint -> bool
  var duplicateCreatedBeforeOriginalDrop: TimedEndpoint -> bool
  var lastPacket: Option[TimedPacket]
  var lastDeliveryWasLive: bool
  var transportLastDecision: TransportDecision

  pure val zeroCopyTime: TimedPacket -> int =
    TimedPackets.mapBy(_ => 0)

  pure val noEndpointFlag: TimedEndpoint -> bool =
    TimedEndpoints.mapBy(_ => false)

  pure val allOnline: Node -> bool =
    Nodes.mapBy(_ => true)

  def canDeliverCopy(copy: TimedPacket): bool =
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
    originalDroppedBeforeDelivery' = noEndpointFlag,
    duplicateCreatedBeforeOriginalDrop' = noEndpointFlag,
    lastPacket' = None,
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
    lastPacket' = lastPacket,
    lastDeliveryWasLive' = lastDeliveryWasLive,
    transportLastDecision' = TransportTicked,
  }

  action sendOriginal(endpoint: TimedEndpoint, delay: int): bool = {
    val packet = packetFor(endpoint, OriginalCopy)
    all {
      TimedEndpoints.contains(endpoint),
      PacketDelayChoices.contains(delay),
      not(sentCopies.contains(packet)),
      transportNow < MaxNetworkTick,
      transportNow' = transportNow,
      online' = online,
      linkUp' = linkUp,
      sentCopies' = sentCopies.union(Set(packet)),
      inFlightCopies' = inFlightCopies.union(Set(packet)),
      droppedCopies' = droppedCopies,
      deliveredCopies' = deliveredCopies,
      sendTime' = sendTime.set(packet, transportNow),
      deliverableTime' = deliverableTime.set(packet, durationEndAt(transportNow, delay)),
      expiryTime' = expiryTime.set(packet, packetExpiresAt(transportNow)),
      originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
      duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
      lastPacket' = Some(packet),
      lastDeliveryWasLive' = false,
      transportLastDecision' = OriginalSent,
    }
  }

  action duplicateOriginal(endpoint: TimedEndpoint, delay: int): bool = {
    val original = packetFor(endpoint, OriginalCopy)
    val duplicate = packetFor(endpoint, DuplicateCopy)
    all {
      TimedEndpoints.contains(endpoint),
      PacketDelayChoices.contains(delay),
      inFlightCopies.contains(original),
      not(sentCopies.contains(duplicate)),
      transportNow < expiryTime.get(original),
      durationEndAt(transportNow, delay) <= expiryTime.get(original),
      transportNow' = transportNow,
      online' = online,
      linkUp' = linkUp,
      sentCopies' = sentCopies.union(Set(duplicate)),
      inFlightCopies' = inFlightCopies.union(Set(duplicate)),
      droppedCopies' = droppedCopies,
      deliveredCopies' = deliveredCopies,
      sendTime' = sendTime.set(duplicate, transportNow),
      deliverableTime' = deliverableTime.set(duplicate, durationEndAt(transportNow, delay)),
      expiryTime' = expiryTime.set(duplicate, expiryTime.get(original)),
      originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
      duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop.set(
        endpoint,
        not(droppedCopies.contains(original))
      ),
      lastPacket' = Some(duplicate),
      lastDeliveryWasLive' = false,
      transportLastDecision' = DuplicateSent,
    }
  }

  action delayCopy(copy: TimedPacket): bool = all {
    TimedPackets.contains(copy),
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
    deliverableTime' = deliverableTime.set(
      copy,
      durationEndAt(deliverableTime.get(copy), 1)
    ),
    expiryTime' = expiryTime,
    originalDroppedBeforeDelivery' = originalDroppedBeforeDelivery,
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastPacket' = Some(copy),
    lastDeliveryWasLive' = false,
    transportLastDecision' = PacketDelayed,
  }

  action deliverCopy(copy: TimedPacket): bool = all {
    TimedPackets.contains(copy),
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
    lastPacket' = Some(copy),
    lastDeliveryWasLive' = true,
    transportLastDecision' = PacketDelivered,
  }

  action dropCopy(copy: TimedPacket): bool = all {
    TimedPackets.contains(copy),
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
      if (copy.copy == OriginalCopy and
          not(deliveredCopies.exists(packet => packet.endpoint == copy.endpoint))) {
        originalDroppedBeforeDelivery.set(copy.endpoint, true)
      } else {
        originalDroppedBeforeDelivery
      },
    duplicateCreatedBeforeOriginalDrop' = duplicateCreatedBeforeOriginalDrop,
    lastPacket' = Some(copy),
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
    lastPacket' = lastPacket,
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
    lastPacket' = lastPacket,
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
    lastPacket' = lastPacket,
    lastDeliveryWasLive' = lastDeliveryWasLive,
    transportLastDecision' = TransportStuttered,
  }

  action step = any {
    tick,
    {
      nondet endpoint = oneOf(TimedEndpoints)
      nondet delay = oneOf(PacketDelayChoices)
      any {
        sendOriginal(endpoint, delay),
        duplicateOriginal(endpoint, delay),
      }
    },
    {
      nondet copy = oneOf(TimedPackets)
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
