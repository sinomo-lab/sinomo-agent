/// Invariant checks for `network_timed_transport`.
module network_timed_transport_safety {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"
  import network_timed_transport.* from "../../models/network/timed_transport"

  def timeInBounds: bool =
    validTick(transportNow)

  def packetSetsReferToSentCopies: bool = all {
    sentCopies.subseteq(TimedPackets),
    inFlightCopies.subseteq(sentCopies),
    droppedCopies.subseteq(sentCopies),
    deliveredCopies.subseteq(sentCopies),
  }

  def sentCopiesHaveBoundedTimes: bool =
    sentCopies.forall(copy => all {
      validTick(sendTime.get(copy)),
      validScheduledTick(deliverableTime.get(copy)),
      validScheduledTick(expiryTime.get(copy)),
      sendTime.get(copy) <= deliverableTime.get(copy),
      sendTime.get(copy) <= expiryTime.get(copy),
      deliverableTime.get(copy) <= expiryTime.get(copy),
    })

  def deliveryRequiresSendDelayAndExpiry: bool =
    transportLastDecision == PacketDelivered implies
      match lastPacket {
        | Some(packet) => all {
            sentCopies.contains(packet),
            sendTime.get(packet) <= transportNow,
            deliverableTime.get(packet) <= transportNow,
            transportNow <= expiryTime.get(packet),
          }
        | None => false
      }

  def deliveryRequiresLivePathAtDelivery: bool =
    transportLastDecision == PacketDelivered implies lastDeliveryWasLive

  def droppedCopiesAreNoLongerInFlight: bool =
    droppedCopies.intersect(inFlightCopies).size() == 0

  def droppedOriginalWithoutPriorDuplicateCannotDeliverLater: bool =
    TimedEndpoints.forall(endpoint =>
      all {
        originalDroppedBeforeDelivery.get(endpoint),
        not(duplicateCreatedBeforeOriginalDrop.get(endpoint)),
      } implies
        not(deliveredCopies.exists(packet => packet.endpoint == endpoint))
    )

  def duplicateCannotExistBeforeOriginal: bool =
    TimedEndpoints.forall(endpoint =>
      sentCopies.contains(packetFor(endpoint, DuplicateCopy)) implies
        sentCopies.contains(packetFor(endpoint, OriginalCopy))
    )

  def duplicatePreservesOriginalExpiry: bool =
    TimedEndpoints.forall(endpoint => {
      val duplicate = packetFor(endpoint, DuplicateCopy)
      val original = packetFor(endpoint, OriginalCopy)
      sentCopies.contains(duplicate) implies
        expiryTime.get(duplicate) == expiryTime.get(original)
    })

  def packetsPreserveOperationAndAttemptIdentity: bool =
    sentCopies.forall(packet => all {
      packet.operationId == StableOperationId,
      packet.attemptNonce == nonceFor(packet.endpoint),
    })

  def safety: bool = all {
    timeInBounds,
    packetSetsReferToSentCopies,
    sentCopiesHaveBoundedTimes,
    deliveryRequiresSendDelayAndExpiry,
    deliveryRequiresLivePathAtDelivery,
    droppedCopiesAreNoLongerInFlight,
    droppedOriginalWithoutPriorDuplicateCannotDeliverLater,
    duplicateCannotExistBeforeOriginal,
    duplicatePreservesOriginalExpiry,
    packetsPreserveOperationAndAttemptIdentity,
  }
}
