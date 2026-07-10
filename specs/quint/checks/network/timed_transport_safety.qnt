/// Invariant checks for `network_timed_transport`.
module network_timed_transport_safety {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import network_timed_transport.* from "../../models/network/timed_transport"

  def timeInBounds: bool =
    validTick(transportNow)

  def packetSetsReferToSentCopies: bool = all {
    inFlightCopies.subseteq(sentCopies),
    droppedCopies.subseteq(sentCopies),
    deliveredCopies.subseteq(sentCopies),
  }

  def sentCopiesHaveBoundedTimes: bool =
    sentCopies.forall(copy => all {
      validTick(sendTime.get(copy)),
      validTick(deliverableTime.get(copy)),
      validTick(expiryTime.get(copy)),
      sendTime.get(copy) <= deliverableTime.get(copy),
      sendTime.get(copy) <= expiryTime.get(copy),
    })

  def deliveryRequiresSendDelayAndExpiry: bool =
    transportLastDecision == PacketDelivered implies
      match lastCopy {
        | Some(copy) => all {
            sentCopies.contains(copy),
            sendTime.get(copy) <= transportNow,
            deliverableTime.get(copy) <= transportNow,
            transportNow <= expiryTime.get(copy),
          }
        | None => false
      }

  def deliveryRequiresLivePathAtDelivery: bool =
    transportLastDecision == PacketDelivered implies lastDeliveryWasLive

  def droppedCopiesAreNoLongerInFlight: bool =
    droppedCopies.intersect(inFlightCopies).size() == 0

  def droppedOriginalWithoutPriorDuplicateCannotDeliverLater: bool =
    all {
      originalDroppedBeforeDelivery,
      not(duplicateCreatedBeforeOriginalDrop),
    } implies deliveredCopies.size() == 0

  def duplicateCannotExistBeforeOriginal: bool =
    sentCopies.contains(DuplicateCopy) implies sentCopies.contains(OriginalCopy)

  def duplicatePreservesOriginalExpiry: bool =
    sentCopies.contains(DuplicateCopy) implies
      expiryTime.get(DuplicateCopy) == expiryTime.get(OriginalCopy)

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
  }
}
