/// Causally coupled randomized exploration for the timed network layers.
module network_timed_delivery_smoke {
  import common.* from "../../lib/common"
  import network_time.* from "../../lib/network_time"
  import delivery_contract.* from "../../contracts/network/delivery_contract"
  import network_timed_transport from "../../models/network/timed_transport"
  import network_timed_delivery_retry from "../../models/network/timed_delivery_retry"
  import network_timed_transport_safety from "../../checks/network/timed_transport_safety"
  import network_timed_delivery_retry_safety from "../../checks/network/timed_delivery_retry_safety"

  action init = all {
    network_timed_transport::init,
    network_timed_delivery_retry::init,
  }

  /// The standalone layers retain separate clocks so they can be verified in
  /// isolation. Composition advances both clocks atomically.
  action tickTogether: bool = all {
    network_timed_transport::tick,
    network_timed_delivery_retry::tick,
  }

  action prepareRequest: bool = {
    nondet fallbackConfigured = oneOf(Set(true, false))
    nondet primaryOutcome = oneOf(network_timed_delivery_retry::EndpointOutcomes)
    nondet fallbackOutcome = oneOf(network_timed_delivery_retry::EndpointOutcomes)
    nondet primaryDelay = oneOf(PacketDelayChoices)
    nondet fallbackDelay = oneOf(PacketDelayChoices)
    all {
      network_timed_transport::stutter,
      network_timed_delivery_retry::beginRequest(
        fallbackConfigured,
        primaryOutcome,
        fallbackOutcome,
        primaryDelay,
        fallbackDelay
      ),
    }
  }

  /// Starting an endpoint attempt causally creates that attempt's transport
  /// packet with the same stable operation key and fresh attempt nonce.
  action startAttemptAndSend(endpoint: TimedEndpoint): bool = all {
    network_timed_transport::sendOriginal(
      endpoint,
      network_timed_delivery_retry::responseDelay.get(endpoint)
    ),
    network_timed_delivery_retry::startAttempt(endpoint),
  }

  /// Caller-visible response decisions can only occur in the same transition
  /// as delivery of a packet for that endpoint. A delivered packet may also be
  /// irrelevant to the retry loop, so retry stuttering remains an option.
  action deliverPacket(endpoint: TimedEndpoint, copy: network_timed_transport::PacketCopy): bool = {
    val packet = network_timed_transport::packetFor(endpoint, copy)
    all {
      network_timed_transport::deliverCopy(packet),
      any {
        network_timed_delivery_retry::acceptResponse(endpoint),
        network_timed_delivery_retry::observeIdentityMismatch(endpoint),
        network_timed_delivery_retry::ignoreLateResponse(endpoint),
        network_timed_delivery_retry::observeDuplicateResponse(endpoint),
        network_timed_delivery_retry::stutter,
      },
    }
  }

  action acceptDeliveredPacket(
    endpoint: TimedEndpoint,
    copy: network_timed_transport::PacketCopy
  ): bool = {
    val packet = network_timed_transport::packetFor(endpoint, copy)
    all {
      network_timed_transport::deliverCopy(packet),
      network_timed_delivery_retry::acceptResponse(endpoint),
    }
  }

  /// Keep one deterministic success lane in the randomized composition. This
  /// makes the causal-delivery witness measurable under the fixed regression
  /// seed without removing any adversarial transition from exploration.
  action guidedPrimaryDelivery: bool = any {
    all {
      network_timed_delivery_retry::phase == network_timed_delivery_retry::RetryIdle,
      requestDeadlineFits(network_timed_delivery_retry::retryNow),
      network_timed_transport::stutter,
      network_timed_delivery_retry::beginRequest(
        false,
        network_timed_delivery_retry::EndpointResponds,
        network_timed_delivery_retry::EndpointDrops,
        1,
        1
      ),
    },
    all {
      network_timed_delivery_retry::phase == network_timed_delivery_retry::RetryPrepared,
      startAttemptAndSend(TimedPrimary),
    },
    all {
      network_timed_delivery_retry::phase == network_timed_delivery_retry::RetryAttempting,
      network_timed_delivery_retry::currentEndpoint == Some(TimedPrimary),
      network_timed_delivery_retry::retryNow <
        network_timed_delivery_retry::responseDueAt.get(TimedPrimary),
      tickTogether,
    },
    all {
      network_timed_delivery_retry::phase == network_timed_delivery_retry::RetryAttempting,
      network_timed_delivery_retry::currentEndpoint == Some(TimedPrimary),
      network_timed_delivery_retry::responseDueAt.get(TimedPrimary) <=
        network_timed_delivery_retry::retryNow,
      acceptDeliveredPacket(TimedPrimary, network_timed_transport::OriginalCopy),
    },
  }

  action adversarialStep = any {
    tickTogether,
    prepareRequest,
    {
      nondet endpoint = oneOf(TimedEndpoints)
      startAttemptAndSend(endpoint)
    },
    {
      nondet endpoint = oneOf(TimedEndpoints)
      nondet copy = oneOf(network_timed_transport::PacketCopies)
      deliverPacket(endpoint, copy)
    },
    {
      nondet endpoint = oneOf(TimedEndpoints)
      nondet delay = oneOf(PacketDelayChoices)
      all {
        network_timed_transport::duplicateOriginal(endpoint, delay),
        network_timed_delivery_retry::stutter,
      }
    },
    {
      nondet packet = oneOf(network_timed_transport::TimedPackets)
      all {
        any {
          network_timed_transport::delayCopy(packet),
          network_timed_transport::dropCopy(packet),
        },
        network_timed_delivery_retry::stutter,
      }
    },
    any {
      all {
        network_timed_transport::setLink(true),
        network_timed_delivery_retry::stutter,
      },
      all {
        network_timed_transport::setLink(false),
        network_timed_delivery_retry::stutter,
      },
    },
    {
      nondet node = oneOf(Nodes)
      nondet isOnline = oneOf(Set(true, false))
      all {
        network_timed_transport::setNodeOnline(node, isOnline),
        network_timed_delivery_retry::stutter,
      }
    },
    {
      nondet endpoint = oneOf(TimedEndpoints)
      all {
        network_timed_transport::stutter,
        network_timed_delivery_retry::timeoutAttempt(endpoint),
      }
    },
    all {
      network_timed_transport::stutter,
      network_timed_delivery_retry::expireBeforeAttempt,
    },
    all {
      network_timed_transport::stutter,
      network_timed_delivery_retry::stutter,
    },
  }

  action step = any {
    guidedPrimaryDelivery,
    adversarialStep,
  }

  def clocksAgree: bool =
    network_timed_transport::transportNow == network_timed_delivery_retry::retryNow

  def responseDecisionsRequireTransportDelivery: bool =
    any {
      network_timed_delivery_retry::retryLastDecision ==
        network_timed_delivery_retry::ResponseAccepted,
      network_timed_delivery_retry::retryLastDecision ==
        network_timed_delivery_retry::IdentityMismatchObserved,
      network_timed_delivery_retry::retryLastDecision ==
        network_timed_delivery_retry::LateResponseIgnored,
      network_timed_delivery_retry::retryLastDecision ==
        network_timed_delivery_retry::DuplicateResponseObserved,
    } implies
      network_timed_transport::transportLastDecision ==
        network_timed_transport::PacketDelivered

  def acceptedResponseMatchesDeliveredAttempt: bool =
    network_timed_delivery_retry::retryLastDecision ==
      network_timed_delivery_retry::ResponseAccepted implies
        match network_timed_transport::lastPacket {
          | Some(packet) => all {
              network_timed_transport::transportLastDecision ==
                network_timed_transport::PacketDelivered,
              network_timed_delivery_retry::acceptedEndpoint == Some(packet.endpoint),
              network_timed_delivery_retry::deliveryKey == Some(packet.operationId),
              network_timed_delivery_retry::attemptNonce.get(packet.endpoint) ==
                Some(packet.attemptNonce),
            }
          | None => false
        }

  def witnessCausalTransportDelivery: bool = all {
    network_timed_delivery_retry::retryLastDecision ==
      network_timed_delivery_retry::ResponseAccepted,
    network_timed_transport::transportLastDecision ==
      network_timed_transport::PacketDelivered,
    acceptedResponseMatchesDeliveredAttempt,
  }

  def safety: bool = all {
    network_timed_transport_safety::safety,
    network_timed_delivery_retry_safety::safety,
    clocksAgree,
    responseDecisionsRequireTransportDelivery,
    acceptedResponseMatchesDeliveredAttempt,
  }
}
