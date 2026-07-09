/// Invariant checks for `network`.
module network_safety {
  import common.* from "../../lib/common"
  import network.* from "../../models/network/network"


  /// Safety: every observable delivery came from an explicit send.
  def deliveredWasSent: bool =
    delivered.subseteq(sentHistory)

  /// Safety: the adversarial buffer cannot invent messages.

  def inFlightWasSent: bool =
    inFlight.subseteq(sentHistory)

  /// Safety: the network model never changes cluster authority. Later
  /// membership specs should update trust only from committed membership state.

  def networkDoesNotGrantTrust: bool =
    trusted == Set(N1)

  /// Safety: discovery is only an observation about node identifiers.

  def discoveryIsOnlyObservation: bool =
    Nodes.forall(n => discovered.get(n).subseteq(Nodes))

  /// Safety: self-links stay disabled, so the model does not hide bugs by
  /// delivering loopback cluster traffic.

  def noSelfLinks: bool =
    Nodes.forall(n => not(link.get(n).get(n)))


  def safety: bool = all {
    deliveredWasSent,
    inFlightWasSent,
    networkDoesNotGrantTrust,
    discoveryIsOnlyObservation,
    noSelfLinks,
  }
}
