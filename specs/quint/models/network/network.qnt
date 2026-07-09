/// Adversarial network abstraction for Sinomo cluster protocol models.
///
/// This module deliberately models the network as mechanism, not authority:
/// it can carry observations and evidence, but it cannot decide membership or
/// trust. Membership specs should consume delivered messages and committed KV
/// evidence explicitly rather than treating reachability as authorization.
module network {
  import common.* from "../../lib/common"

  type MessageKind =
    | Discovery
    | JoinRequest
    | JoinAccepted
    | JoinRejected
    | Heartbeat
    | SnapshotEvidence
    | ObjectRpc

  type Evidence =
    | NoEvidence
    | MembershipEvidence
    | SnapshotEvidenceProof

  type Message = {
    src: Node,
    dst: Node,
    kind: MessageKind,
    evidence: Evidence,
  }

  pure val MessageKinds: Set[MessageKind] =
    Set(Discovery, JoinRequest, JoinAccepted, JoinRejected, Heartbeat, SnapshotEvidence, ObjectRpc)

  pure val EvidenceKinds: Set[Evidence] =
    Set(NoEvidence, MembershipEvidence, SnapshotEvidenceProof)

  var online: Node -> bool
  var link: Node -> (Node -> bool)
  var inFlight: Set[Message]
  var sentHistory: Set[Message]
  var delivered: Set[Message]
  var discovered: Node -> Set[Node]
  var trusted: Set[Node]

  pure val healthyLinks: Node -> (Node -> bool) =
    Nodes.mapBy(src => Nodes.mapBy(dst => src != dst))

  pure val emptyDiscovery: Node -> Set[Node] =
    Nodes.mapBy(_ => Set())

  pure def setDirectedLink(
    oldLinks: Node -> (Node -> bool),
    srcNode: Node,
    dstNode: Node,
    isUp: bool,
  ): Node -> (Node -> bool) =
    Nodes.mapBy(src =>
      if (src == srcNode) oldLinks.get(src).set(dstNode, isUp) else oldLinks.get(src)
    )

  def canDeliver(msg: Message): bool =
    inFlight.contains(msg)
      and online.get(msg.src)
      and online.get(msg.dst)
      and link.get(msg.src).get(msg.dst)

  action init = all {
    online' = Nodes.mapBy(_ => true),
    link' = healthyLinks,
    inFlight' = Set(),
    sentHistory' = Set(),
    delivered' = Set(),
    discovered' = emptyDiscovery,
    trusted' = Set(N1),
  }

  action send(srcNode: Node, dstNode: Node, kind: MessageKind, evidence: Evidence): bool = {
    val msg = { src: srcNode, dst: dstNode, kind: kind, evidence: evidence }
    all {
      srcNode != dstNode,
      online.get(srcNode),
      inFlight' = inFlight.union(Set(msg)),
      sentHistory' = sentHistory.union(Set(msg)),
      delivered' = delivered,
      discovered' = discovered,
      trusted' = trusted,
      online' = online,
      link' = link,
    }
  }

  /// Delivery does not consume the in-flight message. That over-approximates
  /// duplicate delivery and retry behavior; the separate drop action is what
  /// removes a message from the adversarial buffer.
  action deliver(msg: Message): bool = all {
    canDeliver(msg),
    inFlight' = inFlight,
    sentHistory' = sentHistory,
    delivered' = delivered.union(Set(msg)),
    discovered' =
      if (msg.kind == Discovery) {
        discovered.set(msg.dst, discovered.get(msg.dst).union(Set(msg.src)))
      } else {
        discovered
      },
    trusted' = trusted,
    online' = online,
    link' = link,
  }

  action drop(msg: Message): bool = all {
    inFlight.contains(msg),
    inFlight' = inFlight.exclude(Set(msg)),
    sentHistory' = sentHistory,
    delivered' = delivered,
    discovered' = discovered,
    trusted' = trusted,
    online' = online,
    link' = link,
  }

  action setLinkState(srcNode: Node, dstNode: Node, isUp: bool): bool = all {
    srcNode != dstNode,
    link' = setDirectedLink(link, srcNode, dstNode, isUp),
    online' = online,
    inFlight' = inFlight,
    sentHistory' = sentHistory,
    delivered' = delivered,
    discovered' = discovered,
    trusted' = trusted,
  }

  action setNodeOnline(node: Node, isOnline: bool): bool = all {
    online' = online.set(node, isOnline),
    link' = link,
    inFlight' = inFlight,
    sentHistory' = sentHistory,
    delivered' = delivered,
    discovered' = discovered,
    trusted' = trusted,
  }

  action step = any {
    {
      nondet srcNode = oneOf(Nodes)
      nondet dstNode = oneOf(Nodes)
      nondet kind = oneOf(MessageKinds)
      nondet evidence = oneOf(EvidenceKinds)
      send(srcNode, dstNode, kind, evidence)
    },
    {
      nondet msg = oneOf(inFlight)
      any {
        deliver(msg),
        drop(msg),
      }
    },
    {
      nondet srcNode = oneOf(Nodes)
      nondet dstNode = oneOf(Nodes)
      any {
        setLinkState(srcNode, dstNode, true),
        setLinkState(srcNode, dstNode, false),
      }
    },
    {
      nondet node = oneOf(Nodes)
      any {
        setNodeOnline(node, true),
        setNodeOnline(node, false),
      }
    }
  }
}
