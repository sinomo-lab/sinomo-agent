/// Safety and bounded reachability for TCP-tunnel stream backpressure.
module network_tcp_tunnel_stream_backpressure_safety {
  import common.* from "../../lib/common"
  import network_tcp_tunnel_stream_backpressure.* from "../../models/network/tcp_tunnel_stream_backpressure"

  def frameAccountingIsExact: bool =
    tunnelStream.wireFrames
      + tunnelStream.queuedFrames
      + heldFrameCount(tunnelStream)
      + tunnelStream.deliveredFrames
      + tunnelStream.discardedFrames
      == TotalFrames

  def receiveMemoryIsBounded: bool = all {
    tunnelStream.wireFrames >= 0,
    tunnelStream.queuedFrames >= 0,
    tunnelStream.queuedFrames <= QueueCapacity,
    tunnelStream.deliveredFrames >= 0,
    tunnelStream.discardedFrames >= 0,
    tunnelStream.queuedFrames + heldFrameCount(tunnelStream)
      <= QueueCapacity + 1,
  }

  def resetRequiresExplicitPeerCancellation: bool =
    tunnelStream.streamReset implies tunnelStream.peerCancelled

  def saturationRetainsHeadWithoutReset: bool =
    tunnelStream.lastTransition == SaturatedHeadHeld implies all {
      tunnelStream.heldHeadFrame,
      tunnelStream.queuedFrames == QueueCapacity,
      not(tunnelStream.streamReset),
      not(tunnelStream.peerCancelled),
    }

  def completionRequiresExactHealthyDelivery: bool =
    tunnelStream.complete implies all {
      tunnelStream.deliveredFrames == TotalFrames,
      tunnelStream.discardedFrames == 0,
      tunnelStream.wireFrames == 0,
      tunnelStream.queuedFrames == 0,
      not(tunnelStream.heldHeadFrame),
      not(tunnelStream.streamReset),
      not(tunnelStream.peerCancelled),
    }

  def terminalStatesAreExclusive: bool =
    not(tunnelStream.complete and tunnelStream.streamReset)

  def safety: bool = all {
    frameAccountingIsExact,
    receiveMemoryIsBounded,
    resetRequiresExplicitPeerCancellation,
    saturationRetainsHeadWithoutReset,
    completionRequiresExactHealthyDelivery,
    terminalStatesAreExclusive,
  }

  def witnessReceiveQueueSaturated: bool = all {
    tunnelStream.queuedFrames == QueueCapacity,
    tunnelStream.wireFrames == TotalFrames - QueueCapacity,
    not(tunnelStream.heldHeadFrame),
    not(tunnelStream.streamReset),
  }

  def witnessSaturatedHeadWaitsWithoutReset: bool = all {
    tunnelStream.lastTransition == SaturatedHeadHeld,
    tunnelStream.heldHeadFrame,
    tunnelStream.queuedFrames == QueueCapacity,
    not(tunnelStream.streamReset),
  }

  def witnessReverseControlProgressesWhileDataWaits: bool = all {
    tunnelStream.heldHeadFrame,
    tunnelStream.queuedFrames == QueueCapacity,
    tunnelStream.reverseControlCount > 0,
    not(tunnelStream.streamReset),
  }

  def witnessConsumerReleaseAdmitsHeldFrame: bool = all {
    tunnelStream.lastTransition == HeldFrameAdmitted,
    not(tunnelStream.heldHeadFrame),
    tunnelStream.queuedFrames == QueueCapacity,
    tunnelStream.wireFrames == 0,
    not(tunnelStream.streamReset),
  }

  def witnessHealthyTransferCompletes: bool = all {
    tunnelStream.complete,
    tunnelStream.deliveredFrames == TotalFrames,
    tunnelStream.discardedFrames == 0,
    not(tunnelStream.streamReset),
  }
}
