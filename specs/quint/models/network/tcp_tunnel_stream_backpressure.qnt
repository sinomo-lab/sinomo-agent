/// Bounded receive admission for one multiplexed TCP-tunnel stream.
///
/// The physical tunnel reader may decode one frame before discovering that
/// the per-stream/global receive window is full. That frame is retained as a
/// bounded head waiter and physical reads pause until the application consumes
/// queue capacity. Queue saturation is flow control, not a stream reset.
/// Reverse-direction control remains independently admissible because the TCP
/// tunnel is full duplex.
///
/// Evidence class: bounded transport-admission refinement. Frame sizes are
/// abstract units; the Rust refinement uses byte permits and bounded channels.
module network_tcp_tunnel_stream_backpressure {
  import common.* from "../../lib/common"

  type TunnelStreamTransition =
    | TunnelStreamInitialized
    | FrameQueued
    | SaturatedHeadHeld
    | QueuedFrameConsumed
    | HeldFrameAdmitted
    | ReverseControlProcessed
    | HealthyTransferCompleted
    | PeerCancelledStream
    | TunnelStreamTerminalStutter

  type TunnelStreamState = {
    wireFrames: int,
    queuedFrames: int,
    heldHeadFrame: bool,
    deliveredFrames: int,
    discardedFrames: int,
    peerCancelled: bool,
    streamReset: bool,
    complete: bool,
    reverseControlCount: int,
    lastTransition: TunnelStreamTransition,
  }

  pure val TotalFrames: int = 3
  pure val QueueCapacity: int = 2

  pure def heldFrameCount(state: TunnelStreamState): int =
    if (state.heldHeadFrame) 1 else 0

  pure def canAdmitHeldFrame(state: TunnelStreamState): bool = all {
    state.heldHeadFrame,
    state.queuedFrames < QueueCapacity,
    not(state.streamReset),
  }

  pure val initialState: TunnelStreamState = {
    wireFrames: TotalFrames,
    queuedFrames: 0,
    heldHeadFrame: false,
    deliveredFrames: 0,
    discardedFrames: 0,
    peerCancelled: false,
    streamReset: false,
    complete: false,
    reverseControlCount: 0,
    lastTransition: TunnelStreamInitialized,
  }

  var tunnelStream: TunnelStreamState

  action init = tunnelStream' = initialState

  action queueFrame: bool = all {
    tunnelStream.wireFrames > 0,
    tunnelStream.queuedFrames < QueueCapacity,
    not(tunnelStream.heldHeadFrame),
    not(tunnelStream.streamReset),
    not(tunnelStream.complete),
    tunnelStream' = tunnelStream
      .with("wireFrames", tunnelStream.wireFrames - 1)
      .with("queuedFrames", tunnelStream.queuedFrames + 1)
      .with("lastTransition", FrameQueued),
  }

  /// The physical reader has already decoded this bounded frame. It waits
  /// outside the queue instead of converting temporary saturation into loss.
  action holdSaturatedHead: bool = all {
    tunnelStream.wireFrames > 0,
    tunnelStream.queuedFrames == QueueCapacity,
    not(tunnelStream.heldHeadFrame),
    not(tunnelStream.streamReset),
    not(tunnelStream.complete),
    tunnelStream' = tunnelStream
      .with("wireFrames", tunnelStream.wireFrames - 1)
      .with("heldHeadFrame", true)
      .with("lastTransition", SaturatedHeadHeld),
  }

  action consumeQueuedFrame: bool = all {
    tunnelStream.queuedFrames > 0,
    not(tunnelStream.streamReset),
    not(tunnelStream.complete),
    tunnelStream' = tunnelStream
      .with("queuedFrames", tunnelStream.queuedFrames - 1)
      .with("deliveredFrames", tunnelStream.deliveredFrames + 1)
      .with("lastTransition", QueuedFrameConsumed),
  }

  action admitHeldFrame: bool = all {
    canAdmitHeldFrame(tunnelStream),
    tunnelStream' = tunnelStream
      .with("heldHeadFrame", false)
      .with("queuedFrames", tunnelStream.queuedFrames + 1)
      .with("lastTransition", HeldFrameAdmitted),
  }

  /// ACKs and keepalives in the reverse direction do not consume this receive
  /// queue and may progress while the data direction is backpressured.
  action processReverseControl: bool = all {
    not(tunnelStream.streamReset),
    not(tunnelStream.complete),
    tunnelStream' = tunnelStream
      .with("reverseControlCount", tunnelStream.reverseControlCount + 1)
      .with("lastTransition", ReverseControlProcessed),
  }

  action completeHealthyTransfer: bool = all {
    tunnelStream.wireFrames == 0,
    tunnelStream.queuedFrames == 0,
    not(tunnelStream.heldHeadFrame),
    tunnelStream.deliveredFrames == TotalFrames,
    not(tunnelStream.streamReset),
    not(tunnelStream.complete),
    tunnelStream' = tunnelStream
      .with("complete", true)
      .with("lastTransition", HealthyTransferCompleted),
  }

  /// Reset remains available only for an explicit peer cancellation, not as a
  /// local response to bounded-queue saturation.
  action cancelFromPeer: bool = all {
    not(tunnelStream.streamReset),
    not(tunnelStream.complete),
    tunnelStream' = tunnelStream
      .with(
        "discardedFrames",
        tunnelStream.discardedFrames
          + tunnelStream.wireFrames
          + tunnelStream.queuedFrames
          + heldFrameCount(tunnelStream)
      )
      .with("wireFrames", 0)
      .with("queuedFrames", 0)
      .with("heldHeadFrame", false)
      .with("peerCancelled", true)
      .with("streamReset", true)
      .with("lastTransition", PeerCancelledStream),
  }

  action stutterTerminal: bool = all {
    any { tunnelStream.complete, tunnelStream.streamReset },
    tunnelStream' = tunnelStream
      .with("lastTransition", TunnelStreamTerminalStutter),
  }

  action step = any {
    queueFrame,
    holdSaturatedHead,
    consumeQueuedFrame,
    admitHeldFrame,
    processReverseControl,
    completeHealthyTransfer,
    cancelFromPeer,
    stutterTerminal,
  }
}
