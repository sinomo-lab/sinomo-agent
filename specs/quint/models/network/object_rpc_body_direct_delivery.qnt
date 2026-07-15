/// Bounded direct-delivery grace for one received object-RPC body frame.
///
/// The wire reader first uses a non-blocking send into the bounded application
/// queue. If that queue is full, the decoded frame waits for a small, bounded
/// grace period. Consumer progress during that grace keeps the frame entirely
/// in memory; an expired grace falls back to the existing disk spool so an
/// ignored body or simultaneous full-duplex transfer cannot deadlock the
/// session reader.
///
/// Evidence class: bounded transport-admission refinement. One model tick is
/// an abstract portion of the Rust timeout; the implementation uses wall-clock
/// time while preserving the same finite Direct / Spill / Closed outcomes.
module network_object_rpc_body_direct_delivery {
  import common.* from "../../lib/common"

  type BodyDeliveryTransition =
    | BodyDeliveryInitialized
    | BoundedDirectWaitStarted
    | DirectWaitTicked
    | ConsumerFreedQueueCapacity
    | WaitingFrameDeliveredDirectly
    | WaitingFrameSpilledAfterGrace
    | BodyConsumerClosed
    | BodyDeliveryTerminalStutter

  type BodyDeliveryState = {
    queueDepth: int,
    pendingFrame: bool,
    waitingDirect: bool,
    waitTicks: int,
    consumerProgressCount: int,
    deliveredDirectly: bool,
    spilledToDisk: bool,
    consumerClosed: bool,
    closedWhileWaiting: bool,
    lastTransition: BodyDeliveryTransition,
  }

  pure val QueueCapacity: int = 2
  pure val DirectWaitBudget: int = 2

  pure def outcomeCount(state: BodyDeliveryState): int =
    (if (state.deliveredDirectly) 1 else 0)
      + (if (state.spilledToDisk) 1 else 0)
      + (if (state.consumerClosed) 1 else 0)

  pure val initialState: BodyDeliveryState = {
    queueDepth: QueueCapacity,
    pendingFrame: true,
    waitingDirect: false,
    waitTicks: 0,
    consumerProgressCount: 0,
    deliveredDirectly: false,
    spilledToDisk: false,
    consumerClosed: false,
    closedWhileWaiting: false,
    lastTransition: BodyDeliveryInitialized,
  }

  var delivery: BodyDeliveryState

  action init = delivery' = initialState

  /// Refinement of the failed `try_send`: retain the decoded frame outside
  /// the queue while a finite grace gives an active consumer time to drain.
  action startBoundedDirectWait: bool = all {
    delivery.pendingFrame,
    not(delivery.waitingDirect),
    delivery.queueDepth == QueueCapacity,
    outcomeCount(delivery) == 0,
    delivery' = delivery
      .with("waitingDirect", true)
      .with("lastTransition", BoundedDirectWaitStarted),
  }

  /// Time advances only while capacity remains unavailable. Every such step
  /// consumes one unit of the finite grace budget.
  action tickDirectWait: bool = all {
    delivery.pendingFrame,
    delivery.waitingDirect,
    delivery.queueDepth == QueueCapacity,
    delivery.waitTicks < DirectWaitBudget,
    delivery' = delivery
      .with("waitTicks", delivery.waitTicks + 1)
      .with("lastTransition", DirectWaitTicked),
  }

  action consumerFreesCapacity: bool = all {
    delivery.pendingFrame,
    delivery.waitingDirect,
    delivery.queueDepth > 0,
    delivery' = delivery
      .with("queueDepth", delivery.queueDepth - 1)
      .with(
        "consumerProgressCount",
        delivery.consumerProgressCount + 1
      )
      .with("lastTransition", ConsumerFreedQueueCapacity),
  }

  action deliverWaitingFrameDirectly: bool = all {
    delivery.pendingFrame,
    delivery.waitingDirect,
    delivery.queueDepth < QueueCapacity,
    delivery.consumerProgressCount > 0,
    delivery' = delivery
      .with("queueDepth", delivery.queueDepth + 1)
      .with("pendingFrame", false)
      .with("waitingDirect", false)
      .with("deliveredDirectly", true)
      .with("lastTransition", WaitingFrameDeliveredDirectly),
  }

  /// The disk spool remains the bounded liveness escape hatch. It is not
  /// reachable before the entire direct-delivery grace has elapsed.
  action spillWaitingFrameAfterGrace: bool = all {
    delivery.pendingFrame,
    delivery.waitingDirect,
    delivery.queueDepth == QueueCapacity,
    delivery.waitTicks == DirectWaitBudget,
    delivery' = delivery
      .with("pendingFrame", false)
      .with("waitingDirect", false)
      .with("spilledToDisk", true)
      .with("lastTransition", WaitingFrameSpilledAfterGrace),
  }

  /// Closing the receiver wins over either direct delivery or spilling. The
  /// frame can be discarded because no application consumer remains.
  action closeBodyConsumer: bool = all {
    delivery.pendingFrame,
    outcomeCount(delivery) == 0,
    delivery' = delivery
      .with("pendingFrame", false)
      .with("waitingDirect", false)
      .with("consumerClosed", true)
      .with("closedWhileWaiting", delivery.waitingDirect)
      .with("lastTransition", BodyConsumerClosed),
  }

  action stutterTerminal: bool = all {
    outcomeCount(delivery) == 1,
    delivery' = delivery
      .with("lastTransition", BodyDeliveryTerminalStutter),
  }

  action step = any {
    startBoundedDirectWait,
    tickDirectWait,
    consumerFreesCapacity,
    deliverWaitingFrameDirectly,
    spillWaitingFrameAfterGrace,
    closeBodyConsumer,
    stutterTerminal,
  }
}
