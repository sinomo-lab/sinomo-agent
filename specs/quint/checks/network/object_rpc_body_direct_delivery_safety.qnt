/// Safety and bounded reachability for object-RPC incoming-body delivery.
module network_object_rpc_body_direct_delivery_safety {
  import common.* from "../../lib/common"
  import network_object_rpc_body_direct_delivery.* from "../../models/network/object_rpc_body_direct_delivery"

  def queueAndWaitBudgetStayBounded: bool = all {
    delivery.queueDepth >= 0,
    delivery.queueDepth <= QueueCapacity,
    delivery.waitTicks >= 0,
    delivery.waitTicks <= DirectWaitBudget,
  }

  def frameHasExactlyOneLifecycleLocation: bool = all {
    outcomeCount(delivery) <= 1,
    delivery.pendingFrame == (outcomeCount(delivery) == 0),
    delivery.waitingDirect implies delivery.pendingFrame,
  }

  def waitingAlwaysHasFiniteRemainingWork: bool =
    delivery.waitingDirect implies all {
      delivery.waitTicks <= DirectWaitBudget,
      any {
        delivery.queueDepth < QueueCapacity,
        delivery.waitTicks < DirectWaitBudget,
        delivery.waitTicks == DirectWaitBudget,
      },
    }

  def directDeliveryRequiresObservedConsumerProgress: bool =
    delivery.deliveredDirectly implies all {
      delivery.consumerProgressCount > 0,
      delivery.queueDepth <= QueueCapacity,
      not(delivery.spilledToDisk),
      not(delivery.consumerClosed),
    }

  def spillRequiresExpiredGraceWithoutCapacity: bool =
    delivery.spilledToDisk implies all {
      delivery.waitTicks == DirectWaitBudget,
      delivery.queueDepth == QueueCapacity,
      not(delivery.deliveredDirectly),
      not(delivery.consumerClosed),
    }

  def consumerCloseNeverCreatesSpool: bool =
    delivery.consumerClosed implies all {
      not(delivery.deliveredDirectly),
      not(delivery.spilledToDisk),
    }

  def terminalOutcomesDoNotRetainWaiter: bool =
    outcomeCount(delivery) == 1 implies all {
      not(delivery.pendingFrame),
      not(delivery.waitingDirect),
    }

  def safety: bool = all {
    queueAndWaitBudgetStayBounded,
    frameHasExactlyOneLifecycleLocation,
    waitingAlwaysHasFiniteRemainingWork,
    directDeliveryRequiresObservedConsumerProgress,
    spillRequiresExpiredGraceWithoutCapacity,
    consumerCloseNeverCreatesSpool,
    terminalOutcomesDoNotRetainWaiter,
  }

  def witnessFullQueueEntersBoundedWait: bool = all {
    delivery.lastTransition == BoundedDirectWaitStarted,
    delivery.waitingDirect,
    delivery.queueDepth == QueueCapacity,
    delivery.waitTicks == 0,
  }

  def witnessWaitTickConsumesFiniteBudget: bool = all {
    delivery.lastTransition == DirectWaitTicked,
    delivery.waitingDirect,
    delivery.waitTicks > 0,
    delivery.waitTicks <= DirectWaitBudget,
  }

  def witnessConsumerProgressDeliversWithoutSpill: bool = all {
    delivery.lastTransition == WaitingFrameDeliveredDirectly,
    delivery.deliveredDirectly,
    delivery.consumerProgressCount > 0,
    not(delivery.spilledToDisk),
  }

  def witnessGraceExpiryFallsBackToSpool: bool = all {
    delivery.lastTransition == WaitingFrameSpilledAfterGrace,
    delivery.spilledToDisk,
    delivery.waitTicks == DirectWaitBudget,
    not(delivery.deliveredDirectly),
  }

  def witnessConsumerCloseEndsWaitWithoutSpill: bool = all {
    delivery.lastTransition == BodyConsumerClosed,
    delivery.consumerClosed,
    delivery.closedWhileWaiting,
    not(delivery.spilledToDisk),
  }
}
