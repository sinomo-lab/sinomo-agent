/// Randomized smoke exploration for the timed network layers.
module network_timed_delivery_smoke {
  import network_timed_transport from "../../models/network/timed_transport"
  import network_timed_delivery_retry from "../../models/network/timed_delivery_retry"
  import network_timed_transport_safety from "../../checks/network/timed_transport_safety"
  import network_timed_delivery_retry_safety from "../../checks/network/timed_delivery_retry_safety"

  action init = all {
    network_timed_transport::init,
    network_timed_delivery_retry::init,
  }

  action step = any {
    all {
      network_timed_transport::step,
      network_timed_delivery_retry::stutter,
    },
    all {
      network_timed_transport::stutter,
      network_timed_delivery_retry::step,
    },
  }

  def safety: bool = all {
    network_timed_transport_safety::safety,
    network_timed_delivery_retry_safety::safety,
  }
}
