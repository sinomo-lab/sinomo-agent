# Plans

This directory contains proposed or unfinished work plus selected completed and
historical records that preserve useful design reasoning. Plans are not the
source of truth for current behavior; use the
[architecture overview](../architecture.md), subsystem documents, code, and
tests for that.

| Plan | Status |
|---|---|
| [Network verification suite](network-verification-suite.md) | First local harness slice implemented; expanded fault scheduling, route evidence, replay, and performance comparisons remain planned. |
| [Network layer separation](network-layer-separation.md) | Active internal separation of the single `sinomo-network` crate into direct-link, connectivity, routed-stream, and stream-native object-RPC contracts. |
| [R0/R1 crate boundary refactor](r0-r1-crate-boundary-refactor.md) | Completed implementation record. |
| [Reactive cluster KV/Raft improvements](reactive-cluster-kv-raft-improvement-plan.md) | First implementation slice completed; retained P1-P3 follow-up work is classified in the plan. |
| [DFS policy and authority hardening](dfs-policy-and-authority-hardening.md) | Proposed follow-up work. |
| [Indexed-file memory implementation](indexed-file-memory-implementation.md) | Active; foundational contracts and the first local file-to-evidence slice are implemented. |
| [Node health, device monitoring, and DFS storage locations](device-health-and-dfs-storage-locations.md) | Proposed follow-up work. |
| [Agent session placement and workspace projection](agent-session-placement-and-workspaces.md) | Active implementation plan. |
| [Agent session activity and unified channels](agent-session-activity-and-unified-channels.md) | Active implementation plan; phases 1–5 implemented, follow-ups open. |
| [Formal verification roadmap](formal-verification-roadmap.md) | Active verification backlog. |
| [System architecture proposal](system-architecture-proposal.md) | Historical and superseded by the current architecture. |

Keep plan status and review dates in the plan itself. When a plan completes,
reduce durable decisions to the appropriate architecture document and retain
only history that still helps future maintainers.
