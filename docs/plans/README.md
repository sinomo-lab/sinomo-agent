# Plans

This directory contains proposed or unfinished work plus selected completed and
historical records that preserve useful design reasoning. Plans are not the
source of truth for current behavior; use the
[architecture overview](../architecture.md), subsystem documents, code, and
tests for that.

| Plan | Status |
|---|---|
| [Network verification suite](network-verification-suite.md) | First local harness slice implemented; expanded fault scheduling, route evidence, replay, and performance comparisons remain planned. |
| [R0/R1 crate boundary refactor](r0-r1-crate-boundary-refactor.md) | Completed implementation record. |
| [Reactive cluster KV/Raft improvements](reactive-cluster-kv-raft-improvement-plan.md) | First slice completed; bounded group commit landed; P2.9 batching and P1.6 election follow-ups are partial; P2.11 resumable snapshots are planned. |
| [DFS policy and authority hardening](dfs-policy-and-authority-hardening.md) | Partially implemented: the quorum-loss authority contract is done; reserved object modes, structured policy, and remaining placement inputs are proposed. |
| [DFS abrupt-node-loss availability](dfs-abrupt-loss-availability.md) | Proposed: shard metadata write sets, automatic takeover of an offline primary, automatic W maintenance and content-target substitution. |
| [DFS single-round write commit](dfs-single-round-write-commit.md) | Proposed: content and the prepared change reach their participants in one parallel exchange, success is reported before the commit record is durable, and recovery confirms named content holders. |
| [Indexed-file memory implementation](indexed-file-memory-implementation.md) | Active; Phase 0 and the local Markdown/text first slice are implemented; structured formats, scale hardening, and Phases 2–6 remain. |
| [Node health, device monitoring, and DFS storage locations](device-health-and-dfs-storage-locations.md) | Partially implemented: DFS storage-health sampling, aggregate advertisement, and host resource evidence landed; general node health, additional content locations, storage management, and app surfaces remain proposed. |
| [Agent session placement and workspace projection](agent-session-placement-and-workspaces.md) | Active; lifecycle client, VFS file I/O, and cluster chat hosting are done; remote placement, migration, sandbox CAS, and grant audit remain. |
| [Agent session activity and unified channels](agent-session-activity-and-unified-channels.md) | Active; phases 1–5 are implemented and recorded in the agent-system architecture; concurrent participants, participant live control, dotted events, and detached subagents remain. |
| [Agent usage tracking](agent-usage-tracking.md) | Proposed: durable usage ledger, cost estimation, display-only budgets, and the Cluster Usage view. |
| [Formal verification roadmap](formal-verification-roadmap.md) | Active backlog; network slices retired with the legacy transport; post-V7 Rust test restoration and native whole-shard conformance are open. |
| [System architecture proposal](system-architecture-proposal.md) | Historical and superseded by the current architecture. |

Keep plan status and review dates in the plan itself. When a plan completes,
reduce durable decisions to the appropriate architecture document and retain
only history that still helps future maintainers.
