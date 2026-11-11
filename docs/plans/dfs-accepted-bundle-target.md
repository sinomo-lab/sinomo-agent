# DFS accepted-bundle target and refinement obligations

This is a bounded target model, not a proof of the Rust implementation, native
crash durability or full liveness. Abstract promise persistence, immutable
proposal identity and reservation authority are assumptions requiring concrete
refinement. Retain exact commands, model inputs, bounds, seeds, traces and outcomes
in execution artifacts; sample success is not exhaustive verification.

## Model scope

| Dimension | Bound or assumption |
|---|---|
| Metadata assignment | Three fixed members, A/B/C; quorum sets AB, AC, BC |
| Slots / values | One slot, two injective immutable bundle aliases |
| Reservations | Three ordered ballots; ballot number identifies its fixed proposer |
| Control authority | Independent reservation oracle, not content W or per-operation KV |
| Persistence | Promise/captured cut and acceptance are atomic durable abstract actions |
| Proposal identity | One immutable proposal per ballot, globally assumed |

The checked witness must be tied to the current outage, its promise/acceptance
and ACK-delivery events. Reconnect consumes that proof from the prestate.
Observed ballots require their delivered quorum and matching proposed bundle;
historical proof and injected minority observations cannot manufacture a witness.

## Causal assertions

The delayed-proof trace withholds an old quorum-completing ACK until after a
higher durable promise quorum and the first higher chosen-proof observation.
It then observes the old proof for the first time. An early ACK and repeated
delivery/observation do not manufacture this witness. A higher promise never
retroactively invalidates an already issued ACK.

Promise persistence and reply delivery are separate. Choice remains disabled
while the proposer is available but lacks the captured reply. A second trace
changes the acceptor's current accepted value before delivering its older promise
reply, requiring the original captured cut rather than a reread of current state.
Availability changes preserve state by construction; these are not WAL restart
or actual lost-network-reply tests.

The third-ballot trace recovers on AC after BC chose bundle 2. A reports private
lower-ballot bundle 1, C reports higher-ballot bundle 2; only C intersects the
chosen quorum. The trace must adopt bundle 2, and attempting bundle 1 is disabled.
Requiring two identical cut reports would lose this valid intersection evidence.

Private A-only acceptance precedes the outage and any higher reservation. BC
can choose a different value when neither reports that unchosen acceptance.
After reconnect, A must acquire its higher promise before the stale rejection
marker is enabled. The rejection preserves the complete modeled state except
its explicit marker flags. Ghost acceptance history proves event order, not
surviving original-body custody.

Negative controls separate reservation from fencing, minority from quorum,
promise persistence from reply visibility, and current outage from historical
proof. A disabled Quint action has no next state: those checks terminate at
`fail()`. Actual rejection transitions have separate full-state comparisons.

## Reproduce the small checks

These commands use the already installed tool and bounded TypeScript evaluator.
They neither install a backend nor invoke an invariant solver:

```bash
quint typecheck specs/quint/scenarios/dfs/availability_accepted_bundle_witnesses.qnt
timeout --kill-after=2s 20s quint test \
  specs/quint/scenarios/dfs/availability_accepted_bundle_witnesses.qnt \
  --backend=typescript --max-samples=1 --seed=0x51a0f04d20260710 --match='Test$'
timeout --kill-after=2s 20s quint run \
  specs/quint/checks/dfs/availability_accepted_bundle_target.qnt \
  --backend=typescript --max-samples=16 --max-steps=32 \
  --seed=0x51a0f04d20260710 --invariant=safety
```

Any future execution must retain bounded worktree-local ownership, exact inputs
and actual outcomes. These commands are reproducibility instructions, not evidence
of a new run. This draft authorizes no production action or extra test scope.

## Rust refinement still required

| Boundary | Missing evidence or integration |
|---|---|
| V7 admission and exact frame identity | Certified old prefix/custody cut; deterministic prospective prepare/terminal bytes and ancestry; original terminal identity cannot be inferred from a prepare. |
| Durable promise versus old acceptance | One actual writer gate must serialize fsynced promises, accepted history and old ACK issue; restart, snapshot, compaction and downgrade must retain fences. Current ACK floors are not promises. |
| Wire/proposer uniqueness | Authenticated assignment/slot/ballot/body binding, conflicting same-ballot rejection and durable proposer selection; global immutable proposal is currently assumed. |
| Decision and visibility | Majority-durable exact decision evidence before success; ghost proof observation is neither canonical publication nor serving activation. Latest-head reads need freshness evidence. |
| Ordered history | Multiple slots, predecessor conflicts, holes, bounded prefix recovery and pipelining are not modeled. |
| Original effects and attachments | Complete original command identities/bodies, delivery uncertainty and completion/custody before ambiguous dispatch; detached effects need separate coverage. Repair needs none: the successor derives it from the replicated tables and receipt-slot generations fence late replies. No KV metadata/effect ledger. |
| Serving epoch and physical generation | Checked wrapper rebind around retained physical stores/indexes, immediate old-effect fencing and stronger real-replacement fences; runtime integration and native verification remain pending. |
| Content policy and placement | Content W/N, valid holders, admitted target inventory, pins/residency and managed supersession are outside this metadata model. The unsafe admission experiment remains withdrawn. |
| Process/transport/disk and quietness | Actual abrupt stop, live partition, lost replies, original custody, reconnect/no failback, bounded transfer and Sinomo-only CPU/disk/network/writeback evidence remain outstanding. |

The [original-effect custody plan](dfs-original-effect-custody.md) defines
additional command, payload and completion obligations outside this one-slot
model. Native writer/fsync, receiver authority, process-loss and partition
controls remain separate from abstract traces.
