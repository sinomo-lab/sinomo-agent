# DFS accepted-bundle target: bounded evidence and refinement gaps

This is a target-hardening layer for one metadata slot, not the current Rust
publication protocol. Full assigned-participant proof remains unchanged.
The model assumes a certified cooperative V7 admission cut and injective aliases
for complete immutable request/prepare/terminal-frame bytes. It does not recover
an unknown legacy A-only commit timestamp or manufacture missing original bytes.

Development status on 2026-10-24: this draft remains separate from the native
custody implementation. R8 Rust source `3a445694` has a successful fresh check,
own-source no-run build and eight operation-tooling controls; its 35 Rust controls
remain unrun at this checkpoint. Historical local DATA `3eb87ca5` passed twenty
controls; N3 R7 executed 23 passes and seven setup failures. The original R67
positive successor assertion remains failed. None of these results implements
this model's durable promises, metadata quorum or abrupt-loss protocol.

DFS development now uses independent worktree-local serialization and bounded
verification. No peer build lock, capacity reservation, source-release request or
handback approval governs this lane. Local commits and reviewed actual merges of
isolated DFS subtask branches are its integration boundaries. The Docker debug
cluster belongs to `dev/debug`, and the real rollout belongs to `main`; this draft
authorizes neither operation. See the [availability plan](dfs-abrupt-loss-availability.md)
for the current source-qualified summary and remaining gates.

The contract, transition model, checks and deterministic scenarios are respectively:

- `specs/quint/contracts/dfs/availability_accepted_bundle.qnt`
- `specs/quint/models/dfs/availability_accepted_bundle.qnt`
- `specs/quint/checks/dfs/availability_accepted_bundle_target.qnt`
- `specs/quint/scenarios/dfs/availability_accepted_bundle_witnesses.qnt`

This draft is deliberately not registered as current conformance in
`scripts/formal/check-quint.sh`. The existing `dfs-whole-shard` case continues to
represent the current full-participant contract. The draft filename deliberately
avoids the registered `*_safety.qnt` inventory; it remains discoverable by broad
Quint typechecking and the direct bounded commands below. Earlier R19/R23 source
snapshots retain the old filename and exact hashes. This path-only successor
changes the scenario import, not its transitions or assertions.

## Scope and actual results

| Dimension | Bound or assumption |
|---|---|
| Metadata assignment | Three fixed members, A/B/C; quorum sets AB, AC, BC |
| Slots / values | One slot, two injective immutable bundle aliases |
| Reservations | Three ordered ballots; ballot number identifies its fixed proposer |
| Control authority | Independent reservation oracle, not content W or per-operation KV |
| Persistence | Promise/captured cut and acceptance are atomic durable abstract actions |
| Proposal identity | One immutable proposal per ballot, globally assumed |
| Deterministic checks | 18 witness/marker/sensitivity traces plus 13 terminal disabled-action checks |
| Longest emitted deterministic trace | 28 states, including initialization |
| Sampled smoke | 16 traces, at most 32 transitions each; one fixed seed |
| Tooling | Quint 0.32.0, TypeScript backend, Node v26.12.0; no Rust backend or solver |

On 2026-10-23, all 31 deterministic tests executed and passed. The sampled smoke
found no violation within its bounds. These are finite execution results, not an
exhaustive invariant proof, liveness argument, Rust conformance or native crash
acceptance. Samples do not establish deep witness reachability; the prescribed
deterministic traces supply that evidence for their exact action sequences.

The byte-exact earlier target model/checks failed the same three final regressions:
historical proof reused for a new outage, historical proof reused for a later
reconnect, and missing quorum provenance for an injected minority observation.
The first two expose false witness claims through permitted model transitions.
The third is a checker-sensitivity test using a deliberately broken transition
excluded from `model.step`; it is not a reachable protocol counterexample.

Current checks bind survivor proof to the current outage's B/C promise, acceptance
and ACK-delivery events. Reconnect uses that proof in the prestate. Observed
ballots additionally require their delivered quorum and matching proposed bundle.

Exact commands, source snapshots, outputs and ITF hashes remain in
`data/operations/dfs-availability-20261023/`: `formal-witness-fixed-r19`,
`formal-witness-pre-fix-r20`, `formal-safety-smoke-r23` and
`FORMAL-TRACE-CATALOG.json`. Earlier static/runner failures are retained separately.
The largest-child memory figures there are not aggregate or compiler-only peaks.

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
| Original effects and attachments | Complete original command identities/bodies, delivery uncertainty and completion/custody before ambiguous dispatch; detached effects need separate coverage. No KV metadata/effect ledger. |
| Serving epoch and physical generation | Checked wrapper rebind around retained physical stores/indexes, immediate old-effect fencing and stronger real-replacement fences; runtime integration and native verification remain pending. |
| Content policy and placement | Content W/N, valid holders, admitted target inventory, pins/residency and managed supersession are outside this metadata model. The unsafe admission experiment remains withdrawn. |
| Process/transport/disk and quietness | Actual abrupt stop, live partition, lost replies, original custody, reconnect/no failback, bounded transfer and Sinomo-only CPU/disk/network/writeback evidence remain outstanding. |

The historical native rejection/retry control is separate evidence. Its corrected
Main/Core setup passed one case while preserving original prepared identity; all
metadata participants remained alive. It establishes no degraded-write or abrupt
metadata takeover behavior. The full availability protocol review remains open.

The separate [original-effect custody draft](dfs-original-effect-custody.md)
maps the baseline durable tuples versus process-retained heads/readers and specifies
proposed pre-dispatch, terminal-compaction and activation coverage obligations.
Its historical bounded custody and companion negative traces have their own
source-qualified results, including intended mutant failures. They are not covered
by these one-slot tests and are not registered Rust conformance. None substitutes
for native writer/fsync, receiver authority, process-loss or partition evidence.
