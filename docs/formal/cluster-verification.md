# Cluster Formal Verification

This directory starts the executable specification track for Sinomo cluster
infrastructure. The first model is intentionally small: a three-node
Raft-backed KV skeleton with one abstract KV cell and a bounded three-entry log.
The goal is to verify the control-plane safety shape before adding membership,
leases, watches, snapshots, DFS metadata, and rollout.

## Current Specs

- `specs/quint/common.qnt` defines the shared finite cluster model: three nodes,
  voter quorum helpers, online voter selection, and a small `Option` type.
- `specs/quint/raft_kv.qnt` models the first Raft/KV slice: leader election,
  node online/offline transitions, catch-up, leader proposals, quorum commit,
  committed-prefix agreement, and KV-state replay from the committed log.

This maps to the repository boundary documented in
`docs/clustering-architecture.md`: `sinomo-kv` owns KV/Raft semantics and stays
transport-free, while `sinomo-cluster` moves consensus traffic over the network.

## What The First Model Checks

The `raft_kv.safety` invariant combines these properties:

- a node that claims a committed index agrees with the canonical committed log
  prefix;
- every globally committed slot is filled;
- at most one leader is recorded for each bounded term;
- the visible KV cell equals replay of the committed log prefix;
- the global commit index and next log index stay inside the model bound.

The model already caught one useful bug during drafting: an offline node was
allowed to come online without catch-up, then accept a later committed entry
without the earlier prefix. The Rust in-process cluster catches up a node when
`set_node_online(..., true)` is called, so the spec now models that behavior.

## Running

Fast local check:

```bash
bash scripts/formal/check-quint.sh
```

Equivalent commands:

```bash
quint typecheck specs/quint/raft_kv.qnt
quint run specs/quint/raft_kv.qnt --main=raft_kv --invariant=safety --max-steps=20 --max-samples=50 --verbosity=1
quint verify specs/quint/raft_kv.qnt --main=raft_kv --invariant=safety --max-steps=5 --verbosity=1
```

The default symbolic bound is deliberately modest. On this checkout,
`--max-steps=5` took about 40 seconds, while `--max-steps=8` was already too
slow for a normal development loop. Use a larger bound for local audit runs, not
as a routine CI gate:

```bash
QUINT_VERIFY_MAX_STEPS=8 bash scripts/formal/check-quint.sh
```

## Current Abstractions

This slice does not yet model:

- per-node durable hard-state fields such as `current_term`, `voted_for`, and
  `leader_id`;
- rejected append entries, partial message loss, or follower log truncation;
- WAL and snapshot restore;
- watches, leases, compare-and-swap transactions, and revision histories;
- dynamic membership, invite/join/leave, or learner promotion;
- DFS metadata-shard authority.

Those should be added as layered modules rather than folded into one large
state machine. The next useful step is to split out a `membership` module that
owns voters, learners, and join/leave authority, then connect DFS metadata
primary epochs to the abstract KV guarantees.
