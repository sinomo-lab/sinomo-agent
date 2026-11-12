# DFS abrupt-node-loss and reconnection availability

Status: proposed. The current implementation does not meet these requirements;
[Current behavior](#current-behavior) lists the differences.

The required behavior is one safe metadata history through abrupt node loss,
with content W kept separate from desired N and metadata authority. When a
metadata-shard primary goes offline, a surviving member of the shard's write set
that can commit to cluster KV takes over automatically, without any state, freeze
or reply from the lost primary. Reliability comes from redundancy: everything a
successor needs is already replicated in the shard tables, on content holders or
with the caller. This plan specifies the target protocol, its obligations and
validation boundaries; it does not certify an implementation or authorize
deployment.

## Requirements

1. **One history.** Each shard has exactly one ordered metadata history and one
   serving primary per epoch. A timeout or connectivity change requests a
   takeover attempt; it never grants one.
2. **Write set.** A metadata change is acknowledged only after every member of
   the shard's current write set durably holds it. A bare majority of a larger
   set never replaces the write set. Cluster KV orders every change to the write
   set and to the primary through its existing guarded transactions. Journals,
   intents, receipts and effect evidence stay in DFS; no per-operation KV permit
   or DFS ledger enters KV.
3. **W floor.** The write set never shrinks below the filesystem's content W. A
   shard whose live write set is below the floor keeps writes pending instead of
   weakening durability, so metadata is never less durable than the content it
   commits.
4. **Automatic takeover.** When the primary is offline, a write-set member that
   can commit to cluster KV takes over automatically.
5. **Keeping W.** Members leave the write set when the network reports them
   unreachable, rejoin once caught up, and are replaced by recruited members
   when too few remain. All of this is automatic.
6. **Metadata replicas.** Each shard keeps N metadata copies, the primary's
   included, N being the filesystem's content N, on non-mobile metadata-capable
   members. They need not be cluster-KV voters. Mobile members are used only
   when too few non-mobile members exist to reach the W floor.
7. **Desired N survives unavailability.** A W=2, N=3 eventual write may commit on
   B and C with A unavailable once metadata authority is valid; it keeps N=3 and
   the missing copy as repair work. When an open write's chosen targets cannot
   reach W, reachable admitted targets replace the unreachable ones and repair
   restores N later. Required nodes, pins, residency and confirmed-replication
   requirements remain hard constraints and are never substituted.
8. **Reads.** An existing exact version needs one reachable authorized valid
   holder; it does not need W reachable copies. Missing A-only W=1 bytes affect
   that content, not unrelated objects or metadata activation. A primary that has
   been replaced but has not yet learned it may answer reads from its last state;
   it can no longer commit.
9. **Redundancy, not custody.** A successor derives all remaining work from the
   replicated shard tables: repair, staging, commit promotion, prepared changes,
   namespace requests and jobs. Receivers apply repeated effects idempotently,
   and the caller owns delivery of its own result. Recovery needs no
   survivor-durable copy of the lost primary's original requests, outbox,
   delivery uncertainty or exact journal bytes.
10. **Reactive.** Takeover, write-set changes and recruitment run on events:
    delivery failures, network reachability changes, membership and
    configuration changes, and catch-up completion. There are no heartbeats or
    scans. Slowness is never evidence of loss: only the network layer's verdict
    that a peer has no path counts, and DFS adds no deadline of its own.
11. **Serving epoch and physical store are distinct.** Takeover reuses the
    successor's existing store and indexes; the new epoch fences old writes
    without replacing the store.
12. **Rejoin.** Offline members keep their membership and recorded copies. A
    returning member compares exact prefixes, transfers only the missing bounded
    history and content, and does not become primary again automatically.

## Current behavior

The implementation differs from these requirements:

- Every metadata commit needs a durable acknowledgement from the primary and
  every configured replica (`authorize_metadata_shard_operation`). A missing
  member makes the commit fail and leaves the change prepared.
- After setup, every metadata-capable service node replicates every shard, so
  one offline node of any kind stops metadata writes on all shards.
- A primary changes only through the cooperative membership handoff, whose
  freeze, fence and prefix steps run on the retiring primary. With that primary
  offline the handoff stays pending and nothing cancels it. The recovery-ballot
  and recovery-fence code is test-only.
- An epoch change also advances the store generation, which makes the metadata
  service reopen.
- Members keep no durable record of the highest epoch they have promised, and
  acknowledgement floors are not scoped to an epoch.
- The network layer announces route changes with a bare generation counter that
  does not name the peer. Failed deliveries leave short-lived evidence that
  nothing can subscribe to; otherwise unreachability waits for the periodic
  probe.
- A caller whose primary is unreachable fails at the first network error unless
  the shard record already names a new primary.
- Namespace-job resume selects jobs on every locally replicated shard rather than
  only on shards this node is primary for. Cross-shard source-adoption sessions
  exist only in memory.

These existing properties support the design:

- Before a caller sees success, the complete prepared change is durable on every
  member; only the commit record starts on the primary alone. Every member
  therefore holds everything the primary acknowledged.
- Prepared-change recovery at a newer epoch (`recover_one_prepared_operation`)
  republishes the sealed body with its read set, effects and caller identity.
- Repair work is derived from the shard tables, and receipt-slot generations
  fence late replies.
- Hydration on the current primary rebuilds staging for open intents and commit
  promotions for committed intents.
- Namespace requests, request receipts, jobs and advisory locks are rows in the
  replicated tables.

## Design

### Shard record

The cluster-KV shard record keeps its primary, replicas and epoch, and gains:

| Field | Meaning |
|---|---|
| Write set | The members a commit must reach: the primary plus the replicas currently in sync. Replicas outside it keep their copies and catch up. |
| Generation | Advances on every write-set change. The epoch still advances only when the primary changes. |
| Epoch start | The journal position and prefix proof at which the current primary's epoch began. |

The W floor and N come from the filesystem's replication policy; the record does
not copy them.

### Invariant

A member joins the write set only after it holds everything the primary has
written, and every commit is acknowledged by the whole write set. Every
write-set member therefore holds every acknowledged change, which is why
write-set membership is the eligibility rule for takeover: any member can
continue the history without the primary.

### Commit

The primary acknowledges a change only after every member of the write set at
its current generation has durably stored the complete prepared body. The commit
record may follow afterwards. A caller that does not receive success treats the
outcome as unknown and resolves it by retrying with the same operation id. DFS
never reports a definite failure for a change that a successor could still
complete.

### Detecting loss

Any of these starts an evaluation:

- a member or caller fails to deliver a message to the primary;
- the network layer reports a change in the primary's reachability;
- cluster membership marks the primary offline or removed.

Only the network layer's verdict that the primary has no path counts: a connect
failure, a disconnect or its own no-progress limit. A slow reply never counts, so
a primary on a high-latency mobile link that still answers keeps its role. After
a failed delivery the network layer verifies that peer at once instead of
waiting for its next probe, and it publishes per-peer reachability changes.
These belong to `sinomo-network`; DFS consumes them and does not duplicate
connectivity logic. An idle shard is taken over when someone next needs it or
when the network layer's own probing reports the primary unreachable.

### Takeover

A candidate is a write-set member that can commit to cluster KV.

1. **Corroborate.** The candidate asks the other reachable write-set members, in
   one round, whether they still have a path to the primary and how far their
   journal goes. Any member that still reaches the primary vetoes the attempt, so
   one node's poor link cannot depose a healthy primary. The remaining members
   are ranked by journal position, then non-mobile before mobile, then node id. A
   candidate that does not rank first hands the attempt to the member that does.
   Unreachable members are left out, so a two-member write set needs no
   corroboration.
2. **Confirm quorum.** The candidate reads the shard record linearizably. If it
   cannot, it is not in quorum and stops.
3. **Promise.** The candidate durably records that it accepts nothing more from
   the old primary at the current epoch, serialized with its acknowledgement
   path. From then on the old primary cannot complete a commit, because each one
   needs this member.
4. **Swap.** One guarded KV transaction: if the record still matches what the
   candidate read, the candidate becomes primary, the epoch advances, the old
   primary leaves the write set and epoch start becomes the candidate's journal
   position. If another change won, the candidate follows it. An unknown outcome
   is reconciled from the record before the candidate acts. The candidate
   releases its promise only when its own takeover did not happen and the old
   primary is still primary.
5. **Activate.** The candidate rebinds its existing store to the new epoch and
   republishes every prepared change it holds through prepared-change recovery,
   so changes the old primary may already have committed complete with the same
   effects and caller identity. A recovered change gets a new journal record; the
   old primary's own commit record, if it exists, is discarded when that node
   rejoins.
6. **Align followers.** A follower whose prefix matches at epoch start drops any
   records past it. Those records were never acknowledged, because the candidate
   lacks them. A follower whose prefix differs installs a snapshot. Acknowledgement
   floors from the old epoch block neither.
7. **Resume work** from the tables, as listed below, then remove unreachable
   members and recruit.

Every write-set and primary change is one guarded transaction on the same record,
so these changes are totally ordered. The winning candidate was in the write set,
so it holds every acknowledged change. It stopped accepting the old primary's
records before the swap, so the old primary cannot complete a commit afterwards.
Nothing from the old primary is needed.

### Keeping W

- **Shrink.** When the network reports that a write-set member has no path, the
  primary removes it with a guarded transaction, but never below the W floor. A
  dead member that would take the set below the floor stays in it, and writes
  wait until it returns or a recruit replaces it. Keeping it there also keeps it
  eligible to take over.
- **Re-add.** A returning member keeps its copy and catches up incrementally from
  its matching prefix. Once it has acknowledged the primary's current journal
  position, the primary adds it back and requires its acknowledgement from the
  moment it submits that change.
- **Recruit.** When the write set has fewer than N members and no out-of-sync
  replica is reachable, the primary adds an eligible metadata-capable member to
  the replicas, preferring non-mobile members. The recruit catches up by
  snapshot and joins the write set; only then is a dead member at the floor
  removed.
- **Retire a substitute.** When a shard has more than N members and the
  original member is back in the write set, the primary removes the most recent
  recruit, which deletes its copy as any node outside a shard's placement does.

These steps run on write-set changes, reachability changes, membership and
configuration changes, and catch-up completion.

### Content W for open writes

When an open write's reachable chosen targets and existing receipts cannot reach
W, the primary replaces the unreachable targets with reachable members of the
admitted content-target inventory described in the
[content inventory plan](dfs-content-admission-inventory.md), using the normal
placement ranking and constraints. The replacement is recorded in the intent, so
a successor sees it. The write commits at W, and repair restores N against the
updated desired placement. A target is replaced only on the network layer's
no-path verdict, never on a single failed endpoint probe.

### Old primary returns

The old primary learns of its replacement from the new shard record, from a
rejection by a write-set member or from a failed commit, and then redirects its
callers. It follows the new primary from epoch start, dropping or replacing its
unacknowledged tail, and rejoins the write set once caught up. It never becomes
primary again automatically. Its late messages are harmless: anything that needs
a metadata commit fails, and receivers apply repeats idempotently. Until it
learns, it may answer reads from its last state.

### Remaining work after takeover

| Work | Successor derives it from | Why repeating it is safe |
|---|---|---|
| Repair toward N | The derived repair index | Receipt-slot generations fence earlier attempts. |
| Staging for open writes | Intent and receipt rows; bytes from the writer or receipt holders | Receivers store by object, version and hash; receipts publish through slot generations. |
| Prepared changes | The prepared bodies every write-set member holds | Replay revalidates the sealed body against the same history. |
| Commit promotions | Committed intents and their receipts | The receiver treats an already-durable promotion as settled. |
| Caller results | Nothing; the caller owns them | The caller retries with its operation id, and replicated intent and namespace-request receipts answer it. |
| Namespace requests and jobs | Request and job rows | Job receipts; recovery moves only forward. |
| Advisory locks | Lock rows | The new epoch invalidates old grants. |

A content version held only by the lost node, because it is W=1 or its bytes were
never staged elsewhere, stays unavailable until that node returns; only those
objects are affected. A writer whose bytes never left its own node re-sends them
on retry.

### Compatibility

A shard switches to the write-set protocol only after every member of its replica
set advertises support. Until then it keeps the all-member rule and has no
automatic takeover; old and new nodes do not share a write set. On the switch the
primary reduces the shard to N chosen members, and removed members delete their
copies. The cooperative handoff remains the planned-transfer path for a
live primary.

## Model

Before shard authority changes, model the protocol in Quint with three or four
members, a few journal entries and these actions: prepare, acknowledge, commit,
shrink, add, recruit, promise, takeover swap, crash, partition, recovery and
rejoin. Cluster KV is a linearizable compare-and-swap register. Required
invariants:

- committed histories never diverge;
- every write-set member holds every committed entry;
- after a takeover swap, the old primary commits nothing;
- no acknowledged change is lost across any sequence of takeovers;
- a shrink never takes the write set below the W floor.

Liveness, storage and transport stay outside the model and are covered by native
tests.

## Implementation order

1. The model above.
2. Network: per-peer reachability change events and immediate verification
   after a failed delivery.
3. Shard record and member state: write set, generation and epoch start;
   guarded transactions for shrink, add, recruit and takeover; capability gate
   and replica-set reduction; a durable promised-epoch record that survives
   restart and snapshot install; epoch-scoped acknowledgement floors.
4. Commit: require the write set at the current generation instead of every
   configured member; report a missing acknowledgement as an unknown outcome;
   bound the wait by the caller's deadline only.
5. Keeping W: shrink, re-add, recruitment and substitute retirement; N metadata
   replicas from non-mobile members.
6. Takeover: corroboration, promise, swap, store rebind without reopening,
   prepared-change recovery and follower alignment.
7. Callers and successor work: callers wait within their own deadline for the
   shard record to change and retry at the new primary with the same operation
   id; namespace-job resume only on shards this node is primary for; cross-shard
   source adoption rebuilt from job rows.
8. Content-target substitution for open writes.

## Acceptance matrix

| Scenario | Required evidence |
|---|---|
| Abrupt A stop; exact committed bytes only on A and C, B lacks them | Verify fixture and on-disk bindings before the stop; actually stop A abruptly; B or C takes over without A; read the exact C version; a new B/C W=2, N=3 write commits; A's membership and copy facts, desired N and the durable deficit remain. |
| Write-set member unavailable, primary alive | The primary removes it, writes continue at or above the W floor, and the member rejoins after catching up. |
| Live old primary partitioned | Attempts on both sides; quorum-side takeover; the old side cannot commit, including after restart and with delayed messages; its stale reads end when it learns of the replacement. |
| Slow but reachable primary | Replies delayed by several hundred milliseconds or more never start a takeover or a shrink. |
| Concurrent candidates | Exactly one swap wins, the others follow it, and no promise is left blocking either the winner or a primary that kept its role. |
| Crash or lost reply at prepare, commit and transfer | Cut each durable boundary; a retry with the same operation id returns the committed result or completes the change once; nothing is applied twice. |
| Follower with a longer unacknowledged tail | It drops the tail past epoch start or installs a snapshot; acknowledged changes are unaffected. |
| Write set at the W floor loses another member | Writes wait; a recruit catches up and joins; writes resume; the dead member is removed only after that. |
| Open W=2, N=3 write with two chosen targets offline | Reachable admitted substitutes receive the bytes, the write commits and repair restores N. Required and pinned targets are never substituted. |
| Repeated reconnect | Matching prefixes and content are reused; bounded missing transfer; membership and copies retained; no churn, wholesale rewrite or failback. |
| W=1 bytes only on A, A unavailable | Affected content is explicitly unavailable; unrelated objects stay readable and writable under valid authority. |
| Mobile members | A mobile member is not chosen as a metadata replica while enough non-mobile members exist. |

Native and disposable process/transport/disk tests must cut the real ownership,
persistence and reply boundaries. Preserve failed roots, exact source/artifact
identities, original requests and unknown outcomes; a later pass never rewrites
an earlier result. Code synchronization and local fixtures do not establish
runtime acceptance or activation authority.

Use the standard performance acceptance, per-node I/O, node-window and coverage
tables. Measure timings directly. Stabilized quietness requires zero physical
writes, including during maintenance, across Sinomo main/shadow and attributable
helpers, delegated work, final I/O and writeback. Time-only freshness stays in
memory; persistence requires semantic change. Incomplete ownership or network
coverage remains unqualified, and unrelated whole-host activity is diagnostic.
