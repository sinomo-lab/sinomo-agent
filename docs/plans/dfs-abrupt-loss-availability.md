# DFS abrupt-node-loss and reconnection availability

Status: proposed. The protocol model exists; the current implementation does not
meet these requirements, and [Current behavior](#current-behavior) lists the
differences.

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
3. **W floor.** The write set never shrinks below the shard's W floor: the
   highest content W among the rules that can apply to the shard's paths. A
   commit whose own content W, from its object or write policy, is higher needs
   a write set at least that large. A shard whose live write set is below what a
   commit needs keeps that write pending instead of weakening durability, so
   metadata is never less durable than the content it commits.
4. **Automatic takeover.** When the primary is offline, a write-set member that
   can commit to cluster KV takes over automatically.
5. **Keeping W.** Members leave the write set when the network reports them
   unreachable, rejoin once caught up, and are replaced by recruited members
   when too few remain. All of this is automatic.
6. **Metadata replicas.** Each shard keeps N metadata copies, the primary's
   included, N being the highest content N among the rules that can apply to
   the shard's paths, on non-mobile metadata-capable members. They need not be
   cluster-KV voters. Mobile members are used only when too few non-mobile
   members exist to reach the W floor. W is never above N: a policy or rule
   that states a W above its N has its N raised to W, and a shard's N is raised
   to the content W of any change it commits.
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

- Every metadata commit on a shard that has not switched to the write-set
  protocol, which is every shard while no node advertises it, needs a durable
  acknowledgement from the primary and every configured replica
  (`authorize_metadata_shard_operation`). A missing member leaves the change
  prepared and the caller receives an unknown outcome, not a failure.
- A shard that has not switched keeps every metadata-capable service node as a
  replica, as set up, so one offline node of any kind stops metadata writes on
  every such shard. Only a switched shard is reduced to N members and keeps its
  write set.
- On a shard that has not switched, a primary changes only through the
  cooperative membership handoff, whose freeze, fence and prefix steps run on
  the retiring primary. With that primary offline the handoff stays pending and
  nothing cancels it.
- On a shard that has not switched, an epoch change also advances the store
  generation, which makes the metadata service reopen.
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

The shard record is two cluster-KV keys: the shard configuration, which keeps
its primary, replicas and epoch, and a write-set key beside it. Every write-set
and primary change compares the exact bytes of both keys in one transaction, so
together they behave as one record. The write-set key holds:

| Field | Meaning |
|---|---|
| Write set | The members a commit must reach: the primary plus the replicas currently in sync. Replicas outside it keep their copies and catch up. |
| Generation | Advances on every write-set change. The epoch still advances only when the primary changes. |
| Epoch start | The journal position and prefix proof at which the current primary's epoch began. |
| Raised N | The highest content W the shard holds or a commit needed above the N its rules give, starting from the content the shard held when it switched. It only grows. |

The shard configuration keeps its format, so every node keeps decoding it and
routing by primary and epoch, including nodes that predate write sets; they
never read the write-set key. A write-set record not bound to the
configuration's epoch and primary grants nothing, and the shard keeps the
all-member rule.

The W floor and N are the highest content W and N among the rules that can
apply to the shard's paths; the record does not copy them. The shard's N is
never below its W floor or the record's raised N. A prefix-assigned
shard takes the deepest rule at or above its assignment root, or the default
policy when there is none, and every rule inside its subtree that is not inside
another shard's nested assignment. Any path can land on a hash-range shard, so
every rule applies to it.

### Invariant

A member joins the write set only after it holds everything the primary has
written, and every commit is acknowledged by the whole write set. Every
write-set member therefore holds every acknowledged change, which is why
write-set membership is the eligibility rule for takeover: any member can
continue the history without the primary.

### Commit

The primary acknowledges a change only after every member of the write set at
its current generation, and every member it has submitted to add, has durably
stored the complete prepared body, and only while that write set meets the
commit's floor: the shard's W floor, raised to the content W of what the change
makes visible. Below the floor the change stays prepared, and so does a change
whose content W is above the shard's N until that N is raised. The commit
record may follow afterwards. A shard that has not switched treats every
configured member as its write set. File and directory sync certify the same
set: every write-set member and every submitted add holds the synced prefix,
and below the W floor sync certifies nothing.

Once the body is durable, any result other than success is an unknown outcome:
the change can still commit through a retry or through a successor, so it is
never reported as a failure. A caller that does not receive success treats the
outcome as unknown and resolves it by retrying with the same operation id. DFS
never reports a definite failure for a change that a successor could still
complete.

DFS sets no deadline of its own on the acknowledgement wait. It ends when the
required members have acknowledged, at the caller's deadline, on cancellation,
when the network layer reports no path to a required member, on a definite
rejection, or when the shard record moves to another epoch. A member that still
answers, however slowly, is waited for.

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

A candidate is a write-set member that can commit to cluster KV. If its verdict
already said the primary had no path when it first observed that primary at the
current epoch, it asks the network to examine the peer. The campaign can start
once a later examination of that peer concludes from its own probe and the
verdict is still no path. An examination that ends without a conclusion,
because it was cancelled, the peer has nothing this node dials, or its probes
produced no verdict, certifies nothing, and the candidate asks again. A node
that dials no endpoint of the primary therefore never certifies a verdict that
already stood when it first observed that primary at the epoch. A path
lost while the primary already holds that epoch qualifies directly. The primary
uses the same rule to remove a write-set member, and a member handed the
attempt uses it before it swaps. If two surviving members lose their path to
each other but both can commit to cluster KV, the first guarded change to the
shard record wins.

1. **Corroborate.** The candidate asks the other reachable write-set members, in
   one round, whether they still have a path to the primary and how far their
   journal goes. A member asked says it lost the primary only by the
   candidate's own rule: its verdict turned to no path while it already observed
   that primary at the current epoch, or an examination of the primary has
   concluded with no path since. Otherwise it has the network examine the
   primary first, so connectivity records that have not yet aged out can neither
   veto a primary that died nor depose one that is alive. A member whose
   examination ends without a conclusion answers that it has no evidence, which
   neither vetoes nor ranks it, and a later question has the network examine
   the primary again. Any member that still
   reaches the primary vetoes the attempt, so one node's poor link cannot depose
   a healthy primary. The remaining members are ranked by journal position, then
   non-mobile before mobile, then node id. A candidate that does not rank first
   hands the attempt to the member that does; that member runs it only if it
   still ranks itself first with its own journal, and otherwise hands it back
   once. A member that cannot confirm the record declines and goes on re-sending
   its own read as step 2 describes, because cluster KV elects a leader only
   through a voter's own request and that member may be the only voter that can
   win. The candidate then confirms the record itself and, if it can, asks that
   member once more, so a momentary loss of cluster KV at the first-ranked
   member does not pass the attempt down the ranking. Unreachable members are
   left out, so a two-member write set needs no corroboration.
2. **Confirm quorum.** The candidate reads the shard record linearizably. If it
   cannot, it promises nothing. A failed read can be momentary while the KV
   voters elect, so it is re-sent on the same timer as an unknown swap for as
   long as the primary is still judged lost and the record is unchanged; any
   change ends it, and only a read that succeeds lets the attempt continue.
3. **Promise.** The candidate durably records that it accepts nothing more from
   the old primary at the current epoch, serialized with its acknowledgement
   path. From then on the old primary cannot commit anything this member does
   not already hold, because each commit needs this member's acknowledgement.
4. **Swap.** One guarded KV transaction: if the record still matches what the
   candidate read, the candidate becomes primary, the epoch advances, the old
   primary leaves the write set and epoch start becomes the candidate's journal
   position. If another change won, the candidate follows it. An unknown outcome
   is settled from the transaction's own outcome before the candidate acts;
   re-reading the record cannot tell a lost swap from one that may still apply.
   The candidate re-sends the identical request until it settles; the re-send
   is a timer, because while the swap is unknown no primary can commit and no
   change may arrive to prompt it. Re-sends of a failed confirm read, of a
   failed rejoin step and of a request to examine a peer whose examination
   ended without a conclusion are the only other timers, for the same reason:
   nothing would prompt them again, and the network starts no examination on
   its own. The candidate releases its promise
   only when its own swap was rejected and the old primary is still primary.
5. **Activate.** The candidate rebinds its existing store to the new epoch and
   republishes every prepared change it holds through prepared-change recovery,
   so changes the old primary may already have committed complete with the same
   effects and caller identity. A recovered change gets a new journal record; the
   old primary's own commit record, if it exists, is discarded when that node
   rejoins.
6. **Align followers.** A follower whose prefix matches at epoch start drops any
   records past it. Those records were never acknowledged, because the candidate
   lacks them. A follower whose journal ends before epoch start reports its head,
   and the new primary confirms it against its own journal. A follower whose
   prefix differs installs a snapshot. A follower never extends its journal from
   an unchecked head: the new epoch's first batch aligns it under the same lock
   or is refused until it is aligned. Acknowledgement floors from the old epoch
   block neither.
7. **Resume work** from the tables, as listed below, then remove unreachable
   members and recruit.

Every write-set and primary change is one guarded transaction on the same record,
so these changes are totally ordered. The winning candidate was in the write set,
so it holds every acknowledged change. It stopped accepting the old primary's
records before the swap, so anything the old primary still commits afterwards,
from acknowledgements already sent, is in the candidate's journal at epoch start.
Nothing from the old primary is needed.

### Keeping W

- **Shrink.** When the network reports that a write-set member has no path, the
  cluster no longer authorizes it as a member, or it refuses the primary's epoch
  because it promised a takeover candidate, the primary removes it with a
  guarded transaction, but never below the W floor. The last case keeps a live
  primary writing when a candidate cannot reach it and its swap is stuck; the
  shrink changes the record, so that swap can no longer apply. A dead member that would
  take the set below the floor stays in it, and writes wait until it returns or
  a recruit replaces it. Keeping it there also keeps it eligible to take over.
- **Re-add.** A returning member keeps its copy and catches up incrementally from
  its matching prefix. Once it has acknowledged the primary's whole journal
  under the current record generation, the primary adds it back; an older
  acknowledgement can describe a copy the member has since deleted. From the
  moment the primary submits that change, it durably requires the member's
  acknowledgement until the record shows the change applied or no longer able
  to apply.
- **Recruit.** When the write set has fewer than N members and no out-of-sync
  replica is reachable, the primary adds an eligible metadata-capable member to
  the replicas, preferring non-mobile members. When every configured member is
  in the write set but one of them should be replaced, a dead member held at a
  floor equal to N or a mobile member a non-mobile one can replace, one recruit
  beyond N is admitted; another waits until it has joined. The recruit catches
  up by snapshot and joins the write set; only then is a dead member at the
  floor removed.
- **Raise N.** A change whose content W is above the shard's N stays prepared,
  and the primary raises the shard's N to that W with a guarded transaction on
  the record, so the raise is durable, ordered with write-set changes and seen
  by a successor. The primary then recruits up to the new N and the prepared
  change commits. A raised N never comes down, because content committed at
  that W can exist until its version is collected.
- **Retire a substitute.** When a shard has more than N members and the
  original member is back in the write set, the primary removes the most recent
  recruit, which deletes its copy as any node outside a shard's placement does.
  Removal never takes the shard below N.

These steps run on write-set changes, reachability changes, membership and
configuration changes, catch-up completion and changes that need the record to
change first.

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
callers. A write-set member that refuses its epoch sends its own record, so the
unknown outcome of the old primary's commit names the new primary even before
the old primary's own record shows it. The old primary follows the new primary
from epoch start, dropping or replacing its unacknowledged tail, and rejoins the
write set once caught up. It never becomes primary again automatically: nothing
hands primacy back. Once re-added it is an ordinary write-set member, and like
any other member it may take over if the new primary is later lost. Its late
messages are harmless: anything that needs a metadata commit fails, and
receivers apply repeats idempotently. Until it learns, it may answer reads from
its last state.

### Remaining work after takeover

Activation requests the successor's authoritative reconciliation, which derives
this work from the tables it already holds:

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
set advertises support. A node advertises it only when it implements the whole
member-side contract of a write-set shard. Until then the shard keeps the
all-member rule and has no automatic takeover; old and new nodes do not share a
write set. On the switch the primary reads the content versions its own copy
of the shard holds and starts the raised N at their highest content W. If any
content version does not record its W, the switch keeps every member. The
primary then reduces the shard to N chosen members, and removed members delete
their copies. The cooperative handoff remains the
planned-transfer path for a live primary. A switched shard does not switch back:
rolling a member of a switched shard back to a build without write-set support is
unsupported.

## Model

The protocol is modeled in Quint before shard authority changes, with four
possible members, a few journal entries and these actions: prepare,
acknowledge, commit, shrink, add, recruit, retire, promise, takeover swap,
crash, partition, recovery and rejoin. Cluster KV is a linearizable
compare-and-swap register. Required invariants:

- committed histories never diverge;
- every write-set member holds every committed entry;
- after a takeover swap, the old primary commits nothing the successor did not
  hold when the swap applied, even from delayed acknowledgements;
- no acknowledged change is lost across any sequence of takeovers;
- a shrink never takes the write set below the W floor.

Without the promise nothing is lost, because the successor still holds what it
accepts, but epoch start no longer bounds the old primary's history. Liveness,
storage and transport stay outside the model and are covered by native tests.
The [formal verification reference](../formal/cluster-verification.md#shard-write-sets-and-automatic-takeover)
describes the model, its deterministic traces and its counterexamples.

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
   id; the requester's remaining budget reaches the primary serving its
   request, so a served commit's wait ends when its requester stops waiting;
   namespace-job resume only on shards this node is primary for; cross-shard
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
