# Bounded DFS maintenance turns

The resident Main owner retains maintenance progress in its existing message
cache. This is scheduling state, not publication authority. Each turn still
acquires the normal data owner, mutation admission and current authoritative
metadata projection. Protocol reads and mutations keep their existing foreground
preemption behavior.

## Phase checkpoints

An uninterrupted pass retains the existing bounded handoff, namespace-request,
namespace-job, write-intent, placement, metadata-lifecycle, content-lifecycle,
repair and publication order.
Each completed phase updates its report and next phase before a cooperative
yield. Dropping the future saves that position before admission is released;
resumption does not repeat all the completed earlier phases.

An interrupted asynchronous placement page remains unfinished. On reentry the
node's repair executor receives one bounded turn before placement is
retried. This prevents repeated observation requests from placing pending repair
behind the same interrupted placement page. That repair-only invocation returns an
explicit incomplete result to Main before placement resumes, so it cannot count
as a completed reconciliation or consume two repair-page budgets in one call.
Placement still uses its native page cursor, configuration checks and guarded progress CAS. Its effects are not
reported as completed merely because a repair turn ran.

The checkpoint is bound to the actual authority/storage generation, filesystem,
node, shard configurations and maintenance options. Main obtains the generation
from its retained native authority; other callers conservatively use their
supplied projection revision. A changed scope, generation or budget restarts
phase bookkeeping. It does not grant a new owner or undo any durable effect.

Main reports `Continued` when an unfinished retained checkpoint advances, or
when foreground preemption interrupts reentry to an already-saved turn. The
latter retains its scheduling continuation without inventing a progress epoch.
This includes preemption while acquiring admission, before the service lane is
borrowed. Only an existing cache owner matching the preflight authority generation
can supply that saved turn; the actual resumed pass still validates its full
native generation and scope before effects.

An authoritative reconciliation or mounted-recovery caller also retains its
unfinished request when foreground preemption happens before the first phase
checkpoint, including after an authority generation rollover. The next turn
still acquires current authority and starts fresh phase bookkeeping when the
old generation was retired; it cannot reuse old publication permission.

That incomplete outcome requests resumption through the existing scheduler even
if a transient work probe is empty. It cannot acknowledge mounted recovery or
completed reconciliation. A routine call with no saved turn, and a source-fence
Pending that makes no progress, remain `Deferred` with the existing retry pacing. Protocol
reads continue to preempt both admission and execution.

A returned phase error restarts advisory phase bookkeeping on the next call.
Earlier phases may need to advance the failed phase's dependencies, such as a
placement scan preceding repair-permit admission. This reset occurs only after
the turn drains; it preserves durable cursors. Foreground cancellation retains
completed phase progress.

## Durable work and cancellation

The source of repair work remains the native committed intent and its exact
version, content identity, policy and target. The repair index in the shard
tables is its derived scheduling record, from which the shard primary's repair
executor schedules. No producer writes repair work anywhere else: a commit that
leaves a replica missing changes the index, and that change is the executor's
wake. Attempt state lives only in the executor's memory, so a restarted node or a
successor primary starts from its own index.

A resident background receipt read, publication planner or replica materializer
tries the existing physical metadata writer fence once. Contention retains the
same native source owner and yields the background turn; it does not pin a read
snapshot, advance a replay cursor or report successful materialization. This
allows the source owner's in-flight remote operation to receive protocol replies
without a background waiter holding global mutation admission. Ordinary protocol
read classification and foreground read semantics remain unchanged.

A successful attempt performs the same writer-fenced journal certification,
exact table refresh and prefix validation as before. An already-pinned nested
read keeps its existing cut. A multi-shard attempt releases any earlier pins if
a later shard is busy. Stable corruption and authority errors remain errors.
Background publication/materialization retains its existing retry owner and
pacing.

The executor registers each attempt before its payload is handed to the
detached-send owner, which starts after admission is released and holds the
request and immutable payload. A send failure, a rejected reply and a reply that
publishes all end the attempt and wake the executor. A reply publishes only
through the receipt-slot generation compare, so a late or duplicate reply cannot
publish over a newer withdrawal.

The blocking maintenance child observes both component shutdown and shutdown of
its actual registered runtime owner. Runtime shutdown wakes a waiting child even
if the component watch remains open. The normal task registry still joins that
same child and retains its data-owner lease until the maintenance future ends;
notification is cancellation, not evidence that the child has drained.

No backend borrow, admission guard or publication capability is stored in a phase
checkpoint. A completed report requires every phase to finish and the bound
generation to remain current.
Main rechecks the live retained authority before acknowledging reconciliation
readiness. A report from a retired generation is not readiness for its successor.

## Restart and replay

Phase position and partial reports are process-local. Restart begins a new
bounded pass over the existing durable journals, intents and request outcomes.
Already completed effects are reconciled through their normal native identities
and guards; the phase marker cannot manufacture a receipt. Repair resumes when
the executor first observes the node's primary assignments and reads those
shards' repair index.

Metadata storage preparation has a separate source-custody controller. Its
contended calls return no-progress Pending and preserve the same physical source
lock and validated cursor; see [storage migrations](storage-migrations.md).

## Validation boundaries

The progress-state tests cover cancellation, exclusive turn ownership, scope and
budget changes, native generation rollover and rejection of incomplete reports.
The networked overwrite case retains repeated routed reads, its original
completion deadline, both replica receipts, the original sealed body and single
mutation dispatch.
File-sync, restart, authority and process-cluster tests remain separate required
checks. A focused pass does not establish cluster acceptance.

A file-sync request can find more than one committed plan for its frozen
filesystem, object, version and hash. Its repair loop visits one matching current
plan per turn at the strongest required replica count. A retained placement plan
whose executor is parked cannot prevent the original committed write plan from
being visited. The existing per-turn send budget and request deadline still apply;
each selected owner passes the normal native authority and transfer-custody checks.
The request-local cursor is scheduling data only. Cancellation or exact request
replay rediscovers the durable plans, while unresolved requests belonging to an
older owner keep their identities and delivery evidence. Success still requires
the full named content-receipt set and the existing intersecting metadata proof.

The existing per-phase record/job/byte limits, caller deadlines, worker policy
and actual CPU/network quietness requirements are unchanged.

A background table checkpoint captures the existing journal certificate under
the canonical writer immediately before installing its validated image. After
installation and journal pruning succeed, it records the same owned-materialization
transition used by ordinary metadata writers. This preserves the stronger exact
deferred prefix when present; it does not create author authority. Changed file
identities, uncertain owners and pruned coverage still receive the existing
validation. Cancellation before this cache update and process restart retain the
full cold-certification path. An arbitrary snapshot replacement cannot use the
checkpoint owner's transition.
