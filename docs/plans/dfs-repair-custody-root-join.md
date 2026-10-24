# Same-producer Repair custody: queue birth and payload-root join

## Current development boundary — 2026-10-24

This remains an unfinished local custody design, extended by selected native
non-executing DATA. Current R8 source `3a445694` passed R4's fresh typecheck
(16.165 seconds), eight operation-tooling controls, own-source no-run build
(218.667 seconds), artifact sealing (3.021 seconds) and archive phase
(2.506 seconds over four recorded groups). **Its 35 Rust controls remain unrun at
this checkpoint.** These stage results are neither native custody acceptance nor
takeover/quietness measurements.

The earlier `3eb87ca5` local DATA panel actually passed twenty controls. R7 source
`0344ec57`/executable `c9966eb3` executed all thirty controls with 23 passes and
seven selected-directory setup failures. All thirty preservation checkpoints
passed, while the inner archive worker separately failed with exit 124 after
120.056863 seconds. Both complete 420-file archives, raw failures and inventories
remain preserved. R8 supplies a directory-specific physical identity predicate
and five focused controls; it leaves the regular-file predicate and named-owner
checks unchanged. The operation-local archive correction retains ownership
coverage without repeating the maintained helper's full scan for every group.
Neither correction retrospectively changes the failed R7 result.

Original R67's positive successor-continuation assertion remains failed on its
`40394f47`/`17295eb6` source/artifact. Its original request/body, committed intent
and readable content cannot grant B execution. Selected DATA does not close that
failure or complete the receiver Terminal, current-application, payload/root
retirement or C1 admission protocol. Four N7 native controls remain unrepresented:
invalid source/prefix/scope, unbudgeted selected input, install-cap expiry and
freeze-before-first-capture restart. Current-source DFS/KV lint, metadata quorum,
abrupt-loss and Sinomo-only quietness also remain open.

DFS development now serializes only its own worktree using a local build/test
lock. It requires no peer capacity window, file release or handback approval and
sends no peer progress/completion messages. Parallel DFS subtasks integrate at
coherent local commits and reviewed, tested actual merges into `dev/dfs`.
`dev/debug` owns Docker debug-cluster operations; `main` owns real rollout. This
lane operates neither. The [availability plan](dfs-abrupt-loss-availability.md)
records the current workflow and source-qualified limitations.

## Historical local design and evidence

The descriptions below preserve the decisions and qualifications of the earlier
same-producer slice. Then-current grants, unrun counts and dirty-source labels
are historical evidence, not current source permissions or current test totals.

This was the reviewed local design in an unfinished development candidate. R52
executed archived R49 source: eighteen of nineteen controls passed; the native
case passed seed preparation but failed its unchanged five-second durable-closure
budget. The same-production paired seed comparison passed with Core timeout and
Main success. R53 corrected a source-confirmed redundant-enrollment/token
invalidation mechanism and adds focused controls/diagnostics. Its own R54 execution
passed all twenty scoped controls, including native closure, and R56 passed the
full current-source DFS/KV gate with eleven UI controls. No R52 verdict transfers
to R53; these are separate source-qualified executions. The slice extends only
the existing repair queue/content log; FullShard, wire authorization and cluster
KV remain unchanged. The bounded custody target's forty-six controls and six
intentional counterexamples assume the storage/closure primitives and do not
prove their Rust implementation or abrupt-loss availability.

## Selected decisions for the first integrated slice

1. **A durable queue identity precedes payload enrollment.** Add an explicit V2
   queue incarnation to the existing snapshot/log format. Synchronize its initial
   publication before returning an enrollment subject. Compaction preserves it;
   a physical queue replacement does not silently inherit it. Existing V1 rows
   are legacy/uncovered. This is a header/schema extension, not another queue.

2. **Birth is not a guessed append sequence.** The original producer already
   owns a stable per-target request ID. Bind that original identity to the queue
   incarnation before content enrollment; record the actual append sequence
   separately once the queue row is written. The subject includes producer,
   receiver, effect kind and original request scope. Original head digest and
   payload descriptor are immutable values, not extra key fields allowing a
   changed body to create a second birth under the same request identity.

3. **The current logical transfer cannot silently change IDs.** The queue writer
   must retain the active original birth for its `(intent,target)` transfer slot.
   A new ID or kind cannot replace an unresolved original. Content-root lookup
   also retains that logical scope so losing/replacing the queue cannot turn a
   still-live root into permission to start a fresh effect. Missing original
   head or a foreign queue incarnation means explicit pending/fenced recovery.
   It does not authorize reconstruction under another identity.

4. **Use a distinct retained-effect root in the existing content log.** Ordinary
   pre-catalog reservations are insufficient: positive catalog coverage releases
   them immediately. Add typed live/released Repair-root records to the current
   inline binding log/view and bounded compaction. They preserve immutable
   identity/body bindings and release tombstones. `Reserve`'s current ability to
   overwrite a Released reservation is not the transition rule for these roots.
   Generic catalog reservation reconciliation cannot release this category.

5. **Capture source, reader and root together.** A content-owner method selects
   and validates the same exact representation used by the original send,
   captures its reader, and synchronizes the retained root under
   `ContentMutationGuard`. Return the root token and owned reader together.
   This avoids reopening a mutable version manifest after the service has
   already selected another exact byte stream. Existing service selection has
   both exact-version-manifest and blob-only paths.

   A manifest source retains the immutable manifest identity/layout plus every
   referenced extent; aggregate hash alone is insufficient. A blob-only source
   retains that exact hash/size and physical owner binding without inventing a
   version manifest. Root enrollment checks actual availability/bindings; later
   reads retain the normal hash/length validation. Missing/corrupt material is an
   error, never a successful exact-body claim.

6. **Then append the original queue row.** With content locks released, the queue
   writer enrolls the complete original head and matching root token only if
   that exact birth is open and its immutable binding matches. Sync the existing
   framed log before carrier eligibility. A failed or uncertain append does not
   authorize dispatch; retain the root and original reader while replay/resync
   resolves its tail. No queue/content lock nesting or network wait under either
   lock is introduced.

7. **Orphan roots stay retained in this slice.** No age-based or absence-based
   cleanup is permitted. Root-fsync success followed by queue failure can precede
   delayed enrollment. Queue absence cannot prove the original was unsent or
   cannot still enroll. Future orphan collection needs a durable monotonic
   cancellation/closed-birth decision, not another scan or a delay. This safe
   over-retention is a limitation to expose and measure, not hidden GC success.

8. **All destructive owners see current roots.** `collect_garbage` and
   `collect_versions` read live effect roots under their existing content mutation
   guard, alongside normal reservations/catalog roots. Protect loose/inline
   blobs, version manifests and all referenced extents. Extend `RemoveBlob`'s
   validation and compaction's canonical key/count/iteration as well. An older
   installed metadata catalog cannot erase current queue-owned roots.

9. **Dispatch requires the same row and live dependency.** The
   `checkpoint_detached_replica_delivery` callback checks the actual routed head
   against its stored original and root token before its normal carrier poll.
   Capture attachment metadata/root in `service_outbound`; no transport-event
   shape change is needed. An absent row/root, changed head, released token or
   foreign incarnation fails closed for V2, retaining unresolved ownership.
   Existing producer/authority/freeze checks remain authoritative.

   The current worker already owns its `DfsRoutedSend` and attachment reader
   before this callback. `DfsOutboundAttachment::from_reader` owns the
   `ObjectVersionReader`; its captured content snapshot holds generation, file
   reclamation and store leases. A later exact terminal can therefore close
   the birth without invalidating an already admitted reader. This is source
   evidence only; the race still needs an executed native control. A restarted
   worker must reacquire and validate the root/reader; an old token alone is
   not a reader lease or new authority.

10. **Record result, apply, then close irrevocably.** After exact existing reply
    correlation, store the complete observed response and disposition before
    metadata receipt/local application. This is a new ordering seam; today's
    `record_owned_repair_terminal_response` is after receipt publication and
    stores completion identity. Preserve observed-result/application-pending
    state across restart. Only exact settlement and durable admitted local
    completion can produce a closed birth. Unexpected Durable progress must not
    be promoted into a stronger Repair Committed certificate. Current wire
    progress semantics and any pending root are kept distinct.

11. **A closed proof is durable and non-reopenable.** The existing queue writer
    installs an immutable closed record binding queue incarnation/birth, original
    head/payload, root token, exact terminal result and local application, checked
    dependency generation and actual closure sequence. Every Set, enrollment,
    whole-view replacement, Remove and compaction preserves that closed identity.
    Scheduling-only APIs cannot alter it or recreate the birth. A process-local
    `DfsRepairQueueObservation`, queue absence or equal tuple cannot mint it.

    A typed `ClosedRepairBirth` value is constructed only from the synced closed
    record by the queue owner; it is not caller-asserted/deserialized authority.
    Independent queue progress need not invalidate this immutable proof. Any
    raw-head borrower must first retain its own complete command or prevent
    closure. A child gets its own durable input root before enrollment, so
    closing the parent's root cannot erase the child's reference.

12. **Release only that root under its physical owner.** Drop queue locks, then
    use the immutable closed proof for an exact content-root CAS. Match subject,
    head/payload binding, dependency generation and content-store incarnation;
    synchronize a non-resurrecting release tombstone. Crash before release keeps
    bytes. An uncertain release append requires the existing resync discipline
    before reporting removal. Duplicate close/release is idempotent; a changed
    binding or released-root re-enrollment rejects. Logical catalog compaction
    preserves roots/tombstones without masquerading as store replacement.

## Required interfaces in existing owners

Names below describe proposed typed operations, not disconnected Rust stubs:

| Existing owner | Required operation / durable result |
|---|---|
| `LocalDfsRepairQueueStore` | Ensure V2 queue incarnation; bind/enroll original birth; retrieve exact original for restart; preserve custody inside every scheduling/rewrite path |
| Same queue writer | Persist observed result, mark durable local application, close exact birth and return a sealed `ClosedRepairBirth`; retain closure through snapshot/reopen |
| `LocalContentStore` / inline binding owner | Open-and-retain exact Repair source under mutation ownership; validate live root and capture reader; release exact root using closed proof |
| Inline replay, compaction and GC | Preserve immutable live/released root states, root referenced manifests/extents and enforce `RemoveBlob`'s live-root veto |
| `service_outbound` / `service_repair` seams | Capture the actual original command/descriptor, recover the stored call, record exact reply before application and close only its original birth |
| Runtime-message callback | Validate the original row/root at existing before-dispatch/failure checkpoints; no new transport event or host callback installation |

Whole-batch encoded size, V2 replay admission and old-reader failure behavior
remain mandatory implementation checks. Native content/log versioning must reject
unsupported complete records rather than truncating them as torn tails. Queue
and content snapshots preserve immutable bindings and released/closed tombstones.
An independently restored old queue cannot dispatch against a released content
root. A physical replacement/import must retain and validate these obligations
or fence pending work; jointly rolling back both stores is not silently supported
by the proposed local proof. No new metadata/store-authority rebind is implied.

## Historical initial review and execution gates

These were the initial obligations before the integrated local slice and its
source-qualified R54 results. They remain useful failure-edge requirements, but
the unrun statement below is not a present-day blanket test count. Current R8
qualification and the still-open distributed joins are stated above.

The missing closed-birth/root operations are precisely why a serializer-only Rust
patch is unsafe. They must be integrated with producer capture, every writer
rewrite, restart and GC. Source handoff permits the named implementation areas
but is not correctness approval, a resource slot or permission for protected
runtime/provider/authority changes.

Before calling the join reviewed, close the exact subject/key and queue-incarnation
admission; old-image handling; result/application proof; closed-birth retention;
root representation for extents/blob-only input; and full writer preservation
enumeration. The first slice can retain failed-enrollment orphans indefinitely;
collection of those roots is explicitly deferred, not guessed from absence.

Required native negatives: both fsync reply-loss cuts; delayed enrollment after
attempted orphan cleanup; catalog-covered reservation retirement while an effect
root is live; old Set/Remove/whole-view ABA; close/release versus an already owned
reader; child enrollment versus parent release; compact/reopen of live/released
and open/closed states; extent/blob-only source after semantic retirement; and
replacement-store rejection. They are UNRUN. The target's assumed certificates
and 41 controls cannot establish these physical storage properties.

## Historical implementation refinements (R42–R53)

- Retry observations are process-local DATA tokens over retry rows/cursor, not
  metadata or custody authority. Owned Initialize/Enroll/Attempt/Observe-only
  appends preserve that retry revision; physical head still orders all records.
  Ordinary retry operations and Close invalidate the revision. External reloads
  invalidate it even when decoded rows match. Public exact-head reconciliation
  and cooperative handoff remain physical-head exact. Latest custody is checked
  under the queue writer lock on every ordinary write/replay; ordinary Remove
  cannot erase an open birth's runnable row. No metadata participant rule changes.
- Native result recovery visits an indexed, bounded page in existing maintenance
  before its empty-job early return. A local advisory cursor rotates pending
  originals. It does not gate activation on the entire backlog or add a worker.
- Stored Committed observations can close without another metadata publication
  only under actual native execution admission for the original physical shard,
  exact durable receipt/hash and the required current-head location effect.
  R42 REMOVES the earlier publication fallback: absence can mean a later
  authoritative withdrawal, so stable original identity/current authority cannot
  authorize re-creating the old positive. Missing or compacted application evidence
  retains custody; it never authorizes
  a fresh original ID, replays under a different identity or rewrites a newer head.
- Durable queue closure atomically settles any remaining compatible scheduling
  row. Already-closed retries do not append another closure or overwrite a later
  child. Root release remains a separate exact tombstone write after queue unlock.
- V1 unknown work is explicitly pending; rejected/unexpected Durable originals
  remain rooted pending a separately reviewed effect-specific settlement rule.
  Orphan collection and closure after application evidence itself is collected
  are deferred. These conservative limits are not availability acceptance.

## R42 causal-replay correction and exact metadata-owner dependency

Source review found a concrete gap in the R40 candidate: observed original success
→ local receipt publication → crash before queue closure → later authoritative
receipt/location withdrawal → stored-response replay could publish a fresh positive
without contacting the holder. `RemoveReceipt` retains no negative generation;
ordinary immutable intent/tuple/hash checks do not order this old positive against
that later negative. The R35 target assumes a durable monotonic local-application
fact and does not model disappearance of current copy evidence. Its dependency
ABA tests do not prove this native ordering.

R42 removes only the introduced stored-response publication path. Recovery now
closes using existing admitted exact receipt/location evidence or stays pending,
with original body/root retained. This conservative restriction cannot complete
an observed-but-unapplied result automatically. The ordinary delayed live-reply
handler's causal ordering is a separate unresolved requirement; removing this
fallback does not certify it.

Enabling complete automatic application needs a reviewed contract in the existing
metadata prepared-operation journal: stable original application/effect identity,
conditional positive application ordered against durable withdrawal evidence for
(object,version,target), and a retained exact application witness through snapshot/
compaction. It touches service_commit::put_metadata_receipt,
service_read_transfer::mark_remote_read_location_missing, prepared effect/backend
validation and receipt/application retention. Those source owners were not included
in the historical queue/content/callback grant. That old ownership restriction is
superseded; the exact causal contract and correctness review remain necessary
before implementing the join independently in DFS. Queue revision, wall clocks,
absent receipts or cluster KV are not substitutes for durable application evidence.

## Cooperative compatibility remains open

V1 handoff carries retry descriptors but not V2 original heads, attempts, observed
responses or valid successor-owned payload roots. Activation can finish under its
existing coverage gate while the transferred Repair remains pending at new V2
admission. This is a compatibility blocker for the unfinished candidate, not an
activation barrier introduced by Remove. A new versioned custody/checked executor/
physical-root transfer contract is required across the existing handoff, rebind
and authorization owners. The historical four local grants did not cover those
joins; current independent ownership does not remove their protocol obligations.
Later selected DATA can transfer one genuine original without certifying its
successor executor or payload readiness. The historical operation record
COOPERATIVE-V2-CUSTODY-SOURCE-DEPENDENCY.md retains the original dependency trace.
Do not silently reconstruct old requests,
copy foreign root tokens, weaken V2 guards or wait for every queued effect to finish.

The local retirement planner preserves open births before proposing removals,
so a terminal scheduling DATA checkpoint without Close cannot reject unrelated
retirement in the same page. Its then-new control covered that assembled crash
state in the historical same-producer panel; that result does not qualify R8.
This does not make V1 handoff a complete original-custody transfer.

## Bounded withdrawal companion evidence

R43 static checking passed; R44 actually passed46 TypeScript controls, including5
new withdrawal controls/sensitivity witnesses. R45 actually failed6 deliberately
unsafe test-only mutations with QNT508, including stale-positive restoration.
Exact model sources,46ITFs/max36states and bounds are retained in
CUSTODY-WITHDRAWAL-TRACE-CATALOG-R44.json. The new mutant can leave the earlier
custodySafety checks true while violating withdrawalSafety.

Historical localApplied is monotonic; currentReceipt, knownWithdrawal, deficit and
localClosedBirth are separate fields. Bounds remain2effects,3stores,2attempts/effect,
onewithdrawal/effect,one sample/test,20s+2sgrace,TypeScript only. Exact application/
withdrawal authority is assumed; fresh holder revalidation is excluded. These
traces reject stale-positive restoration in that restricted slice. They do not
implement/prove native causal receipt ordering, survivor custody or full metadata
consensus. Old R35/R36 source/results remain preserved separately.


## R52 post-seed failure and R53 token correction

R52's actual nineteen-case execution had eighteen passes and one post-seed native
closure deadline failure. Its source/binary/fixtures are sealed independently.
The old Core seed arm timed out5.001990s; registered Main prepared0.038172s under
the same current source/configuration and five-second before-write deadline.
The native seed prepared0.039768s before its later closure timeout. These results
do not establish the historical R41 failure's sole cause.

One source-confirmed mechanism is independent of the closure predicates: the
maintenance page observes retry revision R; first original enrollment appends a
redundant EnqueueIfMissing for an already-present row together with Enroll; the
existing operation-category rule advances retry revision although DATA is
unchanged. Attempt itself is custody-only. The later exact page/cursor checkpoint
then rejects the old retry token before private payload publication, and ordinary
maintenance intentionally reports deferred work. The actual archive is consistent
with this path but does not expose the precise runtime token comparison, so native
causal attribution remains pending.

R53 keeps the writer, all legacy/immutable-body/closed-birth checks and physical
history fences. It explicitly validates an existing row's stable identity while
omitting only its redundant enqueue; a real row insertion still changes DATA and
invalidates the old token. The strengthened positive uses the real token taken
before enrollment through Attempt and named-claim checkpoint. The new negative
covers actual insertion and conflicting identity without damaging the existing
row/token. Failure-only native observations retain eight returned ticks and exact
captured heads, births, queue and RPC request correlations without dispatch or
publication. All caller budgets/assertions remain unchanged. The twenty-case
successor actually passed under its own R54 coordinated grant; R56 separately
passed the maintained full DFS/KV boundary gate. These local/tmpfs checks do not
close the distributed protocol or compatibility blockers.

External DATA changes can still correctly reject a pass. This correction does not
prove liveness/recovery of every prepared private call, abrupt survivor custody,
receiver terminal replay, V1-to-V2 handoff or causal application/withdrawal safety.
The complete distributed contract remains required before changing participation.
