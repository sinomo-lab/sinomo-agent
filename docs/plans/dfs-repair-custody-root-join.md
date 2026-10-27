# Same-producer Repair custody: queue birth and payload-root join

This design joins the existing repair queue and content owners while keeping
original request identities, payload roots and irreversible closure explicit.
Local file/codec controls do not establish distributed Terminal, successor
execution or abrupt-loss acceptance. Every concrete implementation needs its
own source-bound native evidence; partial results remain in execution receipts.

## Local custody design

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
   the birth without invalidating an already admitted reader. The reader/closure race requires a native control. A restarted
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

## Validation boundaries

Exercise queue incarnation replacement, changed original heads, released-root
re-enrollment, root fsync followed by failed queue append, uncertain tails,
closure raced with an admitted reader, response persistence before local
application, and crash between closure and root release. Compare exact original
identities, complete body/payload bindings and retained files after reopen.

A physical directory requires directory identity checks; a regular-file
predicate cannot establish ownership of a directory. Native startup must carry
the admitted selected layout before creating or reopening selected stores.
Cancellation retains the actual worker and its physical owner through drainage.
Default readers/writers must reject selected roots rather than bypassing that
admission. Failure to collect or archive evidence remains a separate failure.

Cooperative transfer and successor execution must retain the same typed origin,
current executor authorization, exact receiver result and local-application
proof. A readable committed object alone cannot grant another node execution.
The [original-effect custody plan](dfs-original-effect-custody.md) supplies the
survivor-coverage and terminal-compaction obligations beyond this local join.
