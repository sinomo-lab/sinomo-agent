# Original-effect custody before abrupt node loss

## Current development boundary — 2026-10-24

R8 Rust source `3a445694` contains the local custody/content-witness foundations
and selected native non-executing DATA candidate. R4 passed fresh typecheck in
16.165 seconds, all eight operation-tooling controls, own-source no-run in
218.667 seconds and artifact sealing in 3.021 seconds. **All 35 R8 Rust controls
remain unrun at this checkpoint.** Compilation and tooling checks grant no
original-custody, receiver Terminal or successor-execution certificate.

Preserved evidence includes the twenty passing local DATA controls on `3eb87ca5`;
the R7 `0344ec57`/`c9966eb3` panel's 23 passes, seven actual setup failures and
separate inner archive timeout; and the R67 `40394f47`/`17295eb6` positive
successor-continuation failure. R8 corrects the selected directory predicate;
neither source review nor an older pass qualifies changed Rust. The original
request/body, committed intent and content evidence remain unchanged.

Immutable origin and current fenced executor remain separate. Selected DATA
installation does not grant B execution, assert complete survivor coverage or
erase legacy uncertainty. P0 preserves default V1 continuation; missing historical
bodies cannot be invented. The four missing N7 native controls, current-source
DFS/KV lint, checked successor joins, C1 floor, Terminal/current application,
root retirement, metadata quorum, abrupt-loss and Sinomo-only quietness remain
open. The [availability plan](dfs-abrupt-loss-availability.md) records the exact
scope and evidence summary without requiring local operation artifacts.

DFS subtasks now work independently in isolated branches when edits overlap,
using a worktree-local build/test lock and bounded owned cleanup. Peer capacity
windows, source releases and handback messages are historical coordination only.
Coherent local commits and reviewed, tested actual merges into `dev/dfs` govern
integration. `dev/debug` owns Docker debug-cluster work; `main` owns the real
rollout. This lane operates neither and introduces no rollout dependency.

## Historical checkpoints and target contract

The following checkpoint and later R38/R41/R50 entries retain their original
source qualifications. Their then-unrun counts and conditional source/resource
grants are historical, not present approval requirements or current test totals.

Historical R53 checkpoint: frozen R53 Rust has R54's 20 local controls and R56's full
current-source DFS/KV boundary verification. These do not establish V2 custody
transfer or abrupt-loss availability. R57 adds a reviewed C4/C7 companion target:
26 controls passed and six deliberate mutants produced their intended assertion
failures. It changes no Rust. The first failed target attempt is retained.
See the [R57 report](../../data/operations/dfs-availability-20261023/CUSTODY-ACK-R57-REPORT.md),
[transition contract](../../data/operations/dfs-availability-20261023/CUSTODY-TRANSITION-CONTRACT-R57.md)
and [V1 control map](../../data/operations/dfs-availability-20261023/V1-BASELINE-CONTROL-MAP-R57.md).
Full legacy admission policy, receiver terminal/current-application joins,
executor authorization, payload/root retirement and complete multi-slot protocol
review remained open; the production source handoff was conditional at that time.
R58 separately specifies the C5/C6 existing-writer chain and executes 44 new
TypeScript controls plus ten intended mutant failures. It preserves R57 sources
and R53 Rust. The [R58 report](../../data/operations/dfs-availability-20261023/C5-C6-REPORT-R58.md)
records exact native witness/interface dependencies; these traces do not provide
those implementations. Historical source releases did not establish correctness.

This is a proposed recovery contract and review checklist for the availability
lane, not an implemented persistence format, an approved replacement protocol or
a conformance result. Full assigned metadata proof remains unchanged. All request,
delivery and outcome records described here belong in DFS-owned state, outside
cluster KV. Existing cluster authority governs actual epoch/topology changes.

The accepted-bundle target covers one metadata slot. It does not cover detached
transfer, promotion, repair or response commands created afterward. Those effects
need separate custody before a sender can disappear. A cooperative export of the
old primary's local queue cannot establish abrupt-loss recovery.

## What must survive

Do not collapse these objects into one request-ID/hash tuple:

| Object | Recovery obligation |
|---|---|
| Logical effect identity | Preserve original requester/origin, operation identity, effect kind and scope, including the original transfer request identity used by receiver idempotency. |
| Immutable semantic command | Preserve its complete versioned body, predicates, target/content/version identity and policy, with an exact encoding/digest binding. A digest alone cannot reconstruct an unresolved body. |
| External payload | Identify exact bytes, length, layout/ranges and encoding, plus who retains them and until which dependency settles. A descriptor naming A-only bytes is not survivor-recoverable payload. |
| Attempt and authorization envelope | Distinguish transport attempt/correlation and current executor authority from the original logical identity. An authorized successor must not impersonate A or create a new effect to resolve an uncertain old effect. |
| Delivery evidence | Retain every unresolved attempt and its uncertainty. A later definitely-unsent attempt cannot erase an earlier possibly-applied attempt. |
| Receiver terminal outcome | Bind the exact original command to its effect-specific settled result and durable evidence. Transport acceptance, content staging and metadata publication are different boundaries. |
| Client delivery | Retain known/unknown delivery evidence separately from receiver settlement. Constructing, enqueueing or sending a response does not prove caller receipt. A lost response can require result replay without another mutation. |

The native rejection control preserves the logical caller/operation/payload/
options and sealed prepared digest/body/terminal reference across Core/Main
replay. Only its already-classified nonsemantic observed-base envelope may
refresh; original conditional base/read predicates remain sealed. This is not
permission to refresh semantic predicates. The control does not establish that a
successor can recover every original wire request or attachment after abrupt loss.

## Historical baseline source boundary

The independent read-only trace found the following distinctions in the base
checkout before the local custody extensions. These are source findings, not
executed crash or custody evidence. Later bounded implementations and their
separate qualifications are summarized above and in the historical correspondence
section; this table does not describe every field present in R8.

| Baseline artifact | Retained material and missing boundary |
|---|---|
| `protocol.rs::DfsStageBlobCommand` / `DfsRepairReplicaCommand` | Typed head includes request/intent IDs, filesystem/object/version/hash, base, object metadata and authority assignment. Inline commands also contain bytes. |
| `service_outbound.rs::prepare_replica_attachments` | Streamed commands set `bytes=[]` and carry a separate owned reader. `body_rpc.rs::DfsRpcAttachmentDescriptor` carries size/hash, not durable payload custody. Reconstructing from current metadata/authority need not reproduce the original head. |
| `protocol.rs::DfsCommitVersionCommand` | Complete small promotion head, separate from staging durability; no content attachment. |
| `metadata_records/prepared_operation.rs::DfsPreparedOperationRecord` | Exact canonical metadata transaction/digest and terminal reference. It does not include every per-target transfer command or object payload bytes. |
| `sinomo-api/src/dfs/maintenance.rs::DfsRepairAttemptRecord` | Intent/object/version/hash/target, transfer kind/request ID, uncertainty and completion identity. It omits filesystem ID, original base/object-metadata/authority head, attachment bytes and full terminal reply. |
| `repair_queue_store.rs` | Queue append requests `DurableFileSync::All` and recovers uncertain tails. Persistence covers those record fields, not omitted bodies. |
| `node_transport.rs::DfsCheckpointedReplicaCall` / `checkpoint_background_replica_dispatch` | Owns the routed head and reader through a process-memory shared owner. “Checkpoint” here moves retained ownership; it is not durable serialization. |
| `node_transport.rs::DfsNodeDetachedSendOutbox` | Retains complete process-local messages/failed local completions. Promotion terminal suppression is explicitly process-local; restart reconstructs semantic work from intents/receipts. |
| `repair_handoff.rs::DfsRepairHandoffPage` | Prefix-bound, digest-checked cooperative pages of repair records. They require the retiring owner and include neither original transfer heads nor payload bytes. |

Stable write intent/version identities derive from the original requester,
object and operation (`intent.rs`). The executing metadata primary and current
transport source may differ from that requester and from the retiring producer.
The durable queue preserves the original per-target transfer ID and refuses to
replace unresolved/unknown custody (`service_repair.rs`). That useful rule does
not prove byte-identical reconstruction of the missing original command body.

Receiver staging stores/binds bytes before returning Durable; repair separately
validates and commits replica metadata. CommitVersion is promotion, with its own
canonical evidence and matching Committed reply. Sender correlation checks peer,
destination, request/intent/filesystem/object/version and successful hash. If its
local completion handler fails, the detached owner retains the authenticated
response and original head; the outbound attachment has already been consumed.
This remains process ownership until an appropriate durable record is established.

Captured attachment readers retain physical locators, generation/reclamation
leases and the store lease (`content_store/inline_bindings/read_snapshot.rs`).
Those protect live readers, not a successor after process loss. Likewise, a
bounded response inbox accepts process custody only; a full inbox rejects so the
sender retains its response (`response_delivery.rs`). Neither is a durable
survivor outbox. No existing queue descriptor alone was shown to root a complete
original head after its semantic owner/content roots retire.

## Proposed durable states and gates

### Concrete candidate records in the existing store

This design is a proposal for review at Rust checkpoint
`f520a20d482ca2b069dd51ced9f220c365797644`, not a new queue or implemented format.
The first possible production slice is background **RepairReplica only** on the
same producer after restart. Stage/StagePatch, promotion, cross-primary replay and
survivor replication require separate integration. The wire command types live
in `sinomo-dfs`; embedding them in the shared `sinomo-api` scheduling record
would invert that ownership boundary.

Use a DFS-owned `StoredRepairQueueRowV2` in the existing queue snapshot/log.
Retain `DfsRepairAttemptRecord` as its scheduling/status projection. Existing
Enqueue/Set/Remove, maintenance, hydration, snapshot and cooperative export paths
must preserve the original-effect fields. A missing repair deficit must not erase
an unresolved original effect. The current store is a restart accelerator for
semantic repair state; enrolled original custody would become retained evidence
which rebuilding that projection cannot discard.

Enforce preservation **inside the writer**: scheduling-only Set updates replace
only the projection; custody removal compares the exact terminal witness and
dependency generation. Today `replace_records` and whole-view reconciliation
remove omitted keys, ordinary Set/Remove can replace/delete rows, and compaction
serializes the remaining rows. All those paths need V2-aware rules; caller
discipline is insufficient. Removing a repair deficit can park scheduling while
retaining an unresolved custody slot. Cooperative pages need a V2 stored-row
representation; their current `Vec<DfsRepairAttemptRecord>` loses these fields.

| Candidate field group | Exact retained contents | Existing owner / required extension |
|---|---|---|
| Format and birth | Explicit custody schema/codec version, queue incarnation/generation and actual append sequence; never timestamp-only identity | `LocalDfsRepairQueueStore`, sequenced snapshot/log and disk-generation observation |
| Scheduling projection | Existing intent/target, retries and pacing, plus exact observed-row CAS | `DfsRepairAttemptRecord`; all scheduling updates preserve the stored wrapper |
| Original identity | Effect kind, original wire request ID, intent/operation, filesystem/object/version, target, original logical requester/coordinator and producer | Capture from the already prepared command and producer; do not infer omitted fields from the current head |
| Original head | Complete immutable encoded domain command, encoding version, route/category and digest; original base/patch predicates, metadata, authority assignment and flags stay sealed | `service_outbound.rs::prepare_replica_attachments` and the actual `DfsRoutedSend` before detachment; current transport source/envelope remains separately classified |
| Inline material | Original inline bytes remain inside the exact command encoding | Same queue row if it fits the existing frame bound; no replacement with a hash or a different attachment command |
| Attached material | Exact descriptor (size/hash/layout/ranges/encoding) and a durable root for those exact bytes, including object/version/patch binding | Existing content store and reservation/catalog owner; a reader lease or descriptor is insufficient |
| Physical dependency | Store incarnation, physical generation, owner-minted reservation causal stamp and root identity | `ContentStoreReservationRecord` / `ContentStoreCausalStamp`; serving epoch is separate |
| Delivery | Original logical transfer ID plus retained unresolved attempts, joined uncertainty and exact completion correlation | Existing queue claim/checkpoint CAS; a later definitely-unsent/rejected envelope cannot clear an older possibly-applied attempt |
| Observed terminal | Exact encoded/typed result and digest, captured disposition, original-request digest, authenticated peer/context and actual receiver terminal reference when available | Existing correlation handler; missing receiver durable evidence remains a gap, not a synthetic certificate |
| Local application | Terminal recorded versus receipt/location/cache application pending versus settled; retained client-response reconstruction dependencies | Existing admitted completion handler; receiver settlement, sender application and client delivery stay distinct |
| Dependency release | Monotonic dependency generation, exact users/roots, compact-witness identity and checked generation | Queue CAS plus the existing content-root/GC owner; stale checks retain material |

The current queue key is `(intent_id, missing_replica_node_id)` and its transfer
kind is Stage/Repair. A future promotion obligation needs a separately identified
slot **within the same row/store**, with explicit transfer/promotion dependencies.
Adding Commit to the mutable kind and allowing ordinary repair cleanup to remove
the row would lose custody. The first Repair-only slice must not claim promotion
coverage; choosing and validating the combined slot schema remains open.

The queue log currently limits a batch record to 32 MiB
(`repair_queue_store.rs::DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES`). Background
streamed Repair heads have separate payload readers, which makes a bounded
head-plus-reference row plausible. Arbitrarily large original inline commands
cannot simply be copied into that frame. Their representation needs either an
existing, explicitly rooted immutable content artifact containing the **exact
original encoding**, or a reviewed framing extension. No synthetic object/version
identity, new silent request limit, rewritten inline command or cap relaxation
is selected here. Until that storage decision closes, oversized inline custody
is unsupported by this candidate and cannot be certified complete or dispatched
under a purported guarantee. This is an unresolved design boundary, not a new
runtime rejection added by this lane.

Size the **whole encoded log batch**, including every operation, original head,
result and framing. Partition only independent updates into bounded batches;
an atomic per-effect enrollment/result/reference group must remain intact. A
group which cannot fit needs the reviewed external representation above, rather
than partial enrollment being treated as dispatch-ready. Large exact terminal
results need the same representation/root decision. No payload, result or batch
limit is silently added or weakened.

The current attachment descriptor contains only size/hash. Layout/ranges/encoding
must come from the sealed command, its referenced manifest or the proposed
dependency record. `ContentStoreCausalStamp` contains incarnation and catalog-scan
epoch; it does not certify physical payload generation. The physical-generation
join in this design is an additional checked dependency, not an existing field.

Use an explicit new snapshot/log schema and capability admission. Old queue
writers must not reopen V2, strip the wrapper through a scheduling Set, then
acknowledge a lossy snapshot. Existing V1 rows import as **legacy/uncovered**;
an absent body is not an empty exact body. Enrollment needs the original living
producer or other exact retained source. Uncertain legacy requests cannot be
recreated under new IDs. The full history/custody admission cut and fail-closed
downgrade rules remain prerequisites to abrupt-loss guarantees.

### Persistence and ownership order

| Step | Required durable action and crash meaning | Concrete current boundary |
|---|---|---|
| 1. Capture | Freeze the complete original head, input descriptor and identity while the producer retains admission. No transport polling yet. | `prepare_replica_attachments`; prepared calls currently retain only process ownership in `DfsCheckpointedReplicaCall` |
| 2. Root bytes | Persist the input/root under its real content owner before queue enrollment. An orphan root after a crash is safe; it grants no dispatch or new request identity. | `ensure_content_reservation`, content mutation/reservation guards and owner-minted causal stamp |
| 3. Enroll locally | Append the complete stored row and dependency reference through the existing queue writer, with full sync. This establishes only **locally recorded** restart material. A failed/uncertain append cannot authorize dispatch; replay/resync its tail first. | `LocalDfsRepairQueueStore::append_operations`, `DurableFileSync::All`, `resync_on_load` |
| 4. Cover survivors | Before effects become ambiguous, the reviewed DFS history/custody protocol must certify exact complete material and unresolved delivery ownership on the surviving set. Local queue fsync alone does not do this. | New integration into the reviewed multi-slot protocol; current cooperative handoff cannot supply it after owner loss |
| 5. Claim and send | Revalidate the exact row, dependency generation and current authority; durably install conservative attempt uncertainty before the carrier may dispatch. Then release the existing owned head/reader to its normal transport worker. | Existing before-dispatch claim and queue CAS, native authority gate and networking path; exact origin/executor joins still require implementation and verification |
| 6. Record result | Correlate original source/destination/request/intent/filesystem/object/version/hash/kind; persist exact observed terminal material before releasing sender completion ownership. | `all_background_replica_response_identity_matches`, `checkpoint_transfer_record`, admitted reply handler |
| 7. Apply locally | Retry the original result through the normal handler until its receipt/location/cache effect is durably settled. Persist “terminal recorded, local application pending” across restart. | `runtime_host/message_host.rs` delivery-checkpoint callback and `handle_daemon_dfs_message_on_runtime_with_background_response`, then `service_repair.rs::handle_repair_replica_response`; current outbox retains head/response only in memory |
| 8. Compact | Persist the exact outcome witness, recheck dependency generation, install the compact row/index transition, then release only unneeded roots. Physical collection follows the existing GC/reader owners. | Queue writer/CAS and content reservation/catalog root machinery; cross-owner ordering still needs review |

These are ordered durable operations, not a claim of atomicity across queue,
content and replica-metadata stores. In particular, the payload/GC join needs a
defined causal-cut protocol: new references after a captured root cut must protect
their bytes, and release must compare the same exact terminal certificate and
dependency generation under the owning writer. Existing content reservation
epochs and catalog causal cuts provide mechanisms to reuse, but currently do not
prove that every unresolved queue row is a durable root. The lock order,
reference enrollment/retirement API and crash recovery across these owners must
be closed before implementation. A new queue Boolean cannot solve that join.

Steps 6–7 propose a **new** durable observed-result/application-pending transition.
Current repair success handling publishes the metadata receipt before
`record_owned_repair_terminal_response` and persists completion identity, not the
complete result. Merely extending that final marker would preserve the old order;
the new producer/checkpoint/handler boundary must be designed and sequenced.
Local enrollment in step 3 cannot be consumed as the survivor-based **Custody
ready** state below. The first same-producer slice supplies no step-4 certificate.

### Effect-specific terminal evidence

| Effect | Terminal material needed by this target | Existing distinction / remaining gap |
|---|---|---|
| StageBlob / StagePatch | Exact correlated Durable/Committed content result, original head and full inline/attachment or patch descriptor/bytes until settlement | Stage storage returns Durable after binding bytes; it does not settle a separate promotion. Patch identity/ranges must remain sealed. |
| RepairReplica | Exact receiver Committed result plus durable content/replica-cache settlement evidence; sender receipt/application may remain pending | Receiver returns Committed after its cache commit. Current sender handlers also accept Durable as receipt/transfer progress; that alone is not the stronger full-repair terminal certificate proposed here. |
| CommitVersion | Exact matching Committed promotion result; Durable remains pending. Not-desired and repair-pending/pruned dispositions require their own authoritative evidence and retained result. | Detached promotion classification already distinguishes these outcomes in `node_transport.rs`; generic receipt handling is not its settlement boundary. |

Current receiver content receipts do not retain every original request digest and
exact response. A proposed operation-bound terminal record belongs with the
existing receiver content/replica writer, not a KV ledger or a receiver-authored
FullShard metadata mutation. The latter could introduce an authority/circular
dependency before content staging finishes. Define crash recovery across durable
bytes, replica-cache effects and exact response publication explicitly. Receiver
idempotency/terminal lookup remains a protocol dependency, not an assumption that
two successful local writes were atomic.

### Paged coverage and activation

Extend the existing queue cut/page mechanism. A versioned cut must bind queue
incarnation, snapshot/log generation and head sequence; relevant metadata prefix;
custody schema; exact indexed row/slot set; dependency generation/root; raw-entry
and byte bounds; continuation identity; and page digest chain or equivalent
authenticated indexed proof. Every pending original and result/payload reference
must be covered. Reject mixed cuts, missing terminal pages and references without
retrievable material. A root hash, total count or loaded first page is insufficient.

In the future admitted regime, enrolling/updating an effect must preserve the
coverage root through the same decided history and durable dependency protocol.
That pre-established certificate can cover unhydrated backlog pages at takeover;
the successor need not fetch every row or object to certify what was already
durably covered. Current V1 cooperative pages require the retiring owner and
contain only descriptors. They are not that certificate. The multi-slot protocol
must define root creation, continuation after new work and survival of compaction;
this document supplies no replacement majority protocol.

### Historical first implementation proposal

This records the boundary before the R38 same-producer implementation. Its
source-release dependencies and lack of an implementation were facts at that
time; subsequent native DATA work and current verification status are summarized
above. The requirement for an integrated persistence/authority contract remains.

The first candidate is same-producer background RepairReplica restart custody:
the DFS-owned stored-row extension in the **existing** queue, complete original
head and durable input root before dispatch, complete observed result before
sender release, and exact reconstruction or explicit missing/fenced dependency
after restart. It keeps current wire semantics and FullShard unchanged. It cannot
claim Stage/Commit migration, receiver replay, successor authority or abrupt loss.

No safe independent production patch was ready at that checkpoint. Making that candidate real
crossed then-reserved producer/dispatch/completion and content/GC integration, changed
queue snapshot/log/handoff compatibility and every row-rewrite/removal path, and
needed the durable root join above. An isolated codec/type would have been
disconnected scaffolding. The reviewable result for that milestone was this
record/persistence design plus executable target traces. The later implementation
does not remove the unresolved survivor-custody and executor/root joins.

These states name obligations; they introduce no second production queue or
parallel protocol. The actual storage/wire versions and owning writer gates need
review before implementation.

| State | Evidence that must remain recoverable |
|---|---|
| Recorded and definitely unsent | Exact identity/body ownership and evidence that no attempt crossed dispatch. Absence from a local queue or one recent send failure is insufficient. |
| Locally recorded | Complete local head/dependency/result material is synced for same-producer restart. This is not survivor coverage and cannot satisfy an abrupt-loss activation gate. |
| Custody ready | Complete material required for this original request is durably recoverable from the surviving set under the reviewed recovery protocol. Its immutable root binds identity, body, payload dependencies and destination. |
| Dispatched and unresolved | The same custody plus all unresolved delivery identities/uncertainty. Cancellation or sender loss does not release it. |
| Receiver terminal, observed locally | An exact correlated terminal result exists, but survivors may still know only the unresolved request. Raw-body release remains forbidden. |
| Survivor terminal | The exact terminal identity/result/evidence and dependency state are durably recoverable under the recovery protocol. |
| Compacted terminal | Raw material may be released only after installing the retained exact replay/rejection witness and satisfying every dependent reference. |

Dispatch must consume custody-ready evidence. The protocol must define how
receiver epoch checks, distributed fencing and already-issued permits/messages
order effects. A local writer gate alone cannot revoke A across a partition.
Any permitted late original effect still needs complete custody and preserved
history, consistent with delayed old proof in the accepted-bundle target. The
receipt that proves body durability must not be confused with evidence that the
effect itself completed. A timeout can schedule reconciliation; it grants neither
promotion nor a new request identity.

A surviving terminal certificate is a possible compaction boundary, not an
exception to custody before dispatch. It must bind the complete-body digest,
original identity, receiver/target, effect kind, relevant content/version identity,
canonical result, sufficient surviving result material for exact replay, and
actual terminal evidence. An output hash naming A-only result bytes is insufficient
when the promised response requires those bytes. A terminal rejection must settle
the original effect; a stale-epoch or retryable rejection of a later envelope
cannot clear an earlier possibly-applied attempt. A locally observed response on A
does not become survivor-terminal merely because A intends to replicate it.

Each effect kind needs a settlement rule. For example, terminal durable staging
can settle a particular Stage command without settling a distinct CommitVersion
command or proving caller-local Live publication. A metadata commit does not
settle every later repair, promotion or detached response. An effect may remain
unresolved after another effect in the same operation has settled.

## Crash-safe compaction and bounded activation

The proposed release ordering is:

1. Persist the exact survivor-terminal replay/rejection witness under the recovery
   durability rule.
2. Prove no unresolved preparation, child effect, response reconstruction or
   retained-version/storage promise still requires the original body or payload.
3. Under the owning writer/transaction gate, compare the same exact certificate
   and dependency generation, then atomically install the compact index/witness
   and release only its now-unneeded references. If dependencies changed, retain
   the body and retry. Persisting terminal evidence in step 1 is distinct from
   this reference/index transition. A crash may retain both body and witness,
   but must never leave neither.
4. Reclaim physical bytes through the normal reference/GC owner after readers and
   remaining references permit it.

Deleting the compact witness is a separate retirement decision. A certified
identity/epoch rejection floor or another retained exact outcome source must
prevent an old message from becoming a fresh mutation after receipt expiry.
Age, current head equality or a newer authority alone is insufficient.

Existing source offers precedents, not abrupt-loss guarantees:

- `DfsNamespaceRequestReceipt` in `metadata_records.rs` retains original requester,
  request/operation identities, command digest and terminal result after request
  body compaction.
- `metadata_history_gc.rs` protects exact receipt/prepare dependencies; its
  `metadata_history_gc_retains_terminal_body_until_exact_pending_request_settles`
  control requires a pending request to settle before collecting its prepare.
- `DfsPreparedOperationEviction` in `metadata_records/prepared_operation.rs`
  retains an exact terminal digest/reference and requires certified authority-
  epoch retirement rather than age for its own retirement.

Those rules do not establish survivor durability for detached outboxes or their
payloads. Existing bounded namespace receipt windows must be traced to their
actual replay/rejection semantics; they are not a generic safe expiry rule for
new effect custody.

Activation requires a durable, indexed coverage cut for unresolved obligations,
not hydration or execution of the entire backlog. Recover a bounded active
frontier and checkpoint continuation. Covered but unhydrated unresolved bodies
may remain pending under their original identities; never-dispatched work may
await unavailable content under the stated unsent proof. A potentially applied
request whose required body is not survivor-recoverable is a coverage failure.
Parking that row does not satisfy activation's custody requirement. A target
design must prove coverage before
activation even when it defers loading individual pages. Queue length, a capped
page or a hash root without retrievable exact material cannot establish coverage.

## W=1 and payload custody

A completed W=1 object may have its exact terminal result survivor-durable while
its committed content bytes exist only on A. After justified request compaction,
A's loss makes that content unavailable; unrelated objects may still proceed.
Completion of a parent write does not release payload needed by an unresolved
detached child. Every such dependency must independently permit release. The
result certificate preserves the outcome but creates no content holder,
receipt or extra N copy.

An unresolved request is stricter. If inline or attachment bytes form part of
its complete original body, they must be survivor-recoverable before ambiguous
dispatch. Retaining only a descriptor/hash for A-only bytes does not meet that
requirement. Any proposed separation of complete control body from externally
unavailable payload needs an explicit reviewed recovery contract; it must state
what recovery can actually finish. This boundary is unresolved and is not relaxed
here. Temporary payload custody for active effects has a real storage/transfer
cost to measure, distinct from migrating every object during takeover.

A never-dispatched repair waiting on unavailable A-only content can remain pending
without blocking unrelated authority activation, provided it is not falsely
certified custody-ready or sent. This does not erase its desired-N deficit or
recorded A copy.

## Required negative traces and execution mapping

The physical persistence/transport cases below remain required and unexecuted.
The separate target checks described afterward execute their abstract guards;
they do not execute these native failures. Neither the 31 one-slot accepted-bundle
checks nor the native rejection/retry control covers this custody protocol.

| Cut or adversarial trace | Required outcome / affected boundary |
|---|---|
| Dispatch before survivor body/payload persistence; A crashes | Dispatch is rejected before effects; real disk/transport cut must show no unowned uncertain request. |
| Only a tuple/digest survives; A held the sole body | Coverage/activation cannot claim complete custody; no reconstructed body or new identity. |
| Receiver completes; reply is lost; A crashes | Recover exact result or preserve unresolved ownership under the original identity. |
| Terminal response reaches only A; A crashes before survivor persistence | Raw-body release is rejected; survivors retain the unresolved material. |
| Survivor-terminal witness persists; crash before body release | Replay the exact result without a second effect; extra retained bytes are safe. |
| Body release precedes witness persistence; crash in between | Forbidden gap is detected by the durability protocol, not hidden by an in-memory flag. |
| Later definitely-unsent attempt follows earlier possibly-applied attempt | Earlier uncertainty remains until exact reconciliation. |
| Stage result is offered as CommitVersion completion | Reject effect-kind mismatch even if content/version tuples match. |
| Compaction drops an attachment needed by a child or retained version | Reference/dependency gate prevents release and survives restart. |
| Expired receipt followed by delayed original command | Exact replay or explicit certified rejection, never fresh mutation. |
| Successor loads only the first custody page | Continuation/coverage proof prevents claiming the omitted tail settled or absent. |
| Old executor races custody-ready dispatch against a higher fence | Receiver checks and the reviewed distributed ordering govern acceptance; already-issued late effects retain original custody/history. A local gate alone cannot establish remote revocation. |

Model checks can establish specified bounded state transitions, but actual native
and disposable process/transport/disk cases must cut persistence, reply and
dispatch boundaries. Required evidence includes exact source/binary/request IDs,
retained files before/after restart, effect count and unchanged acknowledged
results. Transfer volume, takeover latency and Sinomo-only quietness use the
standard performance tables; unavailable counters stay explicit.

Further review must close the exact owning DFS records, complete payload-retention
rule, effect-specific terminal certificates, actual writer/fsync gates and bounded
coverage/retirement scheme for each implementation slice. These are correctness
dependencies, not cross-worktree source-release gates. This draft does not justify
a FullShard count change, W/N relaxation, KV metadata ledger or production action.

## Executed custody target and its bounds

The four new draft files reuse the existing Quint contract/model/check/scenario
workflow:

- `specs/quint/contracts/dfs/availability_effect_custody.qnt`
- `specs/quint/models/dfs/availability_effect_custody.qnt`
- `specs/quint/checks/dfs/availability_effect_custody_target.qnt`
- `specs/quint/scenarios/dfs/availability_effect_custody_witnesses.qnt`

They deliberately remain outside registered `*_safety.qnt` conformance inventory.
The maintained inventory/help check passes with them present; the existing
FullShard case is unchanged. This target is not composed with the accepted-bundle
target into a complete history/promise/decision/custody protocol.

Actual R35 execution passed **41/41 controls**, one sample each, TypeScript
backend, initial seed `0x51a0f04d20260710`, process timeout 20 seconds plus 2-second
kill grace. Wall time was 2.410 seconds; largest-child RSS was 234,772 KiB,
not aggregate memory or a compiler measurement. There are 41 retained ITFs, at
most 36 states including initialization (35 transitions). These are finite
prescribed traces, not exhaustive finite-state verification. Repeated actions
can increase the revision counter without bound.

The same exact source ran five deliberate unsafe **test-only** counterexamples
in R36: dispatch without custody; surviving body deletion before terminal witness,
then A loss; stale dependency release; fresh execution of a retired identity; and
activation after only the first page while Child is in the cut. All five failed
their safety assertions as expected (actual QNT508 errors, exit 1, wall2.009s).
They are checker-sensitivity evidence, not reachable correct-model or current-Rust
counterexamples. Quint emitted no failed ITFs; original assertion output and
source are retained, and passing sensitivity traces expose the corresponding
bad states. Do not invent failed trace files.

`data/operations/dfs-availability-20261023/CUSTODY-TRACE-CATALOG-R35.json`
records every case/ITF/hash, the exact shared positive/negative source and limits.
`custody-final-witness-r35` and `custody-mutant-counterexamples-r36` retain full
commands, logs and source snapshots. R30's unsupported-operation/reserved-name
typecheck failure remains; R31/R32 typechecks and earlier R33 34/0 execution are
historical source qualifications, not current extra case counts. Final R34
typecheck passed before R35/R36.

| Obligation | Representative executed case / assertion |
|---|---|
| Original head and attachment must persist before dispatch | `attachmentMissingPayloadArmDisabledTest`, `attachmentMissingPayloadRootDisabledTest`, `digestOnlyHeadCannotCertifyTest`, `completeAttachmentDispatchTest` |
| Inline material differs from a hash/descriptor | `inlineHeadContainsBodyTest`, `inlineDigestCannotBindTest` |
| Lost reply then A loss keeps original uncertainty and one reachable exact holder | `lostReplyThenOwnerLossTest`: one B holder, original claim, one receiver effect, exact result replay |
| Local terminal is not survivor persistence | `localTerminalCannotCheckCompactionTest`, `localTerminalThenLossRetainsUnresolvedTest`, `oneTerminalCopyCannotCertifyTest` |
| Exact result material/disposition must stay bound | `resultDigestIsNotResultMaterialTest`, `changingObservationCannotRelabelStoredResultTest`, same-kind Durable versus Commit/Repair controls |
| Receiver settlement differs from sender application | `survivorTerminalStillNeedsLocalApplicationTest`, `localApplicationReleaseSensitivityTest` |
| Later NotSent/rejection does not erase earlier uncertainty | `laterUnsentCannotClearEarlierUncertaintyTest`, `sentAttemptCannotBecomeDefinitelyUnsentTest` |
| Completed parent does not release unresolved child input | `parentW1CannotReleaseChildPayloadTest` |
| Missing A-only input may remain definitely unsent without blocking the custody gate | `aOnlyUnsentPayloadAllowsUnrelatedCustodyGateTest`; this is input-payload availability, not W=1 object-write proof |
| Witness-before-body-release survives owner loss | `terminalWitnessBeforeBodyReleaseCrashTest`; opposite ordering fails after a causal A loss in the deliberate mutant |
| Exact dependency generation and live references both matter | stale-check, ABA, fresh-head-dependency and separately isolated sensitivity cases |
| Delayed original cannot become fresh mutation | `delayedOriginalAfterCompactionAndRetirementTest`, `witnessRetirementWithoutFloorDisabledTest` |
| Coverage includes Child and every certified page, while hydration can stay bounded | `incompletePageCoverageBlocksGateTest`, `coverageDoesNotRequireBacklogHydrationTest`, `mixedGenerationPageRejectedTest`, `newObligationInvalidatesCompleteCutTest` |

Fixed data bounds are three custody stores, two original effects, two transport
attempts per effect, one shared input, per-effect exact result aliases and two
indexed pages. Effect 1 is Stage or Commit by fixture; Effect 2 is Repair.
One child is admitted/settled, with dependency generations 0/1/2 and no wraparound.
Only A's availability loss is exercised. Full exact aliases include original
identity/body bytes; inline bytes are part of the head, attachments are separate.
The two complete co-located copies model a pre-dispatch durability requirement,
**not** metadata authority, content W or a change to current FullShard. After A
loss, one reachable exact holder is sufficient for original material recovery.
Split head/payload retrieval is outside this target.

Recovery/terminal/retirement certificate issuance, exact receiver settlement and
idempotency are assumed primitives. The model does not implement receiver RPC
lookup, receiver-node failure, actual codecs/digests/fsync or partial disk writes.
`localApplied` specifically means a **survivor-recoverable durable application
fact**, not a volatile cache update or A-only marker. If the native result is
only local/volatile, the head must remain rooted for reconstruction. Coverage
also checks this pending-application head dependency. Actual distributed fencing,
multi-slot ordering and liveness remain separate obligations.

The W=1 parent marker is an assumed external completed write with a terminal
prerequisite; there is no object receipt/holder state or canonical write protocol
in this target. The A-only test retains an unsent Child's head on B while its
input is unavailable on A. It does not certify full payload custody, actual
unrelated metadata availability or the required W=1 outage acceptance.

Reproduce the small checks without installing a backend or invoking a solver:

```bash
quint typecheck specs/quint/scenarios/dfs/availability_effect_custody_witnesses.qnt
timeout --kill-after=2s 20s quint test \
  specs/quint/scenarios/dfs/availability_effect_custody_witnesses.qnt \
  --backend=typescript --max-samples=1 --seed=0x51a0f04d20260710 --match='Test$'
# Intentionally exits 1 with five safety assertion failures:
timeout --kill-after=2s 20s quint test \
  specs/quint/scenarios/dfs/availability_effect_custody_witnesses.qnt \
  --backend=typescript --max-samples=1 --seed=0x51a0f04d20260710 --match='Counterexample$'
```

Input seed and Quint's suggested per-case repeat seeds remain in the original
outputs. No native build, solver, heavy slot or Docker/process outage was used.
R25/R29 and the f520a20d Rust inputs remain unchanged by this milestone.

## Historical local Repair implementation correspondence (R38)

This table preserves the initial candidate and its then-open checks. Later R54,
R62, R67 and DATA panels have separate exact-source evidence; no pass is transferred
to R8. Later references to R42 describe the conservative successor restriction.

Frozen Rust manifest `472df6abda43003a49cd66115da55664a6cf5bd71a582274170d9cec49e8daed`
is the first integrated local implementation candidate. It is UNCOMPILED and
UNEXECUTED at this checkpoint. The R35/R36 target-model source and results remain
unchanged. Neither those results nor historical R25/R29 qualify this candidate.

| Target obligation | Concrete candidate owner and persisted material | Remaining qualification |
|---|---|---|
| Complete original head before dispatch | `service_outbound::prepare_replica_attachments` captures the actual current background Repair command; `repair_queue_store::RepairOriginalBirth` stores the complete routed original, attachment descriptor and immutable root. Named SOBJ digest input binds producer/target/command/descriptor; carrier IDs remain separately recorded envelopes. | Only same-producer attached current Repair; inline, Stage, Commit and survivor export are outside this slice. |
| Retained body dependency | `LocalContentStore::capture_repair_payload` owns source/reader/root capture under the existing content writer and reservation lock. `InlineOperation::RetainRepairPayload` uses the existing synchronized binding log. The root stores captured manifest/extents or blob-only hash/size plus physical store incarnation. | Capture validates binding/layout/length, not a fresh full-byte hash audit; receiver integrity checks remain necessary. No survivor body persistence claim. |
| Root before queue; uncertainty before effects | Queue incarnation is initialized under the native queue writer flock. Enrollment appends the full original through existing `DurableFileSync::All`; original attempts are appended before transport handoff. The callback checks the actual head/attempt/live root before carrier polling. | Abrupt process/power-loss cuts and uncertain-fsync injection controls still need execution. Orphans remain retained. |
| Unknown delivery is not fresh work | Per-carrier attempts preserve raw observations; a later NotSent attempt cannot erase an older unknown. First enrollment rechecks legacy uncertainty under the queue writer and during replay. Anonymous None stays unknown even with zero attempts. | Original head/body cannot be reconstructed from a V1 scheduling row. Existing native prepared enrollment supplies explicit NotSent births; derived queue snapshots do not. |
| Observed result is not application | `handle_repair_replica_response` records complete exactly correlated response material before restoration/application. The observed-pending index survives queue snapshot/replay and supports bounded visits in existing maintenance. | Non-Committed dispositions retain custody until their effect-specific settlement is separately reviewed. No receiver terminal-cache/replay implementation. |
| Exact durable application | `already_applied_original_repair` requires actual `publication_execution.current().admits(...)` for the original physical metadata shard, exact persisted receipt/hash and the location effect if that exact version remains visible. R42 removes the earlier stored-response publication fallback; missing evidence remains pending because a later withdrawal is indistinguishable from never-applied without durable causal evidence. | A retained provider, receipt count, absent job, timer or current-location list alone cannot mint application proof. Missing/collected application evidence stays pending; newer heads are never rewritten merely to settle an old effect. |
| Irreversible settlement before root release | Only private `AppliedOriginalRepair` can enter `close_applied_original_repair`; the existing queue log atomically stores closure and a compatible completed scheduling projection. Public raw custody appends reject. Idempotent closed retries reuse the original closure. The content owner then writes the exact release tombstone after dropping queue locks. | Cross-store closure/release crash windows and native admission-negative controls remain unrun. This is not survivor settlement or full metadata commit evidence. |
| GC/compaction and delayed originals | Current content roots join ordinary GC roots; extent/hash indexes veto deletion. Bounded content compaction preserves live/released root records; queue compaction preserves originals/results/closures. Closed request identities and released roots cannot reopen. Actual owned readers retain existing generation/file/store leases. | Prepared controls include GC/reopen/compaction and an owned-reader storage-state release race; real delayed transport/crash acceptance is outstanding. |
| Bounded progress and incomplete coverage | Indexed pending originals are visited within a fixed page bound independently of retry-row existence, but actual admitted scope is still required. Pending visits do not consume ordinary send capacity. Retry DATA tokens are separate from the combined physical log head; current custody is validated under the writer. | No namespace-wide body migration or backlog activation barrier is introduced. Missing historical coverage is retained, never interpreted as completion. |

The first verification request is `346a9a9a-f5a8-4675-8ccb-eab562a0d172`, sent
once through supported noninterrupting Orca coordination. It requests typecheck
first and only the 17 listed local controls afterward if the bounded window
permits. A source review closure is permission to attempt verification, not a
Rust, durability, performance or availability verdict.


### R41 execution and R42 supersession

R41 on corrected R40 manifest de0fae4d actually passed library/test-target typecheck
and libtest build, and executed all17authorized exact cases:16local queue/root
controls PASS,1native fixture FAIL during seed maintenance before custody assertions.
Tests used tmpfs/in-process native fixtures; no physicaldisk/process-crash or
availability acceptance. Source, sealed executable29833a20, each case result,
failed native archive and full drain/resource receipts are preserved in the operation
directory. R39's earlier fixture-import typecheck failure stays separately FAILED.

After the bounded window and verified drain, R42 corrects the native seed to the
maintained registered-Main write/sync contract with exact receipt/binding checks,
without changing maintenance's5s limit. R42 also removes the unsafe original-result
publication fallback described above. R42 is NEW UNCOMPILED/UNEXECUTED source;
R41 results must not be transferred to it. The exact causal trace and reserved
metadata-owner dependency are recorded in dfs-repair-custody-root-join.md.


### Historical qualification after R50

R49 frozen source `af23248187c65ac38050d1127cb9f8adfcd2f961a34d821e805439a40115b335`
contains the conservative stored-positive restriction, open-birth retirement
isolation, and same-production paired Core/Main seed preparation control. R48
failed typecheck on test JSON serialization, preserved as FAILED101 with zero
executed cases. Fixture-only R49 replaces the non-serializable stat projection
with its four immutable fields. Actual R50 library/test-target typecheck passed
in14.977404seconds with all4,899inputs stable. No codegen or case ran because the
absolute latest codegen start had passed before that amendment arrived. All
nineteen current cases, including two sequential paired subarms, remain UNRUN;
no current executable or native custody acceptance exists.

Both arms keep the same current production/configuration/effectiveW1N2/options,
27bytepayload and five-second caller deadline before write. Core success is
allowed; registered Main must prepare within budget. Failures and both actual
owner drains are recorded, paired fixtures preserved unconditionally, and all
post-seed custody assertions remain intact. Current production differs from
R40/R41, so a passing pair cannot attribute the historic failure solely to setup.
Exact source, case/runner seals and standard evidence tables are under
`data/operations/dfs-availability-20261023/CONTROLLED-SEED-COMPARISON-R47.md` and
`REPAIR-CUSTODY-R50-REPORT.md`. One NEW bounded window has been requested on the
same frozen Rust; no old grant is reused or new Rust delta added.

At R50, conditional source ownership for three cooperative-handoff subsets did not
close the V2 coverage/executor/root contract. Subsequent selected native DATA work
does not settle checked successor authorization/correlation or physical holder
retirement. Those remain protocol dependencies under the independent development
model. Stored-result causal application versus later withdrawal remains a separate
integration blocker.
