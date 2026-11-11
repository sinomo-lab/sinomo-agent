# Original-effect custody before abrupt node loss

This target separates local restart material from survivor-durable custody.
An implementation must preserve complete original effects before dispatch can
become ambiguous. The obligations below do not certify the current Rust paths
or authorize a metadata-quorum, content-policy or receiver-authority shortcut.

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

## Proposed durable states and gates

### Repair is outside original-effect custody

Repair needs no original-effect custody. A replica below desired N is a need in
the replicated shard tables, and the shard primary's repair executor derives its
work from them. A successor primary derives the same work after takeover, so
recovery never depends on the retiring owner's repair state. Each repair attempt
carries the receipt-slot generation it was planned against, and a reply
publishes only through that generation's compare: a late or duplicated reply
from an older attempt cannot publish. Retry timing restarts at the successor.
The obligations in this document apply to Stage/StagePatch, CommitVersion
promotion and client delivery; RepairReplica is not among them.

### Candidate records

The wire command types live in `sinomo-dfs`; embedding them in a shared
`sinomo-api` scheduling record would invert that ownership boundary. Custody
records belong to a DFS-owned store whose writer enforces preservation itself:
scheduling-only updates replace only a projection, and custody removal compares
the exact terminal witness and dependency generation. Caller discipline is
insufficient. The owning store for the remaining effects is not chosen yet.

| Candidate field group | Exact retained contents | Required owner behavior |
|---|---|---|
| Format and birth | Explicit custody schema/codec version, store incarnation/generation and actual append sequence; never timestamp-only identity | Sequenced snapshot/log with disk-generation observation |
| Original identity | Effect kind, original wire request ID, intent/operation, filesystem/object/version, target, original logical requester/coordinator and producer | Capture from the already prepared command and producer; do not infer omitted fields from the current head |
| Original head | Complete immutable encoded domain command, encoding version, route/category and digest; original base/patch predicates, metadata, authority assignment and flags stay sealed | Captured from the actual `DfsRoutedSend` before detachment; the current transport source/envelope stays separately classified |
| Inline material | Original inline bytes remain inside the exact command encoding | Same record if it fits the store's frame bound; no replacement with a hash or a different attachment command |
| Attached material | Exact descriptor (size/hash/layout/ranges/encoding) and a durable root for those exact bytes, including object/version/patch binding | Existing content store and reservation/catalog owner; a reader lease or descriptor is insufficient |
| Physical dependency | Store incarnation, physical generation, owner-minted reservation causal stamp and root identity | `ContentStoreReservationRecord` / `ContentStoreCausalStamp`; serving epoch is separate |
| Delivery | Original logical transfer ID plus retained unresolved attempts, joined uncertainty and exact completion correlation | A later definitely-unsent/rejected envelope cannot clear an older possibly-applied attempt |
| Observed terminal | Exact encoded/typed result and digest, captured disposition, original-request digest, authenticated peer/context and actual receiver terminal reference when available | Existing correlation handler; missing receiver durable evidence remains a gap, not a synthetic certificate |
| Local application | Terminal recorded versus receipt/location/cache application pending versus settled; retained client-response reconstruction dependencies | Existing admitted completion handler; receiver settlement, sender application and client delivery stay distinct |
| Dependency release | Monotonic dependency generation, exact users/roots, compact-witness identity and checked generation | Store CAS plus the existing content-root/GC owner; stale checks retain material |

A promotion obligation needs its own identified slot with explicit
transfer/promotion dependencies; a mutable kind field that ordinary cleanup can
remove would lose custody.

Arbitrarily large original inline commands cannot simply be copied into a
bounded log frame. Their representation needs either an existing, explicitly
rooted immutable content artifact containing the **exact original encoding**,
or a reviewed framing extension. No synthetic object/version identity, new
silent request limit, rewritten inline command or cap relaxation is selected
here. Until that storage decision closes, oversized inline custody is
unsupported and cannot be certified complete or dispatched under a purported
guarantee.

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

Use an explicit schema and capability admission. Old writers must not reopen a
custody record, strip it through a scheduling update, then acknowledge a lossy
snapshot. Enrollment needs the original living producer or other exact retained
source. Uncertain requests cannot be recreated under new IDs. The full
history/custody admission cut and fail-closed downgrade rules remain
prerequisites to abrupt-loss guarantees.

### Persistence and ownership order

| Step | Required durable action and crash meaning | Concrete current boundary |
|---|---|---|
| 1. Capture | Freeze the complete original head, input descriptor and identity while the producer retains admission. No transport polling yet. | The prepared send before detachment; detached calls currently retain only process ownership |
| 2. Root bytes | Persist the input/root under its real content owner before custody enrollment. An orphan root after a crash is safe; it grants no dispatch or new request identity. | `ensure_content_reservation`, content mutation/reservation guards and owner-minted causal stamp |
| 3. Enroll locally | Append the complete stored record and dependency reference through the owning writer, with full sync. This establishes only **locally recorded** restart material. A failed/uncertain append cannot authorize dispatch; replay/resync its tail first. | The owning store's writer, with `DurableFileSync::All` and tail resync on load |
| 4. Cover survivors | Before effects become ambiguous, the reviewed DFS history/custody protocol must certify exact complete material and unresolved delivery ownership on the surviving set. Local fsync alone does not do this. | New integration into the reviewed multi-slot protocol; current cooperative handoff cannot supply it after owner loss |
| 5. Claim and send | Revalidate the exact record, dependency generation and current authority; durably install conservative attempt uncertainty before the carrier may dispatch. Then release the existing owned head/reader to its normal transport worker. | A before-dispatch claim under the store's CAS, the native authority gate and networking path; exact origin/executor joins still require implementation and verification |
| 6. Record result | Correlate original source/destination/request/intent/filesystem/object/version/hash/kind; persist exact observed terminal material before releasing sender completion ownership. | The effect's response-identity correlation and admitted reply handler |
| 7. Apply locally | Retry the original result through the normal handler until its receipt/location/cache effect is durably settled. Persist “terminal recorded, local application pending” across restart. | `runtime_host/message_host.rs` background response handling, then the effect's own reply handler; the current outbox retains head/response only in memory |
| 8. Compact | Persist the exact outcome witness, recheck dependency generation, install the compact record/index transition, then release only unneeded roots. Physical collection follows the existing GC/reader owners. | The owning writer/CAS and content reservation/catalog root machinery; cross-owner ordering still needs review |

These are ordered durable operations, not a claim of atomicity across the
custody, content and replica-metadata stores. In particular, the payload/GC join needs a
defined causal-cut protocol: new references after a captured root cut must protect
their bytes, and release must compare the same exact terminal certificate and
dependency generation under the owning writer. Existing content reservation
epochs and catalog causal cuts provide mechanisms to reuse, but currently do not
prove that every unresolved custody record is a durable root. The lock order,
reference enrollment/retirement API and crash recovery across these owners must
be closed before implementation. A new Boolean flag cannot solve that join.

Steps 6–7 propose a **new** durable observed-result/application-pending transition;
the producer/checkpoint/handler boundary must be designed and sequenced. Local
enrollment in step 3 cannot be consumed as the survivor-based **Custody ready**
state below. A same-producer slice supplies no step-4 certificate.

### Effect-specific terminal evidence

| Effect | Terminal material needed by this target | Existing distinction / remaining gap |
|---|---|---|
| StageBlob / StagePatch | Exact correlated Durable/Committed content result, original head and full inline/attachment or patch descriptor/bytes until settlement | Stage storage returns Durable after binding bytes; it does not settle a separate promotion. Patch identity/ranges must remain sealed. |
| RepairReplica | None: repair is outside custody. The receiver's receipt publishes only through the receipt-slot generation compare; the need stays in the shard tables until then | A late reply from an older attempt cannot publish; the successor's executor re-derives the need |
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

The custody store needs a cut/page mechanism. A versioned cut must bind store
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
durably covered. Cooperative handoff requires the retiring owner and is not
that certificate. The multi-slot protocol
must define root creation, continuation after new work and survival of compaction;
this document supplies no replacement majority protocol.

### Recovery-state obligations

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

A repair need whose content is only on unavailable A stays in the shard tables
without blocking unrelated authority activation; it needs no custody. This does
not erase its desired-N deficit or recorded A copy.

## Required negative traces and execution mapping

The following physical persistence and transport cases require native evidence;
abstract guards do not exercise these failures.

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
