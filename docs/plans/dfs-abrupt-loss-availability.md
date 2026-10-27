# DFS abrupt-node-loss and reconnection availability

The required behavior is one safe metadata history through abrupt node loss,
with content W kept separate from desired N and metadata authority. This plan
specifies target protocol obligations and validation boundaries; it does not
certify implementation or authorize deployment. Source-specific executions,
failures, original operation identities and retained DATA belong in task receipts.

## Protocol constraints

The baseline journals `PrepareOperationV7` with the complete sealed operation
and publishes using `CommitPreparedOperationV7` with its exact digest. Ordinary
metadata publication and sync require every configured participant. Cooperative
rotation requires the old primary's durable freeze, exact prefix, both assignment
majorities, both primaries and its complete original repair custody cut. That
protocol deliberately cannot activate after abrupt loss of the source.

1. Exactly one ordered metadata history and serving authority. A timeout or
   connectivity change requests an attempt; it never grants promotion. Epochs,
   vote promises/fences, prepares, exact commit evidence and unknown outcomes
   survive process loss and reconnect.
2. Metadata replication quorum, content W and cluster-control quorum are three
   independent proofs. No isolated replacement of a full participant count by
   a majority; no operation-bound KV metadata permit as a shortcut.
3. Cluster authority orders genuine topology/epoch changes through existing
   guarded reads and transactions. DFS journals, request bodies, intents,
   receipts, completion evidence and delivery custody remain DFS-owned.
4. Desired N survives temporary unavailability. A W=2,N=3 eventual write may use
   B/C receipts with A unavailable only after metadata authority is valid. It
   retains N=3 and durable repair obligations. Required nodes, pins, residency
   and confirmed-replication requirements remain hard constraints.
5. An existing exact version needs one reachable authorized valid holder for a
   read. It does not need W reachable copies. Missing A-only W=1 bytes affect
   that content, not unrelated objects or all metadata activation.
6. Abrupt recovery must find complete original effect identities/bodies and
   unresolved delivery/completion evidence on survivors before dispatch can
   become ambiguous. No uncertain effect is recreated under another identity.
   Advisory scheduling remains local. Activation cannot require replaying an
   entire backlog or copying all object bytes.
7. Serving-authority epoch and physical store generation are distinct. A checked
   authority rebind may reuse the store and indexes while fencing old writes;
   actual replacement retains the stronger physical-generation fence.
8. Keep offline membership and recorded copies. Rejoin compares exact prefixes
   and immutable identities, transfers only missing bounded history/content,
   and does not automatically fail back to the original primary.

## Protocol design obligations

The first protocol review must specify durable state and transitions before
changing metadata admission. Candidate design: retain a fixed three-member
metadata assignment; durably persist epoch-bound promises on an intersecting
metadata majority, select a survivor using existing cluster authority, recover
the accepted history and original effect custody from those survivors, and
activate only with an exact epoch/prefix certificate. This is a proposal, not a
new authority path. In particular, a fresh KV epoch alone cannot certify an
old prepare or erase an uncertain result.

Review must resolve all of the following:

- How every acknowledged and possibly committed prepare/commit is preserved
  when the old primary cannot freeze or report its tail. Prefix length alone
  is insufficient; exact digest/slot/epoch identity and quorum intersection
  must govern recovery, including a prepare accepted on only part of a group.
- Which durable promise prevents a live partitioned old primary completing
  further writes, and how prepare, commit, sync, replica ingest and restart
  validate that promise. Every publication path must share the fence.
- The precise commit certificate, when it is durable on an intersecting set,
  and what a lost final reply means. Recovery must not choose a conflicting
  value or infer commit merely from a prepare or a larger sequence number.
- How changes between the current full-participant protocol and the new
  protocol are capability-gated, and how metadata membership transitions keep
  overlapping quorums. Existing cooperative handoff remains supported.
- How original transfer bodies/identities, attempt/delivery uncertainty and
  terminal evidence become survivor-durable before effects. Recover a bounded
  active frontier first, retaining the backlog without gating all activation.
- How a checked authority rebind reuses the exact store, index and matching
  prefix while stale handles remain unable to author or dispatch effects.

Models must be classified as implemented refinement or target hardening, mapped
to Rust transitions, with case IDs, bounds, seeds/samples and required witnesses.
A model pass is not real-process, transport or disk evidence.

### Target transitions and compatibility gap

These target obligations do not establish executable protocol acceptance:

| Transition | Durable obligation / rejection rule |
|---|---|
| Reserve | Existing cluster authority orders one exact reservation, unchanged metadata assignment, nominated successor and successor serving epoch. Route failure only requests this action. |
| Promise | Under the same participant writer/acceptance gate as old acknowledgements, persist the successor promise and capture a stable accepted-history/decision/custody cut. Synchronize before replying. Snapshot install, restart and physical replacement preserve this promise. |
| Recover | Compare exact ancestry and accepted values/ballots, not sequence heights. Preserve every operation/result an old proof could cover. A later majority protocol must recover a chosen value when only one member of its old accept quorum intersects the new promise quorum. |
| Decide | Bind exact operation, predecessor, slot, epoch, result and quorum evidence. Before successful new-protocol replies, the exact decision frame must be survivor-recoverable, or a reviewed deterministic construction must seal its exact identity earlier. A prepare alone is not a decision certificate. |
| Activate | Existing cluster authority plus DFS promise/recovered-history/custody coverage authorize a successor. Checked rebind preserves physical identity while changing serving authority. No object-byte migration or backlog execution prerequisite. |
| Rejoin | Persist active epoch/fence before old-node effects. Reuse matching exact prefixes and content identities, transfer missing bounded suffixes and retain offline membership/copy records. No automatic failback. |

A may hold complete old proof before B/C promise the new epoch and receive its
last old ACK afterward. Promises cannot retract issued replies or instantly
freeze A. The successor must preserve the exact old body/result before activation
so a delayed old completion cannot add a competing result. Old acceptance and
promise capture must serialize durably. A fresh promise alone is also not a
read-freshness proof; stale authoritative reads need an explicit epoch rule.

There is a concrete V7 compatibility gap. A commit reference includes its own
`sequence` and `created_at`
(`metadata_records/prepared_operation.rs::DfsPreparedOperationCommitReference`),
and prefix identity hashes the exact encoded frame bytes
(`metadata_records.rs::dfs_metadata_prefix_proof_v2_after_bytes`). A complete
prepare surviving on B/C cannot reconstruct an unknown A-only commit timestamp
or original terminal frame. Preserving its semantic result through an explicitly
linked recovery decision is different from recovering that original frame.
Do not invent it or claim the legacy evidence complete. Protocol admission must
define how an exact certified old cut becomes survivor-durable before claiming
new-protocol abrupt-loss guarantees; an already-lost legacy-only terminal frame
remains an explicit evidence gap until authoritatively reconciled.

Likewise the old sealed payload/digest contains `owner_epoch`, and the current
publication guard requires that epoch. Current cross-epoch recovery constructs a
new body and supersedes the old preparation. A new recovery envelope must retain
the original immutable request/body/digest and separately bind new executor
authority, with an explicit typed authorization/version contract. Rewriting the
epoch does not preserve the old digest.

Before a potentially ambiguous outbound effect, survivor-durable DFS state must
cover the complete original command/body, request identity, source/destination,
content identity, delivery uncertainty and completion evidence. Tuple-only repair
rows and A's local outbox do not provide this. Separate current executor from
immutable origin; a successor cannot impersonate A or issue a replacement request.
Bounded immutable cut roots, indexed pages and reservation-bound checkpoints
provide recovery coverage; hydrate and execute the backlog incrementally after
activation. Missing bodies or uncertain outcomes stay pending. Complete evidence
stays in DFS; KV holds only the genuine compact authority transition/references.

Required negative/race checks before shared protocol implementation: old ACK versus
promise serialization; delayed complete old proof; A-only commit marker; crash
after promise fsync/before reply; ambiguous decision persistence; concurrent
takeovers; a second takeover whose majority intersects the old chosen quorum at
only one holder; custody-before-dispatch; stale reads; downgrade/reopen that would
ignore promises. Existing local acknowledgement floors contain only shard,
sequence and prefix proof and cannot substitute for these contracts.

### Target exact decision identity

The target admits the new regime only after a certified cooperative cut closes
the V7 prefix and complete original-effect custody. Source loss before that cut
leaves admission pending; it does not reconstruct missing legacy frames.

Under the new regime, an immutable accepted proposal bundle contains the exact
prospective prepare and terminal frames, including their sequence/timestamp
headers. The terminal references the prepared-body digest; an outer digest may
cover both exact frames, but neither frame embeds that outer digest. Recovery
adopts those original bytes, response and effect identities unchanged; new
acceptor/executor certification is separate. This resolves the identified
timestamp reconstruction gap for newly admitted bundles only.

Prospective bundles need a separate DFS-owned accepted-state log. They must not
enter ordinary V7 replay as actionable commit markers. Reserve the bundle's
canonical order before acceptance, and finish or recover that slot before a
conflicting successor can be chosen. Current operations may admit durable pending
content work without choosing its final publication bundle; missing A-only W1
bytes must not hold all later unrelated metadata work behind a byte fetch.
The exact storage/order and pipelining refinement remains a review/model gate.

Each acceptance binds assignment, reserved serving epoch, slot, exact predecessor
and full bundle bytes. A participant accepts only one value per epoch/slot and
durably records a higher promise before acknowledging it. A promise cut contains
every acceptance completed before that promise. Recovery selects the highest
accepted ballot/value per slot from the promise quorum and validates exact
ancestry, then reaccepts the same chosen bytes. One member of a previously chosen
majority can be sufficient evidence after quorum intersection; requiring two
matching reports would incorrectly lose that history.

Canonical visibility and successful replies require the exact decision evidence
to be durably recoverable from a metadata majority. A private A-only preparation
not reported by the promise quorum cannot later acquire an old quorum. Retain it
as unresolved/orphan evidence on reconnect; neither merge it as committed nor
infer absence of independent effects. A's delayed old proof may still complete
only the value already preserved by successor recovery.

Latest-head and namespace reads need a fresh metadata-quorum barrier in this
target. Cached old authority or a historical write certificate does not prove a
current head after takeover. Fetching an already-certified exact immutable
version remains a one-valid-holder operation and does not acquire a content-W
availability requirement. A separate lease design is not assumed.

Detached repair, promotion and response commands need their own complete
immutable body/identity/delivery/completion coverage before ambiguous dispatch;
accepting the original metadata bundle alone does not cover later generated
effects. Attachment descriptors bind exact original bytes, length/layout and
retention custody. Missing W1 payload bytes leave the affected operation pending,
without inventing its body or blocking unrelated metadata. The full-body versus
attachment-retention refinement must establish the user's survivor-recovery
requirement; a tuple or hash alone is insufficient. New executor authorization
must not change the logical origin used for receiver idempotency or manufacture
fresh request identities.

Admission/activation requires durable coverage of an immutable history/custody
cut, with bounded indexed pages and checkpointed continuation. It does not require
executing or hydrating every backlog row. Existing tuple-only V1 queue pages are
not a substitute where they omit original command bodies.

Promises, accepted ballots and highest-value adoption constitute consensus.
Existing cluster CAS orders authority reservations but does not prove this new
DFS protocol. Review reuse of suitable existing consensus machinery, or verify
the DFS protocol explicitly with its own DFS-owned storage. Neither choice puts
journals/effect ledgers in cluster KV or duplicates network reachability logic.
Explicit persistence/wire capability admission and fail-closed downgrade/reopen
are mandatory before these semantics can replace FullShard. The target still
requires model/refinement review and real process/transport/disk acceptance.

## Content admission and original-effect recovery

The [content inventory plan](dfs-content-admission-inventory.md) defines the
coherent membership/provider/content inputs and bounded fresh-create scope.
Historical placement grants cannot authorize a new obligation after provider
revocation. Existing copies and offline membership remain evidence to preserve,
not a way to widen new-target eligibility.

The [original-effect plan](dfs-original-effect-custody.md) separates complete
request/body custody, attempts, receiver settlement and caller delivery. Its
[queue/content join](dfs-repair-custody-root-join.md) reuses the existing physical
owners. Local enrollment and cooperative transfer do not by themselves prove
survivor durability before abrupt loss. The [accepted-bundle target](dfs-accepted-bundle-target.md)
records the bounded abstract model and required Rust refinement.

## Acceptance matrix

| Scenario | Required evidence |
|---|---|
| Abrupt A stop; exact committed bytes only A/C, B lacks them | Verify fixture and on-disk bindings before stop; actually stop A abruptly; read exact C version; new B/C W=2,N=3 write; retain A membership/copy facts, desired N and durable deficit. |
| Metadata replica unavailable, primary alive | Remaining metadata majority preserves one ordered history and exact acknowledged results; no content-W substitution. |
| Live old primary partitioned | Attempts on both sides; quorum-side activation; old side cannot publish/ack a divergent history, including restart and delayed messages. |
| Crash/lost reply at prepare, commit and transfer | Cut each durable boundary; recover exact original IDs/bodies, unknown delivery and terminal evidence; exact replay or unresolved status, never fresh identity. |
| Repeated reconnect | Reuse matching prefix/content; bounded missing transfer; membership/copies retained; no churn, wholesale rewrite or failback. |
| W=1 bytes exclusively A, A unavailable | Affected content explicitly unavailable; unrelated readable/writable objects continue under valid authority. |

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
