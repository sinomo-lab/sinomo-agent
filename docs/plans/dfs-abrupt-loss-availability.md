# DFS abrupt-node-loss and reconnection availability

## Current development checkpoint — 2026-10-24

The independent `dev/dfs` lane has local code checkpoint `c4bc7289`. R14 source
`d2d87d604c5d7d15cb62d317738e20501874098c01fcef08732fec1e6318eb3f`
contains 4,900 qualified inputs. Own-source typecheck passed in 16.477 seconds,
libtest generation in 235.017 seconds, and artifact sealing passed. The sealed
executable is `9a4808e7adf85894cb54b3ea42d21b45101e7612b3f7372bf53618dbd68be9ca`.

The exact 55-case panel executed **54 PASS / 1 FAIL / 0 incomplete / 0 unrun**.
Its new install-expiry case failed in the shared genuine-original lost-reply
setup before reaching the expiry assertion. One unchanged, panic-capture-only
diagnostic then passed that case in 5.764 seconds and reached the actual expiry,
fenced-handle, retained DATA and fresh cold-rebind assertions. The full-panel
failure remains timing-sensitive and unattributed; the diagnostic does not turn
that panel green. The prior 54 controls passed on R14, including authenticated
native DATA installation/reopen, exact invalid-input rejection, both receiver
clock controls, selected queue persistence, disabled B association and hard-policy
rejection. Imported DATA and disabled association still cannot dispatch.

Current-source full DFS/KV boundary lint passed all 11 UI controls (two accepts,
nine intended rejects) and DFS/history/attachments library analysis in 12.242
seconds. Default library analysis does not cover test-only branches. Native
fixtures, per-case checkpoints, unconditional archives and owned cleanup passed;
the panel retained 2,297 files and drained 113 recorded groups. No Docker or
production process was operated, and no abrupt process/partition result follows.

The exact receipts are retained under
`data/operations/dfs-availability-20261023/`: I25 build `d9ec96c2`, I26 panel
`bd5c6cfa`, I27 diagnostic `12036b1d`, and I28 boundary `353d42aa`.

Recent local integration boundaries, all without merge conflicts:

- `25a8f32c` merges the disabled-association hard-prohibition preflight. It reads
  retained exact-filesystem policy only, rejects immutable/current forbids and
  foreign LocalOwned retention, and introduces no KV fallback or execution permit.
- `9abd1e12` adds the genuine committed forbidden-B native control.
- `931a6ab2` merges the test-only exact source pause and early routed-carrier
  allocation, preserving production retries and existing deadlines.
- `6044bb87` merges the actual-owner selected-reader forwarding and post-marker
  expiry/cold-rebind control. `c4bc7289` creates missing nested test parents without
  changing selected-store physical checks.

The directory identity correction retains directory/non-symlink type and
before/opened/after device/inode binding. Ordinary child-entry metadata changes
are allowed; the shared regular-file predicate and named-owner checks are unchanged.
Operation-local archive tooling checks every owned group/session and batches
native fixture ownership scans. It changes neither the maintained helper nor the
historical archive timeout verdict.

| Retained checkpoint | Actual result and limit |
|---|---|
| Selected local DATA `3eb87ca5` | Twenty local persistence/V1 controls passed; no native authority proof. |
| N3 R7 `0344ec57`, ELF `c9966eb3` | 23 PASS / 7 FAIL at directory setup. Inner archive timed out at 120.056863 seconds; both complete 420-file archives remain retained. |
| R8 `3a445694`, ELF `f5227860` | 31 PASS / 4 FAIL across two disjoint panels, including all five directory controls. |
| R12 `1544262d` | 47 PASS / 2 FAIL; exact trace exposed the receiver-pause fault-budget exhaustion. |
| R13 `1e7af604` | 50 PASS / 4 FAIL; current-source lint passed. Helper reader-context and nested fixture-parent defects were subsequently corrected. |
| R14 `d2d87d60` | Full 55-case panel 54 PASS / 1 setup FAIL; separate unchanged expiry diagnostic PASS; current-source lint PASS. |
| Original R67 `40394f47`, ELF `17295eb6` | Eight observation controls passed; the unchanged positive successor-continuation assertion failed. Original request/body, committed intent and content evidence remain retained. |

The next N7 control is freeze-before-first-capture with normal native-owner
restart and externally retained selected context. It is not yet implemented or
executed. The expiry test covers repeated rebind of already-installed DATA, not
first-install crash or abrupt process loss. Receiver Prepared/Terminal provenance,
checked B replay/dispatch and causal application remain separate. A response,
readable content, receipt or disabled association cannot provide those guarantees.
V1/P0 remains a required compatibility boundary; the original R67 positive
successor assertion is still open. C1 serving/admission, root retirement,
metadata quorum, W/N degraded admission, abrupt-loss acceptance and Sinomo-only
CPU/disk/network quietness remain unimplemented or unexecuted acceptance gates.

Local commits and reviewed, tested local merges between isolated DFS subtasks are
the integration boundaries. New launches use only the DFS-local lock and own
16 GiB/no-swap limits, source/artifact/runtime qualification and actual cleanup.
There is no peer capacity/source-release/handback workflow. `dev/debug` owns its
Docker cluster and `main` owns real rollout; this lane operates neither, remains
nonblocking, and has no authorization to merge into either lane, push or deploy.

## Historical checkpoints

The following entries preserve earlier source-specific findings. Their pending
reservations, source grants and unrun statements describe those checkpoints,
not current permission gates or current test totals.

Historical build-only checkpoint (2026-10-24): local HEAD f520a20d remained separate from rollout.
The selected-original DATA candidate was frozen at manifest3eb87ca5 (4,900 inputs),
with only repair_queue_store.rs and repair_handoff.rs changed since retained40394.
Both existing reviewers closed its bounded source and verification-tooling review;
its fresh check passed57.007s and own-source libtest no-run passed627.140s.
The artifact was sealed; its17 new controls were unrun at that build-only checkpoint.
The subsequent local panel passed all20 controls as recorded above. It adds a private, explicitly
admitted non-executing DATA path through the existing handoff staging and queue
writer, with full original/history binding, atomic row/import persistence,
restart/compaction preservation and physical synchronization checks. It supplies
no serving floor, execution permission or root retirement. P0 preserves existing
V1 behavior; missing historical bodies are never reconstructed.

R67 actually executed eight observation controls successfully and failed the
unchanged positive successor-continuation assertion. Its40394 source, exact
17295eb6 executable, original request/body, committed intent and content evidence
remain preserved. DATA import alone cannot close that failure. Current full
boundary lint is also pending: R64's historical full pass qualifies1d6928dc,
not40394 or3eb87. Actual authenticated ingress/checked successor execution,
metadata failure tolerance, W/N degraded admission and abrupt-process acceptance
remain open. The build-only reservation was independently closed after verified drain.
At that checkpoint, exact-artifact execution and current-source lint required
separate future grants. That cross-worktree scheduling model is now superseded.

Historical checkpoint (2026-10-23, R53/R54): the independent same-producer Repair
slice typechecks and executed twenty exact local/native controls, all passing.
It corrects redundant queue enrollment invalidating a maintenance token; preserves
complete original material/attempt/result/root evidence; and exercises actual
in-process completion/closure/root release. This is not abrupt-loss availability,
metadata quorum or cooperative V2 compatibility. The paired preparation test passes
with Main prepared; Core remains an explicit live-version-binding negative.
Historical failed sources/binaries/results remain retained.

For the historical R53 checkpoint, DFS/KV boundary verification passed the complete maintained gate in
R56, including all eleven UI controls and DFS/history/attachments analysis. Old R29
continues to cover only committed f520 bytes. That checkpoint was frozen
at manifest d0554509 (4,899 inputs). The new V1 review found that old retry records
cannot generally prove an ambiguous request's complete original head/body binding;
a reviewed transition and executor/root/retirement protocol remains an integration
blocker. No conditional source grant or twenty-case pass closes that contract.

Optional local detail, including retained raw reports and review records:
[data/operations/dfs-availability-20261023/CURRENT-STATUS.md](../../data/operations/dfs-availability-20261023/CURRENT-STATUS.md).
Earlier checkpoints below retain their original source-specific qualification;
the current checkpoint above supplies the source-qualified outcome summary for
a clean checkout. All full abrupt-node-loss acceptance remains outstanding.

Status: authorized development, NONBLOCKING for rollout. The read-only
content-accounting slice and one corrected native rejection/retry control have
execution evidence; full availability remains unimplemented. This document describes a target contract,
not deployed or execution-proven behavior. Current complete-participant and
cooperative-handoff checks remain authoritative until a replacement protocol is
defined, reviewed, implemented and verified.

## Historical lane and overlap record

This section records the initial coordination arrangement. The independent
development model above supersedes its per-file releases, peer messages and
shared resource reservations; it does not change protocol correctness rules.

- Worktree: `/home/ix/orca/workspaces/sinomo-agent/dev-dfs`, branch `dev/dfs`.
- Clean starting HEAD: `02c457f7f74aa071d5902b8fb5b64da488ee24de`, the completed
  cooperative original-repair-custody checkpoint. No branch reset or import.
- Debug owner: task `01a13715-bc97-7173-ac80-c242183e7d84` in
  `/home/ix/Workspace/sinomo/sinomo-agent-debug`, branch `dev/debug`. It owns
  ordinary metadata quorum/startup/routing failures and networking/KV/concurrency.
  Its initial six observed instrumentation files were `service.rs`, `test_runtime.rs`,
  `tests.rs`, `placement/native_noop_tests.rs`, `runtime_host/data_plane.rs`, and
  `runtime_host/reconciliation_host.rs` under `crates/sinomo-dfs/src/`.
  Later owner evidence reserves metadata replication/diagnostics, runtime publish,
  message/data/publish/reconciliation hosts and their regression children as well.
  Consult fresh owner records before shared work; the initial list is not an
  exhaustive current boundary. Resource grants do not hand off source ownership.
- Read its operation record
  `data/operations/dfs-quorum-startup-concurrency-20261023T005443Z/CURRENT-RESUME.md`
  and fresh terminal output before shared-surface work. Do not edit its checkout,
  duplicate its debugging, interrupt it, or import its dirty files.
- Coordinator request `9b687c11-f9eb-44d3-9166-c4b4b6d91342` was accepted for
  noninterrupting input. A fresh terminal screen shows that note in the debug
  session; this is not evidence of an owner agreement or a reserved build slot.
  No duplicate send. Preserve that identity for any supported retry.
- Parent owns integration and isolated placement/storage changes. Independent
  read-only agents own invariant review and acceptance mapping; neither may edit,
  build, run Docker or coordinate external services.

## Current baseline and hard invariants

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

## Protocol design and review gate

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

The independent review confirmed a stale sentence in the distributed skill
mentioning an operation-bound Raft metadata permit. The explicit user contract,
current architecture and Rust full-participant checks take precedence. This
lane does not revive that historical path or edit the shared skill.

### Refined target transitions and compatibility gap

These are reviewed design obligations, not an approved executable protocol:

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

### Reviewed target choice for exact decision identity

The follow-up review selected an explicit target direction, not implemented
behavior: admit the new regime only after a certified cooperative cut closes
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

## First independent implementation slice

The first slice is deliberately smaller than planner admission or store rebind:
`DfsIntentReplicaAvailability` and
`DistributedEventualCoordinator::replica_availability` in `distributed.rs`,
exported through `lib.rs`, with an external
`tests/availability_content_accounting.rs` regression suite. It is a read-only,
process-local projection of one exact original intent and accepted receipts.
It separates desired membership/W/N, missing receipts, observed writable
targets and reachable durable-holder candidates. It issues no KV/storage/network
work and does not mutate authority, receipt evidence or repair scheduling.

Receipt qualification remains owned by existing publication/hydration. The
projection checks the exact receipt tuple but cannot independently verify bytes
or grant read permission. Missing receipts are not identical to executable
repair work: historical roots can be inactive, and a durable receipt can still
have unsettled visible-location work. Connectivity observations do not grant
writability or clear deficits. Pins are not modeled by this coordinator and
remain a separate planner/service obligation.

The existing all-N planner admission is unchanged. The service discards the
planner's explicit preferred-commit list in some paths, so wiring degraded
placement requires a separate reviewed scheduling/admission slice. This code
does not itself enable survivor publication or abrupt promotion.

### Write admission requires a composed target inventory

Production `service_write.rs` and `placement_environment.rs` now match the base
exactly. A deferred-target experiment passed five selector and three record
controls from its sealed libtest, but independent source review found a missing
admission contract. Its source/patch/tests and exact execution evidence are
preserved under `data/operations/dfs-availability-20261023/`; its production wiring
was withdrawn. Those eight passes do not qualify safe degraded-write admission.

Concrete counterexample: an exact previous A/B/C grant and fresh unreachable-A
path survive after A's content provider is disabled/incompatible. B/C can satisfy
W while healthy metadata B/C/D supplies FullShard proof. Reusing the old grant
can create an invalid new A obligation. Historical owner/location candidates can
also restore A when absent from snapshot nodes. This creates no A receipt/write
permission and weakens no metadata/W proof, but it is invalid new-plan selection.

A coherent retained admitted-target inventory is required before extending normal
selection. Bind authorized membership, active compatible `dfs.object.transfer`
provider/interface/protocol, content offer, current hard policy/constraints and
target identity to one owner-certified configuration cut. Advisory storage/path
observations may rank/defer only members of that inventory. They add neither
members nor holders. Historical grants/copies retain their old obligations; they
cannot certify a new version's eligibility. Missing/malformed/incomplete admission
fails closed. No fresh per-write KV read or content journal/receipt ledger in KV.

Existing retained write configuration contains filesystem/content/provider scopes
but omits members. The host's advisory snapshot joins authorized members/content
but omits provider and strips authority provenance. Joining unbound generations
is not the missing certificate. Its producer maps endpoint failure to Unknown;
do not invent whole-path unavailability. Exposing the composed inventory crosses
currently reserved host/provider/configuration surfaces and needs owner sequencing.
Pins, required nodes, residency/zones, storage refusal and confirmed W=N stay hard.
A future admitted unavailable A may be desired for a NEW object without inventing
an A copy or receipt, once that inventory contract exists.

General desired placement across metadata-only versions separately needs complete
bounded supersession evidence. V3 may carry V1's A/B/C data grant while intermediate
V2 owns B/C/D. Source-version/exact-version lookup can miss that owner. Capped
operator activity cannot prove absence; compact retry receipts never regain live
intent authority. A candidate accessor must bound raw visits, expose incomplete/
ambiguous outcomes and validate exact content lineage plus current job/config.
The old exact-version veto has no proven visit bound and is no longer added to
normal writes by this lane.

The current native target is a rejection regression: all metadata/providers stay
alive; an advisory unavailable-A path plus historical copies must not admit a new
intent or remote stage. Original metadata/receipts/bytes must remain; healing
strict planning retries the same original logical identity/body. The corrected
registered-Main/Core fixture actually PASSED 1/0 after feature typecheck, source
manifest `56c658ee5cc25e3006b3a18f77f0bc885450011f29e156b20fc9dbf5cd086696`.
It compares the original prepared digest/body/exact terminal reference across
Main replay and retains strong all-three raw Live byte checks. All metadata and
providers remain alive. Earlier b72/b29 fixture executions failed their raw Live
setup checks; their exact source, binaries and disk archives remain preserved.
The prior two-overwrite positive source is retained as a superseded experiment.
Future positive acceptance must include removed/disabled/incompatible A outside
healthy metadata B/C/D, so FullShard failure cannot mask bad content admission.
Neither fixture replaces abrupt process/transport/disk or quietness acceptance.

Seven projection controls executed (five external and two inline). The separate
experiment controls executed PASS8/0 at07:07:40 UTC without rebuilding sealed
libtest SHA256 `2c3ff08c447731da17f9c11288062458e69084dee6c165a84d4be90e5714f57e`,
source `9db1a853d0ca7415cc2ad56a36ac917b6781fbb9762734937df723425ad61342`.
Current source differs after the correction; projection inputs are unchanged,
but no current whole-library or positive degraded-write validation is inferred.
The corrected single native rejection case is qualified separately in
`NATIVE-NORMAL-MAIN-R25-RECEIPT.json`. Exact archived source,
failed intermediate builds, resource events/swap and fixture cleanup remain in the
operation receipts. No count-only quorum change or shared owner edit occurred.

The [original-effect custody draft](dfs-original-effect-custody.md) specifies
proposed pre-dispatch coverage, effect-specific terminal evidence, crash-safe
body compaction and the unresolved external-payload/W=1 boundary. Its negative
traces remain unexecuted; current queue tuples and metadata acceptance alone do
not establish those obligations.

The accepted-bundle target now has 31 executed deterministic checks and a separate
16-trace/32-transition sampled smoke. Its one-slot assumptions, delayed-proof
witnesses, pre-fix failures and Rust refinement gaps are documented in
[the bounded target evidence](dfs-accepted-bundle-target.md). This is neither
exhaustive verification nor an implemented majority protocol. The mandatory
DFS/KV Dylint check subsequently passed in R29: own-source lint library, 11 UI
controls and DFS/history/attachments library analysis, with exact source and
normal guard/fixture drain recorded in `BOUNDARY-R29-RECEIPT.json`.
The isolated five-file Rust slice is checkpointed locally as
`f520a20d482ca2b069dd51ced9f220c365797644`, direct child of the starting base.
It changes no admission or metadata participant proof. Protocol/model documents
remain separate drafts; no integration, push or deployment occurred.

### Separate checked store-rebind dependency

The read-only source review confirms that actual wiring crosses debug-owned
authority invalidation, resident-cache rollover and publication-provider gates.
A resource window is not a shared-source handoff. Do not implement reuse by
ignoring epoch/primary bytes in `metadata_storage_configuration_changed`.

Three generations have different jobs: runtime serving/configuration generation;
resident storage lifecycle generation; and physical table/journal-source identity.
Every shard-config byte change currently retires storage in
`metadata_authority_projection.rs` / `metadata_authority_runtime.rs`.
`DfsRuntimeMessageServiceSignature::shares_metadata_storage` and
`runtime_host/message_host.rs` then roll over the cache; router reconstruction in
`metadata_sharded_backend.rs::into_reconfigured_for_replica_repair` refuses config,
root, open-mode or operator-status disagreement. Its slot config/open mode are
immutable even though status can change during handoff. That wrapper seam must be
handled as well as the classifier.

The smallest future integration should initially cover completed, membership-
preserving cooperative primary rotations. Represent “physical store retained,
serving rebind pending” explicitly before exposing authority readiness. Advance
authority fencing immediately; after exact leases drain under exclusive admission,
reuse the existing completed-handoff checks and real native writer rebind. Only
then construct the current authority/router/status wrapper around the retained
physical backend/index. Never increment storage generation and later roll it back.

Existing proof anchors are
`service_sync.rs::rebind_completed_metadata_shard_membership_handoff_once`,
`rebind_completed_local_metadata_shard_membership_handoff_if_frozen` and the
physical-instance variant. They validate active assignment, absent pending state,
exact completion/marker identity and an equal or authenticated contiguous prefix.
`metadata_backend/handoff.rs` repeats prefix checks under the writer lock, installs
bounded custody pages and binds only that physical instance. The durable freeze
marker and publication provider's physical generation/root identity still fence
retained handles. New serving authority must not revive a queued old operation.

Actual root/filesystem identity changes, membership/range/import transitions,
snapshot replacement and malformed structural state keep stronger physical
retirement. Independent raw handles retain their old binding. Required later
native evidence: same resident slot/index identity across checked rotation;
old A and pre-opened B handles fenced; missing/wrong proof and incomplete custody
not ready; valid prefix extension plus under-lock prefix race; queued old-generation
publication rejected; same-path root/snapshot replacement rejects old certificates.
Existing `metadata_store_lifecycle` tests currently expect physical retirement on
handoff, so their expectation can change only with this integration and identity
evidence. No generation classification or rebind implementation changed here.

## Source review and acceptance mapping

Both reviewers were read-only and ran no builds, tests or Docker commands.
Findings are source evidence only.

| Boundary | Source / current meaning |
|---|---|
| Ordinary proof | `service_replica.rs::authorize_metadata_shard_operation`, `publication_guard.rs::DfsPublicationGuard::from_authorized`, `protocol.rs::DfsCanonicalAuthorization`: exact full assignment, only FullShard authorization. |
| Receiver floor | `service_metadata_exchange.rs`: exact prepared-prefix validation and durable local authority floor; this is not an election promise. |
| Cooperative-only activation | `service_sync.rs::reconcile_pending_metadata_shard_membership_handoffs`, `repair_handoff.rs`: retiring-primary-owned freeze, complete custody pages and joint proof. |
| Prepared recovery | `service_prepare_recovery.rs`: complete old prepare can be recertified after independently established successor authority; it does not elect a successor. Internal recovery envelopes must preserve original external effect identity. |
| Store lifetime | `metadata_authority_runtime.rs`, `metadata_authority_projection.rs::metadata_storage_configuration_changed`: distinct generation counters exist, but any shard-config byte change currently retires storage. Ignoring that change is not a checked rebind. |
| Current model | `dfs-whole-shard` in `scripts/formal/check-quint.sh`: all-participant abstract contract. Older fallback models are not current conformance evidence. |
| Negative regression | `tests/networked_dfs.rs::native_whole_shard_partial_metadata_probe_uses_actual_operation_bound_native_permit`: despite its name, 2/3 remains hidden with no KV permit, then exact original replay after healing. Preserve it. |
| Existing Docker cut | `docker/test-cluster.sh`: disconnect of assigned node6 proves nonpublication then healing/same-ID recovery; restart rereads after recovery. It does not prove survivor operation while primary is down. |
| Harness topology | Existing baseline uses six metadata participants and initially one control voter plus learners. Add an explicit availability selector for three metadata members and independent survivable control voters; do not weaken the six-member baseline. |
| Quietness coverage | Existing Docker CPU/network gate alone does not establish disk/writeback coverage. Missing final-I/O/writeback ownership remains BLOCKED. |

## Phases and correctness gates

| Phase | Work | Gate before advancing |
|---|---|---|
| 0 | Verify base, read architecture/contracts, record exact source and owned resources; publish this plan and operation status. | Existing edits and historical evidence preserved. |
| 1 | Parallel invariant review and acceptance mapping; define epoch/commit/custody state machine. | Review closes each concrete protocol dependency before its implementation. |
| 2 | Validate and fix native original-custody DATA; analyze checked successor joins in parallel. | Preserve hard policy/physical fences; use bounded worktree-local verification and exact source/artifact evidence. |
| 3 | Implement checked successor execution, then metadata quorum/fencing and survivor-durable custody in independent DFS subtasks. | Coherent local commits, isolated branches for overlapping edits, reviewed actual merges and focused recovery checks. |
| 4 | Implement survivor activation and bounded rejoin, exact alternate holders, no automatic failback. | Full protocol safety and recovery checks; no byte migration prerequisite. |
| 5 | Execute disposable real-process abrupt-loss/partition/reply-loss matrix and performance/quietness gates. | Source/binary/process/disk identities, full coverage and retained evidence. |

No production rollout, service restarts, credential/security/network changes,
destructive migrations, pushes, Windows work, accounting setup or rollout
dependency. Local native commits and reviewed merges of DFS subtask branches into
`dev/dfs` are permitted with honest validation and conflict reporting. Integration
into `dev/debug` or `main` remains excluded. Any need for those excluded actions
is a new approval boundary.

## Acceptance matrix (all execution pending)

| Scenario | Required evidence |
|---|---|
| Abrupt A stop; exact committed bytes only A/C, B lacks them | Verify fixture and on-disk bindings before stop; actually stop A abruptly; read exact C version; new B/C W=2,N=3 write; retain A membership/copy facts, desired N and durable deficit. |
| Metadata replica unavailable, primary alive | Remaining metadata majority preserves one ordered history and exact acknowledged results; no content-W substitution. |
| Live old primary partitioned | Attempts on both sides; quorum-side activation; old side cannot publish/ack a divergent history, including restart and delayed messages. |
| Crash/lost reply at prepare, commit and transfer | Cut each durable boundary; recover exact original IDs/bodies, unknown delivery and terminal evidence; exact replay or unresolved status, never fresh identity. |
| Repeated reconnect | Reuse matching prefix/content; bounded missing transfer; membership/copies retained; no churn, wholesale rewrite or failback. |
| W=1 bytes exclusively A, A unavailable | Affected content explicitly unavailable; unrelated readable/writable objects continue under valid authority. |

The process/transport/disk matrix remains an acceptance obligation, not permission
for this lane to operate the Docker debug cluster or the real cluster. Any later
Docker acceptance belongs to `dev/debug` and its maintained verifiers/reporters;
the real rollout belongs to `main`. Preserve first failures and retained evidence.
In-process tests and source review are development evidence only.

Performance evidence follows `skills/sinomo-performance-analysis/SKILL.md`:
acceptance, per-node stage/I/O, node windows and coverage tables, including
unavailable counters. Measure takeover latency and metadata/content transfer
volume directly with no invented SLO or observation-overhead subtraction.
Keep debugging/tracing/profiling and existing thresholds. Mandatory quietness
covers Sinomo main/shadow, attributable helpers/delegates, network and writeback;
whole-host activity is diagnostic only. Incomplete ownership/final-I/O/network/
writeback coverage remains BLOCKED. Do not reapply runtime accounting setup.

## Independent resource ownership

New launches pass the maintained measurement helper an explicit worktree-local
lock, `data/operations/dfs-availability-20261023/.dfs-build-test.lock` under this
checkout. Do not use `/tmp/sinomo-maintenance-build.lock` for this lane. Two
worktrees may build concurrently; another worktree's compiler, scope or activity
does not become an idle prerequisite. No peer reservation, external start-window
readback or handback approval is required.

Keep bounded attempts, jobs=1, default test-worker settings, 16 GiB MemoryHigh and
MemoryMax, zero swap, sensible local disk reserves and the existing fixture
lifecycle. Every attempt retains exact source/artifact/runtime bindings, failures
and unconditional preservation, then proves its own process/group/cgroup/root
cleanup. Do not change host accounting, numeric limits or the maintained helper.
Quietness acceptance remains Sinomo-only; whole-host activity is diagnostic.

### Historical shared resource gate — superseded

The following paragraph records the old launch contract. It remains relevant to
interpreting immutable old receipts only and must not govern new DFS launches.

Debug critical-path builds/tests have priority. Existing lock is
`/tmp/sinomo-maintenance-build.lock`, with 16 GiB MemoryHigh/MemoryMax and no swap.
Its `run-check-export-v3.py` owns the guard internally; do not double-wrap it.
The coordinator explicitly clarified that preserving the existing authorized
Cargo jobs=1 guard takes precedence over generic skill guidance and needs no
fresh approval. Keep jobs/limits/accounting unchanged. Before a heavy run,
inspect the current helper and fresh debug-slot evidence; the measurement helper
already uses the existing lock nonblocking. Do not double-wrap it. Yield to debug
critical-path work; independent source and lightweight static checks continue
while the slot is unavailable. A momentarily free lock is not a general resource
handback, and separate targets do not isolate CPU/RAM.

Current evidence and remaining work are maintained in
`data/operations/dfs-availability-20261023/CURRENT-STATUS.md`.
