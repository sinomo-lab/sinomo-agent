# DFS abrupt-node-loss and reconnection availability

## Governing quietness invariant — 2026-10-25

Quiet windows require **absolute zero writes**, including maintenance. The window
deliberately spans maintenance so it can detect unwanted persistence. Time-only
keepalive and renewal information stays in memory; durable updates require an
actual semantic change. The 1 MB allowance applies only to genuine cluster
activity, never to quiet maintenance. Earlier interpretations allowing a quiet
maintenance write budget are superseded; historical failures and raw evidence
remain unchanged. Do not discount or reclassify a failed quiet window as a pass.

Coverage remains Sinomo-only: main/shadow processes, attributable helpers and
delegated work, network and writeback. Whole-host activity is diagnostic. Missing
ownership or final-I/O coverage remains blocked. This instruction changes no
rollout permission, resource limit, accounting setup or production scope.

## Current development checkpoint — 2026-10-26 R70

The recorded post-publication cut is implemented in `fbd8588d` and qualified
at `3c07cce6`, source `29255ab5` /4,929 inputs. The scoped one-use callback runs
only after the ACTUAL create-new atomic storage operation and sync returns,
before final current-provider/record/deadline validation. It alters no storage
result or authority gate, and restores its thread-local state on unwind/return.

I206/R69 remains FAILED101 with two diagnostics from one reporting typo;
all248 controls were unrun. The sole correction removes mistaken call
parentheses. I209 fresh check/build/seal passed; I210 executed the new control:
**1 PASS,247 other inventory controls UNRUN**. I211 full DFS/KV boundary passed
11UI controls and three library analyses. Exact reports/archives/source/ELF and
35-identity owned drain are in R70-REPORT.md and combined receipt `25a3543b`.

The actual cut observes the published file once and waits until the SAME
installed5s intake deadline expires. The existing final check reports
publication/durability uncertainty, leaving the bytes intact. SAME-expired-budget
retry still refuses. Normal reopen preserves original round, commands, attempts,
peer responses and non-reconstructible original deadline DATA. A new actual
complete native round returns the FIRST record as ExistingHistoricalData; no
second publication callback, write, renewal or ACK occurs. The first outcome
remains uncertain. File identity/bytes/mtime/ctime and authority markers stay
unchanged. This is not a claim of fsync failure, SIGKILL or power-loss acceptance.

Continue with one bounded canonical-record refusal control: structural decoding
must not make incomplete participants, duplicate attempts, mismatched replies or
foreign command epochs acceptable historical DATA. Use actual owner/read/reopen,
preserve the original and each deliberately supplied fault image, and never send
or relabel those images as native provenance. This requires no activation or new
owner. A later healthy-full3 process-reopen helper cannot borrow the existing
deferred-genesis constructor by fabricating its required original.

R67/R68 counts and every failure remain unchanged. Serving/genesis/C1, quorum,
survivor execution, abrupt-loss/rejoin and absolute-zero quietness acceptance
remain open. No production/Docker, peer arbitration, account/model switch or
main-rollout dependency.

## Historical development checkpoint — 2026-10-26 R68

R65 completed both requested deadline/mixed-native-source controls. Development
then continued to an integrated, dormant historical-DATA intake using the real
metadata owner. Implementation `ebc5483a` and native controls `fa68de0f` merged
without conflicts at `9a6a56d4`. The complete private native round remains input
DATA, distinct from separately checked current provider, exact-shard admission,
metadata writer/epoch/freeze and physical-root ownership. APIs and schema remain
`cfg(test)` with no automatic runtime caller or production format activation.

The existing create-new atomic storage path syncs the first immutable SOBJ slot.
Exact or fresh-semantic duplicates return the FIRST record without write, rename,
fsync, identity/deadline renewal or another durable ACK. Conflicting, malformed,
partial, expired, foreign-writer and physically rebound input refuses. Cold DATA
cannot reconstruct native provenance. Ordinary table checkpoint and normal
component reopen preserve this historical file; same-directory table generation
replacement is not certified by its physical root identities.

R66/I197 FAILED typecheck with two E0624 private-method accesses; all247 controls
were unrun. Correction `00d09a99` uses a test-only boolean query under the existing
context mutex without exposing offer/witness capabilities or weakening refusal.
R67 source `f32c2893` passed build and executed **3 PASS / 1 FAIL**, with243 other
controls unrun. The positive case reached its final marker assertion after
successful history persistence, duplicate, checkpoint and reopen checks. Its
assertion wrongly required identical marker bytes across deliberate Main
namespace activity: actual rebind prefix advanced6->10 while original handoff,
epochs and cut remained unchanged. The failure and all exact evidence remain.

Fixture-only correction `66d14f4f` requires marker bytes unchanged at the actual
intake/duplicate no-effect boundary, then permits only the EXACT genuine semantic
descendant after explicit activity/checkpoint/reopen. No production code, original
identity, authority gate, deadline or native case cap changed. Current R68 source
`b199ea4f` /4,929 inputs passed fresh check/build/seal and the affected native
positive: **1 PASS;246 other controls UNRUN on R68**. The three negative bodies
and production inputs are byte-exact to R67, but their older passes are not
transferred. Full current-source DFS/KV lint passed11 UI controls and three
library analyses. See R68-REPORT.md and combined receipt `8b687f8b` for exact
source/ELF/commands, all failures,637-file failed-panel and159-file successor
archives, standard measurement/coverage tables and actual complete owned drain.

The next finite test boundary is interruption after the actual no-replace
publication/fsync returns but before intake final validation/return in
`LocalSobjMetadataBackend::retain_native_prefix_history_owned`. An owned test-only
cut at that exact caller boundary can expire the existing intake deadline, then
check retained uncertainty, unchanged original record, normal reopen and
ExistingHistoricalData without recertification. It must not replace the storage
writer, simulate an ACK, widen deadlines or reconstruct native capability from
bytes. Power-loss/storage-fsync cuts remain a separate unexecuted requirement.

This adds no serving/genesis/C1/floor activation, effect authority, receipt/W
application, Terminal/Close/root retirement, participant weakening or periodic
writes. Quorum, survivor execution, abrupt-loss/partition/rejoin and absolute-zero
quietness acceptance remain open. No Docker/production operation, account/model
switch, peer arbitration, push or rollout dependency was introduced.

## Historical development checkpoint — 2026-10-26 R65

The two recorded R64 edge controls are now executed. Test-only `ea77bcd1`, source
`414da787` with 4,927 inputs, appends them without changing production code or the
complete R64 test prefix. Fresh check/build/seal (I194), both exact native cases
(I195), and full DFS/KV boundary analysis (I196: 11 UI controls and three
libraries) passed. **241 other inventory controls remain UNRUN on R65.** R63/R64
and all historical failures retain their exact source qualification.

The first-peer deadline control retains an actual PossiblyApplied result and
leaves the next fixed command Unattempted, with no invented response, native
value, error or delivery classification. The same original round deadline is
preserved; no RPC is sent to that second peer.

The mixed-source control delegates unchanged attempts to real native adapters.
After retaining B's genuine return it releases all old host references, normally
stops A, verifies old-owner release, retains/replaces the disposable fixture's
lock inode, and normally reopens A from unchanged metadata. C supplies a genuine
return under the new physical owner within the original round budget. Both
values retain the same assignment/prefix but different actual owner identities.
Finalization refuses at the existing first live physical-binding check, before
the literal cross-source comparison. Both observations are retained and both
sequential owners drain. No concurrent owner, fabricated evidence, marker
removal, deadline extension or weakened gate is involved.

The 317-file archive passed comparison/hash checks. R65-REPORT.md and its combined
receipt retain exact source/ELF, commands, raw reports, outcomes and complete
owned cleanup. No production, Docker, peer arbitration or model/account switch
occurred. This remains historical observation, not current serving/effect
permission or full abrupt-loss acceptance.

Development continues with a finite immutable historical-DATA intake under the
existing metadata storage/fsync owner. Its contract requires genuine complete
private input plus independent current writer checks; duplicates must preserve
the first record without time-only rewrites, conflicts must refuse, and cold
reads must never reconstruct private provenance or serving authority. No durable
consumer or genesis/serving activation is yet qualified. Stabilized maintenance
remains absolute zero writes.

## Historical development checkpoint — 2026-10-26 R64

The next bounded prerequisite collects one in-memory native observation round
from R63's actual private returns. Source `70e74527` and controls `0f5dca5d` /
`4c640868` are integrated at `53d67371`, source `486ff574` with 4,927 inputs.
The existing R63 producer, ordinary sync/fanout and Repair paths remain unchanged.

The direct-capture owner fixes the round identity and both peer commands before
any dispatch, then invokes R63 once per attempted peer under the same absolute
deadline. Completion requires the exact configured remote set and matching
native source assignment, prefix and local physical observations. It does not
invent a self RPC acknowledgement or use a majority count. Complete and
incomplete results retain no runtime, store or owner lease and have no raw,
Clone, serialization or partial-adoption constructor.

Incomplete results preserve actual native returns, response DATA without native
proof, exact network/delivery errors, and commands/reasons for unattempted peers.
Unknown outcomes never become not-sent merely to complete a set. This result is
historical captured evidence, not current serving, effect or commit authority.

Fresh check/build/seal (I191), all three new native controls (I192), and full
DFS/KV boundary analysis (I193: 11 UI controls and three libraries) passed.
**238 other inventory IDs remain UNRUN on R64.** R63's seven passes retain their
original source qualification and were not repeated. The controls verify the
exact native remote set and owner release, duplicate/subset and generic-DATA
refusal, and retention of actual B evidence plus C's PossiblyApplied failure.
After exact fault release/drain, the SAME target completes under a fresh round
with new native calls; old partial evidence is not adopted. Complete and partial
values remain held through successful normal source-owner close.

The 473-file fixture archive passed comparison/hash checks. R64-REPORT.md,
R64-COMBINED-RECEIPT.json and I191/I192/I193 receipts retain exact source/ELF,
commands, per-case outcomes, raw reports and complete owned cleanup. All previous
failures remain immutable. There was no model/account switch, competing owner,
peer build arbitration, Docker or production action.

This slice supports observation of an already initialized three-member scope.
Deadline-before-next-peer Unattempted and mixed-native-source finalization refusal
branches are defined but not executed by this panel. Physical late delivery,
durable intake/replay, genesis selection, current serving/C1, receipt/W
application, Terminal/Close/root release, recovery quorum and full abrupt-loss /
reconnection acceptance remain open. Stabilized maintenance still requires
absolute zero writes; this slice adds no periodic work.

## Historical development checkpoint — 2026-10-26 R63

The bounded direct native PREFIX return capture is implemented (`bd3e40c5`),
integrated at `3a721197`, with native controls `2190406a` and test-only borrow
correction `8268a4cf`. Qualified source `9cc0740a` contains 4,926 inputs. The new
opt-in NodeTransport producer owns one private per-call attempt with exact
command, peer, fixed deadline and fresh local correlation identity. Generic peer
output remains NoNativeProof. Only the actual registered native peer override
constructs the opaque value after its one target-bound RPC, response matching and
local physical/source-prefix checks. Trace, inbox, decoded data and generic peer
results cannot mint that value; a genuine old value cannot be rebound to a new
attempt. Completed evidence retains no host, runtime, DATA-owner or store lease.

Fresh check/build/seal (I188), all seven selected controls (I189), and full DFS/KV
boundary analysis (I190: 11 UI controls and three libraries) passed. The panel
contains four new native controls plus two affected adapter controls and the
existing native Repair-return regression. **231 other inventory IDs remain
UNRUN on R63.** R61 was not rerun, and earlier results retain their own source
qualification. R62/I185 remains FAILED101: two test-only Handle borrow errors,
no codegen/native execution, all238UNRUN, with original source/logs/archives
preserved. The successor reversibly changes only those two borrows.

The executed controls establish a genuine direct return and actual receiver
prefix-floor DATA; generic matching data remains unproven; old-attempt, wrong
peer/request/root and actual foreign native-owner joins refuse; actual response
loss yields no capability and the SAME query retries; named physical-owner-file
replacement refuses reuse and new capture before RPC. All four native reports
show normal close and source-owner release while historical evidence remains
held. Getters retain immutable history; live binding validation refuses after
close. Remote physical generation is unavailable because the protocol supplies
none.

The physical fault occurs after a completed capture and before the next call;
replacement during RPC remains untested. A canceled/lost reply is not proof of
physical late delivery. Routed and asynchronous provenance capture are disabled.
No durable attestation, full-set aggregate, genesis/serving/C1 activation,
receipt/W publication, Terminal, Close or root release is added. Existing Repair,
ordinary fanout, wire and receiver behavior remain unchanged.

`R63-REPORT.md`, its combined receipt and I188/I189/I190 receipts retain exact
commands, source/ELF, raw observations, 809 archived fixture files, comparison/hash
checks and complete owned cleanup. The same model/account was used; the old
capacity-failed integration role was not retried. Original abrupt-node-loss,
quorum, survivor custody, bounded reconnect and absolute-zero quietness gates
remain open. No Docker or production/rollout action occurred.

## Historical development checkpoint — 2026-10-26 R61

The existing metadata synchronization engine now has a strict exact-prefix,
full-assignment mode (`1474f66f`). It revalidates the actual current source
assignment, epoch, head and fingerprint; it never raises the requested target,
never repairs through a checkpoint snapshot, and never enters the membership
handoff partial-proof path. Existing journal backfill remains bounded by the
requested sequence. Ordinary durability and handoff behavior stay on their
existing paths. The result is live barrier DATA, not a durable attestation,
unforgeable native-provenance capability, serving floor or genesis activation.

R59's first typecheck failed at five new reporting sites because DfsError is not
serializable; no native tests ran. Reporting-only correction `97ccdd54`
preserves that failure. R60 source `6e12732b` actually passed exact-target/full-set
sync and strict source-advance refusal with the ordinary stronger-head regression.
Its reply-loss case FAILED: the fault covered only the direct route, and a genuine
C reply arrived through the routed fallback. The returned certificate contained
all three members; this was not observed two-member proof acceptance.

Fixture-only `cbddf3fd` covers both exact A-to-C metadata routes with one gate,
16+16 hit budgets (aggregate32), and evidence capture before assertion. R61
code is `57927e3b`, source `1224d677` with 4,924 inputs. Fresh check/build/seal
(I182), the affected native case (I183), and full DFS/KV boundary analysis
(I184: 11 UI controls and three libraries) passed. **231 other inventory IDs
remain UNRUN on R61**. The first two R60 passes retain their own source
qualification; this is not a three-case R61 pass.

I183 observed one eligible call on each route, two of32, with no fault-budget
exhaustion. Holding C's completed responses produced an original-deadline error
and no certificate. After exact gate release/drain, the SAME target obtained fresh
A/B/C acknowledgements with disjoint per-invocation probe IDs. The legacy
`quorum_size` field remains2; the complete member set is required independently.
An old canceled response may have been lost rather than physically delivered late;
that stronger delayed-delivery scenario remains unproved. Waiter count is not
exposed. The measured 5.027-second failed attempt is not a latency pass.

All 161 fixture files were archived with comparison/hash checks. Exact source,
ELF, commands, actual outcomes and complete owned cleanup are retained in the
I182/I183/I184 receipts and `R61-REPORT.md` / combined receipt. Root finished saved
aggregation after the integration child reported model capacity, following
already completed cleanup; nothing was rerun and no account/model was switched.
Every earlier failed source, request and result remains immutable.

Healthy fixed-three genesis is still OPEN. Claude R10's proposal was not accepted:
a missing new-genesis record cannot silently become legacy, and a public/replayed
certificate cannot itself grant current serving authority. The strict-sync mode
is only a bounded prerequisite. Protocol selection, durable native completion
provenance, the post-commit/full-sync admission interval, complete original-effect
custody, recovery quorum, checked successor serving and actual single-node-loss
acceptance remain unresolved. No C1/floor/receipt-application/Close/root-release
activation, Docker, production or rollout action follows. Quiet windows including
maintenance still require absolute zero writes.

## Historical development checkpoint — 2026-10-26 R58

R58 qualifies actual process-death reconstruction of the default-P0 native test
fixture. Helper `02a0a794` and process control `5b1046f5` are integrated at
`cc53f774`, source `269d725a` with 4,923 inputs. The helper is test-only: expected
manifest values are DATA, checked against retained physical roots, native Raft
files and the exact original prepare frame/body before native writable opening.
Reconstruction uses existing WAL owners and real ReadIndex, without fresh
bootstrap, leader forcing, projection copying or new assignment. It never
adopts the external fixture root for deletion. Selected private layouts refuse.

Fresh check/build/seal (I173), both selected native parents (I174), and full
DFS/KV boundary analysis (I175: 11 UI controls and three libraries) passed.
**227 other inventory parent controls remain UNRUN on this source.** The ignored
same-executable helper is an actual owned child, not an additional passing test.

The parent verified its child's PID/start/group/UID, actual SIGKILL signal 9,
reap and group drain. It captured 167 raw files before any parent native open.
Missing-root, foreign genuine-node physical identity and altered-original
preflight controls refused without changing those retained files. Native
reconstruction then passed actual startup/current-owner checks and refused the
SAME stamped mutation while the original genesis stayed Prepared. Explicit
completion of that SAME original through the current retained owner, ordinary
reopen, full assigned-singleton sync, and SAME request produced Applied and the
expected object. The affected normal-component-reopen regression also passed.

The 350-file fixture archive passed comparison and hashing. I173/I174/I175
receipts, `R58-REPORT.md`, the combined receipt and raw report `ca7c2d37` retain
source/executable, commands, process identity, pre/post state and complete owned
cleanup. R56/R57 and every earlier failed result keep their own immutable source
qualifications.

All three native fixture members died in ONE owned libtest process. This proves
that process cut and retained-owner path; it does not prove A loss with B/C
surviving, power-loss durability or a complete production daemon restart. The
target metadata assignment remains singleton A. Raw retained file volume is not
an activity-I/O or quietness measurement. Absolute-zero quietness, fixed-three
initialization, recovery quorum, complete survivor custody, checked successor
serving and full abrupt-loss/reconnection acceptance remain open.

The next finite protocol dependency is healthy initial three-member metadata
genesis through existing owners, preserving ALL assigned-participant proof and
blocking effects until exact commit synchronization finishes. Its contract must
close the live/restart admission interval after A commits but before B/C have the
exact commit. No count-only quorum change, C1 activation, KV operation ledger or
fixture flag as authority is authorized by this checkpoint.

## Historical development checkpoint — 2026-10-26 R57

R57 adds only a native normal-component-reopen control (`633a9581`, corrected
endpoint predicate `53cd4601`), integrated at `ae250b84`. Source `24d06b0b`
contains 4,922 inputs. All production bytes and the prior 53,945-byte test prefix
are unchanged from R56. Fresh check/build/seal (I170), the one new native case
(I171), and full DFS/KV boundary analysis (I172: 11 UI controls and three
libraries) passed. The other **227 of 228 inventory IDs are UNRUN on R57**;
R56's five passes retain their own source qualification.

The actual case released the old A DATA owner during normal stop, observed
NoPath, and remounted a distinct owner over the same physical owner file. The
reopen returned successfully and the endpoint was actually callable. The SAME
stamped request then received the exact known-Prepared pre-effect refusal,
without changing the original body/digest or journal. Explicit completion used
the actual remounted owner and SAME original. A second normal stop/reopen,
exact full assigned-singleton sync, and SAME request produced Applied and the
expected object. No endpoint acceptance flag was forced.

The final-startup-wait-error branch exists in the control but was not exercised
in this run. Normal component restart is not SIGKILL, power loss, one-node loss
with surviving peers or a three-member recovery quorum. Those gaps, complete
survivor custody/history/authority, fixed-member initialization, bounded reconnect
and absolute-zero quietness remain open. All failures and source/artifact seals
from earlier phases are preserved. `R57-REPORT.md`, the combined receipt and
I170/I171/I172 receipts retain the actual commands, source, executable, archives,
comparison/hash checks and complete owned cleanup.

The next bounded development target is actual process-death reconstruction of
the same owned native fixture. A retained constructor, if needed, must reuse
persisted native cluster state and real ReadIndex/owner paths; it must not repeat
fresh bootstrap, overwrite assignment, fabricate an original or treat readable
files as serving authority. This has no production or rollout activation.

## Historical development checkpoint — 2026-10-26 R56

Cold native mutation admission now prepares the existing strict resident owner
before content ingestion or Core request planning. Scoped owner waits retain
neither the root cache mutex nor mutation admission. Current storage
certification may replay already durable frames; this does not run semantic
prepared-operation recovery or author a genesis commit. The affected assignment,
authority generation and certified record are checked again before effects.
Unknown or stale state remains unavailable; unrelated ready scopes retain their
ordinary progress. No readiness flag, timer write, participant-count change or
serving-floor activation is introduced.

Production correction `bf31155c` was merged at `5d388a06`. R56 code is
`d6c92466`, source `b03c212c` with 4,922 inputs. Its only additional source delta
is the test file: append-only cold-held control `8d5fe8cb` and fixture correction
`1be25649`. The latter requires W=2/N=2 for the original reproduction's unchanged
remote-content positive assertion. Native served W=1 can legitimately return a
local durable commit with an N repair obligation even when foreground repair is
requested; production W1 behavior is unchanged. Within each run the pre/post
request, body, target version and identity remain identical.

Fresh check/build/seal passed (I167). I168 actually executed all five selected
native controls, each with exactly one libtest summary, and all five passed:

| Control | Actual result |
|---|---|
| Incomplete genesis rejects stamped write and global Rename before effects; SAME genesis completion/full assigned sync then SAME requests succeed | PASS, including real remote content-effect positive |
| Unrelated ready scope progresses while another scope remains Prepared | PASS |
| Ordinary initial write and cold-cache root isolation | PASS |
| Ordinary singleton smoke | PASS |
| Actual cache eviction/cold opening retains exact Prepared body/digest/journal and refuses mutation, then same-owner completion/sync permits the SAME request | PASS |

The other **222 of 227 inventory IDs remain UNRUN on this source**. I169 full
current-source DFS/KV analysis passed its 11 UI controls and three library
analyses. Actual source/artifact bindings, unconditional fixture archives,
comparison/hash checks and owned process/group/scope/root drains are retained in
the I167/I168/I169 receipts under the operation directory. These results do not
transfer to later source changes.

R51-I153, R52-I156 and R53-I159 remain failed. R53 showed real premature intent
and namespace bookkeeping while the original genesis stayed Prepared. R54-I162
remains 2 PASS / 2 FAIL: its guard blocked those effects but rejected cold/busy
resident ownership. R55-I165 remains 3 PASS / 1 FAIL: cold ownership and
post-completion Rename succeeded, but its W1 fixture did not produce the required
remote-content positive. The original raw requests, source/binaries, outcomes and
archives are immutable; no failed result was reclassified.

These fixtures run native members within one libtest process. The newly held
metadata scope is assigned singleton A; exact sync proves that full assigned set,
not a three-member recovery quorum. Cold cache reconstruction is not node/process
restart. Normal component reopen is the next separate control; OS crash, full
metadata history/promise/decision custody, checked successor effects, B serving,
C1 floor, application/withdrawal, Terminal/Close/root retirement, degraded W/N,
abrupt-loss/partition/reconnect and absolute-zero quietness remain open. This
lane performs no Docker, production/main rollout or activation work.

## Historical development checkpoint — 2026-10-25 R50

The test-only native local-fence RPC observation is integrated. Receiver/wire
`87794b51`, sender `37982842` and native controls `fb9a004c` merged normally;
exact test-path correction `563a255a` reached code `51a0ecce`. Source `09baa97c`
contains 4,921 inputs; own sealed ELF is `72bd8cc1`. I149 fresh check/build/seal
passed in 33.425440/619.710806/3.263074 seconds. I150 executed **9 PASS**: six
new native controls and three affected local-fence/cooperative regressions.
I151 full boundary passed 11 UI controls and three libraries. Other **213 of
222 inventory IDs remain UNRUN on R50**; prior results do not transfer.

The actual registered candidate B sends the full current R42 reservation and
expected target to eligible metadata replica C. Only the authenticated service
branch can construct sealed ingress. The receiver verifies its actual owner and
current pending request before the existing local writer. The sender makes one
native invocation, checks exact outer/carrier/reservation/acceptor correlation,
then revalidates its local physical owner, pending request and original accepted-
result deadline. Remote physical paths, device/inode values and marker digest/
length remain reported DATA and are never opened locally.

The private nonserialized return has no raw constructor or effect consumer.
It is not a PromiseAck, ChosenFrame certificate, custody/residency proof,
executor or serving permission. Explicit recalls preserve the same full logical
reservation/target body and renew only transport IDs. No per-call KV ledger,
generic sender/inbox fallback, application retry, periodic renewal or durable
clock was added. Ordinary/default production activation stays unchanged.

Actual native controls cover B-to-C return/duplicate, authenticated wrong source/
target/request refusals, lost real reply and same-subject recall, expired or
cancelled return with no owned observation, generic/direct/reply-as-request
refusal and DATA correlation probes, plus default/cooperative refusal with
ordinary Main continuation. The deliberate late-return case exceeds five seconds
and proves absence of an accepted owned result; it is not a successful five-second
RPC latency sample. All original case caps and participant/authority guards remain.

R49/I146 is a preserved **FAILED101** typecheck at 42.315433 seconds, with no
codegen or native execution. Two test references used a private module path.
The correction uses the existing public re-export only; reverse replacement
reconstructs the failed file byte-for-byte, with all 4,920 other source hashes,
IDs, assertions and caps unchanged. No visibility or production fix was needed.

`R50-REPORT.md` and `R50-COMBINED-RESULT.json` retain exact source/artifact/results,
failure and standard acceptance/stage-I/O/node-window/coverage tables. Receipts
are I149 `b29aa00f`, I150 `d1b8582b` and I151 `c6da934f`. I150 preserved 1,894
regular files with archive/hash/compare PASS. All recorded processes, groups,
sessions, own scopes/cgroups and private roots drained; no resource blocker remains.

Quiet windows still require **absolute zero writes**. Explicit RPCs and duplicate
recertification are operation activity, not quietness or writeback evidence.
Physical replacement during RPC, cold Main restart, older-binary interoperability
and attachments remain unrun at this new boundary. All-writer/old-decision fencing,
immutable promise-cut/current accepted state, exact ChosenFrame evidence, complete
custody/settlement/residency, serving transition, real survivor availability,
reconnect/no-failback and quietness remain open. No peer review, Docker,
production activation or push occurred. The original availability goal continues.

## Historical checkpoints

### Preserved R48 checkpoint

Three bounded local-fence failure-cut controls are integrated through normal
local merges: writer hooks `96ead4dd`, in-flight control `90f18926` and owned-process
control `b528320f`, with declaration checkpoint `ea4c460c`. Source `2b4dfd82`
contains 4,919 inputs; own sealed ELF is `9fc4bd84`. I143 fresh check/build/seal
passed in 45.589723/419.316414/3.209887 seconds. I144 executed **6 PASS**: three
new controls and three affected duplicate/namespace/cooperative regressions.
I145 full boundary passed 11 UI controls and three libraries. Other **210 of
216 inventory IDs remain UNRUN on R48**; historical results do not transfer.

The actual registered A-to-eligible-B RPC was constructed and held before
dispatch. B installed its real local DATA fence; the same carrier then resumed
within its original five-second deadline and was refused. Exact B journal
frames, prefix and both ACK floors remained unchanged. This does not claim an
already-issued ACK or A's delayed local decision was fenced.

One owned-process parent ran two real three-member private-fixture children.
Each was pinned while alive and observed killed/reaped by signal9 at its exact
fresh writer cut: before marker publication, and after namespace sync/exact-byte
verification but before any local result. Raw files were captured before recovery
or writer open. The first arm establishes absence only at that owned marker
path; the second preserves the exact stored marker. This is whole-fixture
process death, not one-node loss with surviving peers, power loss or cold Main
readiness. Historical cut DATA grants no RPC promise, ChosenFrame, custody,
lease, recovery or serving authority. Cooperative markers stay preserved.

I140's pre-compilation tooling refusal is immutable: a copied runner asserted
the old R47 HEAD. Normal maintained version probes ran, but no compiler/native
stage started and all216 controls stayed unrun. Its outer failure archive and
drain were verified. I143 binds the sealed current plan HEAD without fallback;
no Rust correction, cap increase or source relaxation was required.

`R48-REPORT.md` and `R48-COMBINED-RESULT.json` bind exact source/artifact/measurement,
standard acceptance/stage-I/O/node-window/coverage tables, failures and cleanup.
Passing receipts are I143 `7fc72538`, I144 `85897ac4` and I145 `0044bd58`.
I144 preserved 938 regular files with archive/hash/compare PASS. Sampled
identities/groups were I14313/5, I14411/15 and I14512identities/2sessions;
all recorded processes, own scopes/cgroups and private roots drained.

Development continues. All-writer/old-decision fencing, immutable promise-cut
versus current accepted state, exact majority ChosenFrame evidence, complete
original-effect custody/settlement/residency and compatible serving transitions
remain separate requirements. Actual survivor operation, abrupt loss, partition,
reconnect/no-failback and Sinomo-only quietness remain open. No peer review,
Docker, production activation or push occurred.


### Preserved R47 checkpoint

The selected local recovery-fence DATA prerequisite is integrated and its
corrected controls are executed. Source/native commits `8f895b89`/`d07c83a9`
merged at `b5d24430`; fixture corrections `db3f30fa`/`f16f4f24` merged normally
at `bec81869`. Current source `3b9e138f` contains 4,917 inputs; own sealed ELF is
`921e758a`. I137 check/build/seal passed in 9.797635/173.475585/3.288828 seconds.
I138 executed exactly the seven corrected cases: **7 PASS**, including six
native controls. I139 current-source boundary passed 11 UI controls and three
libraries. Other 206 of 213 inventory IDs remain unrun on R47. Five other file
controls and three cooperative regressions passed only on R46, not transferred.

The native acceptor is an actual eligible replica B with no cooperative marker.
A's real cooperative markers and serving assignment stay intact. Actual A-to-B
nonempty/empty authority and prefix RPCs apply before the local fence and refuse
after it without changing the real stored floors or prefix. The tests also
execute duplicate/namespace recertification and cold DATA read, malformed/expired/
spent/default/foreign-owner refusal, and pending/activated cooperative byte
preservation. The immutable local ACK and cut are DATA only: not an RPC response,
ChosenFrame certificate, custody proof, lease, serving or recovery authority.

**Primary-writer before/after coverage is unrepresented** because the actual
primary has an ineligible cooperative marker. Its delayed already-authorized
local decision is not claimed fenced. Snapshot support is also not claimed:
normal valid-snapshot refusal and unreadable replacement's existing default-reader
protection are separate outcomes. Metadata participant proof/count, production
activation and all hard identity/policy/store fences remain unchanged.

R46 I135's **8 PASS / 7 FAIL101** remains immutable. The malformed/mixed fixture
was rejected while constructing an invalid zero-sequence initial ACK floor; the
fix establishes one genuine persisted preparation/prefix1 without relaxing that
guard. Native setup met a real primary cooperative marker and used an unsuitable
seed observation. The fix selects actual eligible B and a genuine stable Main
overwrite with a canonical owned read, never deleting a marker or fabricating a
body. Only two test files changed; IDs, order, caps and assertions stay intact.

`R46-REPORT.md`, `R46-COMBINED-RESULT.json`, `R47-REPORT.md` and
`R47-COMBINED-RESULT.json` retain exact source/artifact/result/measurement and
failure evidence. Passing receipts are I137 `41028897`, I138 `25a1b3b2` and I139
`cbc6b4d4`. I138 preserved 1,216 regular files; every case checkpoint, archive,
hash/compare and owned process/group/cgroup/root drain passed. Standard acceptance,
stage/I/O, node-window and coverage tables keep unavailable counters explicit.
No peer review, debugger arbitration, shared build lock, Docker, production,
account/accounting change, push or deployment occurred.

Development continues. Actual all-writer/delayed-decision fencing, immutable
promise cut versus mutable accepted state, exact majority ChosenFrame evidence,
complete original-effect custody/settlement/residency, compatible activation and
physical replacement remain separate protocol requirements. Full survivor
operation, real abrupt-loss/partition/rejoin/no-failback and mandatory Sinomo-only
quietness remain open. The reservation and local DATA fence do not authorize a
count-only weakening of FullShard or complete the original availability goal.


### Preserved R45 checkpoint

The actual native V7 local-commit/reply-loss/delayed-dissemination control now
passes completely. Test-runtime helpers `dcd64f5a`, native control `e808fab1`,
inspection correction `d0331e99` and one-call fault-lifetime correction `e83238b6`
are retained in normal local Git ancestry. Current code is `8f7d717d`; source
`1860de21` contains 4,913 inputs and own sealed executable is `2b5b0afc`.
I131 fresh check/build/seal passed in 15.498832/200.159140/3.204852 seconds.
I132 executed only the new lifetime control and native case: **2 PASS**.
I133 current-source DFS/KV boundary passed 11 UI controls and three libraries.
The other 199 of 201 inventory IDs remain unrun on R45; old results do not transfer.

The native fixture uses the actual three-member metadata assignment and a managed
4,096-byte stable conditional overwrite with content W=1/N=1 required on A.
A real completed Committed reply is captured before loss; normal automatic
physical-call reconciliation can return success. Both metadata peers are still
Prepared while A's actual V7 prepare/commit, original body/base and timestamp are
durable and the exact original commit RPCs are held. After those gates release,
the SAME operation/body/options/base replays the same outcome without a second
target prepare/commit or timestamp change. Both peers' actual frames converge,
one peer normally reopens, content readback matches and strict runtime close passes.

This characterizes existing ACK semantics; it does not add a gate requiring
client ACK to await all peer commit dissemination. `published: false` means an
exact already-local-committed retry, not a dissemination certificate. FullShard,
source authority, semantic durability and all original deadlines remain unchanged.
The test is native in-process evidence, not real one-node loss with survivors,
power loss, a ChosenFrame certificate, recovery quorum or serving activation.

The failures remain explicit. R43 I127 had four passes and a 600-second incomplete
native case: its authored inspection nested a locking journal reader inside the
same held inspection fence. The correction keeps that fence and reads existing
durable entries/exact frames without reacquiring it. R44 I129 reached the semantic
print but failed at close when the one-hit write-loss rule matched an automatic
retry. The lifetime fix binds only the first physical call; distinct retries
bypass that rule while the same call still exhausts. There is no hit/cap increase,
reset, ignored close error or timestamp/identity substitution. I125's earlier
zero-spawn tooling preflight refusal also remains immutable.

`R43-REPORT.md`, `R44-REPORT.md` and `R45-REPORT.md` preserve source-qualified
results, failures, standard timing/I/O/coverage tables and exact artifact hashes.
The passing R45 receipts are I131 `01af2c77`, I132 `459c7b14` and I133 `da94fe23`.
All case checkpoints, archives/hash/compare and owned process/group/cgroup/root
drains passed. No peer review, debugger coordination, shared build lock, Docker,
production, account/accounting change, push or deployment occurred.

Development continues with a separately selected local recovery-fence prerequisite.
Its contract refuses every existing cooperative marker, including activated cuts;
local immutable ACK DATA must not become RPC, ChosenFrame, custody or serving
permission. That source and its native tests remain uncompiled/unrun until their
own coherent integration. Original-effect coverage/settlement/residency, actual
promise/accepted/chosen history, compatible activation/replacement, survivor
operation, real abrupt-loss/partition/rejoin and mandatory Sinomo-only quietness
remain open. The original availability goal is not complete.

### Preserved R42 checkpoint

The non-activating recovery-ballot reservation prerequisite is implemented and
executed in `cfg(test)` only. Implementation `2ca9578d` and native controls
`6e5b0bf6` merged without conflicts at `b9998af5`. Source `561dbcac` covers4,912
inputs; own sealed ELF `e43fa785`. Fresh I121 check/build/seal passed in
40.129955/598.314902/3.012237s. I122 executed **all14 new controls:5 structural
and9 native PASS**, with exactly-one-case summaries. The full I123 DFS/KV
boundary passed11UI controls and3library analyses. The other182 of196 inventory
IDs remain unrun on R42; no prior pass is transferred.

The existing synchronous control-plane owner retains one compact sibling record
per shard: monotonic attempt floor, floor epoch and an optional exact pending
request. The request binds complete serving configuration, a replica candidate,
caller-expected floor, checked epoch+1/attempt+1 and deterministic identity.
Authoritative exact-key reads and CAS prevent reminting. Cancel clears only the
matching pending body and preserves the floor. The root's ordered rule is
normative: old expected floor is Spent unless the exact own request is still
pending and passes current-authority checks; expected-current floor with pending
is PendingHeldByOther. Existing is not ownership. Malformed IDs, invalid floor
records, leases, overflow, stale authority and pending handoff refuse.

Native in-process three-node Raft execution covers distinct/identical CAS races,
repeated and concurrent cancellation, stale request refusal, one peer restart and
readback, ordinary create/maintenance without freeze, real pending-handoff and
wrong/absent authority negatives, no-I/O malformed/expired requests and corrupt
stored values. Each used unchanged outer caps and native absolute deadlines.
This is reservation/verification DATA, not a lease, serving epoch, promise,
accept, ChosenFrame or metadata publication permission. The module is not
`test-support`; no production caller, activation API, ordinary handoff behavior,
metadata participant proof or count changes.

`R42-REPORT.md` and `R42-COMBINED-RESULT.json` bind I121 build `2d7aef4f`, I122
execution `970e01f2` and I123 boundary `9fd0aa9b`. Source and tracked state stayed
unchanged through all three runs. I122 retained1,371 regular files; every exact
case/checkpoint/archive/hash/compare and owned process/group/cgroup/root drain
passed. Standard acceptance, stage/I/O, node-window and coverage tables keep
unavailable metrics explicit. No Codex peer review, debugger coordination,
shared build lock, Docker, account/accounting change, push or deployment occurred.
R41 seals and all earlier failures remain immutable.

Development continues beyond this prerequisite. An actual durable acceptor
promise with immutable accepted/cut evidence and old-write fencing needs its own
finite protocol/caller contract; the unsafe larger Claude R1/R2 gaps are not
closed by the narrower R3/R4 reservation work. Chosen-frame evidence, original
effect bodies/uncertainty and settlement/residency, compatible activation/store
replacement, full survivor authority/history, real abrupt-loss/partition/rejoin
and mandatory Sinomo-only quietness remain open. This checkpoint does not
complete the original availability goal or authorize changing FullShard.

### Preserved R41 checkpoint

The next test-only durability slice is implemented and executed. Helper
`7e62af7e` and process controls `05d0ce37` merged without conflicts at
`0fe92fe8`. Source `0316a026` has4,910 inputs; own sealed executable
`3debf871`. Fresh check/build/seal, both new tests and the full11-UI/3-library
DFS/KV boundary passed. The other180 of182 inventory cases remain unrun on this
source; R40/R39 results stay separate.

One parent owns two actual child processes running the genuine normal native
fixture and custody writer. It SIGKILLed/reaped child1163567 after intake ACK and
child1163820 after ACK plus queue compaction. Ready took10.638613s/10.570144s;
kill/reap took10.198559ms/10.156030ms, both signal9. The parent pinned physical
A queue/content/metadata files while each child was alive, captured raw files
before recovery, and verified byte-identical alive/postkill data. The bounded
forensic helper only decodes exact expected DATA into temporary maps; it never
opens a writer, recovers/truncates, renews an ACK or recreates native authority.
The original/body/attempt and no-Observe/no-Close state survived.

This kills the whole three-node fixture process. It is local post-ACK process/file
durability evidence, not a surviving-cluster A-loss test, power loss, mid-fsync
crash, C1, quorum or availability acceptance. The internal killed helpers are
not separately passing tests. All fixtures, hashes, archives and owned drains
passed; standard timing/coverage tables keep unavailable counters explicit.

`R41-REPORT.md`, `R41-COMBINED-RESULT.json` and exact child evidence
`I119-ACTUAL-NATIVE-SIGKILL-EVIDENCE.json` bind I118 build `4763a804`, I119
execution `7b262274` and I120 boundary `58f20ab1`. All earlier failures remain
immutable. No Codex peer reviews or production/Docker/deployment action occurred.

Development continues. Read-only Claude proposals address the larger metadata
protocol, but chosen-frame certificates, immutable promise replies, pending-effect
custody/settlement and compatible activation remain open; no proposal changes
FullShard. The next architectural prerequisite is a finite pending-ballot
reservation contract in the existing control-plane owner, with serving activation
refused until independently qualified. Full original availability and mandatory
Sinomo daemon/helper quietness remain unfinished.

### Preserved R40 checkpoint

The selected normal sender now captures admission provenance while its real
producer owns execution, carries the same body/attempt through the native RPC,
and retains the actual typed return in the existing outbox on local intake
failure. The deferred custody path writes the original physical queue before
serving-authority handling and returns without receipt/W publication,
historical application, Close or root release. Generic/raw frames, unverified
candidates and the weaker R37 direct-call capability cannot enter this intake.
Production activation, C1 and B effects remain disabled.

Local storage `0557a0b1`/`9fc3bebc`, normal lifecycle `9e56a439` and native tests
`3a7b9c1d` were merged without conflicts. After compiler correction `afff33dd`,
R39 actually executed14 controls: **12 PASS /2 FAIL**. The current-A case exposed
retry-row/head churn after reopen; the retired-A case compared an unfenced
proposal to the actual fenced handoff. Both failures remain preserved.

Scoped correction `efae6d80` uses the existing exact-key parked-page path to
preserve held rows and deadlines. Test correction `7114095c` independently
matches A's durable source cut/freeze to B's installed cut while preserving every
initial proposal field. Current code merge `595376bb`, source `4e755b2e` with
4,909 inputs and ELF `7c3b711a`, passed fresh check/build/seal. The two corrected
native controls and affected W/N regression **all passed on R40**. The other
177 of180 inventory controls are unrun on this source; old passes do not transfer.
Current-source DFS/KV boundary passed11 UI controls and three libraries.

The native cases execute real normal admission/return, namespace-failure and
same-cap retry, duplicate recertification, queue compaction/cold reconstruction
and component reopen. Current A keeps rows/head and wire stable despite holding
serving admission. After completed handoff, old A's provider still refuses
serving authority while the SAME typed return is durably intaken locally. Exact
original/body/attempt and metadata/receipt/root state are preserved; B gains no
object bytes or execution permission. These remain native in-process tests,
not OS-node-loss, power-loss, full coverage or quietness acceptance.

`R40-REPORT.md` and `R40-COMBINED-RESULT.json` retain source-qualified I109–I117
results, failures, standard measurement/coverage tables, archives and verified
owned cleanup. No Codex peer reviews, debugger arbitration, shared build lock,
Docker, deployment or cross-lane integration occurred.

The original goal continues. The next independently executable durability step
is an owned-process death after genuine native intake, with physical/raw evidence
captured before recovery and DATA-only inspection. Full original/effect coverage,
late-fact forwarding, C1, successor effects, causal settlement/root retirement,
metadata majority history, real abrupt-loss/partition/reconnect and mandatory
Sinomo-only daemon/helper quietness remain open requirements.

### Preserved R37 checkpoint

The next bounded native-provenance slice is implemented. Queue witness
`a4416089`, native-owned capture `65d38b5f`, and native tests `f6904f6e` were
merged without conflicts at `0bcd6abd`. Fixture correction `3d235dd4` fixes one
synthetic foreign-ID constructor exposed by typechecking. Exact source
`4ec69d4f` has 4,907 inputs; sealed executable `c3f7c6b1`.

The opaque queue witness binds a complete persisted original, its retained body,
an exact carrier and one actual Attempt sequence to the resident physical owner.
Cold, uncertain, changed or ambiguous prefixes refuse without load/recovery/sync.
It is observational DATA, not a dispatch permit. The new native entry remains
cfg(test/test-support) and resolves the actual registered host and live DATA
owner. It opens the retained body itself, preserves the existing pre-dispatch
original/freeze checks, performs one target-bound RPC and rechecks the same
owner/attempt/content after an exact response. Only that path can construct the
private, nonserialized completion capability. Generic inbox frames, synthetic
correlation output and R35 unverified candidates cannot be promoted into it.

Fresh check/build/seal passed, followed by **12 new controls passed**: six
file-backed witnesses, two pure correlation controls and four actual native
in-process transport controls. The native positive kept the exact original and
4,096-byte content while A's queue/metadata/receipts/root remained unchanged.
Foreign producer and ambiguous persisted attempt refused before RPC; physical
replacement after dispatch withheld the capability with PossiblyApplied.
Default/source authority and receiver checks were not weakened. Existing
30/600-second outer caps and native 5-second budgets stayed unchanged.

Current-source DFS/KV analysis passed all 11 UI controls and three libraries.
Every fixture checkpoint/archive/hash/compare and owned lifetime drain passed.
The 161 older inventory cases remain unrun on this source; R35 results remain
separate. `R37-REPORT.md` and `R37-COMBINED-RESULT.json` bind I106 build
`bb025b2f`, I107 execution `03390c46` and I108 boundary `8146d693`. I103's
failed typecheck, exact source and archives remain preserved; no runtime ran in
that attempt. No Codex peer reviews, Docker, peer scheduling or shared build lock
were used.

This is ephemeral provenance, not durable authenticated retention or production
activation. The complete normal sender-admission ownership join and custody-only
retired-producer intake remain separate. No C1 floor, B effect, receipt/historical
application, Close or root release is enabled. Selected receiver Terminal replay,
complete handoff coverage, metadata majority history and full abrupt-loss,
partition, reconnect and Sinomo-only quietness acceptance remain open. The
original DFS availability goal is unfinished.

### Preserved R35 checkpoint

Local commit `50959901897351c9fe75aabc750f27c5b9b6521c` was merged into
`dev/dfs` at `dbc468316f40208f30036d242543466c4ecc3787` without conflicts.
The existing selected-private Repair queue now retains completion candidate DATA
against an existing persisted original and carrier attempt. Its caller remains
disabled outside test/test-support. This is a bounded persistence primitive,
not authenticated completion or successor execution.

| Contract | Behavior |
|---|---|
| Accepted inputs | Queue incarnation, immutable original digest, exact persisted attempt sequence/carrier, full supplied response and explicit `FixtureUnverifiedTransportMaterial`. Original identity/body digest and response scope must correlate. |
| Durable output | One immutable candidate per original subject and attempt, with its actual queue append sequence and material digest. The existing framed writer and selected-file/namespace synchronization own persistence. The returned reference grants no effect authority. |
| Duplicate / conflict | Exact duplicate recertifies the existing record and returns the same reference without append. Changed same-attempt material, missing or foreign birth/attempt, wrong scope and replaced physical owner refuse. |
| Default compatibility | Ordinary/default APIs and readers reject selected candidate records. Empty snapshot fields remain omitted and existing enum ordinals are preserved. No V1 migration or quarantine is activated. |
| Reopen / failure | Cold replay and compaction preserve exact DATA and identity. Malformed prefix, duplicates and mixed frames reject before publishing derived state. A namespace-sync failure after append withholds success; retry recertifies the same retained record. This does not prove OS-crash or power-loss behavior. |
| Disabled effects | No runtime caller, authenticated peer proof, delivery observation, receipt application, Terminal, current B permission, Close or root retirement is added. Candidate records are local; handoff capture does not automatically transfer or certify them. |

Merged source `eaaad7c0` contains 4,905 inputs; sealed executable `b5e06e2d`.
Fresh check/build/seal passed. All **14 focused file-backed controls passed**:
eight new candidate controls and six existing queue/history/default/compaction
controls, each discovered and executed once with its original 30-second cap.
Current-source DFS/KV lint passed all 11 UI controls and three library analyses.
Unconditional archives, case checkpoints and owned lifetime cleanup passed.
Of the 161-ID source inventory, 147 were not rerun; older passes remain separate.

`R35-REPORT.md` and `R35-COMBINED-RESULT.json` under the operation directory
bind build receipt `3a3ac4e0`, execution `938773df` and boundary `cded7bcb`.
I98's stale tooling count failed before Cargo. I101 passed typecheck but a tooling
edit around dispatch caused postcheck hash refusal; no codegen or tests ran in
that attempt. Both failures, revisions, archives and the corrected loaded-plan
attribution remain preserved. I102 used frozen tooling and a fresh check.

The next runtime dependency is a distinct, actual transport-owned immutable
completion/attempt input and custody-only admission under the retired producer's
physical owner, before current-authority rejection in
`runtime_host::message_host::owned_repair_completion_scope`. That path must
return a typed deferred outcome before application. Existing unverified candidate
records cannot be relabeled as native evidence. Full coverage, C1/current B
effects, causal withdrawal/settlement, root retirement, metadata majority history
and abrupt-loss/partition/reconnect/quietness acceptance remain open.

### Preserved R34 checkpoint

Local merge `1dadb5122963b865f0cb62f5c22141b0457bf0e0` integrates reviewed
process-storage controls `e17be2cf` without conflicts. Source `32ae5330` has
4,904 inputs; sealed executable `ec4393da`. Only two additive test-only Unix
blocks changed; existing production and test bodies remain byte-identical.
Fresh check/build/seal passed. A nine-case focused storage/owner panel and two
additional existing read controls all passed. Current-source DFS/KV lint passed
all 11 UI controls and three library analyses. All archives and owned lifetimes
were verified after cleanup.

The new parent actually killed and reaped two owned children with signal9 after
synchronized Prepared and Bound. It retained raw files before normal recovery,
then verified exact material, physical incarnation and phase IDs. Prepared had no
Bound/Terminal; Bound preserved exact content and idempotent binding, with no
Terminal. Wrong body/physical expectation/incarnation/default reader rejected.
This is synthetic storage-fixture OS-process/file evidence, not native receiver
admission, authenticated Terminal durability, power loss or three-node takeover.
The internal killed helpers are not counted as separately passing tests.

The existing native W=2,N=2 read case returned exact32KiB after one holder's real
blob was removed, with unchanged metadata prefix/fingerprint; wrong-version
responses also rejected. Metadata participants stayed healthy. No content-W bug
was found or production fix invented; latest/exact metadata authorization is a
separate availability dependency.

Of R34's original153-case inventory, nine ran and144 remain unrun. The two reads
are additional existing IDs outside that inventory. R33's full152-case pass
remains separately qualified below. `R34-REPORT.md` and `R34-COMBINED-RESULT.json`
bind I94 build `0c242485`, I95 storage `ea757cc6`, I96 boundary `156e355f`, and
I97 reads `f7caf36e`; exact two-child evidence is `93f8983f`. The I91 wrapper
mismatch failed before Cargo and remains preserved, including its inner archive
failure and successful outer preservation/drain. No old failure is reclassified.

The next checked-successor contract requires effect-free support before a new
Prepare, durable receiver minimum across restart/replacement, lossless original
admission/late facts and permanent canonical floor lineage. A transient capability,
selected DATA cut or report-only preflight cannot replace those joins. Complete
custody, current B effects, causal settlement/root retirement, metadata majority
history and full abrupt-loss/partition/reconnect/quietness acceptance remain open.
Independent DFS-local ownership continues; no Docker, peer permission workflow,
production activation, push or cross-lane merge is part of this work.

### Preserved R33 checkpoint

Local merge `ee68eeb7612b0aaff1e9a85076061452c59e5d52` integrates reviewed
observation correction `64e4b86f` without conflicts. Source `b4c7e691` contains
4,904 inputs; sealed executable `13d3b288`. Fresh check/build/seal passed. Two
disjoint panels executed **152 unique controls once: 152 PASS, zero failed,
incomplete or unrun**. The current full DFS/KV boundary passed its own lint
library, all 11 UI controls and DFS/history/attachments analysis. Unconditional
archives, case isolation and owned process/group/scope/root cleanup are verified.

The same-C control now proves genuine delayed-response application, exact-subject
closure denial through native recovery, authenticated observed-original handoff,
then current-B withdrawal. Exact historical query/replay preserves C's absence,
desired repair deficit and open custody. The retiring-B control joins exact C
completion to the same authenticated call before accepting the existing precise
B admission refusal. Only test observations/assertions and failure capture changed;
no production authority, participant requirement or deadline was relaxed.

The V7 stored-frame retry/reopen/timestamp-negative case also passed in this full
panel. Historical Terminal remains DATA, not current-holder/application authority,
a B mutation permit or root-release evidence. Prepare-only survivors cannot
reconstruct an unseen old-primary commit. Component stop/reopen and file-writer
tests do not establish OS-process/partition acceptance.

Evidence is `R33-REPORT.md` and `R33-COMBINED-RESULT.json` under the operation
directory: I87 build `f26954ad`, I88 focused `a2a55fbd`, I89 remainder `25722f39`,
I90 boundary `137e9651`. The report includes standard performance/coverage tables
with unavailable measurements explicit. R32 and all earlier failures remain
immutable; missing historical RPC observations are not reconstructed.

Next independent work closes the smallest actual checked-successor record/caller
contract and the existing owned non-Docker process-failure test path. Complete
custody coverage, C1/current B effects, causal settlement/root retirement,
metadata majority history and real abrupt-loss/partition/reconnect/Sinomo-only
quietness remain open. This lane uses its own local lock and limits, with no peer
permission workflow, Docker, production activation, push or cross-lane merge.

### Preserved R32 checkpoint

Local merge `7242584e4a6a669ed332aa1dd615d08d07a160e1` adds the reviewed native
same-C withdrawal control from `56839fed`. Source `3c6430bf` has 4,904 inputs;
sealed executable `354a1916`. Fresh check/build/seal passed. The focused panel
executed **29 cases: 27 PASS, 2 FAIL**, with no incomplete or unrun case inside
that panel. The other123 inventory cases were not run on R32. Current-source
DFS/KV lint passed all11 UI controls and three library analyses; all lifetimes
and unconditional archives were verified after cleanup.

The new same-C case reached the negative closure boundary but failed its exact
RPC-observation predicate before its receipt/recovery/handoff assertions. The
retiring-B case returned a precise native owner/admission/storage refusal outside
its accepted error list. Both remain FAILED. Source review found missing decoded
identity coverage for Repair responses/history requests; the reviewed successor
adds exact authenticated Decoded-to-terminal per-call joins and bounded failure
capture. It changes no production guard. `R32-REPORT.md` and `R32-RESULT.json` in
the operation directory bind I83 `0de104b3`, I84 `f6e51628`, I86 `7bd87cbf`.

The separate R31 exact V7 frame test passed on source `c225bd30`, executable
`55516360`: actual frame/timestamp equality across retry/native owner reopen,
plus timestamp-only export and checked-replay negatives. Its573-byte frame is
preserved. R31 changed only that test body; the other150 cases were not rerun on
R31. Evidence is `R31-REPORT.md`, I80 `ebd74ba0`, I81 `4873df06`, I82 `9ab4dbca`.
Prepare-only survivors still cannot reconstruct an unseen old-primary commit.
No V7 production correction or participant weakening was made.

R30's full150-case pass remains the separately qualified baseline below. Complete
custody coverage, C1/current B effects, causal application/root retirement,
majority metadata history and real abrupt-process/partition/reconnect/quietness
remain open. This lane remains independent and nonblocking, using only its local
lock and owned limits. No peer permission workflow, Docker, production, push or
cross-lane merge is part of this work.

### Preserved R30 checkpoint

Local code merge `3280966922517cc0a546a4eee8acde77e14f0555` integrates reviewed
snapshot correction `8b3163fc`. Source `9a7b0cf3` contains 4,904 inputs; its own
sealed executable is `c01d2db1`. Fresh check/build/seal passed. Two disjoint panels
executed **150 unique controls once: 150 PASS, zero failed, incomplete or unrun**.
The current full DFS/KV boundary passed its lint library, all 11 UI controls and
DFS/history/attachments library analysis. All owned lifetimes and unconditional
fixture archives were verified after normal cleanup.

Named snapshot fields fix R29's two heterogeneous-tuple encoding failures without
changing live tuples, events, writer/sequence validation or default V1 behavior.
Actual controls now preserve exact empty-state compatibility, recorded delivery
states, Pending history and genuine Terminal across compaction/cold replay. The
original clocks and assertions remain intact. All R29 failures remain sealed.

All seven native historical-query controls passed, including no B payload,
missing-Terminal Pending, exact reply binding, C certification/reopen cuts,
physical replacement, retiring B and later real A receipt withdrawal. Historical
results remain observations: no B Repair effect, current receipt application,
birth closure or root release is authorized. Same-C applied-receipt withdrawal
is the next concrete native coverage gap; exact V7 stored-frame retry/reopen is a
parallel test-only task. No general framework or participant reduction was added.

Evidence with standard tables is in `R30-REPORT.md` and
`R30-COMBINED-RESULT.json` under `data/operations/dfs-availability-20261023/`:
I76 build `0fb31f8f`, I77 focused `f751c768`, I78 remainder `fbab7138`,
I79 boundary `189de44c`. These are selected development results, not full abrupt
availability. Original R67 remains unresolved; complete custody coverage,
C1/checked B effects, causal application/withdrawal, root retirement, metadata
majority history/fencing and real OS abrupt-loss/partition/reconnect/quietness
remain open. Local commits/actual merges are the boundaries; no Docker, peer
synchronization, push, deployment or cross-lane merge occurs.

### Preserved R29 checkpoint

Local merge `dc2c96e991ce0d834006432b033e66c237eada5f` integrates the private
historical-query branch `708cad79`. Source `6de91a23` has 4,904 inputs; its own
sealed executable is `9edf0efb`. Fresh check/build/seal passed. The focused panel
actually executed **27 controls: 25 PASS and 2 FAIL**, with no incomplete or unrun
case within that panel. The other 123 inventory IDs are unrun on R29. The current
full DFS/KV boundary gate passed all 11 UI controls and three library analyses.
Earlier results do not transfer to this source.

The query obtains exact retained original history from native C and journals a
B observation through the existing queue writer, without B payload, a new Repair
effect, receipt application, closure or root release. Six native query cases
passed. The local duplicate-prefix control and native restart/certification case
failed during queue compaction: persisted heterogeneous tuples cannot be encoded
by SOBJ's homogeneous sequence serializer. A named-field snapshot correction is
being prepared in isolation; the failed source and all raw evidence are retained.

The one actual merge conflict was in `service_sync.rs`, where two fixture modes
met. Separate auth-probe and Prepared-only wrappers preserve their existing
semantics and reject combined use. Root's auth branch and atomic capture stayed
byte-identical; the query additions were retained. All owned process/group/scope/
root lifetimes and unconditional fixture archives were verified after cleanup.

Exact standard tables and receipts: `R29-REPORT.md` and `R29-RESULT.json` under
`data/operations/dfs-availability-20261023/`; build I72 `fcc0eddf`, focused I73
`d01767f7`, boundary I75 `f180a523`. No generated evidence is committed.
Historical Terminal is not current-holder/application authority. Snapshot replay,
checked B effects, causal withdrawal, C1, root retirement, metadata quorum and real
abrupt-process/partition/reconnect/Sinomo-only quietness remain open. This remains
independent nonblocking development, with only the DFS-local lock and no Docker,
production activation, push or merge into dev/debug or main.

### Preserved R28 checkpoint

Local code merge `e700d3bd4b071ebbb7e1072afb8c89fad0f6a98c` has source
`38bced0d25ad1cede26adc01a4bbb505f7082a70ab8c5f810b2c43fc39df7d3c`
(4,903 inputs) and sealed own executable
`8f6f19dac5697b29da64c2b7438e8e2f4c7c4cf8cbfde339b73c7706398f86c2`.
Fresh check/build/seal passed. Disjoint focused and remaining panels executed
**140 unique controls once: 140 PASS, zero failed, incomplete or unrun**.
The full current-source boundary gate passed its own lint library, all 11 UI
controls, and DFS/history/attachments analyses. All owned lifetimes and fixture
archives were verified after normal cleanup.

The original receiver now tolerates continuous observational configuration
progress while retaining the same native proof origin, authority and physical
fences. Its floor read occurs outside locks; the current view and metadata cache
are copied together under the existing configuration→projection order. No retry,
new counter, metadata participant reduction or persisted floor was added. Six
runtime controls cover update-during-read, withdrawal, rebootstrap, policy ABA
and discontinuity. The actual native Terminal case passed while projection25→26
and revision71→72 advanced with unchanged authority18 and native proof origin.
It preserved original identity/body and reached Prepared→Bound→Terminal before
lost reply, compaction and native-owner reopen. This is not OS-process proof.

The native authenticated-source negative also passed: B could not execute an A
envelope, C files/A queue stayed unchanged, then the same paused genuine A call
completed. Other selected DATA, policy, legacy and fresh W=2/N=3 controls passed.
Fresh placement still has all metadata participants available, preserves desired
N=3/A deficit and heals A; general overwrite and abrupt-loss availability remain
unverified. Full-participant metadata checks are unchanged.

R26/I63 and R27/I66 failures remain preserved. The new successful trace demonstrates
the corrected continuous-progress path; it does not identify those older runs'
unrecorded delta/proof origins. Original R67 positive successor continuation,
request/body and committed intent remain retained and unresolved.

Exact evidence is in `data/operations/dfs-availability-20261023/R28-REPORT.md`
and `R28-COMBINED-RESULT.json`: build I68 `6b5d09c1`, focused I69 `f1770881`,
remaining I70 `e1f93ba0`, full boundary I71 `d8ff22a9`. Standard tables preserve
unavailable measurements. All actual local merges were conflict-free; generated
evidence is uncommitted.

The next isolated candidate queries retained exact historical results using
existing queue/content/native owners, without B payload, Repair execution,
receipt application or root release. It is not yet integrated or executed.
Source review is correcting precise snapshot/certificate and fixture-lifetime
issues before compilation. Checked B effects/application, C1, root retirement,
majority metadata history/fencing, real abrupt-loss/partition/reconnect and
Sinomo-only quietness acceptance remain open.

This remains independent, nonblocking development: DFS-local serialization and
own16GiB/no-swap limits, no peer permission/handback workflow, Docker operation,
production activation, push or merge into dev/debug or main.

### Preserved R25 checkpoint

Local code commit `2d78a291` has source
`e350c2e0fd4c29508095a6ca21b16dfd0b0f1bba846d8ac36ae5505db35759fe`
(4,903 inputs) and sealed own executable
`056cc84114f0504d06e0b9d59bb7ec310ecd98456dd6940452c41cd629f3cd97`.
Fresh check/build/seal passed. The disjoint I59 seven-case and I60 remaining
126-case panels executed all **133 unique controls once: 133 PASS, zero failed,
incomplete or unrun**. Current-source DFS/KV lint passed its own library,
11 UI controls and DFS/history/attachments analysis. This is a selected development
panel, not full availability acceptance.

The genuine native A→C receiver control now reached Prepared→Bound→Terminal
before its reply was lost. It verified the complete original/body, exact response,
A's retained uncertainty/root, C compaction and native-owner stop/reopen. The
sender controls verified renewed carriers with unchanged original identities,
actual owned attachment forwarding and ordinary correlation. Inspection controls
verified physical-owner/layout rejection and cold noncertifying, nontruncating
reads. These are in-process native/object-RPC/file-writer tests, not OS process loss.

The native fresh-create W=2/N=3 control also passed, retaining desired N=3 and
A's repair deficit while B/C held exact content and then healing A normally.
Metadata participants remained healthy. General overwrites and metadata-outage
availability are not established; full-participant metadata proof is unchanged.

R24's native configuration-generation refusal remains failed and unattributed.
R25 kept its guard unchanged and added failure-only diagnostics, which did not
fire in the passing run. Earlier source/compiler/preservation failures and the
original R67 positive successor-continuation failure remain retained. No result
was reclassified or uncertain effect recreated under another identity.

Recent conflict-free local merges: cc073382/a7a9fb56 for actual-owner dispatch
inspection and whole-call forwarding;88fd6bcd/fd84679f for strict fresh child-root
fixtures.2d78a291 adds bounded failure-only diagnostics. No generated artifacts
are committed. Exact tables and receipts are in
`data/operations/dfs-availability-20261023/R25-REPORT.md` and
`R25-COMBINED-RESULT.json`: I58 build43464e6d, I59 focused51d0a73b,
I60 remaininga47ecd6b, I61 boundary6fdc13d8. All owned lifetimes and archives
were verified after normal cleanup; unavailable metrics remain explicit.

Next isolated work is one native authenticated-source negative and a reviewed
private historical-result query using existing queue/content/native owners.
The query requires no B payload, cannot rerun Repair or apply a receipt, and
releases/reacquires current admission around its exact direct-return RPC. It is
not yet implemented or tested in this checkpoint. Checked B execution/application,
C1 serving/admission, root retirement, majority metadata history/fencing,
abrupt process/partition/reconnect and Sinomo-only quietness remain open.

Local commits and actual tested merges are integration boundaries. Launches use
only the DFS-local lock and own16GiB/no-swap limits with exact qualification and
cleanup. No debugger synchronization, Docker operation, push, production/main
rollout or merge into another lane occurs; this development remains nonblocking.


### Preserved R23 checkpoint

Local code commit `37e6129604834e77aabbba56beb81abd9b784d39` has qualified
source `14cb88d94479b813a318694127d21939a488a4a60ee259b0bae080919ff9615d`
(4,903 inputs) and own sealed executable
`30e6e0c816e5da49487e9ba008a9d4a324070eceac2b72a8d8bb04a346bc9ee8`.
Fresh typecheck and libtest generation passed. Across disjoint I49/I52 execution,
127 unique controls yielded **126 PASS / 1 FAIL / 0 incomplete / 0 unrun**.
I49's separate preservation checkpoint failed on a process-exit race; that failure
remains retained. The reviewed operation-local correction passed ten controls;
I52 then executed only its 125 unrun cases with every preservation checkpoint passing.

The fresh-create native W=2/N=3 control passed actual B/C content and receipt
checks, retained N=3 and A's deficit, and healed A normally. It used healthy
metadata participants and denied A's content transfer. Original reply loss was
completed through the same operation/body/options and actual normal Main response;
read-only reconciliation was not changed. This is bounded fresh-create evidence,
not abrupt node loss, general overwrites or metadata quorum.

The only failed Rust control is the prospective receiver Terminal positive.
Unchanged I53 tracing captured default-layout inspection rejecting the actual
private-format-4 content owner before RPC dispatch. Original birth/attempt and
in-flight evidence remain intact. A focused owner-qualified inspection and
whole-call attachment forwarding correction is under review; it has no execution
acceptance yet. Historical Terminal delivery and foreign B execution remain disabled.

Current-source full DFS/KV lint passed all 11 UI controls and all three library
analyses. Standard tables, exact receipts and coverage limits are in
`data/operations/dfs-availability-20261023/R23-REPORT.md`. I48 build receipt05931c3a,
I52 runtime5d1224d1, I50 boundaryfc7cd7cf and I53 failed diagnostic1e6490b8 are sealed.
All local merges were conflict-free; generated evidence remains uncommitted.

The original R67 positive successor continuation remains failed on preserved
40394f47/17295eb6. C1 admission, checked B execution/result correlation, causal
application/withdrawal, root retirement, majority metadata history/fencing,
full W/N degraded operation, abrupt process/partition/reconnect and Sinomo-only
CPU/disk/network quietness remain open. Missing per-node metrics stay unavailable.

Local reviewed commits and tested merges between isolated DFS subtasks are the
integration boundaries. Launches use only the DFS-local lock and own16GiB/no-swap
limits with actual source/artifact/runtime checks and cleanup. There is no peer
capacity/source-release/handback workflow. dev/debug owns Docker and main owns
rollout; this lane operates neither, pushes nowhere and remains nonblocking.


### Preserved R19 checkpoint

Local merge `3e61778bea4c372ee4d36f01c115cc13873c190f` contains the private,
prospective A-origin receiver foundation. Source
`d73c9710274f3a34a41da7021c00d528fabd3308baaf183d227803b4cf57148f`
has 4,901 qualified inputs; sealed executable is
`fff379588e06ca555ce4b92327e5bb4cad65dd3c7e9d6502e664d617dc178876`.
Typecheck, actual libtest generation and artifact seal passed. The exact 86-case
panel executed **83 PASS / 3 FAIL / 0 incomplete / 0 unrun**. All exact summaries,
per-case cleanup checkpoints, unconditional archives and owned drain were verified.

The prior 56 DATA controls yielded 55 passes and one genuine-original setup
failure before the invalid-ingress assertions. All 22 affected receiver/content
regressions passed. Six of eight new receiver controls passed; the default-reader
negative failed its empty-metadata assertion, and the native Terminal positive
failed ordinary startup before reaching its target. Neither failed target is
accepted. The failure-only diagnostic now ran and retained actual RPC, queue and
capture facts; source/evidence attribution continues without inventing a missing
original or replacing its identity.

The maintained current-source DFS/KV gate passed its own lint library, all 11 UI
controls (two accepts, nine intended rejects), and DFS/history/attachments library
analysis. Default library lint does not verify test-only native behavior.

The receiver adds complete immutable origin material to the existing content log,
with an actual Prepared-bound content effect and exact content-only result before
the initial reply. Its explicit private format leaves default V1 behavior intact.
Historical result delivery and foreign B execution remain disabled. Source review
found an additional required immutable/current forbidden-node and LocalOwned
preflight; desired membership alone cannot satisfy it. The current passes do not
close that gap or the remaining native authorization/crash controls.

Recent actual local merges, all without conflicts:

- `1da2220f` adds freeze-before-first-capture controlled native-owner restart with
  externally retained selection; it passed on R16 and R19. This is not OS-process
  loss or automatic reconstruction of selection.
- `e7f56921` adds bounded failure-only observations, preserving the original
  assertion, identity and timing budget.
- `99a7c9a8` merges the prospective receiver writer/caller foundation.
- `9b08cc3b` and `3e61778b` merge compiler-guided cfg/visibility and test trace-label
  fixes. R17 and R18 failed compiler snapshots remain retained.

The independently reviewed placement candidate `8f4da8ce` is not yet merged or
compiled at this checkpoint. It separates desired N from attemptable targets for
a bounded fresh background-create path using retained admitted inventory. Actual
receipts still determine W; ordinary full-participant metadata proof is unchanged.
Its 24 new controls and 12 selected existing regressions remain unrun.

Exact receipts under `data/operations/dfs-availability-20261023/`: I39 build
`a82d0cbe`, I40 panel `ba1d2de0`, I41 boundary `9a952382`; `R19-REPORT.md` retains
standard acceptance, stage/I/O, window and coverage tables. Build/no-run timings
are 39.090801/620.644470 seconds; panel and lint measurements include observation
and preservation overhead. Missing per-node metrics remain unavailable.

The original R67 positive successor continuation remains failed on its preserved
`40394f47` source and `17295eb6` executable. DATA import and disabled B association
cannot close it. C1 serving/admission, checked B execution and exact result
correlation, causal application/withdrawal, root retirement, majority metadata
history/fencing, general W/N degraded writes, actual abrupt process/partition and
reconnect acceptance, and Sinomo-only CPU/disk/network quietness remain open.

Local commits and reviewed/tested local merges between isolated DFS subtasks are
the integration boundaries. New launches use the DFS-local lock and own
16 GiB/no-swap limits, source/artifact/runtime qualification and actual cleanup.
There is no peer capacity/source-release/handback workflow. `dev/debug` owns its
Docker cluster and `main` owns real rollout; this lane operates neither, remains
nonblocking, and does not merge into either lane, push or deploy.


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
Keep debugging/tracing/profiling and existing CPU/network thresholds. Quiet
windows require zero writes even when maintenance runs; the activity-only 1 MB
allowance cannot be used to pass quiet maintenance. Time-only keepalive and
renewal state stays in memory, and persistence requires actual semantic change.
Mandatory quietness covers Sinomo main/shadow, attributable helpers/delegates,
network and writeback;
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
