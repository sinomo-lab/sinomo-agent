# Developing while a distributed subsystem is unstable

This guide assigns the decisions that need human judgment and the work agents
should execute independently. It follows the [DFS development lessons](dfs-development-lessons.md).
It does not change the current [DFS protocol](../dfs-architecture.md) or grant
permission to deploy. Its purpose is to keep a correctness incident from making
every other feature depend on the incident's last failing test.

## Preserve the requirements through implementation changes

Sinomo requires outage tolerance and W/N separation: W is commit durability and
N is desired placement. Roaming devices need not remain online, and repair must
resume after reconnection. An implementation restriction does not replace these
requirements merely because it appears in code or tests.

The human owns product requirements. Once stated, agents own carrying them through
each implementation, optimization, safety fix, test conversion and documentation
update. A recently rewritten architecture paragraph or a green safety test cannot
silently supersede the original behavior. The [development lessons](dfs-development-lessons.md#preserve-requirements-independently-of-implementation)
explains how a full-shard restriction can displace the required positive behavior.

Keep these behavior examples attached to the implementation work. They are
existing requirements or obligations to verify, not questions for the human to
answer again:

| Situation | Required behavior or implementation obligation | Required evidence |
|---|---|---|
| A content replica disconnects; W qualifying targets and safe authority survive | Accept writes under W, retain N as the repair target and preserve hard placement constraints | Write, readback, retained repair obligation, reconnect |
| Primary disappears without freezing | Establish a safe successor from the supported surviving authority; do not depend on the missing primary's cooperation | Crash/partition before and after acknowledgement; old primary rejoins without creating a second history |
| W=1 content exists only on the disconnected node | Report content unavailable while retaining its recorded identity/copy | Explicit unavailable result; no invented content or deletion of the recorded copy |
| Metadata service is unavailable | Keep independent capabilities usable through their declared dependencies | Exercise the promised UI/model/local/diagnostic capability while its unrelated dependency is unavailable |
| Client loses a reply after a durable effect | Preserve ownership and resolve the original operation | Same operation reconciles after reconnect/restart; no duplicate effect |
| Optional History/Attachments component is absent | Keep core operations and stored promises intact | Core-only composition plus previously committed retention/reference evidence |

**Known implementation mismatch at the reviewed revision:** ordinary publication
requires all assigned metadata participants and activation needs retiring-primary
evidence. These constraints are not accepted replacements for the availability
requirement. Metadata safety still needs a valid protocol; deleting a count check
alone is unsafe. Restoring the requirement means correcting the algorithm, not
arguing that a separate metadata layer exempts end-to-end writes from it.

For each promised failure case, specify recovery progress as well as safety:
the event that wakes work, its owner, its terminal result, and the original
deadline or agreed recovery objective. A system that safely stays pending
forever is not an implementation of an availability requirement.

## Preserve a useful baseline, not just a source checkpoint

Before a cross-cutting rewrite, name the working capability to preserve and
retain evidence for it. The baseline needs source/lock/artifact identity, data
format, topology, workload, and known failures. A passing fresh cluster and a
passing upgrade of old DATA are different baselines. A WIP commit is useful
preservation; it is not automatically a release candidate.

The human owner chooses the minimum useful release and accepted limitations.
Correctness, bounded recovery and attributable idle use can gate a release while
a missed latency objective remains an explicitly accepted performance limitation.
An unresolved write outcome cannot be reclassified as a timing miss because the
command timed out.

For a mechanism collapse, agents should propose separately reviewable changes:

1. Establish a behavioral contract and production-shaped fixture for the current
   path, including its actual authority and lifecycle prerequisites.
2. Move ownership or share implementation while retaining observable behavior.
3. Change the protocol/format with its failure model and old-DATA conversion
   evidence. For an intentional breaking change, update all callers and isolate
   migration; do not keep two authoring protocols just to make smaller slices.
4. Remove superseded mechanisms once their responsibilities are accounted for.
5. Measure performance on the resulting behavior and unchanged workload.

These are review boundaries, not a requirement that every format change can ship
halfway. If a migration must be atomic, preserve the old runnable artifact and
validated data copies and make the larger acceptance obligation explicit.
Never assume rolling back a binary also rolls back its durable format.

Judge simplification by fewer state owners, persistence formats, retry paths,
authority sources, and lifetime rules. Net LOC is secondary. Moving legacy code
to another crate or deleting tests can shrink core without simplifying the system.

## Prevent a repair from creating the next failure family

Agents should explain the mechanism delta with a fix: which existing owner was
wrong, what it will do differently, and which state/transition is added, removed
or reused. The human should not need to prescribe individual locks or repeatedly
request simplification. Review the delta when it changes the algorithm's public
behavior or adds another independent owner of already-managed state.

Use these concrete decision tests:

- Before supporting a “legacy” record shape, find a real historical producer.
  An optional serialized field and a malformed negative fixture are not proof
  that a deployed writer produced that shape. An anonymous-permit drain subsystem is unjustified if no supported producer
  can emit anonymous permits.
- Before redesigning a generic API to support recovery, establish whether the
  caller wants a new effect, a retry of an unresolved effect, or observation of
  a committed result. Receipt validation should use existing authoritative outcome reads; resubmitting
  transactions for that purpose can create an unnecessary caller-binding or
  replay API problem.
- Before adding a freshness cache or another scheduler, identify the current
  authoritative owner and the exact invalidation/wake edge. A private copy that
  misses snapshot installation needs coherent observation/reuse, not another
  synchronization protocol over the same history.
- Before converting a passing degraded-operation test into an expected rejection,
  compare the observable behavior with the original requirement. More negative
  assertions can make a narrower system look rigorously verified.

For this unreleased product with no external users, do not assume mixed-version
wire compatibility, multiple active codecs or old authoring paths are required.
Update in-tree callers together unless actual compatibility is requested. Existing
operator DATA and uncertain operations still require preservation or a bounded
conversion plan; that does not require indefinite compatibility machinery in the
normal runtime. Unsupported/corrupt test input should remain rejected, not become
a new supported historical state.

Use the [mechanism review](../../.agents/skills/sinomo-dfs-debugging/references/mechanism-review.md)
to produce a small state-ownership table and exact proposed edit list. This is an
analysis artifact, not another runtime registry or a new approval gate.

## Separate development dependencies from release dependencies

Maintain a capability map for the current revision, rather than saying “DFS is
red, so development is blocked.” Use the existing facades and VFS contracts.
Fixtures establish component behavior; they do not establish distributed
reliability or authorize a production fallback.

| Lane | Work that can proceed | Integration evidence still required |
|---|---|---|
| App presentation | Layout, navigation, transcript rendering, progress/error presentation through `sinomo-client` and `sinomo-api` | Actual daemon contracts and failure states |
| Model/agent algorithms | Provider adapters, context selection, compaction, tool selection, scripted turns | Hosted persistence and provider-backed execution where applicable |
| Local execution | Granted workspace tools and cancellation through existing VFS/runtime seams | Check startup/home/session dependencies; “local” alone does not imply DFS independence |
| Optional filesystem features | History/attachment schemas, UI and pure transformations | Stored grants, core semantic service, restart and disabled-component behavior |
| DFS correctness | Publication, authority, transport progress, repair ownership, same-operation recovery | Native composition, retained DATA, real transport and release gates |
| Availability restoration | Original failure requirements, model counterexamples, pure placement/projection work | Shared authority/admission changes sequenced with the correctness owner |
| Measurement | Offline collector analysis, replay and report rendering | Live accounting coverage and explicit real-host scope |

Normal chat also opens SIFS-backed catalogs, channels and roaming checkpoints. Do not advertise independent chat availability
from a local session-store test. If local work must continue through remote
persistence failure, the human owner settles the durability promise and visible
pending state; agents implement/test it through the owning contracts. A raw
host-filesystem fallback is not an acceptable shortcut.

Assign lanes by owned contract and files, with a concrete integration seam and
testable deliverable. A second agent editing the same authority kernel usually
adds coordination rather than an independent lane. Source analysis, fixture
design, UI work and offline report analysis can proceed while builds share a
resource slot. Delegation depends on current authorization and available tools.

## Keep the debugging loop informative

The human need not prescribe the next Rust edit or approve every experiment.
Ask for a failure inventory, a dependency graph and the next discriminating
experiment. Agents own reproductions, implementation, ordinary validation and
maintaining the inventory.

Each row distinguishes observed symptom, exact failed operation, first missing
boundary, prerequisite state, candidate versus demonstrated cause, product versus
fixture versus environment failure, original evidence, fix coverage, release
impact and work that remains independent. Unclassified is a useful honest state.

Group failures by shared ownership/progress boundary, not just error text. A
missing acknowledgement can be caused by a blocked reply, unnecessary
configuration rebuild, lost detached work or actual authority rejection. The
count alone cannot choose a fix. Audit sibling paths once the shared cause is
known: required FetchBlob, completion replies and journal catch-up can all lose
progress when treated as optional by advisory scheduling.

After a fix fails its predicted experiment, revise the hypothesis before another
full run. A passing retry of an intermittent case does not explain the failed
attempt. Broad suites remain necessary after a coherent change; repeating them
without changed source, a controlled schedule or a new observation rarely
answers the unresolved question.

If an episode keeps finding new prerequisites, reassess the scope: narrow bug,
missing shared fixture, or availability contract the design cannot satisfy?
Agents should bring an exact counterexample and the smallest decision needed,
rather than asking generally whether to continue.

## Make coordination proportional to the decision

Keep design documents independent of the debugging narrative. Architecture owns
the stable requirements and boundaries; a fix's lock order, ACK handling and
state updates belong with source/tests or a focused reference. Status and run
evidence stay in task artifacts. Use the
[documentation boundaries](../README.md#document-status-rules) rather than adding
a permanent paragraph for each repair.

Use one durable handoff per integration milestone: source, artifact, applicable
tests, unresolved operations, owned state and next owner. Avoid a chain of status
pings, reviews or renewed permission for each authorized implementation step.
Historical bridge instructions, model choices and resource exceptions are not
permanent repository policy.

During autonomous work, make scope changes visible before they accumulate.
Distinguish requested work, necessary support and additional improvements; state
whether each preserves, restores or changes the contract. An extra improvement
can be worthwhile without being part of the original request. The
[change-scope workflow](../../.agents/skills/sinomo-change-scope/SKILL.md) binds
these declarations to the actual diff and invalidates stale or incomplete reviews.
It supports explicit decisions rather than a blanket ban on additional work.

Serialize the scarce resource. Agree on a bounded build/test slot and cache
ownership without freezing unrelated development. Keep normal Cargo scheduling
unless the active task explicitly establishes a different policy. Never copy an
old session's `jobs=1` exception as a general cure for compiler pressure. A slot
shorter than known code-generation time wastes work and delays both owners.

Freeze tested source during a build, discover emitted executables, and reuse
those exact artifacts for diagnostic reruns. Changed source/build settings need
new correspondence evidence. Keep unique failure state on durable storage and
use the existing fixture archive/finalize tooling instead of globally retaining
every successful temporary directory until inodes run out.

Choose idle-measurement scope before building the collector. Whole-host activity
includes unrelated workloads. Current native acceptance concerns Sinomo
main/shadow and attributable helpers, detached work and writeback; Docker gates
cover service containers. Incomplete attribution is a measurement blocker, not
evidence that the protocol is faulty or permission to loosen limits.

## Human and agent responsibilities

| Human owner decides | Agent owns within that decision |
|---|---|
| Product requirements and explicit changes to them | Preserve previously stated requirements; expose implementation drift and supply executable positive failure cases |
| Minimum useful release and acceptable timing limitations | Separate correctness, progress, timing, coverage and rollout results |
| Priority between restoration, simplification and new capabilities | Small coherent changes and a preserved baseline |
| Cross-lane ownership and shared-resource priorities | Concrete seams, source/artifact identity and milestone handoffs |
| Changes to authority, compatibility or user-visible behavior | Reviewable options with consequences; implement the selected contract |
| Deployment targets and exceptional operational actions | Complete candidate work before the operational decision; honor existing authorization |

A decision already made in the task stays made. These responsibilities do not
introduce recurring approval gates for routine implementation, fixes or tests.

## Apply the lessons to the next work cycle

1. Preserve the current useful baseline and classify the remaining failures by
   owner and phase. Keep historical uncertain writes in a separate recovery
   record so fresh controls cannot accidentally close them.
2. Turn the existing abrupt-loss requirement and an isolation example into
   executable checks before expanding a protocol change. For isolation, hold a
   real shard dependency unavailable while another supported shard/capability operates, and observe
   both foreground progress and retained recovery work.
3. Give app/agent work a named typed seam and its own acceptance criteria.
   Reserve shared authority/admission edits for their owner; let offline analysis,
   presentation, pure algorithms and fixture preparation proceed independently.
4. At the next integration milestone, compare the failure inventory and the
   capability map. If only the test names changed while the same dependency
   cycle remains, revisit that shared boundary before another broad repair pass.

Do not make the human the test scheduler or require reminders to trace the
algorithm before adding another guard. The [DFS debugging skill](../../.agents/skills/sinomo-dfs-debugging/SKILL.md)
and [DFS validation skill](../../.agents/skills/sinomo-dfs-validation/SKILL.md)
carry the implementation-specific procedures.

For the next substantial change, review whether failures were discovered earlier,
independent work continued, rebuilds answered new questions, and promised failure
cases actually progressed. These outcomes matter more than the number of fixes,
tests, agents or deleted lines.
