# DFS single-round write commit

Status: proposed. A bounded protocol model covers the commit point, restart
and takeover for one write; the implementation does not meet these
requirements, and [Current behavior](#current-behavior) lists the differences.
This plan specifies the target protocol and its obligations. It does not
certify an implementation or authorize deployment.

A committed replicated write needs two things to be durable: the content on W
holders and the complete prepared change on every member of the shard's write
set. Both are independent of each other until the commit point, so one exchange
with each participant is enough. This plan removes every other wait from the
path between a write reaching its metadata primary and the caller's success.

It builds on the write set, takeover and prepared-change recovery defined in
[DFS abrupt-node-loss availability](dfs-abrupt-loss-availability.md) and changes
none of their rules.

## Requirements

1. **One round.** A replicated write whose content fits one bounded message
   reaches every participant in a single parallel exchange: each content holder
   receives the bytes, each write-set member receives the prepared change, and
   a participant that is both receives them together. The primary waits for no
   participant before contacting another.
2. **Same commit point.** The visible version advances only when W named
   holders durably hold the content and every write-set member durably holds
   the complete prepared change. No participant's acknowledgement is assumed.
3. **Acknowledged means decided.** Once those acknowledgements are in, the
   primary reports success. The commit record follows and is not waited for,
   as the write-set commit rule already allows. Nothing can afterwards abort or
   supersede that change with different effects.
4. **A named holder speaks for itself.** A prepared change names its content
   holders before they have answered. A node acknowledges a prepared change
   that names it as a holder only while it durably holds that content, on every
   acknowledgement including repeated ones.
5. **Recovery re-establishes the commit point.** Whoever commits a prepared
   change — the primary after a restart or a successor after takeover — first
   obtains the same evidence the original primary needed: every write-set
   member's acknowledgement and W named holders' confirmation. A prepared
   change without that evidence stays prepared.
6. **No new custody.** The design adds no intent record ahead of the prepared
   change, no per-write cluster-KV state and no second copy of the content.
   Bytes are stored once on each holder, and the journal still carries only
   their hash and size.
7. **Larger content keeps its transfer.** Content that does not fit the bounded
   message is transferred to its holders first, as it is now, and joins the
   same commit point.

## Current behavior

The implementation differs from these requirements:

- The primary stores the content, then stages it on each remaining W holder and
  waits for their receipts, and only then builds the prepared change. The
  prepared change embeds those receipts, so content staging and metadata
  preparation are two consecutive exchanges even when the same node takes part
  in both.
- The primary appends the commit record durably before it reports success.
- A member acknowledges a prepared change from the frames alone. It does not
  check that it holds content the change says it holds, because today the
  primary only names a holder after that holder has confirmed.
- Prepared-change recovery re-collects the write-set acknowledgements and
  commits. It does not ask named holders outside the write set whether they
  still hold the content.
- A local abort of a pending write does not consider whether members already
  hold its prepared change.

These existing properties support the design:

- The prepared change is immutable, covered by one digest, and already carries
  its content holders with the receipt generations the primary planned.
- The same content check runs when the change is prepared, when it commits and
  on every member that validates it.
- Before success, the complete prepared change is durable on every write-set
  member, so any member can finish it.
- Recovery always tries to commit a prepared change and reports an unknown
  outcome, never a failure, for one a successor could still complete.
- The stage message already carries the content, the object and the write's
  identity, and its receiver already returns a durable confirmation.

## Design

### The exchange

The primary plans the content holders, stores the content itself and appends
the prepared change naming every planned holder. It then sends, in parallel:

- to a write-set member that is also a named holder, the content and the
  journal frames in one message;
- to a write-set member that is not a holder, the journal frames;
- to a named holder outside the write set, the content.

A member stores the content before it validates and persists the frames, and
answers once. The primary counts a member's acknowledgement as both its
write-set acknowledgement and, where it is a named holder, its content
confirmation. A holder outside the write set answers with the durable
confirmation it returns today.

### Commit and reply

The primary commits when every write-set member at the current generation has
acknowledged and W named holders, itself included, have confirmed, under the
write-set floor rule. It applies the commit, reports success and appends the
commit record without waiting for it to be durable. Members learn of the commit
through ordinary replication, as they do now.

A read at the primary observes the committed version from the moment success is
reported. A primary that starts or takes over serves no read or write of an
object while it holds a prepared change for it that the write set may have
acknowledged; it finishes that change first.

### Recovery and takeover

Recovery keeps its shape: the committing primary sends the prepared change to
the write set again and commits on their acknowledgements. Requirement 4 makes
each repeated acknowledgement confirm the member's content as well.

A successor needs no answer from the primary it replaced. That primary stored
the content before it prepared the change, so the prepared change itself is the
evidence of its holding, as a recorded receipt is today. The successor checks
its own holding locally and asks only the remaining named holders, which are
the ones outside the write set. A change the old primary acknowledged is
therefore committed by any member that takes over, without the old primary and
without a second content transfer.

When fewer than W named holders confirm, the change stays prepared and the
caller's retry with the same operation id supplies the bytes again. The
recovering primary never commits on receipts it has not confirmed, and it never
substitutes a holder inside the same prepared change.

### Holder failure

A holder that cannot store the content, or cannot be reached, leaves the change
prepared. Substituting a reachable admitted holder publishes a new prepared
change that supersedes the first, with the same operation identity, through the
resolution recovery already uses. Substitution therefore costs an additional
exchange and happens only when a planned holder fails.

### Abort

A write is aborted locally only before its prepared change has been sent to any
participant. After that, it is resolved by commit or by a superseding prepared
change, never by a local decision.

## Obligations

- Requirement 2 holds on every path that makes a version visible, including
  recovery, takeover and a caller retry.
- A member's acknowledgement and its content confirmation cannot diverge: it
  cannot acknowledge the frames and later be found without the content it was
  named for, short of storage damage, which repair handles as it does today.
- An acknowledged change is committed by every primary that serves afterwards,
  with the same effects and caller identity.
- A change that was never acknowledged may commit later or be superseded; the
  caller observes an unknown outcome and resolves it with the same operation id.
- The write-set floor is evaluated at the commit point, not when the prepared
  change is built.

## Validation

The `dfs-single-round-write-commit` model covers one W=2 write on a two-member
write set, with the second holder inside or outside it: the commit point,
success before the commit record is durable, a crash between any two steps,
restart, takeover, and negative controls that commit without holder
confirmation, acknowledge without content and abort after sending. Holder
substitution and write sets above two members are not modelled yet. Further
checks stay within the deep-tier time budget by isolating one concern each:

- the prepared change naming holders that have not confirmed, with the commit
  point requiring their confirmation;
- a crash of the primary between any two acknowledgements, followed by restart
  and by takeover, with recovery confirming holders;
- success reported before the commit record is durable, followed by a crash,
  with every later primary committing the same change;
- a failed holder superseded by a substitute;
- negative controls that commit without holder confirmation, acknowledge
  without content, and abort after sending.

Implementation tests then cover the same cases against the native runtime, and
the Docker write matrix measures both replication policies on storage that
does not serialize the nodes' durable writes and on durable storage.

## Not part of this plan

The primary still makes its own prepared change durable before it sends it.
Sending first would let a member hold a change the primary lost in a crash, and
a restarted primary would then need to adopt its members' longer history before
serving. That is a change to restart and alignment rules and is left to a
separate plan.
