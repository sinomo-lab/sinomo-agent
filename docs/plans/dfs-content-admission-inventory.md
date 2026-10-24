# Retained content-target inventory and bounded degraded creation

Source contract, 2026-10-24. Branch `dev/dfs-placement` starts at
`c4bc7289397f0f689007d802be03dec1232983d9`. No workload has executed for this
contract. This lane owns the existing write-configuration projection/input
helpers, placement selector, write planner and focused tests. It does not change
receiver custody, content logs, repair execution, transport, runtime-host policy
or metadata participant proof.

## I1. One existing configuration owner, four complete scopes

Extend the existing `DfsWriteConfigurationProjection` with the complete
`/cluster/members/` scope. Keep its other three scopes, their original native
authority certificates, common committed watermark, bounded delta journal,
refresh-token/discontinuity fencing and resident Raft-floor checks. The same
existing authority runtime performs one additional bounded authoritative prefix
read during bootstrap/recovery. Normal writes still borrow the retained view;
they perform no new KV read, hydration, watcher, task or consensus operation.

All four reads must satisfy the existing same-term/current-floor and complete
scope checks. Differing native revisions reach one watermark only through
contiguous committed replay. A missing scope, stale token, gap, compaction loss,
changed floor, malformed frame or unknown resident owner leaves the view
unavailable. Keep the existing membership-change invalidation/rebuild behavior;
adding membership rows does not make a membership delta into an authority grant.
Provider/content deltas update the same view and retain their existing native
authority-fencing behavior. No DFS intent, receipt, queue or content enters KV.

## I2. Strict, pure target inventory

Add one crate-private input result/accessor in `write_configuration_inputs.rs`.
It is derived only from a complete retained view. Its private construction keeps
the view revision and selected content advertisements together; it is planning
DATA, never a publication, delivery, receipt or content-holder capability.

Derive the cluster identity from that view's validated resident Raft-config
record. Strictly decode membership, generic service-instance and content records;
verify canonical record keys agree with their embedded identities. Invalid input
returns an error rather than a partial inventory. Admit a target only when all
of the following hold in the same view:

* its member belongs to that cluster and is authorized;
* a cluster-local service instance has the exact current DFS interface/protocol
  and advertises `dfs.object.transfer`;
* its content advertisement explicitly accepts content.

Service discovery health is separate operational evidence. A compatible
Starting/Unavailable provider can remain desired while observed not writable;
Ready/Degraded may contribute to the observed transfer preference only. A health
transition does not revoke membership, provider configuration or recorded copies.

No local-node, historical-owner/location, required-node or missing-provider
fallback widens this inventory. Empty membership/provider/content ranges produce
an empty inventory, never legacy admission. Existing compatibility discovery
accessors keep their existing behavior. An unreachable node remains a possible
desired target while these configuration facts remain present; no copy, receipt
or successful future delivery is implied. Advisory capacity/path freshness is a
separate selector input and cannot replace any inventory fact.

## S1. Pure selector boundary for root review before writer wiring

The proposed next selector consumes the strict inventory plus the already-cached
storage/path snapshot, exact policy and existing pins. It returns exactly N
desired IDs, attemptable transfer IDs, fresh observed-ready IDs and known-deferred
desired IDs separately. Unknown/stale storage/path observations remain attemptable
under admitted configuration and the existing hard-refusal checks; lease expiry
never requires periodic configuration refresh. Missing remote path evidence is
unknown, never observed-ready; a healthy local coordinator needs no self-edge.
Observed-ready is diagnostic and never supplies W. Canonical advertisement zones must agree with the policy zone
map that will be persisted; conflicting/missing material rejects rather than
validating one plan and storing another.
Every desired ID belongs to the inventory. Historical locations may rank an
admitted member but cannot reintroduce an excluded member.

The new mode is limited to a genuine first-content/new-object background
ReplicatedEventual operation with W<N. It does not infer first creation from a
missing hash or absence in a capped scan. Its private creation caller must retain
the existing actual namespace/new-object proof and check stable request replay
before admission. General overwrites, metadata-only lineage and superseded
managed placement remain on the old selector until their bounded current-owner
contract is separately implemented.

Current storage refusal/Critical observations still reject a future target.
Required and pinned nodes keep their existing hard eligibility checks, including
unavailable required/pinned rejection. Forbidden nodes, residency/local-owned
rules, distinct zones, exact N and every request/policy consistency check remain
binding. Confirmed W=N, foreground-all-N, local-owned and unsupported/unknown
native input paths retain existing strict behavior. Unknown path evidence is not
declared unavailable; a directed endpoint failure remains Unknown as networking
currently projects it. Required/pinned attemptable targets retain transfer
priority before optional attemptable targets. Prefer usable B/C observations
before optional deferred A;
actual exact receipts alone satisfy W.

## S2. Reuse the existing write and deficit owners

After separate selector review, the fresh-write planner may retain desired
B/C/A with B/C preferred. It must never shrink N or mint an A receipt/location.
The existing background stage budget already reserves only the W deficit and
consumes the desired order. The existing committed intent and version
`content_durability` retain all N IDs/policy; ordinary commit creates missing
target repair jobs and the normal message/queue capture persists that work.
No repair-loop, receiver, queue or new scheduling framework is needed here.

## S3. Actual creation context, never a wire freshness claim

The private creation context is produced from the POSIX caller's successful
lookup/new-object allocation and from the routed owner's actual `created` result
after its ordinary stable-operation replay check. The transient overlay index
is not this proof: its existing classification intentionally tolerates failed
backend reads. No serialized field, request/intent ID, body or epoch changes.
Foreground requests do not acquire the fresh context even when an existing
server optimization chooses background repair internally.

Requester selection is planning DATA only. The actual native author independently
requires its current shard mutation admission and error-propagating exact
canonical object/path absence (or its exact existing tombstone predecessor).
The current managed policy and complete zone evidence come from the same
retained configuration as target inventory. Existing regular files, retained
writes, patches and non-fresh callers keep the old selector. Only a new regular
file without a base version can use the bounded fresh path. A missing advisory
snapshot means unknown paths; it is not permission and starts no new monitor.

The initial authored native control is a new registered-Main create with healthy
metadata, managed required B/C placement and denied A content RPCs. It records
the actual advisory snapshot without publishing a synthetic path state. The
original write RPC completes at its real receiver before its transport reply is
discarded. Only that completed reply-loss rule is removed; A content remains
denied while the existing read-only stable-create reconciliation checks the
same operation, requester, object, version, hash, size and committed intent.
It must not emit another distributed-write RPC. The fixture then performs a
fresh Core inspection and ordinary all-N heal. It is not an OS/process restart.
The reconciliation receipt represents exact durable operation identity/status,
not the transient repair-job list from the original response. Stable operation
identity is retained, but the existing local POSIX retry path can reach strict planning
before known-result replay when lookup already returns a committed object. This
slice does not claim that direct same-write-API retry under continued content
outage is available; that ordering needs explicit review. No uncertain operation
is replaced under a new identity. Existing operation reconciliation and the
normal owner-side replay rules remain unchanged. This authored control is
uncompiled/unrun; its source is not native acceptance evidence.

## Initial focused controls and evidence limits

Inventory controls cover actual four-proof bootstrap/delta state, differing
prefix revisions, observed-floor mismatch, gaps/stale refresh, member revocation,
provider removal/incompatibility/locality, content refusal, malformed/key-
mismatched records, empty provider scope and a valid configured offline member.
An Unavailable compatible provider remains desired but supplies no observed
writable target or receipt.
Existing three-scope tests are mechanically extended to cover the fourth scope;
old compatibility candidate accessors are unchanged.

After root closes S1, selector controls must cover B/C/A W2/N3, insufficient usable
W, required/pinned/forbidden/storage/zone rejection, stale/unknown observations and
unchanged confirmed/foreground behavior. Native controls then keep all metadata
participants healthy while denying A's content path, require actual B/C bytes and
receipts, reopen with desired N=3 and A deficit, and heal normally. A separate
healthy-metadata B/C/D control with revoked/incompatible A must reject before new
intent/staging despite historical A bytes.

Root owns compilation and execution of integrated local commits under the DFS
local lock. No tests, native process/transport/disk, Docker or performance result
is claimed here. Metadata FullShard, general overwrite placement, abrupt loss,
successor execution and Sinomo-only quietness remain open.
