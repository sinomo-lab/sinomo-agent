# Authoritative distributed stat

`posix_stat_distributed` and `posix_lstat_distributed` read inode metadata from
its current authenticated authority. The canonical object owns its maintained
namespace link count. Ordinary stat does not enumerate aliases or directory
children, validate whole-shard journal generations, replay journals, or wait for
unrelated writers to become quiet.

Local-only stat and namespace-record inspection retain their distinct contracts.
Namespace `lstat` and capability no-link opens expose link records rather than
erase them by following their targets. Ordinary POSIX stat resolves links through
the existing path and inode lookup rules.

## Count ownership

A live non-directory inode counts its namespace names. A live directory reports
two plus its immediate child directories. Hardlink markers refer to the canonical
object; they do not maintain independent copies of its count. Directory fragment
entry counts include regular files and are not the inode's link count.

The count belongs to current namespace metadata, not to an immutable content
revision. Content overwrite, receipt refresh, repair and historical content
restore preserve the current count. Replica installation and physical source
copy eviction do not create or remove logical namespace names. A canonical
object that changes path or authority carries its count with its stable identity.
Removing its canonical name while promoting an alias removes one name without
creating a second inode.

Namespace producers own count changes. A same-shard mutation publishes affected
entries and their canonical inode or parent count successors in the same
namespace transaction. A cross-shard operation binds its count successors to the
original durable job, expected object generations and exact namespace effects.
Completion requires the corresponding count work to be settled. Retrying an
uncertain operation observes or replays its original successor; it must not
increment or decrement the current value again.

Directory moves reserve any foreign canonical parents through the original
namespace job before issuing namespace effects. These reservations exclude
conflicting parent writes without adding stat read fences. Before effects, the
job may release its exact reservations without changing counts. Once effects
may have applied, the job must recover forward and settle its original count
successors before completion.

Counts use checked arithmetic. Unknown counts cannot be inferred from a partial
replica, a missing local alias index, or a default of one or two. A directory
count of two does not establish emptiness: regular files do not contribute to it.

## Read contract

Path resolution selects a namespace entry using existing routing, symlink,
authorization and namespace-reservation checks. A terminal path read addresses
only that path's physical metadata authority. The canonical object and its count
come from the same held materialized read view. Size may be enriched from the
matching version record in that view. Recorded replica locations may be enriched
from object-local receipts; they do not assert that each replica is reachable.
Unknown content size remains unknown unless the existing exact immutable-content
fallback can establish it.

A successful read describes the inode at its authority read point. Later writes
can make it old before the reply is delivered; they do not invalidate that
already captured record. An acknowledged namespace operation must be reflected
by a subsequent successful current-owner read. Readers do not reconstruct a
simultaneous view of unrelated shards to establish that property.

Hardlink target discovery follows the existing bounded identity-resolution path.
A target known only by object ID may require authority discovery. This is
separate from counting names: once the canonical inode is found, its stored
count supplies the result without alias pages or final generation fences.

Validated path observations enter the existing first-read-wins namespace read
set. A later read must not rebase the predicates of a mutation already being
planned. Capability no-link opens preserve the selected object identity for
subsequent content reads rather than resolving the original path again.

Authentication, filesystem/shard/epoch binding, response identity, canonical
path ownership and existing namespace-flight guards remain mandatory. An
unavailable canonical authority, missing count initialization, malformed reply,
or reservation involving the selected namespace may fail a read. Unrelated
publication elsewhere in the namespace is not a reason to retry or reject stat.
Stat grants no publication authority and creates no durable request, recovery
job, or metadata write.

## Wire and compatibility

The existing namespace `MetadataQuery` route carries `stat-at-path-v2` for the
current path owner's metadata. Its result is either that selected record or
explicit authoritative absence. A live canonical record must contain a valid
maintained count. A hardlink marker instead requires canonical target resolution.
`stat-object-v1` remains the exact physical-shard inode-identity lookup used by
that resolution path.

The older aggregate path/projection variants are not a fallback for a
maintained-count read. New callers reject an unsupported selection, mismatched
result, wrong authority, invalid count or count-less legacy canonical record.
They never recover success by scanning history or accepting a client-local count.

An optional absent count preserves the encoding of historical object bodies,
including their prepared-operation digests. It does not make an uninitialized
live count valid. Storage and writer activation must prevent a count-unaware
producer from erasing or failing to maintain an initialized count. Mixed active
writers that do not share the count contract are unsupported.

## Existing metadata

Count initialization belongs to the single migration owner and runs inside each
shard's own conversion. Background discovery covers cold metadata; demand can
prioritize the same conversion without computing counts inside stat. A
converting shard initializes its counts from its own rows plus what every other
shard contributes to them, read per shard pair, not from an independent count
of each shard's local copies. Those contributions are frozen while the shard is
closed: a count change needs the target's canonical object, a cross-shard
rename of a name whose canonical object the shard owns waits until the shard is
Active, and so does each such name a subtree move carries; no shard is born
from a closed parent shard. The shard accepts
the contributions only after two consecutive complete rounds agree and no job
touching it is still running other than one parked on it. A contributing shard
that cannot answer is waited for, never counted as empty. Current assignments
identify contributing entries; stale ancestor copies and physical source-eviction
records must not be counted again.

Conversion preserves original prepared bytes, digests and uncertain operation
identities. In-flight namespace work must be settled or explicitly handled by
its owning recovery protocol. Neither stat nor an ordinary mutation can guess
an initial count. Installation and readiness require actual native source and
activation evidence; a computed census is conversion data, not authority.

A shard is closed for its whole conversion: its ordinary reads and writes are
unavailable until its storage record is Active, while other shards keep
serving. Partial failure retains the attempt and its conversion identities for
forward recovery; restarting a member that installed the image does not open
the shard before its storage record is Active.

## Dispatch, cancellation and bounds

The read adds no deadline of its own: its only bound in time is a deadline
its caller set. Cancellation fails the read without publishing metadata or
creating recovery work. Hardlink resolution retains its sixteen-hop bound.
These bounds do not permit retries merely because an unrelated shard changed.

A Core owning one physical shard cannot impersonate another shard because both
primaries share a node ID. Same-node queries use the existing authenticated Main
path when the actual physical owner differs. An exact local owner may read its
resident view directly. Neither path creates another backend, ingests caller
journal batches, or processes unrelated queued mutations to obtain stat.
