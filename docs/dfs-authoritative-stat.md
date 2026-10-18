# Authoritative distributed stat

`posix_stat_distributed` and `posix_lstat_distributed` derive inode fields from
current authenticated metadata authorities. A caller need not be a metadata
replica, ingest journals, or replay an unrelated backlog. Local-only stat and
namespace-record inspection retain their distinct contracts. In particular,
namespace `lstat` and capability no-link opens must expose link records rather
than erase them by following their targets.

## Read and generation contract

The ordinary path resolver selects the namespace path. A versioned stat query
replaces its terminal object lookup and carries a fresh read identity and the
complete current shard catalog. When the receiving Main already owns every
contributing physical shard, it captures the canonical object and its derived
count through those existing resident read scopes. It releases and reopens the
snapshots to compare exact prefixes; re-reading the same pinned generation is
insufficient. Changed cuts may use the existing four read rounds, sharing one
128-entry namespace budget and the same absolute deadline across all attempts.
No replacement backend, caller replica or journal ingestion is involved.
Stat-only pins use the existing lock-free materialized-table reader and its
storage-generation, rotation and corruption checks. A shared table image must
match the cache allocation supplying its exact prefix. Pins grant no writer
custody and do not run journal recovery; a retained source writer is left intact.
Operator-status snapshots are never used to authorize these fields.

The reply preserves the selected directory entry or explicit absence separately
from the resolved canonical inode. Validated observations enter the existing
first-read-wins namespace read set, so a later stat cannot rebase the comparisons
of a mutation already being planned.

When participants have different owners, or the bounded owner projection cannot
complete, the reply contains no derived count. The caller collects explicit
pages from each configured authority. File link counts include the canonical
link and every live alias whose path belongs to that shard's current assignment.
Directory link counts are two plus live immediate child directories. Old
ancestor-shard copies and physical source-eviction tombstones do not contribute.
The canonical owner and derived fields must belong to the same validated object
and view generation; incompatible evidence never becomes a partial count.

Pages carry an exact materialized-prefix proof, request identity, explicit
completeness flag and native continuation cursor. Continuation requires the
same prefix. The requested page limit cannot exceed the existing 128-record
bound. Aggregate owner reads consume that same budget across physical shards
and validation attempts rather than multiplying it by the number of shards.
Unknown content size remains unknown; file/symlink size comes from the exact
object or matching version metadata, never the caller's historical manifests.
Recorded locations do not assert that every content replica is reachable.

The complete owner reply binds its fresh request, object, derived fields and
prefix vector with an integrity digest. The digest detects corruption and
misbinding; authenticated current authority remains mandatory and is not
replaced by the digest. Paged cross-owner reads validate every collected prefix
again and recheck the catalog. Exact-prefix stability establishes overlapping
read intervals, not a persistent distributed snapshot or new consensus protocol.
Concurrent publication, reconfiguration, namespace reservations or unavailable
validation can make stat fail. Progress under continuous writes or arbitrary
outages is not promised.

Cross-owner collection uses the existing four POSIX read rounds under that same
absolute deadline. A validated current canonical object with a changed version,
or an authenticated typed refusal proving a forward prefix change, discards
the whole collection before starting a fresh read identity. Old pages and counts
are never combined with the new view. Co-owned capture keeps its owner-side
four-round bound; these loops are not nested. Continuous changes exhaust the
bound and return an error. Unsupported or malformed evidence, non-forward prefix
conflicts and authority changes do not become retry permission.
Different content hashes under the same immutable version are inconsistent
evidence and fail immediately; they are not treated as a new generation.

An acknowledged link creation or unlink is therefore reflected by a subsequent
successful stat without requiring local replay. Concurrent changes may instead
produce an explicit error. The caller does not use a stale local count as a
fallback. Existing cross-shard namespace-flight guards, authenticated requester
checks and current-authority response validation remain mandatory.

The canonical-path reservation is checked by the authority that owns that path
in the current authenticated catalog. An alias-only or child-only projection
does not borrow a foreign shard's replica guard for the canonical path. Every
actual object and alias returned by the queried physical scope still passes
the existing namespace checks. Foreign guards remain installed; they are not
deleted to make a stat succeed. Without a certified routing context, the
conservative path guard remains in force.

## Wire and mixed versions

The existing namespace `MetadataQuery` route has three explicitly versioned
query selections and matching results:

- `stat-at-path-v1` replaces the final object lookup with a request-bound owner
  projection, or explicitly reports that distributed paging is required.
- `stat-object-v1` resolves an inode identity within an exact physical shard.
- `stat-projection-v1` returns a bounded alias/child page or revalidates its
  captured prefix. Its page requires the request, prefix, object, entries,
  `complete` and continuation fields as defined by the typed wire model.

`stat-projection-changed-v1` is a refusal of an old prefix, carrying the exact
page request and current prefix. It carries no object, page or count. Only a
well-formed strictly later prefix from the authenticated current owner permits
a bounded fresh collection. Unknown refusal variants and older peers' ordinary
errors remain explicit failures; no diagnostic error-string matching is used.

An older server rejects the unknown selection. New callers require the matching
versioned result and all generation/completeness evidence. Unsupported versions,
wrong result kinds, wrong request identity, malformed or changed prefixes,
stale filesystem/shard/epoch, unauthenticated source/destination, or missing
required evidence cannot become a successful current stat. No compatibility
fallback replays caller journals or substitutes an older local projection.

New servers continue serving the old metadata-query variants unchanged. Old
clients retain their previous stat behavior; upgrading the server does not
retroactively provide the new client-side guarantee. A mixed cluster can serve
this contract only when every required authority implements these variants.
If any alias-owning authority is unavailable or too old, complete stat is
unavailable even when the canonical object's content remains readable.

## Dispatch, cancellation and bounds

The collector uses the existing 15-second read-freshness budget, shortened by
any enclosing caller deadline. That absolute deadline spans pagination, target
resolution and final validation and is restored on return. Individual metadata
queries retain their existing transport limits and respect that enclosing
budget. Pages use the existing 128-record query bound and existing RPC byte
limits; hardlink resolution retains its sixteen-hop bound. Cancellation fails
the read without publishing metadata or creating recovery work.

A Core owning one physical shard cannot impersonate a different shard merely
because both primaries share its node ID. The same-node query uses the existing
authenticated registered Main sender to reach the actual owner. It neither
changes the Core's general dispatch role nor processes unrelated queued
mutations. Returned frames retain their original identity and pass the same
request/route/authority checks as remote responses.
The physical-scope check follows the request's declared read footprint. A
cross-owner path projection needs only its exact path shard and can use a Core
that already owns that scope; unrelated catalog shards do not force it back
through Main. A complete co-owned projection still requires every declared
scope, and a genuinely different physical shard still uses registered Main.

These queries are read data, not publication authority. They introduce no
persistent format, replica, background scheduler, quorum rule or routing
subsystem. Normal publication, source custody and protocol-read preemption
continue to be owned by their existing mechanisms.
