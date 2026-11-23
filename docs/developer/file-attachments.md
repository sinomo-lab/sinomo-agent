# File attachments

The `sinomo-dfs-attachments` component (`module.dfs.attachments`) exposes version 2 `DfsAttachmentRequest` / `DfsAttachmentResponse` on
`/daemon/dfs/browser/attachments-v2`, with streamed upload on
`/daemon/dfs/browser/attachment-upload-v2`. Contracts live in `sinomo-api::dfs`;
hosted implementations use `sinomo_runtime::dfs_attachments`, and clients use
`SifsBrowser::file_attachment` or `upload_file_attachment`.

Checked components use the `sinomo.dfs.attachments` version `2.0.0` offer,
operation/capability `dfs.attachments.manage`. Its component-only route derives
the producer identity from the checked dispatcher. Wire metadata and the
payload's `producer` label cannot impersonate a component. Ordinary browser
routes retain `daemon.dfs.browser` and `sifs.path.access` authorization and are
charged to their authenticated node, or the local operator. Installing a module
does not grant capabilities. Module budgets require checked component calls;
modules granted general filesystem/operator access remain in that wider trust
boundary. Component value requests are bounded to 80 KiB; larger payloads use
the authorized browser streaming transport and its node budget.

Upgrade app and daemon together: version 1 wire endpoints are not retained.
Version 1 on-disk attachments remain readable; writing a legacy slot migrates
it through a conditional version 2 catalog publication. Mixed old/new writers
against the same stored attachments are unsupported.

The shared product catalog enables this component by default. Its routes,
producer admission, upload spools, settings and cleanup worker belong to its
own component scope. The stable `dfs.attachments` setting is owned by this
module; existing values retain their key and schema. Disable with
`sinomo module native-rust disable module.dfs.attachments`, then restart the
daemon process to apply the registry change. Disablement preserves catalogs and
payloads and stops attachment work. Core namespace/content operations remain
available; re-enablement resumes attachment queries and cleanup. DFS disablement
leaves this dependency pending rather than opening another metadata store.

## Storage and identity

Each source has one bounded `catalog.sobj`. Payloads up to 4 KiB are stored in
that catalog within a 128 KiB aggregate inline budget per source. Further values
spill to immutable DFS objects referenced by exact
identity, version, content hash, and size. Catalog compare-and-swap is the
publication point. Ordinary DFS content receipts, metadata authority, retention,
and content GC govern all objects. Cluster KV contains configuration only.
An attachment mutation does not change the source file version.

A stable hash of filesystem/object identity selects one of the existing SIFS
metadata lanes: `/.attachments/v2`, `/services/.attachments/v2`, or
`/users/.attachments/v2`. Two hash buckets precede the source identity directory.
This distributes catalog objects across the existing root/services/users shard
assignments without creating a new shard or bypassing shard handoff. These
system roots must remain stable. The layout does not promise equal byte load:
a source with large attachments still has one catalog owner.

Ordinary directory browsing does not read these catalogs. Direct descriptor
lookup and `GetMany` (up to 64 keys for one source) open one bounded catalog,
independent of global filesystem size. A bounded process cache holds at most
64 decoded catalogs with a 16 MiB estimated budget. Successful publications
populate that cache, and obsolete versions of the same catalog are evicted. Cold lookup and
catalog writes cost O(attachments on that source), bounded by 1 MiB; changing a
small value rewrites that catalog. This trades lower metadata-object count for
write amplification on heavily annotated individual files. Large payloads are
range-read without fetching other payloads. Feature routes and maintenance borrow
the core-owned resident service. Module-local catalog caches and admission state
still contribute to total process RAM; attachment density measurements alone do
not bound total DFS process memory.

Source path is a locator, checked against canonical filesystem/object identity.
Renames and hardlink aliases preserve slots; path reuse cannot transfer them.
Object-scoped annotations survive edits. Content-derived data is bound to an
exact source version; stale descriptors remain available for CAS replacement,
but stale payloads are not served. Attachment revisions are logical catalog
revisions, not raw payload object addresses.

`Put.expected = None` exclusively creates a slot. Replacement and deletion
require the exact attachment revision. Preserve operation ID, payload, and
precondition after uncertain delivery. The catalog keeps 16 recent operation
receipts plus current slot receipts; it is not an unlimited replay log. Reusing
an operation ID with different inputs is rejected when its receipt exists.

## Producer budgets and scheduling

The cluster setting `dfs.attachments` supplies defaults and overrides keyed by
`component:<id>`, `node:<id>`, or `local-operator`. Defaults are:

| Budget | Default scope and limit |
|---|---|
| Admitted mutations | 64 per serving node, at most 4 queued per producer |
| Reserved upload bytes | 128 MiB across queued and active work per serving node |
| Producer work | 1,024 mutations and 256 MiB admitted upload bytes per minute per serving node |
| Producer storage | 128 retained slots and 256 MiB live/reserved logical bytes per source |
| Source catalog | 1 MiB encoded, 1,024 slots, 64 uploads and 64 generation claims |

Deleted slots retain tombstones and count toward the slot quota, preventing
create/delete churn from filling a catalog outside the producer's budget.
Payload bytes are logical sizes, excluding replicas, old DFS versions, temporary
spools, and catalog overhead. These are not fleet-wide aggregate storage quotas.
The minute window and admission queues are process-local and reset on restart;
storage limits are enforced by durable catalog CAS.

One background mutation executes at a time per data directory, with round-robin
producer queues. Upload, publication, and maintenance do not occupy the
foreground browser execution queue. Cancellation releases queued reservations;
a running blocking operation retains its permit and DFS owner until it finishes.
Duplicate queued/running operation IDs from one producer are rejected for
reconciliation. Queue and rate rejection requires caller backoff. Foreground
I/O still competes for underlying disk, network, and metadata locks; there is
no latency guarantee or preemption of an already executing operation.

## Generation and cleanup

Call `ClaimGeneration` before downloading or decoding a source. Supply a
version-bound `DfsAttachmentUploadRequest` with `generation=true`, a stable
operation ID, output byte budget, and expected slot revision. The response is
an existing matching descriptor, a granted five-minute lease, or a busy lease.
Only the same authenticated actor can publish using that operation, metadata,
precondition, and a payload within the reserved budget. Set the actual payload
length when uploading. Publication checks the lease again; expired or replaced
leases cannot publish. Retry the identical claim after uncertain delivery.
Claims survive restart, and abandoned claims expire through maintenance.
The app uses this path before raster decoding. Busy results retain the generic
icon until a later listing/viewport retry or namespace refresh.

Large uploads also obtain a durable upload claim before raw object creation.
Each lease has a fresh payload path; catalog CAS fences superseded uploads.
Background maintenance checkpoints namespace change cursors and bounded source
tasks in DFS-owned local state. It is reactive: namespace events, cleanup
notifications from committed mutations, and authority or cluster changes wake
it. Startup, authority changes and a lagged event stream request a
managed-directory reconciliation, which pages only the managed attachment roots
and never scans the ordinary user namespace. Productive passes continue
immediately; once caught up the worker parks with no feed poll or periodic
rescan. Only payload grace or lease expiry and a ten-second retry after an
error set deadlines. A source or managed-directory page whose read could not
reach its shard's primary is tried again as soon as a `dfs.configuration.published`
event shows that shard's record changed; the retry is only its fallback. Each pass processes at most eight sources with bounded
directory pages, and incomplete work resumes from checkpoints. A true feed gap
reconciles all managed attachment namespaces because source and catalog
placement can belong to different shards. Only nodes that are primary for a
SIFS metadata shard run the cleanup pass.

Cleanup reclaims replaced payloads and abandoned uploads through exact generation
checks. Foreground publication records garbage durably but leaves reclamation to
maintenance. Only the catalog authority runs live-catalog background expiration
and garbage updates, avoiding duplicate cleanup writers on different nodes.
It deletes source catalogs only after current authorities prove a real
source deletion and no retained source versions. Missing authority or transport
failure defers cleanup. Pinned source versions preserve associated attachments;
attachments do not implicitly pin source bytes. Logical deletion still relies
on ordinary DFS content GC for physical reclamation. Automatically migrating
attachments when the source placement policy changes remains separate work.

## Transfer bounds

| Operation | Bound |
|---|---|
| Descriptor read / GetMany | One catalog up to 1 MiB; up to 64 requested keys |
| Listing | 1–64 examined records per page; tombstones can produce empty pages |
| Payload range | At most 1 MiB; exact attachment revision required |
| Upload | At most 64 MiB; 16 KiB request head and streamed semantic body |
| Buffered component request | At most 80 KiB encoded request |
| Tags | At most 256 labels, 128 bytes per label, 64 KiB encoded payload |
| Transcript | NDJSON; 16 KiB text and 128 KiB encoded line per segment |
| Thumbnail | PNG, at most 1 MiB |

Uploads spool to a private DFS staging file, validate with bounded buffers, then
stream into DFS storage. This bounds application memory but adds a disk pass.
The CLI streams transcript/custom inputs; tags use their small bounded buffer
for canonicalization. List cursors are source-specific live continuations, not
snapshots. Continue through empty pages while a cursor exists. Never continue
index extraction across an attachment revision change.

## Built-in schemas

- `sinomo.tags/v1`, kind `sinomo.tags`, name `default`, `application/json`:
  `{"tags":["project","review"]}`. Object scope; the typed encoder trims,
  sorts, and deduplicates labels. Labels remain case-sensitive.
- `sinomo.transcript/v1`, kind `sinomo.transcript`, variant such as `en`,
  `application/x-ndjson`: each line is a `DfsTranscriptSegment` with
  `start_millis`, `end_millis`, optional `speaker`, and `text`. Start times must
  be nondecreasing; overlapping speakers are allowed. Bound to a source version.
- `sinomo.thumbnail/v1`, kind `sinomo.thumbnail`, variants `raster-v1-128`
  and `raster-v1-256`, `image/png`: generated by the app's existing bounded
  raster decoder, bound to a source version. The worker reads the stored PNG
  before reading source content. Misses claim generation and conditionally publish;
  races or temporarily unavailable persistence do not hide a readable image.
  All Files browsing views (Details, Small/Medium/Large Icons, and Flow) fetch
  thumbnails automatically for visible files, with a bounded nearby range.
  Stored thumbnails can cover any regular DFS file type, including previews
  supplied by modules for documents or videos. Another stored size is reused
  before attempting raster generation; non-raster cache misses keep the normal
  file icon without downloading the source. Thumbnail arrival repaints the
  existing rows or tiles without a manual refresh.

## Custom modules

A module can define `org.example.ocr`, a variant such as `en-v2`, schema
`org.example.ocr/v2`, and an appropriate media type, then call the same API.
Use `source_version = Some(version)` for derived data and include the extractor
and configuration fingerprint in `producer`. Authored labels can use object
scope. Module installation order has no effect on existing data, and module
unload does not make its data unreadable. Custom payload validation and
interpretation belong to that schema's producer/consumer; DFS validates the
envelope and bounds. Schema identifiers are not fetched or executed.

## Indexing

Attachment creation/replacement/deletion is an ordinary durable DFS namespace
change. The browser also emits a `dfs.namespace.changed` wake-up hint with
operation `file-attachment` and the source path. Consumers must checkpoint the
durable logical change feed, including the attachment namespace, and reconcile
after a gap; component events alone are not durable indexing work.

`sinomo-memory::extract_attachment_index_page` provides bounded tag facets and
transcript text/timestamp/byte-evidence extraction. Its reader callback must use
the attachment API with the descriptor's exact revision. A job's identity must
include the source identity/version, attachment revision, schema, and extractor
fingerprint. Publish complete index generations and checkpoint their work
durably. Unknown schemas have no implicit text extractor; modules can provide
their own consumer using the same range API. Exclude generated storage from
recursive ordinary-file enrichment.

This is an extraction seam, not a new distributed search service. The existing
memory serving implementation still provides its local-file indexing slice;
automatic DFS catalog ingestion and serving attachment hits remain separate
memory work. Read access and currentness must be checked again when serving
snippets, not inferred from an old index hit.

## CLI

The commands use the running main daemon and print JSON descriptors/pages.
`get`, `tags`, and `transcript` without `--input` stream payload bytes to stdout
or `--output`. Updates load the current descriptor and perform one conditional
write. They do not merge competing tag edits.

```sh
sinomo dfs attachment tags /users/video.mp4 --input tags.json
sinomo dfs attachment tags /users/video.mp4
sinomo dfs attachment transcript /users/video.mp4 --name en --input transcript.ndjson
sinomo dfs attachment transcript /users/video.mp4 --name en --output transcript.ndjson
sinomo dfs attachment list /users/video.mp4 --limit 32
sinomo dfs attachment put /users/video.mp4 --kind org.example.ocr --name en \
  --schema org.example.ocr/v2 --media-type text/plain --bind-version --input ocr.txt
sinomo dfs attachment delete /users/video.mp4 --kind org.example.ocr --name en
```

The registered runtime case `cluster.dfs.file-attachments` exercises the daemon
route, multi-range binary data, tags, transcripts, replay, paging, rename, and
stale-source rejection in a scoped test namespace.

## Measuring small-write latency

Build `cargo build -p sinomo-client --example attachment_latency`, then use
`docker/measure-attachment-latency.py --project <disposable-project>
--artifacts target/attachment-latency/run --count 200 --warmup 20` against a
cluster that has passed the Docker readiness and convergence gates. The probe
keeps one client and runtime alive. Timers cover individual RPCs; process startup,
fixture setup, source inspection, and final byte verification are outside them.
Raw samples, nearest-rank percentiles, workload failures, and daemon process
hashes are retained. Fixtures remain available until the disposable cluster is
removed. This differs from timing repeated `docker exec sinomo ...` commands.
The default write p95 gate is 100 ms. A slow workload retains all its samples
and fails the run; `--max-write-p95-us 0` explicitly selects measurement only.

For diagnostics, enable `sinomo.dfs.perf=trace` through `sinomo observe control`
locally inside each selected container. Check every control response; remote
control may be denied even when remote trace reads are allowed. Attachment phase
records separate policy reads, admission, source/catalog lookup, and publication.
Disable tracing on every node before acceptance measurements, and preserve both
baseline and candidate executable identities.

Attachment writes use the ordinary metadata authority path, including its
fresh full-shard proof or operation-bound recovery permit; see
[Metadata Authority](../dfs-architecture.md#metadata-authority) for its
recovery hints and durability barriers. Internal catalog updates return only
after canonical commit; their subsequent lookups go to the authority instead of
requiring synchronous reconstruction of the requester's namespace mirror.
