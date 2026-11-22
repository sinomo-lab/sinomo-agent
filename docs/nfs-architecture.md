# SIFS NFSv3 Architecture

The `sinomo-dfs-nfs` adapter presents a SIFS subtree through an in-process
NFSv3 server. Windows can use it for a drive-letter filesystem and Linux can
use it as a kernel-mounted alternative to FUSE. Neither path constructs a host
projection tree. The server reads and writes the ordinary DFS content store,
so a local DFS replica and the mounted view do not consume two full persistent
copies.

## Boundary and lifecycle

The server binds only to an IP loopback address. NFSv3 AUTH_SYS does not
authenticate or encrypt clients, so remote binds are rejected rather than
turning the presentation adapter into a cluster transport. Cluster access,
authority, replication, repair, pinning, and placement continue to use the
existing Sinomo paths behind `MountedSifsService`.

The default endpoint is `127.0.0.1:111` and the default export is `/sifs`.
Port 111 is required by Windows Client for NFS discovery and normally requires
an elevated process. Linux `dfs nfs mount` instead binds an ephemeral
loopback port and supplies the selected NFS and mount ports explicitly to the
kernel client.

```sh
sinomo dfs nfs serve --bind 127.0.0.1:111 --export sifs --root /
```

On Windows, the foreground server can be started and mapped together:

```text
sinomo dfs nfs mount S:
sinomo dfs nfs unmount S:
```

The mount command invokes the native `mount.exe` directly with no shell or
PowerShell process and suppresses a child console window. The foreground
Sinomo process remains active until `dfs nfs unmount` sends UMOUNT and removes
the drive mapping. The NFS module can alternatively supervise the native mount
from the main daemon; the existing `storage.sifs-mount` setting still selects
Cloud Files on Windows.

On Linux, FUSE remains the default `dfs mount` adapter. NFS is selected
explicitly and uses the kernel NFSv3 client:

```sh
mkdir /mnt/sinomo
sinomo dfs nfs mount /mnt/sinomo
sinomo dfs nfs unmount /mnt/sinomo
```

The mount point must already exist, be empty, be a real directory rather than
a symlink, and not already be mounted. The CLI uses `mount.nfs`, falling back
to `mount -t nfs`, with NFSv3 over TCP, explicit ephemeral service ports, 1 MiB
I/O sizes, one-second attribute caching, and NLM disabled. The host needs its
normal NFS client package (`nfs-common` or `nfs-utils`) and mount authority,
usually `CAP_SYS_ADMIN` or root. Operators who do not want to grant that host
authority should continue to use FUSE.

## Module settings and errors

The NFS module (an R1 native Rust module whose ID derives from the legacy name
`module.dfs.nfs`) registers its own schema-driven Settings UI for the editable,
node-local `storage.sifs-nfs-mount` setting. The app shows that module card,
with its settings, under **Settings > Storage** beside the SIFS mount; the
**Modules** page lists it as managed in Storage. There an operator can
configure enablement, the Linux directory or Windows drive target, the
loopback endpoint and export name, the exported SIFS root, and read-only
behavior.

The module also registers a read-only `storage.sifs-nfs-mount-status`
operational document. Its presentation metadata identifies `last_error` and
`resolution` as an actionable pair, so the generic module UI displays an
**Action required** block without knowing NFS-specific field names. Typical
resolutions explain how to install the native NFS client, grant Linux mount or
Windows port-111 authority, choose a safe empty target, or repair DFS/daemon
availability.

The daemon supervisor watches only the NFS config document and reacts to file
change notifications or shutdown. It does not poll while idle. Configuration
changes stop the previous mount before starting the replacement; failures
leave the requested config intact and publish diagnostic status so the
operator can correct it and retry by disabling and re-enabling the mount.

## Filesystem identity and confinement

NFS file handles contain the immutable DFS object ID. They do not contain a
host path and the server keeps no directory-sized handle registry. File IDs
and directory cookies are deterministic 64-bit hashes of the same object ID.
A handle therefore survives a rename within the export without growing server
memory.

Every handle lookup resolves the current object metadata and verifies that its
current path remains beneath the configured export root. An object moved out
of the export becomes `NFS3ERR_STALE`; a retained handle cannot escape the
subtree. `..` at the export root resolves to the export root.

## DFS durability

Standalone data directories execute directly. Clustered exports require the
local daemon and open a mounted-mutation outbox session using the `Nfs`
adapter identity. Service construction, export-root discovery, directory
refreshes, writes, namespace mutations, and attribute changes use the same
Prepare, Arm, capture, and daemon-acknowledgement protocol as other mounted
adapters. An error from the operation itself, an unknown outcome included, is
that request's answer: the adapter hands what it observed to the daemon and
the export keeps serving.

A serving export is never fenced. A handoff step the daemon has not
acknowledged is sent again until it is, with no attempt limit: a slow daemon
is not a lost one. A step the daemon definitely refuses ends its request with
`NFS3ERR_IO`, is not sent again, and releases what the request held. While the
outbox is at its marker limit, the next mutation waits for room instead of
being refused. Lookups, attribute reads, and file reads hand nothing to the
daemon and are answered while a mutation waits for it.

A request waits for its outcome without a timeout unless the export has an
operation budget: `--operation-budget <duration>` on `sinomo dfs nfs serve`
and `sinomo dfs nfs mount`, or `operation_budget` in the
`storage.sifs-nfs-mount` setting, in the duration format [SIFS](sifs.md)
describes. A request that reaches its budget returns `NFS3ERR_IO`. The export
keeps serving and keeps sending the handoff step that request left
unacknowledged until the daemon acknowledges it.

NFS WRITE requests are bounded to 1 MiB at the adapter and are published to
DFS immediately using a wait-for-commit write. A successful reply reports
`FILE_SYNC`; because the adapter never retains unstable write data, a later NFS
COMMIT only validates that the object handle is still live. The stronger SIFS
`fsync` barrier (which waits for every desired replica and metadata recovery
quorum) is distinct from NFS stable-server storage and is not falsely imposed
on every Windows write. New objects use the mounted service's discovered
replica candidates and normal replicated-eventual write options. Existing W/N
policy, placement snapshots, repair machinery, and manual content pins are
therefore unchanged rather than reimplemented by the NFS layer.

The server library bounds concurrently active RPC transactions. The adapter
also serializes access to the mounted DFS service, whose state is not
concurrently mutable, while network handling and blocking DFS work run off the
async listener threads.

## Compatibility limits

- NFSv3 has no native Windows ACL model. The adapter exposes SIFS POSIX mode,
  UID, and GID metadata.
- The Windows mapping uses anonymous AUTH_SYS access and disables NLM locking.
  Applications that require Windows ACLs, byte-range locks, alternate data
  streams, or Cloud Files availability badges should continue to use Cloud
  Files.
- Linux also disables NLM because the adapter does not implement the separate
  lock manager protocol. Workloads that require SIFS distributed advisory
  locks should use FUSE.
- NFS EXCLUSIVE create is reported as `NFS3ERR_NOTSUPP`. Correct EXCLUSIVE
  replay requires a durable verifier-to-object record; the adapter does not
  claim that guarantee using process memory. Ordinary guarded and unchecked
  creates are supported.
- SIFS names are UTF-8. Windows Client for NFS exposes only its legacy locale
  encodings, so arbitrary Unicode filename round trips require target-specific
  validation.
- CLI native mount commands own a foreground server. The module settings path
  is daemon-supervised and re-applies enabled local intent after daemon start.

## Verification

The crate tests both the filesystem trait and the actual TCP protocol. The
wire test performs MOUNT, CREATE, stable WRITE, READ, READDIR, and UMOUNT using
an independent NFSv3 client. Additional tests cover loopback-only binding,
root normalization, rename-stable handles, deletion staleness, and export-root
confinement. Linux tests additionally cover explicit port/options planning,
mountinfo decoding, and safe mount-point admission. Windows builds are checked
through the Windows GNU target; native kernel-mount acceptance must run on a
host with the corresponding optional NFS client installed.
