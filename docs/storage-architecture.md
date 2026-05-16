# Sinomo Storage

`sinomo-storage` defines the shared registry interface for files inside the
Sinomo data directory. It does not own the complete Sinomo data-dir catalog.
Each crate that reads or writes durable files registers its own paths, including
the module, trust ring, relative path, encoding, and object schema when the file
is a Sinomo Object document.

The storage crate exposes the vocabulary and mechanics:

- `StorageLayout` is an inspectable snapshot of registered files and escape
  subtrees.
- `StorageRegistry` wraps a layout for runtime registration and deregistration.
- `StorageFileContract` describes an exact file path.
- `StorageEscapePath` describes an intentionally open subtree.

Downstream crates expose `register_storage_paths(&mut StorageLayout)` functions.
The runtime facade composes the R1 catalog from those owner-crate functions
instead of asking `sinomo-storage` to know every subsystem path. Application
commands can extend that composed layout with higher-ring registrations before
verification; for example, install-time verification adds the self-evolution
storage subtree from `sinomo-evolution`.

The default storage rule is binary first. Internal state should use
`sinomo-object` or another binary format. Human-readable text is reserved for
files people are expected to inspect or edit directly, such as local
configuration. External-system files may use the format required by that system.

Layouts may include escape paths for intentionally open subtrees. For example,
`workspace/` can be declared as an escape path when it may contain arbitrary user
files. Escape paths must include a reason so agents and diagnostics can explain
why detailed file ownership stops at that directory. Escape registration still
belongs next to the subsystem that owns or exposes that subtree.

Human and external configuration subtrees are also explicit escapes. The local
crate registers `config/` for local human-readable configuration and
`providers/` for provider credentials or provider-specific files whose formats
are dictated by external systems.

Dynamic DFS roots are registered by `sinomo-dfs` as escapes at the subtree
level. That registration covers content-addressed blob/object storage, metadata
records and journals, the default `dfs/metadata/root` shard, operator records,
pin records, and local-owned `files/`.

Higher-ring agent artifacts are tracked at subtree boundaries by their owning
crate. `sinomo-evolution` registers `evolution/` as an R2 escape because it
stores campaign/job JSON, logs, and candidate workspaces intended for agent and
operator inspection.

Storage contracts are meant to be machine-readable. Agents should be able to ask
which module owns a path, which files a module may touch, which paths are
untracked by design, and which Sinomo Object schema version applies to a binary
file.

`sinomo-storage` also exposes verification APIs. A caller can verify an explicit
set of relative paths or walk a data directory and receive a report split into
covered files and unregistered files. A path is covered only when it exactly
matches a registered file contract or falls under an explicit escape path.

The runtime facade exposes the composed runtime layout as
`sinomo_runtime::storage`, so application-layer commands can verify layouts
without depending directly on lower-level crate internals. `sinomo install`
uses this as an install diagnostic: after writing installed binaries, managed
source, and boot entries, it walks the data directory and warns if any file is
not covered by the composed contract. Storage coverage problems are node health
issues, not startup blockers. `sinomo cluster doctor` reports the same
unregistered-path condition as a warning so operators can clean it up without
preventing an otherwise usable node from running. Boot entries written under a
test or local startup directory are covered by the `startup/` escape path
because their exact format is owned by the local OS integration rather than a
Sinomo Object schema.
