# File Content History

DFS keeps one current filesystem view. By default, overwriting or deleting a
file does not retain its previous content. Save a checkpoint explicitly, or
enable a retention rule before the change you want to protect.

These commands use the main daemon. History captures require history-capable
metadata shards; an unsupported shard returns an error instead of accepting a
capture it cannot preserve. See `sinomo dfs help` for the installed syntax.

## Save and Find a Checkpoint

Choose a pin name that describes why you need the content:

```sh
sinomo dfs history capture /services/report.txt --pin before-edit --json
sinomo dfs history list /services/report.txt --json
```

Keep the returned `identity.object_id` and `identity.version`. They identify
the content even after a rename or deletion. A new file at the old path has a
different object identity. A capture stores file content, not a directory
snapshot or a promise to restore old permissions.

Listing accepts a path or subtree, `--object`, and `--limit` from 1 to 128. Pass
the returned `next_cursor` as `--cursor` with the same filters to continue.
A page can be empty and still have a continuation token. If the authority
changes and the cursor expires, restart the listing.

To save a retained version to a new local file, set `OBJECT_ID` and `VERSION`
to the identity from the listing:

```sh
sinomo dfs history read --object "$OBJECT_ID" --version "$VERSION" --output saved-report.txt
```

Without `--output`, the command streams to stdout. It refuses to overwrite an
existing output file. A failed transfer can leave a partial new file. A version
that is no longer retained and retained content with no reachable replica are
reported separately.

## Restore or Recover

Restore replaces a currently observed file with retained content:

```sh
sinomo dfs history restore /services/report.txt --object "$OBJECT_ID" --version "$VERSION" --json
```

The command observes the destination and writes only if that exact object and
generation are still current. Restore publishes a new generation; it does not
rewind the filesystem or overwrite a concurrent edit. Capture the current
content first if you want to keep it and no retention rule covers it.

Recovery creates a file at an unoccupied path:

```sh
sinomo dfs history recover /services/recovered-report.txt --object "$OBJECT_ID" --version "$VERSION" --json
```

Recovery fails if the destination is occupied. It can use retained content from
a deleted file; find its identity by listing its former path or parent subtree.
Neither command can recover content that was never retained or has been
collected.

In the app, open **History & issues** and choose **Restore this version** on a
retained version. The confirmation keeps the file observation shown when you
made the selection. If the request fails, confirming it again retries that
same operation and expectation. Restoring content does not clear diagnostic
conflict records; review and resolve those separately.

For repeatable automation, choose a unique `--operation-id` and persist the
source identity, destination path, and destination expectations with it. Supply
both `--expected-object` and `--expected-version` to `restore` when you already
have that observation. An ambiguous result must be retried with the same
operation ID and the same inputs, including those expectations. A new operation
ID requests another write. `recover` always requires an absent destination and
does not take destination expectations.

The legacy `dfs conflicts use-version` command now performs this same
conditional restore. It accepts `--object`, `--path`, `--version`,
`--expected-version`, and `--operation-id`; use all five for an exact retry that
does not need to rediscover the destination. It requires explicitly retained
content and a main daemon. Metadata-only version rewrites and automatic conflict
record removal are no longer supported by that command. Use
`dfs conflicts resolve` when you explicitly want to clear diagnostic records.

## Configure Automatic Capture

Inspect the effective policy, or set a rule for a subtree:

```sh
sinomo dfs history retention /services
sinomo dfs history retention /services --keep-last 10 --keep-for-seconds 604800
```

This captures outgoing content on successful replacement or deletion. It keeps
the newest ten policy captures per object **or** captures younger than seven
days, whichever keeps more. Age starts when the content is superseded or
deleted. Attribute-only changes and rejected writes do not capture content.

The most specific matching subtree rule wins. A narrower rule can disable
automatic capture:

```sh
sinomo dfs history retention /services/cache --keep-last 0 --keep-for-seconds 0
```

Setting either retention option replaces both values for that rule; an omitted
value becomes zero. Policy changes affect future captures. Existing captures
keep the minimum retention granted when they were made, and named pins remain
independent. Garbage collection can remove unprotected content asynchronously.

## Add and Release Pins

Use a new pin name to retain an already captured version independently:

```sh
sinomo dfs history pin /services/report.txt --object "$OBJECT_ID" --version "$VERSION" --pin audit
sinomo dfs history unpin /services/report.txt --object "$OBJECT_ID" --version "$VERSION" --pin before-edit
```

`capture` preserves the current version; `pin` extends the lifetime of a version
already in history. Each pin must be released by name. Unpinning does not remove
another pin or an applicable retention guarantee. Supply explicit identities
after deletion or path reuse so the path cannot select a replacement file.

Capture, pin, and unpin also accept `--operation-id` for exact retries. Listing,
capture, pin, unpin, retention, restore, and recovery accept `--json` where shown
by command help. `--filesystem` selects the source filesystem; restore and
recovery destinations are in SIFS.
