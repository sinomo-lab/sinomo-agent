# Remote Node Configuration

Use Sinomo's existing typed configuration service after remote management has
been authorized. Do not invent module-owned configuration proxy routes.

The supported configuration routes are:

- `/config/catalog` to enumerate definitions.
- `/config/search` to locate settings by query.
- `/config/get` to read a typed, resolved value.
- `/config/set` to write through the configuration service's validated target.

Inspect catalog or search results before a write when the setting or target is
uncertain, read the current value, make the smallest requested change, then
read it back. Prefer these typed routes over editing files with remote shell.

Remote-management authorization is deliberately outside configuration. The
target operator must enable the `remote-management` permission locally, and
there is no supported remote RPC or `/config/set` key for changing that local
permission document. A granted shell is technically capable of rewriting host
state, but agents must never use it to change node-local permissions. Module
deployment or activation does not enable the grant. If access is denied, ask
for manual local enablement and stop; do not attempt a remote workaround.
