# Sinomo DFS History

The History module owns explicit file captures, retention settings, historical
discovery, bounded historical reads and restore orchestration. It uses the
distributed semantic filesystem owner's public revision, content-reference,
authority and durable-job contracts.

History follows filesystem and object identity across renames and hardlinks.
Outgoing-content capture shares the canonical namespace publication transaction.
Restore creates a conditional new revision or an exclusive new object, with a
durable source reference and a stable operation identity.

The DFS core owns content integrity, current revision authority, replication,
recovery and collection of unreferenced bytes. Disabling History withdraws its
services without revoking previously committed content references, retention
rules or recovery obligations. The core continues honoring stored rules until
an explicit authorized policy change, and evaluates recorded grant expiry and
reference release independently of module availability.
