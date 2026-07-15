# Agent Session Placement and Workspace Projection

Status: Active implementation plan
Last reviewed: 2026-07-15

The durable ownership model is now represented in `sinomo-agent`:

- homes are keyed by agent id and are independent of sessions and roles;
- chats and channels carry ordered workspace grants, with the first grant as
  cwd;
- agent definitions can model a preferred home server for new sessions, but
  production session startup does not resolve or dispatch to that preference
  yet;
- sessions record their active server and placement generation, plus a
  reference to one independently managed sandbox resource;
- manual migration invalidates session-host-local resources without copying
  them and does not move the sandbox.
- normal local chat/CLI setup provisions the default persistent sandbox before
  tools are exposed and persists the automatic attachment on the session;
- the command tool fails with `sandbox_not_attached` when the resource is
  missing and never creates one during tool execution.

## Implementation progress

The local conversation/runtime boundary now follows the ownership model:

- App chats and channels project their ordered `AgentChannel.workspaces` into
  every participant tool runtime. TUI session records persist the same ordered
  grant list. Both paths use `AgentSession.workspace_binding` only to backfill
  records created before conversations owned workspaces, then keep it as a
  non-authoritative primary-workspace shadow for older readers. A persisted
  projection version distinguishes a legacy missing list from a current,
  intentionally empty grant set. Every exact projection mutation writes a fresh
  head into a persisted multi-value register and carries forward its observed
  causal context. Merge retains all concurrent maximal heads, removes only
  heads observed and superseded by another state, and materializes their
  byte-for-byte identical grant intersection. Retaining heads makes incremental
  merge associative, commutative, and idempotent even when immutable metadata
  records arrive in different groupings. It prevents unrelated saves and stale
  repeated edits from resurrecting a revocation while allowing a mutation that
  observed a conflict to explicitly re-grant access. A separate causal-protocol
  marker normalizes tokenless, unknown, or structurally malformed authoritative
  snapshots as opaque heads instead of trusting their ordering. A headless
  causal context is retained as the canonical result when every known head is
  superseded, including adversarial cyclic claims, so later incremental joins
  remain grouping-independent. A newer unmarked legacy-empty snapshot cannot
  erase an older marked list. Replica-local revision counters
  remain diagnostic compatibility fields and do not order authorization state.
  App callers change grants only through explicit select/replace/clear
  operations; backend reconnect replays sandbox-node placement only and restores
  workspace authority from the channel. This establishes safe replica merge
  semantics, not yet actor authorization or a grant audit boundary.
- The first workspace remains the relative-path and command-cwd root. Additional
  local roots are explicit host-access grants and can be addressed by absolute
  path. Workspace-tool operands and command cwd values outside the ordered
  grant set remain blocked; separately authorized runtime paths and the agent
  home are not workspace grants. Structured file and Git mutations remain
  primary-workspace-only until file tools move behind the sandbox filesystem
  interface.
- A SIFS grant in conversation position one is projected as `/work`; later SIFS
  grants use `/workspaces/<one-based-position>`. The agent home is projected
  separately at `/home/agent` when live SIFS is available and POSIX home
  preparation succeeds. Command policy admits the exact SIFS sources already
  attached by the management-plane manifest; tool calls cannot add grants or
  mounts.
- Sandbox management now supports an optional atomic full mount-set replacement
  on update. The daemon reconciles it under the sandbox lifecycle lease,
  validates policy, resolved sources, and cwd before persistence, advances the
  manifest generation, and emits mount delta events. The local backend uses
  its own execution lease, advances the generation, and performs basic
  environment and cwd validation, but does not perform the daemon's policy or
  resolved-source validation. For a mount-only update it preserves the
  host-absolute cwd used by the `simple_sandbox` compatibility runtime.
  Reconfiguring an automatically attached local sandbox uses this update path
  instead of silently retaining stale mounts.

This is deliberately a foundation, not completion of remote execution: local
CLI orchestration still provisions through `LocalSandboxBackend`, and the
current structured file tools still operate on host paths.

## Remaining work

1. Remove the compatibility `AgentSession.workspace_binding` field after
   persisted app/channel/TUI snapshots containing conversation workspace lists
   have aged out.
2. Replace the current local-backend automatic provisioning adapter with the
   node/cluster sandbox service route, including remote placement. Explicit
   attachment must accept an existing, potentially shared sandbox and must
   never provision a replacement implicitly.
3. Add expected-generation/CAS fencing to every sandbox manifest mutation,
   including automatic reconciliation and explicit/admin updates, and make
   concurrent create/reconcile idempotent. Establish one authoritative mutation
   owner per sandbox resource and hosting placement even when sessions share
   it. The current wire update has no expected generation, and preflight
   equality and not-found checks occur outside the backend lease.
4. Make daemon sandbox-update audit evidence failure-atomic with the manifest
   mutation, and include sandbox generation/incarnation in update and mount
   delta events. Add mixed-version SOBJ/ObjectRPC compatibility coverage; the
   current omission compatibility test covers JSON only.
5. Make conversation tool-runtime reconfiguration transactional across model,
   registry, sandbox manifest, canonical session, and channel persistence. An
   active-turn preflight prevents the deterministic partial-update case, but a
   later persistence failure still needs a durable rollback/retry protocol.
6. Route every agent tool through the attached sandbox service, including file
   tools; tools must not bypass the sandbox filesystem or process runtime.
7. Complete ordered-workspace projection behind the sandbox filesystem:
   preserve per-workspace access policy, expose structured file operations for
   every writable grant, and make the first mounted workspace the guest cwd for
   every supported runtime. Surface least-privilege reconciliation conflicts
   so users can explicitly re-grant access after concurrent edits. Compact the
   grow-only causal token context into a stable-writer version vector or pair it
   with an expected-base/CAS mutation contract before unbounded histories are a
   concern. Host access is only the compatibility path for runtimes without an
   isolated filesystem.
8. Make workspace grant changes an authorized, immutable control-plane record:
   validate the acting user/device against channel policy; preserve actor,
   device, operation, target grant, and expected-base provenance for every
   select/replace/clear; and audit both accepted and rejected mutations. Whole
   channel snapshots and random causal tokens alone are not an authorization
   proof or sufficient grant/revoke audit trail.
9. Persist the authoritative session-server record in cluster-visible storage.
   Wire production session startup to resolve the agent's configured home
   server, or fall back to the daemon processing the request, then start and
   register the session on that server before publishing its placement.
10. Route chat/channel ingress and live controls to the active session server.
   Stale senders refresh placement and retry; they never auto-migrate.
11. Implement a real, explicitly requested migration handoff: fence and
   quiesce the source session, checkpoint only portable session state, restore
   and register the target session, and compare-and-swap the authoritative
   placement generation only after the target is ready. Route subsequent
   ingress and controls to the committed target; define retries and rollback
   for failures before and after the placement CAS, including safely
   unfencing the source when the handoff has not committed. Migration reports
   must show unavailable local resources and the unchanged sandbox placement.
12. Add multi-node acceptance coverage: two agents in one channel on different
   servers, cross-device ingress routed to the current host, SIFS workspace
   continuity, local-workspace loss after migration, remote sandbox execution,
   and rejection of non-portable sandbox migration.

This plan must not introduce local file copying, let tool calls create
sandboxes, or couple session placement to sandbox lifecycle.
