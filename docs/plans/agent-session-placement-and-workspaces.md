# Agent Session Placement and Workspace Projection

Status: Active implementation plan

The durable ownership model is now represented in `sinomo-agent`:

- homes are keyed by agent id and are independent of sessions and roles;
- chats and channels carry ordered workspace grants, with the first grant as
  cwd;
- agent definitions can model a preferred home server for new sessions, but
  production session startup does not resolve or dispatch to that preference
  yet;
- sessions record their active server and placement generation, plus a
  reference to one independently managed sandbox resource;
- the session model's explicit migration (`AgentSession::migrate_to_server`)
  invalidates session-host-local resources without copying them and does not
  move the sandbox; no control operation invokes it yet;
- normal chat/CLI setup provisions the default persistent sandbox through
  `SandboxLifecycleClient` before tools are exposed and persists the automatic
  attachment on the session;
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
  resolved-source validation; it serves only a host running without a daemon.
  For a mount-only update it preserves the host-absolute cwd used by the
  `simple_sandbox` compatibility runtime.
  Reconfiguring an automatically attached local sandbox uses this update path
  instead of silently retaining stale mounts.
- The graphical Chat composer exposes the conversation workspace as a native
  folder selection. Replacing a folder replaces the complete ordered grant set
  rather than retaining hidden prior roots, and removal is an explicit
  revocation. Picker results are fenced to the session and UI generation that
  opened them. The provider-neutral app contract carries only the selected
  path; daemon-hosted chat execution resolves and persists the authoritative
  `WorkspaceRef` and reconciles the attached sandbox before acknowledging it.
- Agent orchestration creates, inspects, updates, and destroys sandboxes
  through `SandboxLifecycleClient`. With a daemon, requests go to the local
  sandbox service, which serves them or forwards them to the hosting node;
  only a daemonless host uses `LocalSandboxBackend` in process. Deleting a
  chat releases each session's automatic, unshared sandbox through the same
  client.
- File tools and context assembly reach workspace content through confined
  VFS handles (`tools::workspace_files`): host-local grants through
  `ConfinedLocalVfsBackend`, SIFS grants through the SIFS VFS.
- A chat has one host in the cluster. The hosting node holds a renewed
  ownership record in cluster KV (`/agent/chat-hosts/<chat-id>`); other nodes
  forward commands and events to it over the `/chat/peer/*` routes, and take
  the chat over only after the owner stops renewing for 45 seconds. The first
  node to open a chat becomes its host; the agent's home-server preference is
  not consulted, and the takeover does not update the session's placement
  record.

This is deliberately a foundation, not completion of remote execution:
automatic sandboxes are created non-portable on the hosting node, commands run
against that node's local sandbox store, and file tools run on the session
host instead of through the attached sandbox service. Agent packages that name
another node as home server or sandbox node are rejected.

## Remaining work

1. Remove the compatibility `AgentSession.workspace_binding` field after
   persisted app/channel/TUI snapshots containing conversation workspace lists
   have aged out.
2. Replace the current local-backend automatic provisioning adapter with the
   node/cluster sandbox service route, including remote placement. Lifecycle
   calls now use the sandbox service; remote placement and execution remain
   open. Explicit attachment must accept an existing, potentially shared
   sandbox and must never provision a replacement implicitly.
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
   tools; tools must not bypass the sandbox filesystem or process runtime. File
   tools now use confined VFS handles but still run on the session host.
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
   register the session on that server before publishing its placement. The
   cluster KV chat-host record covers the chat, not the session placement,
   and is claimed by the first node to open the chat.
10. Route chat/channel ingress and live controls to the active session server.
   Stale senders refresh placement and retry; they never auto-migrate. Chat
   commands and events already reach the chat's host through the peer routes;
   failover after a lost host is a separate takeover path.
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
