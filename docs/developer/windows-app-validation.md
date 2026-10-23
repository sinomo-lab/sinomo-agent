# Windows app validation

This guide describes native Windows app checks and the evidence needed to
interpret them. Offline presentation, connected application behavior, device
access and distributed correctness are separate validation scopes.

## Select the execution boundary

Use an isolated checkout and a fresh task-owned data directory. Preserve the
operator installation and any unrelated running processes. Record source,
binary identity, toolchain, commands and results in the task artifact directory.
A local app check does not authorize production startup or rollout.

Build the app with its ordinary feature profile:

```powershell
cargo build --locked -p sinomo-app
```

For offline UI checks, use the supervised attach-only mode with a fresh root and
an intentionally unavailable daemon executable. Configure the child process:

```text
SINOMO_DATA_DIR=<fresh task-owned data directory>
SINOMO_APP_SUPERVISED=1
SINOMO_APP_DAEMON_BINARY=<task artifact directory>\intentionally-absent-daemon.exe
```

Verify the current launch path still treats supervised mode as attach-only before
using this recipe. A failed connection should expose the offline UI without
starting a daemon. The ordinary supervisor path can start services and needs its
own appropriately scoped run.

## Native GUI checks

Use the actual window and Windows accessibility observations. Retain screenshots
or other evidence in the run artifacts. Re-observe controls if an accessibility
handle expires; do not mistake a stale observation handle for an app defect.

| Flow | Required observation |
|---|---|
| Navigate while connecting, then open the offline UI | Window remains responsive; failure feedback is reachable. |
| Files → storage/activity → reconciliation activity | Sheet opens and filters remain usable offline. |
| Change filters and repeat the selected filter | Exactly one selection is visible; repeating it does not reset state. |
| Close/reopen the sheet with Escape and its close control | Dismissal works and intended state is retained. |
| Add a provider, change its model, close a picker, cancel | Selection is coherent; cancellation removes the draft. |
| Save an agent while offline | The app reports unavailable daemon functionality without starting a service. |
| Select and copy error text | Clipboard receives the complete selected text; reset the clipboard before testing another copy path. |
| Navigate a long summary with its arrows | The visible viewport moves, not only the scrollbar; reverse and repeated navigation work. |

A viewport test must observe content and hit-test/semantics geometry. A changed
scroll-state value alone does not establish that retained layout was invalidated.
Native clipboard tests and headless clipboard assertions cover different paths.

## Automated checks and fixtures

Use explicit task-owned test roots for local backends. Keep each root alive for
the complete test and initialize the native state the test actually requires.
Do not start a daemon merely to hide a fixture setup failure.

Select the affected app tests and run the relevant complete target after a fix.
For dependency or Windows sandbox changes, include:

```powershell
cargo test --locked -p sinomo-core --test workspace_ring_boundaries
cargo test --locked -p sinomo-sandbox-simple --lib windows_
```

Relevant UI controls include retained radio selection, asynchronous provider
loading, model visibility, compact summary scrolling and media-panel lifecycle.
Test repeated input and external state changes as well as the initial render.
Keep native GUI evidence separate from headless assertions.

## Connected and device checks

An offline fixture cannot establish chat/media functionality, live distributed
activity, physical camera/microphone behavior or supervisor startup. Exercise
those in separately scoped environments using the normal runtime and permissions.
Distributed correctness and quietness require their own cluster checks.

Stop only processes owned by the run, using normal lifecycle controls. Retain
raw logs, executable identities, screenshots, failed cases and cleanup outcomes
outside permanent documentation. Report missing coverage explicitly rather than
turning an offline pass into a broader readiness claim.
