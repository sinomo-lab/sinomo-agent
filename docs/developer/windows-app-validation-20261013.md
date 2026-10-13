# Windows validation of the integrated app revision

Validation completed locally on 2026-10-13. These results are Windows app and
isolated-fixture results, not permission to start or roll out a distributed cluster.

## Revision and ownership receipts

| Item | Exact identity |
| --- | --- |
| Requested integrated main | `c685776acfa32e99f7881cc412e8847c932a5534` |
| Tested local fix | `4744bdff342bca7a7518753d19644e43f645959b` |
| Preserved operator checkout | `D:\Workspace\sinomo\sinomo-agent`, clean `main` at `f5db545a4d509524d4bd925a5d31bdd3cf7fa76b` before and after validation |
| Isolated validation checkout | `D:\Workspace\sinomo\sinomo-windows-validation-c685776a` |
| Local fix branch | `validation/windows-c685776a` |
| Existing Orca owner | `term_6bde0774-371d-46a4-af95-81b5f444cff9`, execution host `local`, Codex/Sol xhigh |
| Platform | Windows build `10.0.26200`, `x86_64-pc-windows-msvc` |
| Toolchain | rustc `1.99.0 (b940084d7 2026-09-28)`, cargo `1.99.0 (5f94df478 2026-08-27)` |

The requested commit was absent locally. It was fetched by exact SHA, then checked
out in a detached secondary worktree. The original checkout was never switched,
reset, stashed, cleaned, merged, or committed. Orca inventory showed one Windows
validation owner; no new worker or cross-review was created. No Claude design or
documentation worker was woken. The existing model/account was retained. The
ownership snapshot showed weekly remaining at 100%; no below-5% observation occurred.

## Safe native launch

Both native GUI runs used freshly built `sinomo-app.exe` with default features and
`cargo build --locked -p sinomo-app`. Each run used a fresh disposable data root:

```text
D:\Workspace\sinomo\windows-validation-evidence\20261013-c685776a\app-data
D:\Workspace\sinomo\windows-validation-evidence\20261013-c685776a\app-data-fixed
```

Child-process environment:

```text
SINOMO_DATA_DIR=<fresh fixture above>
SINOMO_APP_SUPERVISED=1
SINOMO_APP_DAEMON_BINARY=<evidence root>\intentionally-absent-daemon.exe
```

Current source was checked before launch: `main.rs` skips the supervisor in
supervised mode; `backend::connect_local_daemon` calls
`sinomo_client::connect_to_supervised_daemon`, which only probes the fixture's
daemon status and never spawns a daemon. Fresh roots had no daemon status or
cluster identity. The bounded 45-second attach attempt intentionally expired,
leaving the real app's offline UI usable. This is a real native GUI launch through
the attach-only path. The ordinary supervisor launch was not exercised because it
can automatically start services.

| Built executable | SHA-256 |
| --- | --- |
| Original integrated revision | `D9473C7799F68F4F099CDE326A413DFC7B4F8DA2F1BF63969E880CA01DEB0084` |
| Local fix revision | `CC23E3E43154AF8FAFF3113B9F1A17996D633FEB16B5BF185B85EEB10CA3407E` |
| Final app test executable | `D97491B6A9CB2C1922E8DA30DBAF11D81111700EEF9DA2F966DA99811592F326` |

## Visible GUI results

These actions used the native Windows app, screenshots, and Windows accessibility
observations. Headless results are listed separately below.

| Flow / reproducible steps | Revision | Result | Evidence prefix |
| --- | --- | --- | --- |
| Launch, navigate during connection, wait for offline recovery UI, then open Files | `c685776a` | Pass: window opens and remains responsive; offline panel is reachable | `01`–`03` |
| Files → More folder actions → Storage and activity → View reconciliation activity | `c685776a` | Pass: sheet and all four radios remain reachable offline | `04`–`06` |
| All → Active → All → Failed → Done; click Done again | `c685776a` | Pass: exactly one visible selection follows each choice; repeated selection remains Done | `07`–`11` |
| Escape, reopen the sheet through the same menu, close through Close | `c685776a` | Pass: Escape dismisses it, Done is preserved on reopening, Close dismisses it | `12`–`14` |
| Settings → Providers → Add provider; choose DeepSeek; reopen picker; Escape; Cancel | `c685776a` | Pass: DeepSeek and `deepseek-flash` survive Escape; cancelling removes the draft | `16`–`22` |
| Settings → Agents → New agent → Save agent while offline | `c685776a` | Pass: expected `agent management requires a daemon connection` feedback appears without a service launch | `23`–`26` |
| Error context menu → Select All → reopen menu → Copy | `c685776a` | Pass: visible selection spans the complete error; Windows clipboard exactly matches all 45 characters | `27`–`31`, `native-clipboard-copy.json` |
| Summary → Next activity summary | `c685776a` | Fail: scrollbar changes from 0 to 55, but retained content does not move | `33`–`34` |
| Same summary Next → Previous → Next, then All → Active → All and Escape | `4744bdff` | Pass: content visibly moves 55 pixels, Previous restores it, the repeat works, radio selection stays coherent, Escape dismisses the sheet | `36`–`42` |

One native accessibility handle expired during the first repeated Done attempt.
The app was reobserved and the action retried with the current handle; this was
recorded separately as `11a`. It was not counted as an app behavior failure.
Keyboard Ctrl+C after the native copy left the correct clipboard value intact,
but the clipboard already contained that value; this does not prove a fresh
native keyboard write. The headless keyboard copy assertion does prove its own
runtime clipboard write.

## Bounded local fix

The summary arrows changed shared `ScrollState`, then requested invalidation on
the arrow itself. The retained sibling viewport kept its cached layout and
content transform. The fix captures the actual ScrollView widget ID and requests
measure, arrange, paint, hit-test, and semantics invalidation on that viewport.
The layout and scroll step remain the same. Native before/after screenshots and
the compact large-text regression verify the functional repair.

Sixteen relevant legacy tests failed during fixture setup because
`Backend::local_fallback()` eagerly mounts test SIFS without an initialized node.
They now use explicit `TestDir` plus `local_fallback_for_data_dir`, retaining each
root throughout the test. No test starts a daemon to satisfy setup. The compact
summary regression uses bounded repeated arrow navigation because Windows text
wrapping requires more than one step; its viewport intersection, error content,
error preservation, filter bounds, and virtual-row assertions remain in place.
No production backend or global test helper behavior was changed.

## Headless and platform checks

| Check | Tested revision | Result |
| --- | --- | --- |
| Default Windows app build, locked dependencies | `c685776a`, `4744bdff` | Pass |
| Initial 26-test app selection | `c685776a` | 10 passed, 16 failed during legacy fixture setup |
| After explicit fixture conversion | Pre-fix working tree | 25 passed, 1 failed on compact summary scrolling |
| Final 26-test app selection | Exact clean `4744bdff` | 26 passed, 0 failed, 758 filtered out |
| `workspace_ring_boundaries` | Exact clean `4744bdff` | 18 passed, 0 failed |
| `sinomo-sandbox-simple --lib windows_` | Fix checkout; sandbox source unchanged from `c685776a` | 11 passed, 0 failed, 2 filtered out |
| rustfmt on the two changed production functions, plus `git diff --check` | `4744bdff` | Pass |

App coverage includes the integrated retained-radio regression with queued inputs,
stable widget identities, repeat-selection query/poll preservation, and external
domain-state changes. It also includes async provider load, catalog model
dropdowns, media-panel identity, hidden-provider selection rules, media close and
release at wide/narrow widths, off-tab preflight release/reopen, cancelled device
open, and late-handle suppression. Media devices are mocked by the test build;
these are not visible GUI or physical-device results.

The changed relative-PATH resolver and its new Unix regressions are explicitly
`cfg(not(windows))`. They were not claimed as Windows execution coverage. The
existing Windows native-runner checks exercise disposable command fixtures,
cancellation, timeout, output limits, streaming, process-tree cleanup, quoting,
and host-path behavior.

Default Cargo jobs and test workers were used. `SINOMO_TEST_KEEP_TMP=1` prevented
test-directory sweeping and retained fixtures. No toolchain or local/global
configuration was changed. Compiler warnings were retained in the logs; no broad
warning cleanup or full-workspace test/clippy run was attempted.

### Reproduce the focused Windows checks

Run from a fresh checkout of `4744bdff342bca7a7518753d19644e43f645959b`:

```powershell
$env:SINOMO_TEST_KEEP_TMP = '1'
$env:SINOMO_DATA_DIR = '<fresh disposable guard directory>'
cargo build --locked -p sinomo-app
cargo test --locked -p sinomo-app --lib -- --nocapture `
  placement_activity_radios_follow_domain_across_retained_frames `
  reconciliation_activity_ settings_error_text_can_be_selected_and_copied `
  device_selection_index_does_not_replace_the_media_panel_tree `
  provider_card_uses_dropdown_for_known_catalog_before_connection_test `
  provider_list_load_applies_async_result `
  rendered_media_call_close_stops_call_and_preview_at_all_widths `
  inactive_preflight_ cancelled_device_open_returns_promptly_and_drops_late_handle `
  disabling_device_open_emits_idle_and_never_publishes_late_ready `
  media_call_control_drain_records_release_and_explicit_reopen `
  media_call_device_transition_opens_only_after_explicit_enable `
  hidden_runtime_provider_does_not_leak_into_visible_model_selection `
  visible_model_selection_command_ignores_hidden_runtime_provider
cargo test --locked -p sinomo-core --test workspace_ring_boundaries
cargo test --locked -p sinomo-sandbox-simple --lib windows_
```

For a GUI reproduction, launch only the built app with the child-process
environment in the safe-launch section, never the ordinary supervisor. The
native navigation steps are in the GUI table. Fresh attach-only fixtures do not
provide a connected Chat/media surface.

## Durable raw receipts and remaining gates

Raw logs, launch/process metadata, screenshots, and the original integrated
executable are retained outside Git at:

```text
D:\Workspace\sinomo\windows-validation-evidence\20261013-c685776a
```

`evidence-manifest.json` records each top-level evidence file's size and SHA-256.
Its SHA-256 is
`3AE45B9975184F742DE0CB910493CA6C9CD47E01D059FA7087BA405FBF21999C`.
Primary logs are `focused-app-tests.log` (original failure),
`focused-fixed-app-tests.log` (scroll failure),
`tested-4744bdff-app-tests.log`, `tested-4744bdff-ring-boundaries.log`, and
`sandbox-windows.log`. The fixture directories and build outputs remain local
and are not committed.

At 18:07:16 UTC, `terminal-guard.json` recorded zero local Sinomo processes and
zero Cargo/rustc processes. Only the two owned validation GUI processes were
closed through their window controls. No daemon, server, Windows cluster agent,
remote cluster command, machine restart, merge, push, rollout, or publication was
performed. The original checkout remained clean at its original SHA.

The ordinary supervisor/service startup, connected Chat/media GUI, physical
camera/microphone, live distributed activity data, and distributed correctness
and quietness acceptance gates remain unverified in this task. The offline daemon
gate hides the Chat/media surface, and exercising normal service startup would
violate the cluster-stop constraint. These local app results do not substitute
for either distributed gate. Both gates and separate user/operator manual startup
remain mandatory before running the real cluster.
