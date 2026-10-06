# DFS KV boundaries

`dfs_implicit_kv` is a deny-by-default Dylint rule. It checks resolved compiler
symbols and follows local helper, closure, async and trait-implementation edges.
It also checks function-pointer references and literal/constant KV RPC routes.
Aliases do not hide a prohibited operation.

Normal DFS service, POSIX and browser functions, and the extracted history and
attachment implementation functions are operation roots. A new entry outside
those modules can declare `/// sinomo:dfs-operation`. The call graph
includes helpers in other local modules. KV client methods, cluster KV access
and capability-admission queries are prohibited without an explicit boundary.

Legitimate global consistency remains available. Place a narrow marker on the
function that owns the requirement, with a concrete reason of at least 24
characters:

```rust,ignore
/// sinomo:kv-consistency: Fence publication against concurrent shard handoff.
fn validate_publication_authority(/* ... */) { /* existing KV protocol */ }
```

Typed configuration readers use `sinomo:kv-configuration: REASON` to identify
the existing configuration access boundary. These include retained filesystem
policy, shard configuration, provider eligibility and explicit stale-view
refresh. They must not grow DFS data/metadata storage responsibilities.

Configuration setup uses `sinomo:kv-lifecycle: REASON`. It is permitted as a
lifecycle entrypoint, but an ordinary operation cannot call it as a hidden KV
escape. Ordinary routing uses existing cached network information; configuration
and consensus logic must not be reimplemented to avoid this lint. A marker is an
explicit code-review contract, not permission to weaken durability or authority.

The lint prevents accidental calls through the supported compiler-visible
boundaries; runtime regression tests additionally verify request counts. It does
not claim to prove arbitrary external implementations or unsafe generated code.

## Run

Pinned tool versions follow the [upstream Dylint workflow](https://github.com/trailofbits/dylint).
The lint library is a separate workspace because it uses compiler-private APIs.
Application builds continue to use the normal stable toolchain.

```sh
rustup toolchain install nightly-2026-08-20 --profile minimal --component rustc-dev --component llvm-tools-preview
cargo install cargo-dylint dylint-link --version 6.1.0 --locked
bash scripts/ci/check-dfs-kv-boundary.sh
```

`ui` runs positive/negative compiler fixtures only; `workspace` checks the DFS
core and its history and attachment component crates.
Artifacts remain under `target/dfs-kv-boundary`. Run one compilation task at a
time and stop the task's Docker cluster before compiling.
