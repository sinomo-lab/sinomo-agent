# Codex provider compatibility

The direct `codex` provider is aligned with the local Codex checkout at
`c775dd3c3` (2026-09-17 comparison). Its source contracts are:

- `codex-rs/models-manager/models.json`: model capabilities and visibility.
- `codex-rs/protocol/src/openai_models.rs` and its `reasoning_effort.rs` module:
  reasoning selections, backend effort mapping, and service tiers.
- `codex-rs/core/src/client.rs`: Responses Lite requests and headers.
- `codex-rs/tools/src/tool_spec.rs`: tool namespaces and declarations.
- `codex-rs/protocol/src/models.rs`: function/custom calls and results.

## Model discovery and configuration

Sinomo sends catalog compatibility version `0.153.0`, the minimum required by
GPT-6 Astra in that checkout. The development workspace version `0.0.0` is not
a suitable capability version for the backend catalog.

The bundled metadata in `crates/sinomo-lm/src/codex/models.json` contains only
model capabilities, without Codex's agent instructions. It supplies request
defaults and configuration suggestions. Authenticated `/models` discovery
remains the authority for account-visible models; hidden entries are excluded.
`OpenAiResponsesCodexClient::list_model_catalog()` and the provider's matching
method expose reasoning levels, service tiers, context limits, and protocol
metadata. Discovery updates the client's metadata cache; server fields override
bundled defaults, and future reasoning/tier names remain strings.

| Model | Reasoning levels | Additional service tiers | Default / maximum context |
| --- | --- | --- | --- |
| `gpt-6-astra` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-5.6-sol` | low, medium, high, xhigh, max, ultra | priority, ultrafast | 272,000 / 872,000 |
| `gpt-5.6-terra` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-5.6-luna` | low, medium, high, xhigh, max | priority | 272,000 / 872,000 |
| `gpt-5.5` | low, medium, high, xhigh | priority | 272,000 / 272,000 |

The provider default remains `gpt-5.6-sol`. These limits describe the Codex
subscription backend, independently of OpenAI API context limits. Reasoning
`max` is sent verbatim. The app and CLI expose `ultra`; the direct provider
resolves it through `multi_agent_reasoning_effort`, falling back to `max` or
the highest non-ultra catalog level, as upstream does. Codex also attaches
delegation behavior to Ultra; selecting it in the direct provider does not
change Sinomo's agent orchestration. `codex-cli` receives `ultra` unchanged.
Known-model effort/tier mismatches return an
explicit error before a Responses request; `flex` is allowed as in upstream,
`fast` maps to `priority`, and `default` omits the service-tier override.
Service tiers and reasoning summaries use the existing `vendor_options`, for
example `service_tier=priority` or `reasoning.summary=auto`.

## Requests and tools

For Astra and the GPT-5.6 family, the direct provider uses Responses Lite:

- `x-openai-internal-codex-responses-lite: true` on HTTP requests.
- An `additional_tools` developer item containing the `functions` namespace,
  followed by developer instructions. Content-derived item IDs remain stable
  across retries. Top-level `tools` and `instructions` are omitted.
- `reasoning.context=all_turns`, encrypted reasoning inclusion, and the model's
  default effort/verbosity unless explicitly overridden.
- Top-level parallel tool calls disabled, matching upstream's Lite envelope.

GPT-5.5 and unknown models retain the ordinary Responses envelope unless live
metadata selects Lite. Both formats support JSON function tools and grammar
custom tools, including the workspace `apply_patch` tool. Custom calls reach
Sinomo's normal tool adapter; replay restores raw inputs and uses
`custom_tool_call_output`. Default namespace calls map to the existing local
tool names; foreign namespaces retain their qualifier to avoid collisions.

Sinomo retains ownership of prompts, tool execution, policy, cancellation,
compaction, and durable sessions. Codex's code-mode executor, delegated agents,
plugin runtime, and WebSocket connection management are not imported into the
provider. HTTP streaming is an upstream-supported transport. Existing hosted
search, image generation/editing, realtime media, credentials, and usage paths
remain available through their Sinomo interfaces.

## Verification

`cargo test -p sinomo-lm` covers catalog parsing/discovery, protocol selection,
exact effort/tier values, custom calls and continuation, legacy requests,
streaming, cancellation, retries, and authentication refresh. The compatibility
HTTP test uses a local server and fake credentials; it does not assert live
account entitlement. Agent tests verify the workspace patch declaration also
offers its grammar to Codex.
