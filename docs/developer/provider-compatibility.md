# Language-model provider compatibility

This reference covers Sinomo's model providers, configuration, wire protocols,
tools, and compatibility boundaries. The general provider review dates to
2026-09-18; Codex tools, DeepSeek, Gemini, Grok, and Kimi were updated on
2026-09-26. Provider-specific source revisions and dates appear below. Hosted
media interfaces retain their existing APIs.

- [Provider coverage](#provider-coverage)
- [Protocol ownership and continuation](#protocol-ownership-and-continuation)
- [Controls](#controls)
- [Codex](#codex)
- [DeepSeek](#deepseek)
- [Gemini, Grok, and Kimi configuration](#gemini-grok-and-kimi-configuration)
- [Gemini](#gemini)
- [Grok](#grok)
- [Kimi](#kimi)
- [Grok and Kimi tool boundaries](#grok-and-kimi-tool-boundaries)
- [Official references](#official-references)
- [Verification](#verification)

## Provider coverage

| Provider | Discovery and defaults | Request and response handling |
| --- | --- | --- |
| Codex | Authenticated `/models` with bundled capability defaults; default `gpt-6.1-sol` | [Responses Lite, native tools, and model controls](#codex). Sinomo owns tool execution and delegation. |
| OpenAI | Authenticated `/models`; suggestions include GPT-6.1 Sol, GPT-6 Astra/Sol/Luna and GPT-5.6; default `gpt-6.1-sol`; hosted web search defaults to `gpt-6-luna` | Responses preserves message roles, custom tool input, encrypted reasoning and native calls. GPT-6 uses the documented effort values and omits unsupported sampling controls. |
| Anthropic / Claude subscription | Paginated Models API; default `claude-sonnet-5-5`; suggestions include Opus 5.5; current Claude 5 families use a 1M context limit | Native tool declarations, tool results, streamed JSON arguments and signed thinking blocks. Both credential modes share the Messages codec and Claude 5.5 validation. |
| Gemini | Paginated Models API, filtered to `generateContent` models; default `gemini-3.8-flash` | Native function calls/results, thought signatures, function IDs and thinking controls. Thought text stays separate from answer text; output usage includes thought tokens. |
| Grok | Authenticated Models API; default `grok-4.7` | Responses, encrypted reasoning replay, native function calls, and model-specific reasoning effort. |
| Kimi | Authenticated Models API; default `kimi-k3`; K2.7 Code, K2.6, and K2.5 suggestions | Chat Completions, full assistant-message replay, native tools, K3 effort/output limits, and K2 thinking controls. |
| DeepSeek | Authenticated Models API; default `deepseek-flash`; V4 Pro remains available | [Messages transport and harness-compatible editor](#deepseek), updated against harness `477b4f4205`. Explicit Chat Completions endpoints retain their previous wire contract. Signed thinking and native tool turns are preserved. |
| NVIDIA | Authenticated `/models`; default Nemotron 3 Super retained | Shared Chat Completions codec carries native tools and results. Third-party IDs use their actual organization prefix. Vendor options reach the request. |
| GitHub Copilot | Account catalog retained; direct default `gpt-6.1-sol` | GPT-5.5, GPT-5.6 and GPT-6 use Responses. Both API dialects retain tools and continuation state. The CLI forwards configured reasoning effort. |
| Ollama / LM Studio | Existing installed/local model discovery and defaults retained | Both Responses paths preserve native calls and continuation state. |
| Claude Code | CLI-owned model selection retained; current Claude context inference shared | Existing effort flags and host-permission checks retained. |

Live discovery determines availability. Built-in lists are configuration
suggestions, and an explicitly configured custom model stays selectable.
Discovery errors are surfaced instead of returning an invented one-model
catalog. API-key discovery respects the selected account's base URL, inline
credential, or supplied Sinomo data directory. Pagination stays on the original
endpoint, encodes cursors, and rejects repeated cursors.

## Protocol ownership and continuation

The providers translate wire protocols. Sinomo continues to authorize and
execute tools, manage cancellation, and own durable sessions and compaction.

`LanguageModelProviderContinuation` stores transport-private assistant items
with their provider, model and tool-call IDs. The agent carries those items to
the next request and persists them in an optional `AssistantText.native_items`
field. Older snapshots remain readable. A matching provider/model replays the
original assistant items exactly once; ordinary messages and tool calls remain
the fallback when the selection changes. Gemini 3 renders unsigned legacy or
switched-model tool history as text instead of manufacturing thought signatures.
Compaction excludes opaque state from summarizer prose while retaining the
ordinary conversation and tool evidence.

Messages, GenerateContent, Responses and Chat Completions streams require their
terminal event. A connection ending after partial text or tool arguments returns
a provider failure, preventing the runtime from treating that response as a
completed turn.

## Controls

- OpenAI GPT-6/GPT-5.6 send `max` without downgrading it to `xhigh`. GPT-6
  maps `minimal` to `low`; Astra and GPT-6.1 Sol also map `none` to `low`.
  Direct OpenAI GPT-6 Sol/Luna retain `none`.
- Current Claude models use adaptive thinking. Older models retain explicit
  thinking budgets. Incompatible adaptive-budget or forced-tool/thinking
  combinations fail with an explicit error.
- Claude Sonnet 5.5 maps `none` or a zero budget to `between_tools`, its lowest
  thinking setting, available at low/medium/high effort. Opus 5.5 requires
  adaptive thinking. Both reject forced tool selection and omit sampling
  controls. Adaptive requests enable the thinking-binding beta with
  `prefix_mismatch_behavior=drop_block`, so context/tool edits can discard
  invalid signed blocks while preserving valid continuation. `between_tools`
  requires append-only history; the API cannot apply that drop policy in this
  mode. Advanced vendor options remain subject to model validation.
- Gemini 3 selects supported model-specific thinking levels, including `minimal`
  on supporting Flash models. Gemini 2.5 uses budgets; Pro cannot disable thinking.
- DeepSeek maps minimal/low to `low`, medium/high/xhigh to `high`, and max/ultra
  to `max`. `none` disables thinking. Messages sends `output_config.effort` and
  rejects forced tool selection; legacy Chat Completions sends `reasoning_effort`
  and permits forced selection with thinking disabled.
- NVIDIA's model-specific controls use vendor options, for example
  `chat_template_kwargs.enable_thinking=false` for Nemotron.
- Copilot CLI receives `--reasoning-effort`; external CLI tools still require
  the existing node-local permission before execution.

## Codex

The direct `codex` provider is aligned with the local Codex checkout at
`2e5fea64e` for model metadata (2026-09-30), with tool contracts compared
against `e72da2b53` on 2026-09-26. Its source contracts are:

- `codex-rs/models-manager/models.json`: model capabilities and visibility.
- `codex-rs/protocol/src/openai_models.rs` and its `reasoning_effort.rs` module:
  reasoning selections, backend effort mapping, and service tiers.
- `codex-rs/core/src/client.rs`: Responses Lite requests and headers.
- `codex-rs/tools/src/tool_spec.rs`: tool namespaces and declarations.
- `codex-rs/protocol/src/models.rs`: function/custom calls and results.

### Model discovery and configuration

Sinomo sends catalog compatibility version `0.155.0`, the minimum required by
GPT-6 Sol/Luna in that checkout. The development workspace version `0.0.0` is not
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
| `gpt-6.1-sol` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-6-astra` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-6-sol` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-6-luna` | low, medium, high, xhigh, max | priority | 272,000 / 872,000 |
| `gpt-5.6-sol` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-5.6-terra` | low, medium, high, xhigh, max, ultra | priority | 272,000 / 872,000 |
| `gpt-5.6-luna` | low, medium, high, xhigh, max | priority | 272,000 / 872,000 |
| `gpt-5.5` | low, medium, high, xhigh | priority | 272,000 / 272,000 |

The provider default is `gpt-6.1-sol`. The bundled snapshot retains older
explicitly selectable profiles and removes GPT-5.6 Sol's retired `ultrafast`
tier. These limits describe the Codex
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

### Requests and tools

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
custom tools, including the workspace `apply_patch` tool, and plain-text
freeform tools. Function declarations explicitly use `strict: false` so optional
arguments retain their runtime defaults. Custom calls reach
Sinomo's normal tool adapter; replay restores raw inputs and uses
`custom_tool_call_output`. Default namespace calls map to the existing local
tool names; foreign namespaces retain their qualifier to avoid collisions.

Tools named `namespace.tool` are grouped into Responses namespaces and decoded
back to the same qualified runtime identity. Unqualified tools use the default
`functions` namespace in Lite. Forced tool choice and transcript fallback use
the same naming convention; provider-owned continuation items retain their wire
identity. The OpenAI API adapter uses the same translation. `functions` is the
reserved default namespace; register its tools with unqualified names.

Sinomo's `tool_search` accepts `limit` (preferred when both are present) and the
existing `max_results`, bounded to 1–50. Results include input/output schemas.
Successful search and describe receipts in the retained tool transcript expose
matching executable tools on the next model request. Context assembly and
execution rebuild discovery against the current registry. Tool discovery never
replaces descriptors, grants permissions, or revives unloaded tools; ordinary
surface and executor checks still apply. After a receipt leaves the retained
context, the model can search again. This is client-owned function-tool
discovery, not the hosted Responses `tool_search` protocol.

The September tool comparison preserves these runtime distinctions:

| Codex tool surface | Sinomo behavior |
| --- | --- |
| Function and custom tools, namespaces | Supported by the direct OpenAI and Codex adapters. |
| `apply_patch` | Existing grammar tool through workspace write policy. |
| Deferred tool discovery | Search/describe now load live tool schemas for direct invocation. |
| `exec_command` / `write_stdin` | Sinomo retains its `run_command` and sandbox lifecycle; no PTY-session compatibility alias. |
| JavaScript `exec` / `wait` | Requires a code-mode execution service; not advertised by the direct provider. |
| User input, delegation, plugins, hosted search/images | Owned by Sinomo's runtime bridges and registered tools, not imported from Codex. |

Sinomo retains ownership of prompts, tool execution, policy, cancellation,
compaction, and durable sessions. Codex's code-mode executor, delegated agents,
plugin runtime, and WebSocket connection management are not imported into the
provider. HTTP streaming is an upstream-supported transport. Existing hosted
search, image generation/editing, realtime media, credentials, and usage paths
remain available through their Sinomo interfaces.


## DeepSeek

Compared against the local `deepseek-harness` checkout at `477b4f4205`
(2026-09-24), particularly its `llm-deepseek` serializer/transport and
`tool-str-replace-editor` implementation. Reference:
[DeepSeek Harness source](https://github.com/deepseek-ai/deepseek-harness/tree/477b4f4205/packages/llm/llm-deepseek).

### Transport selection

The default endpoint is `https://api.deepseek.com/anthropic`, using streamed
Messages requests at `/anthropic/v1/messages`, `x-api-key` authentication, and
`anthropic-version: 2023-06-01`. Synchronous completion consumes the same stream.
Redirects are disabled. An endpoint ending in `/anthropic`, `/anthropic/v1`, or
`/messages` selects Messages; an explicit final `/v1` is not duplicated.
Model discovery uses the Models API outside the official `/anthropic` prefix.

Previously configured Chat Completions roots, including
`https://api.deepseek.com`, `/v1`, and `/chat/completions`, retain that protocol
and Bearer authentication. A Messages-compatible gateway can be selected by
configuring its full `/v1/messages` URL. The model defaults and catalog remain
`deepseek-flash` and `deepseek-v4-pro` with 1,000,000-token context estimates;
authenticated discovery determines account availability.

### Messages contract

- Text, thinking, tool use, and tool results use content blocks. Tool result
  blocks precede ordinary user text. Duplicate/unresolved calls and unmatched
  results fail before sending the request.
- Default output cap: 256,000 tokens, overridden by the request. Thinking
  defaults to `enabled` with `output_config.effort=high`. Low/minimal maps to
  `low`, medium/high/xhigh to `high`, and max/ultra to `max`. `none` or a zero
  thinking budget disables thinking; positive token budgets are unsupported.
- Tools use `name`, `description`, and `input_schema`. `auto` supplies tools;
  `none` omits them. Forced selection is rejected instead of silently ignored;
  callers needing it can retain a Chat Completions endpoint.
- JSON-object output is prompted, not a native response-format guarantee.
  Provider capability reporting distinguishes this from Chat Completions.
- Signed thinking and tool blocks survive same-provider/model continuation.
  Switching wire protocols uses the ordinary transcript instead of replaying
  incompatible native envelopes. Stream completion requires `message_stop`
  and closed content blocks, including complete tool arguments.

### Workspace editor

DeepSeek main, deliberation, and evolution agents include the `deepseek` toolset
by default. Explicit toolset selections remain authoritative. The registered
`str_replace_editor` supports:

- `view`: numbered file content, optional one-based inclusive `view_range`
  (`-1` means through EOF), or non-hidden directory entries two levels deep.
- `create`: create a new file; an existing file is never overwritten.
- `str_replace`: replace exactly one occurrence of `old_str`; omit `new_str`
  to delete it. An explicitly null replacement is rejected.
- `insert`: insert `new_str` after `insert_line`; zero prepends. File style and
  transaction conflict checks are retained, including empty files.

Reads use granted workspace roots; writes stay within the primary workspace.
Absolute write paths are translated only within that root and then pass through
the same validated transaction planner as Sinomo's other editing tools.
Cancellation, file-write enablement, surface permissions, and evidence apply.
Unused null parameters are ignored. Truncated views are marked
`<response clipped>`. The combined descriptor conservatively requires both
read and write capabilities; read-only agents can use `read_file`/`list_files`.

Sinomo retains its own shell, jobs, delegation, and tool-discovery services.
This integration does not advertise the harness's persistent `bash`, PTC
`run_code`, account-login, or Files/image-upload services as implemented tools.


## Gemini, Grok, and Kimi configuration

Updated from official documentation on 2026-09-26. These are direct API-key
providers; subscription or coding-agent OAuth credentials are separate services.

| Provider | Default model | API root | API-key environment variables |
| --- | --- | --- | --- |
| `gemini` | `gemini-3.8-flash` | `https://generativelanguage.googleapis.com/v1beta` | `SINOMO_GEMINI_API_KEY`, `GEMINI_API_KEY`, `GOOGLE_API_KEY`, `GOOGLE_AI_API_KEY` |
| `grok` (`xai`, `x-ai`) | `grok-4.7` | `https://api.x.ai/v1` | `SINOMO_GROK_API_KEY`, `XAI_API_KEY`, `GROK_API_KEY` |
| `kimi` (`moonshot`, `moonshot-ai`) | `kimi-k3` | `https://api.moonshot.ai/v1` | `SINOMO_KIMI_API_KEY`, `MOONSHOT_API_KEY`, `KIMI_API_KEY` |

API keys, custom endpoints, and model options can also be configured per Sinomo
provider account. The providers appear in desktop settings and CLI completion.
`sinomo providers auth grok --account work` and
`sinomo providers auth kimi --account work` use the existing credential flow.
Authenticated model discovery uses the selected account's endpoint and key;
built-in model suggestions do not establish account entitlement.

## Gemini

The GenerateContent adapter retains thought signatures and function-call IDs
through native continuation. Gemini 3.8 remains the default; suggestions now
include 3.6 Flash, 3.5 Flash, and 3.1 Flash-Lite.

Thinking levels are model-specific. `minimal` is preserved on supporting Flash
models and maps to `low` on 3.8/3.7 Flash and Pro. The original 3 Pro maps medium
to high; 3.1 Pro supports medium. Positive legacy thinking budgets remain
accepted, but a request cannot combine a budget and a level. Gemini 3 and 2.5
Pro cannot disable thinking. Vendor overrides are checked against these rules.
See Google's [thinking guide](https://ai.google.dev/gemini-api/docs/generate-content/thinking)
and [model catalog](https://ai.google.dev/gemini-api/docs/models).

Blocked prompts and unsuccessful finish reasons, including malformed function
calls, produce provider errors in both synchronous and streaming requests.
`MAX_TOKENS` remains a valid terminal reason; it may contain truncated output.

## Grok

Grok uses `/responses`, JSON function tools, and provider/model-bound encrypted
reasoning replay. The request disables response storage and requests encrypted
reasoning. When a session ID is available it becomes `prompt_cache_key`.
Grok 4.7/4.6 have a 500,000-token context estimate.

Low/minimal maps to low, medium to medium, high to high, and xhigh/max/ultra to
xhigh on 4.7/4.6. For 4.5 the highest selection maps to high. Thinking-off is
rejected. Other model IDs remain usable without a reasoning-effort override;
unsupported effort overrides fail before sending. See
[Grok reasoning](https://docs.x.ai/developers/model-capabilities/text/reasoning)
and [Grok 4.7](https://docs.x.ai/developers/grok-4-7).

## Kimi

Kimi uses `/chat/completions`. Assistant messages retain `reasoning_content`,
tool calls, and other native replay fields. Streaming thought and answer text
remain separate, and interrupted streams cannot complete a turn successfully.

K3 uses `max_completion_tokens`, has a 1,000,000-token context estimate, and
always thinks. Minimal/low maps to low; medium/high/xhigh maps to high;
max/ultra maps to max. Sampling fields fixed by Kimi are omitted. K2.5/2.6 use
the thinking toggle and reject forced tool choices while thinking is enabled.
K2.7 Code and HighSpeed are offered as model suggestions and reject
thinking-off. K3 supports forced tool selection while thinking.

See [K3](https://platform.kimi.ai/docs/guide/kimi-k3-quickstart),
[K2.6](https://platform.kimi.ai/docs/guide/kimi-k2-6-quickstart), and
[K2.7 Code](https://platform.kimi.ai/docs/guide/kimi-k2-7-code-quickstart).

## Grok and Kimi tool boundaries

Grok and Kimi use flat JSON function declarations. Qualified or long runtime
tool names are deterministically mapped to API-compatible aliases, then restored
before dispatch. Native continuation retains the original wire names. OpenAI
custom/freeform declarations are not sent to these providers.

Sinomo owns tool execution and permissions. Hosted search, code execution,
image/video services, Kimi Formula tools, and vendor subscription clients are
not implicitly activated by selecting a model. This change covers the existing
text/reasoning/function-tool request boundary.

## Official references

- OpenAI: [models](https://developers.openai.com/api/docs/models),
  [GPT-6 compatibility](https://developers.openai.com/api/docs/guides/latest-model),
  [GPT-6.1 Sol](https://developers.openai.com/api/docs/models/gpt-6.1-sol),
  [GPT-6 Luna](https://developers.openai.com/api/docs/models/gpt-6-luna),
  [GPT-5.6](https://developers.openai.com/api/docs/guides/latest-model/gpt-5.6).
- Anthropic: [models](https://platform.claude.com/docs/en/models/overview),
  [Sonnet 5.5 changes](https://platform.claude.com/docs/en/models/sonnet-5-5/whats-new-sonnet-5-5),
  [Opus 5.5](https://platform.claude.com/docs/en/models/opus-5-5/overview),
  [thinking and block preservation](https://platform.claude.com/docs/en/build-with-claude/thinking),
  [effort](https://platform.claude.com/docs/en/build-with-claude/effort),
  [model pagination](https://platform.claude.com/docs/en/api/models/list).
- Google: [Gemini models](https://ai.google.dev/gemini-api/docs/models),
  [Gemini 3.8 Flash](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash),
  [GenerateContent thinking](https://ai.google.dev/gemini-api/docs/generate-content/thinking),
  [function calling](https://ai.google.dev/gemini-api/docs/generate-content/function-calling).
- DeepSeek: [models](https://api-docs.deepseek.com/quick_start/pricing),
  [thinking and tool continuation](https://api-docs.deepseek.com/guides/thinking_mode).
- NVIDIA: [model APIs](https://docs.api.nvidia.com/nim/reference/llm-apis),
  [Nemotron controls](https://docs.api.nvidia.com/nim/reference/nvidia-nemotron-3-super-120b-a12b).
- GitHub: [available models](https://docs.github.com/en/copilot/reference/ai-models/supported-models),
  [CLI options](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-command-reference).
- Local runtimes: [Ollama compatibility](https://docs.ollama.com/api/openai-compatibility),
  [LM Studio Responses](https://lmstudio.ai/docs/developer/openai-compat/responses).

## Verification

`crates/sinomo-lm/tests/provider_protocols.rs` uses loopback HTTP servers and
fake credentials to check native declarations, follow-up tool results, signed
state, model fencing, endpoint switching, qualified tool names, streaming
fragments, premature EOF, malformed responses, account routing, and catalog
pagination. Provider tests also cover model controls, forced selection, and
invalid tool history.

`cargo test -p sinomo-lm` additionally covers catalog parsing, protocol selection,
exact effort/tier values, custom calls and continuation, legacy requests,
streaming, cancellation, retries, and authentication refresh.

Agent tests cover active-turn propagation, snapshot compatibility, resume
projection, compaction, and the workspace patch grammar offered to Codex.
Workspace editor tests cover operations, ambiguity, absolute paths, write denial,
directory depth, and provider-specific exposure. Shared API and CLI tests cover
provider aliases, defaults, and account selection.

These checks do not establish live account entitlement or vendor-side acceptance
with production credentials.

```sh
cargo test -p sinomo-lm -p sinomo-agent -p sinomo-api
cargo test -p sinomo-core --test workspace_ring_boundaries
cargo clippy -p sinomo-lm -p sinomo-agent -p sinomo-cli -p sinomo-evolution --all-targets -- -D warnings
cargo fmt --all -- --check
```
