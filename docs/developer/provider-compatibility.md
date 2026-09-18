# Language-model provider compatibility

Reviewed against the providers' official documentation on 2026-09-18. This
extends the [Codex comparison](codex-provider.md) to the other language-model
adapters. Hosted media interfaces retain their existing APIs.

## Provider coverage

| Provider | Discovery and defaults | Request and response handling |
| --- | --- | --- |
| OpenAI | Authenticated `/models`; suggestions include Astra and GPT-5.6; default remains Sol | Responses preserves message roles, custom tool input, encrypted reasoning and native calls. Astra uses the documented effort values and omits unsupported sampling controls. |
| Anthropic / Claude subscription | Paginated Models API; default `claude-sonnet-5`; current Claude 5 families use a 1M context limit | Native tool declarations, tool results, streamed JSON arguments and signed thinking blocks. Both credential modes share the Messages codec. |
| Gemini | Paginated Models API, filtered to `generateContent` models; default `gemini-3.8-flash` | Native function calls/results, thought signatures, function IDs and thinking controls. Thought text stays separate from answer text; output usage includes thought tokens. |
| DeepSeek | Authenticated Models API; default `deepseek-flash`; V4 Pro remains available | Thinking is retained through tool turns, including earlier `reasoning_content`. Required/named tool choices enforce the non-thinking API constraint. |
| NVIDIA | Authenticated `/models`; default Nemotron 3 Super retained | Shared Chat Completions codec carries native tools and results. Third-party IDs use their actual organization prefix. Vendor options reach the request. |
| GitHub Copilot | Account catalog retained; direct default is Sol | GPT-5.5, GPT-5.6 and Astra use Responses. Both API dialects retain tools and continuation state. The CLI forwards configured reasoning effort. |
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

- OpenAI Astra/GPT-5.6 send `max` without downgrading it to `xhigh`.
- Current Claude models use adaptive thinking. Older models retain explicit
  thinking budgets. Incompatible adaptive-budget or forced-tool/thinking
  combinations fail with an explicit error.
- Gemini 3 maps effort to `low`, `medium`, or `high`. Disabling its thinking
  returns an error; Gemini 2.5 retains the budget-based interface.
- DeepSeek maps minimal/low to `low`, medium/high/xhigh to `high`, and max/ultra
  to `max`. `none` disables thinking. Tool calls no longer disable it implicitly.
- NVIDIA's model-specific controls use vendor options, for example
  `chat_template_kwargs.enable_thinking=false` for Nemotron.
- Copilot CLI receives `--reasoning-effort`; external CLI tools still require
  the existing node-local permission before execution.

## Official references

- OpenAI: [models](https://developers.openai.com/api/docs/models),
  [Astra compatibility](https://developers.openai.com/api/docs/guides/latest-model),
  [GPT-5.6](https://developers.openai.com/api/docs/guides/latest-model/gpt-5.6).
- Anthropic: [models](https://platform.claude.com/docs/en/models/overview),
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
fake credentials to check declarations, follow-up tool results, signed state,
model changes, streaming fragments, premature EOF, account routing and catalog
pagination. Agent tests cover active-turn propagation, snapshot compatibility,
resume projection and compaction. These checks do not establish live account
entitlement or vendor-side acceptance with production credentials.

```sh
cargo test -p sinomo-lm -p sinomo-agent -p sinomo-app-contract
cargo test -p sinomo-core --test workspace_ring_boundaries
cargo clippy -p sinomo-lm -p sinomo-agent -p sinomo-cli -p sinomo-evolution --all-targets -- -D warnings
cargo fmt --all -- --check
```
