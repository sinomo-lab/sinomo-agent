# Gemini, Grok, and Kimi compatibility

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

## Tools, boundaries, and verification

Grok and Kimi use flat JSON function declarations. Qualified or long runtime
tool names are deterministically mapped to API-compatible aliases, then restored
before dispatch. Native continuation retains the original wire names. OpenAI
custom/freeform declarations are not sent to these providers.

Sinomo owns tool execution and permissions. Hosted search, code execution,
image/video services, Kimi Formula tools, and vendor subscription clients are
not implicitly activated by selecting a model. This change covers the existing
text/reasoning/function-tool request boundary.

Loopback fixtures cover native tool declarations, streaming, signed continuation,
model fencing, qualified names, reasoning controls, account discovery/routing,
and Gemini failure responses. Shared API and CLI tests cover provider aliases,
defaults, and account selection. No production credentials are needed for these
tests; they do not constitute live vendor acceptance.
