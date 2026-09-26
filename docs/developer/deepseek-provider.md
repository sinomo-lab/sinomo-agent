# DeepSeek provider and tools

Compared against the local `deepseek-harness` checkout at `477b4f4205`
(2026-09-24), particularly its `llm-deepseek` serializer/transport and
`tool-str-replace-editor` implementation. Reference:
[DeepSeek Harness source](https://github.com/deepseek-ai/deepseek-harness/tree/477b4f4205/packages/llm/llm-deepseek).

## Transport selection

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

## Messages contract

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

## Workspace editor

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

## Verification

Loopback protocol tests use fake keys and exercise signed tool continuation,
fragmented arguments, endpoint switching, forced selection, invalid history,
and incomplete streams. Workspace tests cover editor operations, ambiguity,
absolute paths, write denial, directory depth, and provider-specific exposure.
These tests do not establish production API entitlement or live acceptance.
