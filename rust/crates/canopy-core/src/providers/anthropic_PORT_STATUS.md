# Anthropic Messages API port status

`anthropic.rs` adds a native Rust adapter for Anthropic's `/v1/messages`
endpoint. It is exported as `providers::anthropic`. The adapter is usable as a
provider component. `AgentRuntime::new_anthropic` now selects it for primary
turns, and the Rust ACP host exposes that path with
`--provider anthropic`. The existing `AgentRuntime::new` path remains
OpenAI-compatible, which is also the default for ACP and `canopy run`.
Interactive and resumed `canopy run` sessions now share an explicit provider
entry point for either OpenAI-compatible or Anthropic turns.

The implemented slice covers request building from Gemini-shaped messages,
system text, image/PDF inputs, tool declarations and results, schema
conversion, tool-choice `ANY`, sampling limits, model-gated thinking/effort,
cache breakpoints, direct API-key versus compatible-gateway Bearer headers,
bounded HTTP/SSE transport, cancellation, error-body credential redaction,
retry setup, and conversion of complete/streaming responses into Gemini-shaped
chunks. Streaming handles Anthropic text, thinking/signature, tool-input JSON,
usage, stop reasons, provider error events, and the empty-stream non-streaming
probe. The stream adapter can feed the existing `Turn` response boundary.

Native ACP and `canopy run` resolve the selected `settings.model.name` against
merged `modelProviders` settings. They pair the selection with
`settings.model.baseUrl` when available, resolve custom provider IDs through
`providerProtocol`, and use a matching model's `baseUrl` and `envKey`. Settings
can select the native OpenAI-compatible, Anthropic, or Gemini path when
`--provider` is absent. Explicit `--provider` pins the adapter; `--model`
overrides the saved model, and `--base-url` remains the endpoint override. A
saved model name is considered before the matching provider's model environment
variable. Provider endpoint and key fallbacks retain the provider-specific
environment variables, with `security.auth.baseUrl` and `security.auth.apiKey`
as later fallbacks.

## Remaining parity gaps

- Settings-based routing only selects the native OpenAI-compatible,
  Anthropic, and Gemini adapters. Vertex AI and the Canopy/ChatGPT OAuth paths
  do not yet have equivalent native runtime adapters.
- Both hosts now resolve model-scoped `generationConfig` field by field,
  preferring the selected provider's values over `model.generationConfig`.
  Sampling, reasoning, retry settings, custom headers, extra request bodies,
  cache controls, schema mode, context-window limits, modality gates, media
  splitting, and tool-result format flow into the native runtime. Provider
  metadata/UI setup, live provider-setting reloads, and diagnostics remain
  outside this slice.
- The request converter now merges adjacent assistant turns, removes orphaned
  tool calls/results and duplicate result IDs, keeps unresolved tool calls at
  the end of history, and removes now-untrusted thinking blocks when an entire
  tool-call turn is orphaned. DeepSeek-compatible hosts/models normalize
  missing thinking signatures and add the empty thinking block required on
  tool-use turns when thinking is enabled; stale thinking is stripped when it
  is disabled. Adaptive-thinking models drop unsigned thinking on compatible
  proxies, reject unsigned thinking in a still-active tool-use chain, and
  repair trailing assistant prefill by dropping empty turns or appending a
  synthetic `Continue.` user turn.
- The active-tool-chain unsigned-thinking error currently maps to the shared
  provider `InvalidRequestShape` error, so it does not include the TypeScript
  generator's detailed proxy-signature guidance.
- Prompt cache behavior uses per-session breakpoints. The source also supports
  global cache scope, static-system-prefix splitting, and per-anchor retention
  overrides; those are not represented here.
- Images and PDFs in tool results are converted, but other response metadata
  and provider telemetry content are not carried across. Native token counting
  through the source tokenizer is not exposed by this adapter.
- Retries use Canopy's shared retry loop and the native model-generation
  settings described above; Anthropic SDK-specific retry behavior is not part
  of this implementation.

`cargo check -p canopy-cli --locked` passes. `rustfmt --edition 2024 --check`
passes for the runtime and host entrypoint files. The workspace-wide format
check currently reports differences in the unrelated audio-capture Node addon.
Tests were not run for this slice.
