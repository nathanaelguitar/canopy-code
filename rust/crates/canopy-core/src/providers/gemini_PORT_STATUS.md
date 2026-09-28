# Native Gemini provider port status

`providers::gemini` adds a native Rust adapter for Google's Gemini
`GenerateContent` and `streamGenerateContent` endpoints. The core runtime
selects it through `AgentRuntime::new_gemini`; the existing
`AgentRuntime::new` OpenAI-compatible default and `new_anthropic` path remain
unchanged.

The adapter sends the runtime's Gemini-shaped conversation and tool
declarations, maps configured sampling parameters and reasoning effort to
Gemini generation config, applies the SDK's temperature/top-p/top-k defaults,
and removes unsupported `displayName` media fields. Unsupported audio/video
parts returned by tools are changed to explanatory text as in the TypeScript
Gemini content generator. Native response objects from bounded SSE events flow
directly into the existing `Turn` converter, preserving candidate text,
thoughts, tool calls, finish reasons, citations, and usage metadata.

Request bodies, response bodies, error bodies, and individual SSE events have
limits. Requests support cancellation and retry setup through the shared
provider retry policy; stream reads have idle and total-lifetime bounds and
observe their cancellation token. API keys are sent using `x-goog-api-key`,
custom headers are supported, and configured credential values are redacted
from HTTP error bodies.

## Remaining host and parity work

- Native `canopy run` and ACP expose `--provider gemini`, resolve a selected
  model from settings or `GEMINI_MODEL`, use that model's `baseUrl` and
  `envKey`, accept an explicit `--base-url`, and construct
  `GeminiProviderConfig` for `AgentRuntime::new_gemini`. Model-scoped
  `generationConfig` applies sampling, reasoning, request headers, timeout,
  retry, context-window, modality, and tool-result settings to the runtime.
  Token counting and auxiliary model call sites remain unported.
- This adapter covers API-key Gemini access, not Vertex AI ADC/service-account
  authentication. Hosts may provide a compatible custom endpoint and headers,
  but native Google Cloud credential refresh is not implemented.
- Token counting, embeddings, Gemini SDK telemetry, and Google SDK response
  convenience methods are not part of the Rust runtime slice. Provider error
  retries use Canopy's shared retry defaults rather than Gemini SDK-specific
  retry settings.
- The source content generator also contains richer multimodal handling and
  API-version/model-specific behavior. The native adapter currently preserves
  Gemini JSON parts and only ports the supported-media normalization for
  function-response parts.

No tests were added or run for this slice.
