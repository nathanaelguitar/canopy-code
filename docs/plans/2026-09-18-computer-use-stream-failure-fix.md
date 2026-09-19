# Computer-use HTML, stream, and compression failure

**Date:** 2026-09-18
**Status:** Implemented locally — validation complete; provider benchmark pending
**Scope:** Canopy/Qwen Code plus the sibling `browser-control` checkout

## Purpose

This is an implementation brief for the agent fixing the failure shown in the
Canopy terminal during a browser-assisted research task. It separates three
behaviors that appeared together:

1. `browser_curl` returned a large webpage as raw HTML and Canopy displayed and
   retained it as model-visible text.
2. The main `qwen3.8-flash-next` request remained open without producing its
   first stream chunk until Canopy's 240-second inactivity guard aborted it.
3. `/compress` received a successful response from `glm-5.3-flash:cloud`, but
   the model spent the entire visible output budget on hidden reasoning, so
   Canopy received no compression summary and correctly left the history
   unchanged.

The fix should make this workflow bounded and recoverable. Do not treat this as
a Cua Driver or Apple screen-understanding failure; neither was on the failing
path.

## Executive diagnosis

The raw HTML is expected from the current `browser-control` implementation:
successful UTF-8 curl output is emitted directly as an MCP text block. Its
8 MiB limit is a transport limit, not a useful model-context or terminal-output
limit. A 172 KB HTML document therefore entered the conversation verbatim.

The red error is Canopy's own `StreamInactivityTimeoutError`. The provider
connection was open, but the model endpoint delivered zero chunks for
240 seconds. The HTML result is a strong trigger/amplifier candidate because it
inflates the prompt and increases upstream prefill/queue work, but the evidence
does not prove that it is the only cause. The Qwen endpoint must be tested with
and without the raw HTML.

The compression failure is independent. The compression request already sets
`thinkingConfig.includeThoughts` to `false`, but the OpenAI-compatible pipeline
does not currently emit GLM's provider-specific
`extra_body.thinking.enabled=false` shape for a non-Qwen model. The observed
GLM response used all 2,048 output tokens as thoughts and emitted no visible
summary, matching that gap.

## Evidence from the affected session

The local session was `655b44b7-0610-4dc1-a25b-9ab7d6cce45e`. Do not commit its
raw transcript: it contains private browsing output. The relevant telemetry
was inspected locally and reduced to the following facts:

| Event                | Evidence                                                                                                                                                                                 |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Browser result       | A successful SoFi `browser_curl` response contained `stdout_bytes: 172172` of raw HTML.                                                                                                  |
| Main-model failures  | `qwen3.8-flash-next` produced four `StreamInactivityTimeoutError` events, each after about 240 seconds and `0 chunks`; the screenshot corresponds to prompt suffix `########36`.         |
| Main-model successes | The same Qwen endpoint later succeeded with roughly 192k–197k input tokens and TTFT between about 4.7 and 13.1 seconds. This is not evidence of a deterministic context-window overflow. |
| Compression response | `glm-5.3-flash:cloud` returned HTTP 200 in 31.3 seconds with 233,053 input tokens, 2,048 output tokens, and `thoughts_token_count: 2048`.                                                |
| Compression result   | `/compress` reported `compressionStatus: 4`, with `originalTokenCount: 192238` and `newTokenCount: 192238`; no history reduction occurred.                                               |

## Relevant implementation points

### Stream watchdog

- [`pipeline.ts`](../../packages/core/src/core/openaiContentGenerator/pipeline.ts)
  defines `StreamInactivityTimeoutError`, wraps streamed requests with
  `withStreamGuards`, and reports the exact error shown in the terminal.
- [`constants.ts`](../../packages/core/src/core/openaiContentGenerator/constants.ts)
  sets `DEFAULT_STREAM_IDLE_TIMEOUT_MS` to `240000` and exposes
  `CANOPY_STREAM_IDLE_TIMEOUT_MS` as a deployment override.
- The guard is intentionally separate from the SDK request timeout: an HTTP
  stream can return successfully and then remain silent indefinitely.

### Compression

- [`chatCompressionService.ts`](../../packages/core/src/services/chatCompressionService.ts)
  runs compression as a streamed side query with one attempt and passes:

  ```ts
  config: {
    thinkingConfig: { includeThoughts: false },
    maxOutputTokens,
  }
  ```

- The same service returns `COMPRESSION_FAILED_EMPTY_SUMMARY` when the visible
  summary is empty. That safety behavior should remain; the fix is to prevent
  the provider from consuming the whole budget on hidden reasoning and to make
  the failure observable.
- [`pipeline.ts`](../../packages/core/src/core/openaiContentGenerator/pipeline.ts)
  documents GLM's `extra_body.thinking.enabled` wire shape, but the current
  disable branch is gated by `isCanopyFamilyWireModel`.
- [`modalityDefaults.ts`](../../packages/core/src/core/modalityDefaults.ts)
  currently classifies only model names beginning with `qwen` and the exact
  `coder-model` as that family. `glm-5.3-flash:cloud` therefore misses the
  existing provider-specific disable path.

### Browser output

In the sibling `/Users/nathanaelguitar/repos/browser-control` checkout:

- `src/cli/curl.rs` sets `MCP_RESPONSE_LIMIT` to 8 MiB.
- `execute_mcp` reads raw stdout up to that limit.
- `mcp_result` emits valid UTF-8 stdout directly as an MCP `text` block.
- The browser-control skill currently recommends `browser_curl` for large
  responses, but does not distinguish a large HTML page from an API response or
  download. The model consequently used curl for webpage navigation and put
  scripts, styles, and markup into the conversation.

## Work items for the fixing agent

### 1. Make HTML tool results model-safe

Choose the narrowest implementation that preserves explicit raw-download use
cases while preventing ordinary webpage HTML from becoming unbounded model
context.

Required behavior:

- For successful `text/html` responses, provide a bounded model-visible
  representation containing useful page text and basic metadata such as URL,
  status, title, and whether the result was truncated.
- Strip or omit scripts, styles, comments, and other non-content markup before
  creating the preview. Preserve headings, links, lists, tables, and visible
  form labels where practical.
- Keep the full response retrievable as an artifact/resource or through an
  explicit file-output path. Do not silently destroy data that a user asked to
  download or inspect.
- Use an existing shared tool-response budget/finalizer if one is available;
  do not create a second competing truncation policy. The model-visible cap
  must be materially smaller than 8 MiB and should be measured in both
  characters/bytes and tokens where the existing infrastructure supports it.
- Include a clear omission marker with the original byte count and retrieval
  method when the preview is bounded.
- Preserve current behavior for binary responses, JSON/API responses, curl
  `-o`/`--output`, nonzero exit codes, and explicit raw-file workflows.

The implementation may live in `browser-control`, in a Canopy MCP-result
adapter, or be split between both. Prefer a reusable response-normalization
boundary over a webpage-specific special case in the TUI.

Also update the browser-control guidance so that:

- `browser_snapshot`/semantic page primitives are preferred for ordinary web
  pages;
- `browser_fetch` is preferred for authenticated APIs;
- `browser_curl` is reserved for downloads, large API responses, or requests
  that genuinely need curl behavior;
- users can still explicitly request raw HTML or an unrestricted file output.

### 2. Fix provider-specific reasoning disable for GLM compression

Make `thinkingConfig.includeThoughts: false` an actual wire-level opt-out for
GLM models without changing the semantics of models that require reasoning.

Implementation requirements:

- Confirm the exact request shape accepted by the configured Ollama/OpenAI-
  compatible GLM endpoint, expected to be
  `extra_body: { thinking: { enabled: false } }` based on the existing pipeline
  comments and provider behavior.
- Apply the field only to recognized GLM wire models when reasoning is disabled;
  do not leak GLM fields to Qwen, DeepSeek, OpenAI, or unknown providers.
- Preserve the existing `thinkingMandatory` exception and the current Qwen
  DashScope/non-DashScope behavior.
- Add a request-capture unit test for `glm-*` with
  `includeThoughts: false`, plus regression tests proving that Qwen and
  mandatory-thinking models keep their existing wire shape.
- Add a compression-path test that returns a visible `<state_snapshot>` when
  thinking is disabled and verifies that compression status is `COMPRESSED`
  with a lower post-compression token count.
- Keep the empty-summary safety path. If a provider still returns only hidden
  reasoning, compression should fail fast to a visible NOOP with diagnostics,
  not hang or claim that the history was compressed.

Do not solve this only by increasing `maxOutputTokens`: that can hide the
provider mapping defect while increasing cost and context pressure.

### 3. Reproduce and bound the Qwen first-chunk stall

The stream watchdog must not be removed or globally disabled. Establish whether
the raw HTML is the dominant trigger before changing its default.

Run a controlled comparison against the same Qwen endpoint:

1. A representative prompt/history with the 172 KB raw HTML result.
2. The same prompt/history with the HTML replaced by the bounded page-text
   representation.
3. The same prompt/history with the browser result removed.

Record only non-sensitive diagnostics: wire model, input token count, tool
result byte/token totals, request duration, time-to-first-byte/chunk, chunk
count, and final error class. Check the Qwen server/proxy logs for queueing,
prefill, or upstream timeout evidence.

Possible outcomes:

- If bounded HTML removes the stall, keep the 240-second guard and fix the
  result boundary; this is the preferred outcome.
- If the endpoint legitimately needs more than four minutes before its first
  token even with bounded input, document and configure a deployment-specific
  idle timeout. Do not make `0` the default: an unbounded no-chunk request can
  hang a session forever.
- If the endpoint stalls independently of input size, fix or surface the
  provider/proxy problem rather than attributing it to browser-control.

Add or retain a deterministic unit test for a stream that yields zero chunks
and verify that it aborts at the configured idle limit, cancels the request,
and produces a concise recoverable error.

## Acceptance criteria

- A normal browser research task does not place a full HTML document containing
  scripts and styles into model history or flood the TUI with markup.
- A large HTML response remains retrievable when the user explicitly needs the
  source or download.
- The browser-control and Canopy tests cover HTML, JSON, binary, file-output,
  truncation, and nonzero-exit cases.
- A GLM compression request with reasoning disabled sends the correct provider
  field and returns visible compression XML/state data.
- `/compress` reduces the token count on the affected-session-shaped fixture,
  or reports a bounded, explicit NOOP if the provider cannot produce a valid
  summary.
- The no-first-chunk watchdog remains active, cancellable, and configurable;
  no global infinite timeout is introduced.
- The fix does not require Cua Driver, Apple Foundation Models, OCR, Jev, or
  full-screen capture. Those are separate perception/action architecture
  questions documented in
  [`2026-09-18-cua-apple-screen-understanding.md`](../research/2026-09-18-cua-apple-screen-understanding.md).

## Suggested verification matrix

| Scenario                                      | Expected result                                                          |
| --------------------------------------------- | ------------------------------------------------------------------------ |
| `browser_snapshot` on a normal page           | Semantic, bounded observation; no raw scripts/styles in history.         |
| `browser_curl` on a 172 KB HTML page          | Bounded readable preview plus metadata and explicit retrieval path.      |
| `browser_curl` on JSON/API output             | Existing useful response semantics, subject to the shared output budget. |
| `browser_curl` on binary data                 | Resource/file behavior unchanged.                                        |
| `curl -o <path>`                              | Full file output remains unrestricted by the MCP text preview cap.       |
| GLM compression with `includeThoughts: false` | Wire request disables thinking; visible state snapshot is returned.      |
| GLM compression returning only thoughts       | Safe empty-summary NOOP with diagnostic; no false compression success.   |
| Qwen stream yields zero chunks                | Configured watchdog aborts and cancels cleanly.                          |
| Qwen stream with bounded browser result       | Compare first-chunk latency against the raw-HTML baseline.               |

## Non-goals

- Replacing Cua Driver with screenshots, OCR, Apple Foundation Models, or Jev.
- Making full-screen capture or a new computer-use provider part of this fix.
- Removing the stream watchdog or setting its default to unlimited.
- Logging raw webpage contents, screenshots, prompts, cookies, or private
  session transcripts as diagnostics.
- Treating the 240-second timeout as proof that the provider is out of context;
  the captured successful requests were below the configured Qwen context
  window.

## Implementation update (2026-09-18)

The low-risk fixes in this brief are now implemented locally:

- `browser-control` projects ordinary successful HTML MCP responses into a
  32 KiB readable summary with URL, status, content type, title, visible text,
  links, and an explicit `-o <path>` retrieval instruction. JSON, binary,
  nonzero, and explicit raw-output workflows remain unchanged.
- Compression cache-sharing now sets `thinkingConfig.includeThoughts=false`
  for non-Anthropic providers, and the OpenAI-compatible pipeline emits
  GLM's nested `thinking.enabled=false` wire field for recognized `glm-*`
  models. Mandatory-thinking models are still preserved.
- The Qwen no-first-chunk watchdog remains unchanged and covered by the
  existing deterministic timeout tests; no global unlimited timeout was
  introduced.

Validation completed locally: the focused Canopy pipeline/modality and
compression suites passed, Canopy core typecheck/build passed, and the full
browser-control Rust suite passed (296 unit tests, 9 integration tests, 24 MCP
tests, and 2 start tests). A live provider comparison of raw versus bounded
HTML is still pending and is intentionally not claimed here.

## Implementation update (2026-09-19): silent large-prompt timeout recovery

The latest screenshot showed that bounded `browser_eval` text still reached the
same failure path: the main request produced zero chunks for 240 seconds and
then raised `StreamInactivityTimeoutError`. The two IRS fetches returning 404
were noisy tool errors, but they were not themselves the stream failure.

Canopy now handles a pre-first-chunk `ETIMEDOUT` specially when the estimated
prompt is within 32,000 tokens of the normal auto-compaction threshold:

1. It skips replaying the unchanged large request.
2. It invokes the existing forced reactive-compression path.
3. On successful compression it rebuilds the request history, emits the normal
   `COMPRESSED` and `RETRY` events, and sends the smaller request once.
4. Ordinary transport errors, small prompts, mid-stream output, exact-route
   requests, and the existing watchdog behavior are unchanged.

The regression test is
`GeminiChat > compacts a large prompt after a silent ETIMEDOUT before replaying
it` in `packages/core/src/core/geminiChat.test.ts`. The full GeminiChat suite
passes 319/319, the core typecheck passes, and the core plus CLI artifacts were
rebuilt. The already-running CLI process loaded the previous JavaScript at
startup, so restart that Canopy session before testing the new behavior.
