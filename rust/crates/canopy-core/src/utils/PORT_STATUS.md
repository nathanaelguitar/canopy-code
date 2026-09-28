# Utility port status

`part_utils.rs` ports `packages/core/src/utils/partUtils.ts` for JSON-backed
provider parts: display rendering, first-candidate text extraction, sequential
async text transforms, and text prepend/append operations. It intentionally
retains provider-specific and unknown JSON fields. Presence checks, nullish
text handling, thought truthiness, and the `null` versus empty visible response
distinction are covered by focused tests.

`message_inspectors.rs` ports the function-call/response message predicates;
empty part lists are false and every part must carry a truthy matching field.
`encoding.rs` ports the UTF-8/ASCII encoding-label normalization predicate.
`formatters.rs` ports binary memory-size display, including one-decimal rounded
unit selection and JavaScript-compatible `toFixed` behavior at ties.

`binary_content.rs` ports MIME classification, extension selection,
Content-Disposition and URL filename handling, magic-byte sniffing, the
bounded UTF-8 text heuristic, private binary persistence, and byte-size
formatting. `cron_parser.rs` ports the five-field parser, Vixie day matching,
and bounded next-fire search, including ECMAScript whitespace behavior.
`cron_display.rs` formats only the common step patterns whose human-readable
interval labels are truthful; other inputs remain verbatim.
`context_length_error.rs` collects nested provider error text and classifies
context-window overflow, with source-compatible timeout veto and token-count
extraction. `terminal_safe.rs` strips terminal/display controls and normalizes
bounded notification labels. Native background monitor/shell labels, resource
reference display, ACP display text, and Qoder transcript conversion use it.
TUI result rendering and session-title formatting retain local sanitizers.
`safe_json_parse.rs` tries strict JSON before repairing common model-output
syntax errors, with a caller-provided fallback. `tool_result_cleanup.rs`
removes stale tool-result artifacts and legacy `.output` files while skipping
symlinks and preserving source-compatible error counts.
`safe_json_stringify.rs` serializes JSON-compatible values with compact or
pretty formatting; reference cycles are unrepresentable at its typed JSON
boundary.
`error_parsing.rs` formats structured, string, and API-JSON errors with
provider-specific rate-limit guidance and idempotent wrapper detection.
`internal_prompt_ids.rs` recognizes background prompt IDs and the
`side-query:` family; `runtime_model_prefix.rs` strips valid nested
`$runtime|` model prefixes while retaining malformed values.
`osc8.rs` sanitizes OSC 8 hyperlink envelopes and detects terminal support
with the source's opt-out, TTY, terminal-version, and multiplexer precedence.
`request_tokenizer.rs` ports text, image, audio, and provider-part estimation,
including UTF-16 text counting, image dimensions for PNG, JPEG, WebP, GIF, BMP,
TIFF, and HEIC, token scaling, and malformed-image fallbacks. Its JSON request
boundary accepts the Google GenAI contents shape. It counts text incrementally
to avoid joined-history copies and caps temporary base64 decoding at 64 MiB.
The shared text estimate now serves native OpenAI reasoning-token accounting
and PDF output guards; the full request estimator has no runtime caller yet.

`read_text_range.rs` provides bounded line-number and byte-cursor reads,
handle-bound positional reads, encoding metadata, and cancellation checks.
`bare_mode.rs` ports the `QWEN_CODE_SIMPLE` truth-token and CLI-flag policy
with injected environment access. `ReadFileTool` uses the handle-bound reader
for UTF-8-compatible text, retaining its bounded decoder fallback for
BOM-marked UTF-16/32 and legacy encodings. Native CLI and ACP skill/extension
setup consume the bare-mode helper.
`safe_mode.rs` reuses the same truth-token parser for
`CANOPY_CODE_SAFE_MODE`; native CLI and ACP both use it to configure skill,
extension, and memory behavior.
`runtime_status.rs` writes and validates the snake_case session sidecar through
the shared atomic writer, with cancellable reads and best-effort removal. The
native CLI and ACP publish the sidecar, and session catalog/reference paths
consume its work-directory metadata.
`folder_structure.rs` ports the recursive breadth-first directory tree with
ignore integration, file-name filtering, the combined item cap, and truncation
markers. It is distinct from the direct-entry `list_directory` tool and is used
for directory `@` mentions by `AtFileProcessor`.
`sanitize_child_env.rs` removes only Canopy's three internal secret keys and
is used by shell and stdio MCP child-process paths. Third-party credentials
remain available to user commands.
`shell_pager_env.rs` selects `cat` on non-Windows platforms, clears inherited
pager variables when no pager is available or an empty value is requested,
and sets `GIT_PAGER` only when requested.
`startup_event_sink.rs` provides thread-safe event sink registration and
clearing, no-op dispatch when unset, and panic-isolated callbacks whose failures
go through an injected logger. The registry is instance-scoped and has no
native startup-profiler caller yet.

The Rust boundary uses `serde_json::Value`, matching existing provider and turn
representations in this crate. The source SDK's compile-time `Part` unions and
JavaScript `undefined` cannot be represented as a standalone JSON value; APIs
use `Option<&Value>` where top-level absence affects output, and JSON object
field absence remains distinct from a present `null` field. `flat_map_text_parts`
accepts a JSON value because its nominal source contract excludes null/undefined.
These utility modules are exported from `utils/mod.rs`. `binary_content.rs` is
used by the Rust web-fetch response processor to classify and persist binary
responses. The cron parser feeds `cron_scheduler_primitives`, but the
channel-loop scheduler does not yet use those primitives; `cron_display.rs`
has no runtime caller.

Public utility modules are exported from `utils/mod.rs`; the child-environment
sanitizer is declared crate-private. The last native utility test run selected
188 matching core tests before error formatting, prompt IDs, runtime model
prefixes, bounded text-range reading, bare/safe-mode policy, runtime status,
and folder-structure reading were integrated. The newer module tests and
recent parity regressions still need consolidated native validation.
