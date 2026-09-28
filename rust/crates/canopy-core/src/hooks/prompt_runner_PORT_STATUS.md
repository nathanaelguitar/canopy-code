# Prompt Hook Runner Port Status

`prompt_runner.rs` ports the request policy in
`packages/core/src/hooks/promptHookRunner.ts`: prompt argument substitution,
model override selection, reasoning-aware request options, timeout and
cancellation outcomes, first-candidate projection, thought-part filtering,
JSON/code-fence response validation, and the source's fail-open parse policy.

`prompt_provider.rs` implements the provider executor for the native
OpenAI-compatible, Anthropic, and Gemini transports. It maps the neutral hook
request to Canopy's Gemini-shaped request, uses the existing provider request
and response converters, applies the 500-token cap and reasoning opt-out, and
passes cancellation through model resolution and network requests. OpenAI
compatible requests also retain provider-profile response parsing and the
provider-specific reasoning-disable controls used by the TypeScript pipeline.
Anthropic exposes a cancellation-aware complete helper that applies the same
prompt-hook sampling and reasoning settings.

The native CLI host is in `canopy-cli/src/hook_host.rs`. It loads user hooks
(`userHooks`, falling back to legacy `hooks`), project hooks only for trusted
workspaces (`projectHooks`, falling back to legacy `hooks`), and hooks from
active extension manifests constrained to their canonical extension roots.
Bare/safe mode and `disableAllHooks` bypass hook execution. The host wires
command, async command, HTTP, function, and prompt runners into
`NativeDispatchExecutor` and `HookSystem`. `UserPromptSubmit` runs before
interactive prompts, fresh `canopy run --prompt` requests, and clean resumed
explicit prompts. A CLI executor decorator now runs `PreToolUse` before each
tool call and `PostToolUse` after successful calls, including resumed-turn
continuation. It also runs `PostToolUseFailure` for tool errors and interrupted
calls. The decorator forwards cancellation, concurrency, file-history, and
commit-attribution behavior to the original composed executor.

The CLI resolver routes the current model, `inherit`, `fast`, supported
`authType:model` selectors, and bare configured model IDs (preferring the
active provider). It respects `providerProtocol`, model base URLs and
generation settings, the active CLI proxy, custom API-key environment names,
provider environment credentials, and the current protocol's configured auth
key. The prompt path blocks on `decision: block`/`deny` or `continue: false`;
allowed `additionalContext` is escaped and appended as a reserved transcript
context part. Stats commands and internal history continuation do not fire
this user hook. Tool pre hooks block on `permissionDecision: deny`, base
`decision: block`/`deny`, or `continue: false`. A PreToolUse `ask` now opens a
serialized one-time terminal confirmation prompt; non-terminal input and a
declined prompt fail closed. Approval proceeds with the original tool arguments
and does not rerun the hook chain. Post hooks can stop on `decision: block`/`deny`
or `continue: false`; successful PostToolUse context is escaped and appended by
the core runtime after output finalization, and validated artifacts are added
to the tool result. PostToolUseFailure context is escaped and appended to the
tool error. Hook event inputs use the canonical tool name and original provider
call ID. The current CLI permission mode is supplied as `default`.

PreToolUse `hookSpecificOutput.tool_input` follows the TypeScript dispatcher
semantics: each hook can update the input seen by later hooks in that same
event chain, while the already requested tool arguments stay unchanged.
PermissionRequest hooks now run in the interactive terminal confirmation path
for tools whose permission policy requires a prompt. A hook deny blocks
execution and returns its message. An allow with `updatedInput` reparses and
rechecks the modified arguments, rebuilds the confirmation preview, and still
requires explicit user approval before execution. The modified arguments are
also supplied to PostToolUse/PostToolUseFailure. An allow without modified
input continues through the ordinary confirmation flow. Invalid updated input
fails closed. Without an interactive terminal, the hook cannot approve a
request; existing permission handling rejects requests that need confirmation.
PermissionRequest forwards the executor's cancellation token to the hook runner
when one is exposed. The TypeScript scheduler passes no permission suggestions
for this event, so the native host also supplies none. TypeScript's scheduler
does not consume the returned `interrupt` flag or `updatedPermissions` field;
the native path likewise does not apply either one.

Remaining host gaps: the native CLI executor currently exposes no active
prompt-cancellation token, so PermissionRequest receives none in ordinary CLI
runs. PermissionRequest denials, permission policy rejections, hook
cancellation, declined confirmations, and other native tool errors before the
handler starts now skip PostToolUseFailure. Errors from delegated MCP/tool
adapters can still be reported before a remote operation starts because those
adapters do not expose a start boundary to the CLI wrapper. The synchronous
terminal confirmation read itself cannot be interrupted by a cancellation
token. Native tool hook block/stop outcomes are represented through the
executor's ordinary `Err(String)` result, so they do not retain TypeScript's structured
denied status; the `AgentToolExecutor` error shape also cannot carry artifacts
from a stopped PostToolUse or a PostToolUseFailure event. PostToolBatch and the
other supported-but-unwired hook events still have no native CLI lifecycle call
sites. Tool lifecycle hooks currently use the native CLI's fixed `default`
permission mode. Function hook definitions are dropped by the native loader
because the CLI does not currently supply runtime callback resolution. The
native ACP host does not yet wire these CLI hooks.

The prompt-submit hook still uses a host-created cancellation token rather
than a token connected to the CLI's external Ctrl-C/session cancellation
signal. PreToolUse and PostToolUse hooks also do not receive the optional
executor token. PermissionRequest does, when an executor supplies one, but the
native CLI tool executor currently supplies none.
The CLI run provider currently supports OpenAI-compatible, Anthropic, and
Gemini routes, so Canopy OAuth, ChatGPT OAuth, and Vertex AI model overrides
cannot be resolved by this host. The TypeScript OpenAI pipeline also learns
models that reject disabled reasoning and retries with reasoning enabled;
that dynamic retry state, hook interaction/usage telemetry, and structured
blocked-event reporting are not yet wired here.

`rustfmt --edition 2024 rust/crates/canopy-cli/src/hook_host.rs rust/crates/canopy-cli/src/main.rs`
and `cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked --offline`
pass. The check reports three unrelated warnings in `canopy-cli/src/mcp_host.rs`.
No tests were added or run for this slice.
