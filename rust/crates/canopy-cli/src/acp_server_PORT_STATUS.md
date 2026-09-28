# Native ACP server port status

ACP session construction loads hierarchical instructions once from the
workspace, with the configured runtime path applied before deriving Canopy
storage locations and the workspace trust flag copied from runtime settings.
The parent CLI helper combines those instructions with `--system`; the saved
base instruction is included in every ACP prompt alongside that prompt's
recalled-memory context. Path-conditional rules are registered from the same
startup result and attached to the ACP `WorkspaceTools` executor so its
generic post-tool hook can inject matching rules.

Per-prompt auto-memory recall now uses the OpenAI-compatible JSON selector with
a resolvable `fastModel` on the active OpenAI endpoint, then the session model.
Cross-auth and model-specific endpoint, credential, or custom-header routing
remain unsupported by this adapter. The ACP request cancellation signal
reaches the recall side query; selector errors retain the resolver's
deterministic fallback. The selector also receives up to 16 most-recent
distinct function-call names from bounded API history, deduplicated
case-insensitively, so active tool usage notes can be filtered during recall.

ACP does not automatically schedule extraction, dream, or skill review after
successful prompts, matching the TypeScript ACP session path. The explicit
`qwen/control/workspace/memory/dream` extension request works without an ACP
session. It checks managed-memory availability and bare mode, then runs the
native dream agent with project-memory path and permission guards, manual
trigger metadata, and chat-recording suppression. Its response contains the
summary, touched topics, and deduped-entry count expected by the ACP bridge.
The request honors JSON-RPC `$/cancelRequest`, has a 295-second child timeout,
and returns the TypeScript error codes, messages, and `errorKind` data shape.
When the private ACP parent capability is present along with managed-guard
markers valued `required-v1` and `attached-v1`, the request fails before
workspace setup with the TypeScript `-32602` unsupported-hidden-agent error.
The built-in guard without an attached external provider leaves manual dreams
available.

The ACP tool declaration includes `record_artifact`, with workspace permission
deny rules and core-tool enable/exclude settings checked at dispatch. Its
first-class artifact result is persisted by `SessionRecorder` with the session
transcript. ACP clients still do not receive a live artifact-list update from
this server.

The `ask_user_question` tool now asks the ACP client through
`session/request_permission`, marking the request with `qwenInteractionKind`
and `qwenQuestions` so the bridge renders its question UI. Submitted answers
are validated as a bounded map keyed by question index and passed to the shared
answer formatter. Selecting Cancel remains a declined answer; an ACP-cancelled
request, prompt cancellation, client error, or five-minute timeout remains a
distinct tool error. Question requests are capped at 64 KiB, with answers
limited to 8 KiB each and 32 KiB total.

The ACP executor sends connected-client permission requests for `edit_file`,
`write_file`, `notebook_edit`, `run_shell_command`, `artifact`, and `image_gen`
when configured rules return Ask or Default. Requests offer Allow once and
Reject; eligible requests also offer project- or user-scoped Allow always
choices. Those choices append rules to the selected scope's
`permissions.allow` array through the atomic JSONC settings updater and apply
the rule to the live ACP session. Project-scoped choices are available only in
trusted workspaces. Edit/write/notebook grants are path-scoped when the path
has no glob syntax; shell grants are offered only for a single simple command
without indirection or glob/control syntax. Artifact and image-generation
grants are tool-scoped. Explicit matching `permissions.ask` and `permissions.deny`
rules remain authoritative; unsupported path/command shapes retain Allow once
and Reject only. Shell request previews include the command, resolved working
directory, and purpose; edit requests include the bounded diff; artifact
requests show the publisher confirmation; image requests show model, size,
and prompt. Requests time out after five minutes and fail closed on client
errors, cancellation, malformed responses, persistence errors, or oversized
requests. Shell execution remains blocked when the Rust analyzer cannot safely
evaluate active path/domain deny rules or command-indirection deny rules. The
publishing `artifact` tool is available in ACP only when the existing artifact
setting and environment gates enable it; its configured local or remote
backend is used after approval. Computer Use and separate MCP authorization
flows retain their own permission surfaces.

ACP prompt text resolves `@ext:<name>`, `@mcp:<server>`, and
`@<server>:<uri>`. At session startup, it reads the QWEN_HOME-aware user
extension directory and global extension activation store through
`load_active_local_extension_references`, using the ACP workspace root and
trust state plus the process safe/bare-mode settings. Only validated active
local descriptors are retained; dormant, unsafe, or untrusted project-scoped
extensions are withheld. Inventory reads and diagnostics use the loader's
bounds, with ACP startup diagnostics capped and sanitized before writing to
stderr. ACP has no `--extensions` override, so its inventory uses the default
activation policy.

The TypeScript ACP path also calls `ExtensionManager.refreshCache()`, whose
full refresh reads the configured global user extension directory. The core
`loadExtensionsFromDir()` helper can load a supplied workspace's
`.canopy/extensions` directory, but it is not called by this active ACP path.
Native ACP currently matches the active path by exposing user extensions only;
project-local extensions are not included.

The same inventory supplies MCP server maps from active extensions to the ACP
session's normal MCP settings assembly. Configured settings and project
servers take precedence; active extension servers fill only unclaimed names;
ACP session-provided servers then override those entries. Extension manifests
are read-only inputs. Only active sources reach the MCP runtime; project-scoped
extensions require workspace trust, and safe/bare-mode policy is applied by
the inventory. Existing MCP approval, transport, and session shutdown handling
is used for the resulting servers. ACP has no separate extension activation
override.

The prompt resolver matches extension references against those active local
descriptors and matches MCP references against the current session's
configured server names, discovered resource/prompt registries, and
already-connected manager. Resolved content parts are appended after the
user's model-facing text part; the original display text remains unchanged.
Resource reads carry the ACP cancellation token and use the MCP client's
configured request timeout. Bounded resolver diagnostics are emitted as ACP
`agent_thought_chunk` session updates. The shared per-prompt reference limit
applies across extension and MCP references. Only text prompt blocks are
scanned for these references. ACP image blocks are forwarded as inline data.
Audio blocks are forwarded only when the selected provider and model modality
profile support audio: Gemini accepts WAV, MP3, AIFF, AAC, OGG, FLAC, MPEG,
M4A, L16, Opus, ALAW, MULAW, and WebM MIME types; OpenAI-compatible providers
accept WAV and MP3 when the modality profile enables audio. Anthropic has no
audio conversion path here, and models/providers without audio capability
reject the prompt with an ACP error rather than dropping the attachment.
Audio data must be valid standard base64 and is capped at 10 MiB decoded; ACP
cancellation is checked during validation and media-part construction. This
native path does not provide the TypeScript ACP voice-transcription bridge for
text-only models.

Plain-text `/stats` and its `/usage` alias are handled before memory recall,
reference expansion, and model execution. The bare form reports the live
session's API request, token, and tool totals from that `AgentRuntime`; its
text summary omits prompt count, elapsed time, generation, and file-change
fields that the native metrics collector does not currently provide. The
`model`, `tools`, `skills`, `daily`/`day`, `monthly`/`month`, and `export`
arguments use `stats_command::execute_with_session_metrics`, scoped to the
bound ACP session and workspace. Results and errors are returned as an
`agent_message_chunk`; output is sanitized and capped at 32 KiB, command
arguments at 8 KiB. Cancellation is checked before execution and before the
result event is sent. A synchronous usage query or export already in progress
cannot be interrupted; cancellation suppresses its response but cannot undo
an export that has already started. Slash-command results are not added to the
Rust transcript by this direct ACP path.

The ACP host still lacks live artifact-list updates, explicit extension-name
activation overrides, and the broader application wiring that is not
represented by this native session adapter. Extension MCP sourcing is limited
to the active global user-extension inventory, matching the active TypeScript
ACP refresh path.

The ACP stdio reader drains overlong frames without buffering them in full,
limits each line to 16 MiB, and applies backpressure with a two-line input
queue. At most 16 request tasks run concurrently; the server continues
reading cancellation requests while that limit is reached. These caps bound
transport and request fan-out, but do not cap memory used inside each active
model/tool turn.

Adaptive live-journal growth now uses one ACP-process memory budget. Native
host memory and a valid cgroup limit (when available) determine the effective
memory; the default budget is half of that amount, matching the TypeScript
serve default. The derived growth pool is 5% of the effective budget, capped
at 1 GiB and the remaining child-pool headroom. A single growth registry is
shared across all workspace runtimes so they draw against one aggregate pool.
The default per-session journal caps remain the baseline and grow only when a
live journal exceeds them. Growth stays disabled when memory probing fails or
the effective budget is below 1 GiB. The native probe currently supports
Linux and macOS; other platforms retain fixed-cap journals. On Linux, the
probe reads the standard cgroup v1/v2 paths and ignores limits below 64 MiB;
TypeScript uses Node's `process.constrainedMemory()`, so container layouts
outside those paths or smaller limits can fall back to a host-derived budget
in Rust. This ACP command has no flags to pin a memory budget or journal cap.
The budget only accounts for live-journal growth and does not limit model or
tool memory.

ACP provider credentials follow the TypeScript model resolver order: the
selected configured model's `envKey`, `--openai-api-key`, the provider's
standard environment variable (including the legacy `CANOPY_API_KEY` fallback
for OpenAI-compatible providers), then `settings.security.auth.apiKey`.
Environment lookup uses the effective environment, where process values take
precedence over dotenv and `settings.env`. The selected model's configured
`envKey` is found using its provider protocol and model ID; duplicate model IDs
prefer the configured entry matching the active CLI `--base-url`, then fall
back to the first same-ID match. Empty credential values are ignored. API keys
are not included in ACP errors or diagnostics. This adapter still does not
implement OAuth-based provider credentials.

The native ACP session now builds a `SkillManager` from the workspace,
QWEN_HOME-aware user skill roots, and only the extension skill configs returned
by the validated active-extension inventory. The inventory's existing trust,
safe-mode, and bare-mode gates therefore apply to extension skills. Project
and user skill discovery follows the TypeScript manager behavior and is not
disabled solely because the workspace is untrusted. `skills.disabledLevels`,
`skills.directories`, `skills.disabled`, `skills.defaultDisabled`, and
`skills.enabled` are applied when constructing the session snapshot; safe and
bare mode restrictions are preserved. The `skill` function is advertised
when at least one model-invocable skill is available, returns the skill body
with its absolute base directory, and includes active skills in a bounded
system reminder. Filesystem-tool path matches activate conditional skills and
add a one-turn availability reminder. ACP prompt and tool cancellation can
interrupt per-prompt enumeration, invocation, and path activation after
session startup has populated the manager cache.

The native ACP skill manager resolves bundled skills using the same precedence
as the CLI: `CANOPY_BUNDLED_SKILLS_DIR`, executable-adjacent `bundled/skills`,
then the repository's `packages/core/src/skills/bundled` source path. Safe mode
loads bundled skills only, and bare mode continues to load none. Packaged Rust
binary distributions still need to place the skill tree beside the executable
or set `CANOPY_BUNDLED_SKILLS_DIR`; the repository source fallback is only
available in a checkout. ACP also exposes discovered MCP prompts as
model-invocable commands in the skill reminder and advertises the `skill`
function when either a visible skill or MCP prompt is available. An active
skill takes precedence over a same-named prompt; disabled skills can fall back
to a same-named prompt, while path-gated skills keep the existing activation
error. MCP command arguments follow the TypeScript named/positional mapping
and required-argument checks. Invocation uses the session's connected MCP
client, propagates ACP cancellation, returns the first text content as
JSON-encoded text, and caps argument text at 16 KiB and the response text at
1 MiB. The model-facing MCP prompt listing is capped at 32 KiB.

Skill invocation now adds declared `allowedTools` as session-scoped allow
rules for workspace and MCP tools before returning the skill body, matching
the CLI loaders. Configured deny and ask rules retain precedence. The host
does not yet execute skill hooks, route the skill's `model` override, hydrate
extra files into the response, or track skill telemetry. The
`qwen/control/workspace/skills/refresh` extension now accepts `reason` values
`settings`, `content`, or `all` (default `all`). Settings refresh reloads the
workspace scope while retaining the session's original system, defaults, and
user layers, rebuilds the shared skill snapshot, and sends an
`available_commands_update` to each active session. Content refresh rebuilds
each distinct workspace/session skill-manager cache before publishing session
updates. The response reports `sessionsRefreshed`, `sessionsFailed`,
`configsRefreshed`, `configsFailed`, and `reason`; invalid reasons return
`-32602`. Safe/bare skill-level restrictions and the validated active-extension
inventory remain attached when the snapshot is rebuilt. Native ACP has no
separate workspace `Config` skill manager before its first session, so its
`configsRefreshed` count covers the workspace snapshot created with the first
session plus distinct active-session managers. Skill-manager cache refresh is
best effort and its current Rust API does not return listener or parse failures,
so `configsFailed` remains zero; per-session reload and notification failures
are counted independently. The notification lists native ACP's `/stats`
command, visible skills, and MCP prompts. Other TypeScript slash commands are
not yet represented in the native ACP session runtime.

After a successful `session/new`, `session/load`, `session/resume`, or
`unstable_resumeSession`, native ACP sends that session's
`available_commands_update` after flushing the JSON-RPC response. It selects
the active agent by the exact `sessionId` in the successful result, so clients
learn the session identity before receiving its command catalog. The snapshot
contains `/stats`, visible user-invocable skills, and MCP prompts discovered
by that session's MCP connection, with the same skill metadata as a workspace
refresh. Notification write failures are logged and do not turn a successful
session response into an RPC failure; explicit workspace refresh retains its
existing per-session failure reporting.

Native ACP `session/set_mode` now accepts the TypeScript approval modes
`plan`, `default`, `auto-edit`, `auto`, and `yolo`. New and restored session
responses include the current mode, all five mode descriptions, and a `mode`
select config option. A successful change is kept in session memory and emits
the ACP `current_mode_update` session update; it is not written to settings or
the transcript, matching `Session.setMode`. An untrusted workspace rejects
privileged `auto-edit`, `auto`, and `yolo` changes with the TypeScript trust
gate error shape. New sessions use the configured mode, default to `auto`, and
fall back to `default` in safe mode, bare mode, or an untrusted workspace.
For `session/new`, a valid `_meta["qwen.session.approvalMode"]` overrides the
workspace `/tools/approvalMode` value and is applied before the response, like
the TypeScript bridge's explicit per-session channel mode. Unknown or
non-string metadata values fail with `-32602` before session creation, using
the `Session.setMode` unknown-mode message. Safe and bare mode continue to
force `default`; privileged overrides in untrusted workspaces return the same
trust-gate error and the newly spawned session is closed. This initial override
is session-local and is not persisted.

The native ACP permission wrapper applies `yolo` to its supported approval
paths while still honoring permission denies, disabled tools, and the
`ask_user_question` exception. It applies `auto-edit` to file-edit approvals.
Plan mode blocks native file mutations, publishing, image generation, desktop
control, and MCP operations that need approval; read and planning tools remain
available. For shell commands, Plan mode uses the shared
directory-aware shell safety classifier and allows only commands classified
`ReadOnly`; `Write` and `Unknown` are denied. The resolved invocation working
directory is used for classification, cancellation interrupts classification,
and normal shell permission checks still apply to read-only commands. Full
TypeScript `auto` classifier behavior is not ported, so `auto` retains the
normal approval path. MCP tools with an explicit allow rule can still run in
Plan mode, matching the source permission flow's allow-before-mode behavior.
Todo plan revision and stop guard bookkeeping are not present in the native
ACP runtime.

ACP now declares and dispatches `zoom_image`, classifies its tool-call update
as a read, and permits it in Plan mode. Path-scoped Ask rules are relayed to the
client with the requested image path; Deny rules block execution. The image
tool's workspace restriction and memory/output caps are documented in the
core tool port status.

The native ACP command catalog now advertises `/doctor memory` with `--json`
and `--sample`, plus `/doctor rollback`. RSS sampling can be cancelled through
the active ACP prompt cancellation signal; response text is sanitized and
capped at 32 KiB. The command reports native RSS pressure and OS process
metrics. V8 heap snapshots and JavaScript-only counters remain unavailable in
the Rust runtime.

The ACP prompt path recognizes `/doctor rollback` and checks prompt cancellation
before replying. It returns the TypeScript ACP-mode error message
`Rollback is not available in ACP mode.` through `session/update`; rollback
remains available in the interactive CLI only, matching `doctorCommand.ts`.
