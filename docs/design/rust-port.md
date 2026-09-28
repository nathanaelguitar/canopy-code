# Canopy Code Rust port

> **Direction update (2026-09-27): paused.** The repo-wide Rust rewrite is no
> longer the active product goal. Canopy will build on Codex as its agent
> runtime, preserving CanopyChat remote control and reusing selected Rust work
> only where it solves a Canopy-specific need. See
> [`codex-canopy-integration.md`](./codex-canopy-integration.md). The progress
> notes below are a historical implementation record. Their source-line
> ratios measure code volume, not feature completion or parity; they do not
> imply that the shipped CLI has been ported or that the reported memory crash
> has been fixed.

## Objective

Move the complete Canopy Code implementation to Rust while preserving the
behavior and persisted data contracts of the current repository. This covers
the runtime and every shipped surface, not only the interactive CLI:

- packages/core: provider clients, agent/tool runtime, session state,
  permissions, memory, MCP, hooks, extensions, telemetry, and configuration.
- packages/cli: interactive and non-interactive modes, commands, daemon,
  local HTTP APIs, ACP, session management, and terminal UI.
- packages/acp-bridge, packages/mobile-mcp, packages/channels,
  packages/audio-capture, and packages/cua-driver: protocols, messaging,
  native input, and platform integrations.
- packages/desktop, packages/desktop-shell, packages/webui,
  packages/web-shell, packages/web-templates, packages/chrome-extension,
  packages/vscode-ide-companion, and packages/zed-extension: all shipped
  desktop, browser, and editor surfaces.
- packages/sdk-typescript, packages/sdk-python, packages/sdk-java, and
  integrations/external-context: client APIs and integrations.
- Build, packaging, installation, and release code needed to ship those
  surfaces.

The current implementation stays available as the behavior oracle until the
Rust replacement passes package-level parity and the end-to-end acceptance
gates. A module is not considered ported just because a Rust file with the
same name exists.

## Port rules

1. Preserve serialized formats and public protocols unless a versioned
   migration is added.
2. Port behavior, edge cases, errors, limits, and recovery semantics alongside
   each implementation. Porting only the happy path does not count.
3. Keep each old module paired with its Rust replacement and parity evidence.
4. Persist session events before acknowledging them, bound every in-memory
   queue and model-visible payload, and make resume after process termination
   a first-class path.
5. Keep the macOS executable native. Browser/editor integration may use
   generated WebAssembly glue, but application logic and handwritten source
   move to Rust as part of this objective.

## Framework and license evaluation

Upstream status checked 2026-09-25. The repository and Rust workspace use
Apache-2.0, so evaluating MIT crates does not require changing Canopy's
declared license. Current release tags and main-branch changes are called out
separately where they differ.

No upstream base has been selected. The MIT-licensed candidates cover
different layers, and none removes the need to port Canopy-specific behavior:

- [Rig](https://github.com/0xPlaygrounds/rig) is MIT-licensed and separates
  provider/model contracts in `rig-core` from the `rig-agent` runtime. The
  latest tagged release is [0.42.0](https://github.com/0xPlaygrounds/rig/releases)
  (2026-08-17); it exposes a serializable `AgentRun`, but its released API has
  no `Agent::resume` entry point. Upstream main has moved since that release:
  PR [#2443](https://github.com/0xPlaygrounds/rig/pull/2443) merged on
  2026-09-12 with `Agent::resume` (now in the
  [main API](https://github.com/0xPlaygrounds/rig/blob/main/crates/rig-agent/src/agent/completion.rs)),
  an effect bus, and an ECS checkpoint runtime. That newer runtime is not in
  the tagged release, and its ECS crate
  remains unpublished. Its persistence contract leaves external side-effect
  idempotency and reconciliation to the host; a pending external write without
  a recorded response may run again after restoration. Rig is a reasonable
  MIT candidate for a pinned, isolated runtime spike, while Canopy retains its
  own durable tool-effect policy.
- [rust-genai](https://github.com/jeremychone/rust-genai) is dual
  MIT/Apache-2.0 and provides provider clients, normalized content, and
  streaming. At this check 0.6 is the current stable release; the
  [published versions](https://crates.io/crates/genai/versions) include
  0.7.0-beta.24 (2026-09-23), while main has advanced to beta.25-WIP. It does
  not provide Canopy's agent loop, tool lifecycle, session store, or app
  surfaces. It is a good fit for replacing provider transport while keeping
  Canopy's own runtime and durability contracts.
- [thClaws](https://github.com/thClaws/thClaws) is dual MIT/Apache-2.0 and is
  the closest existing Rust-backed product: one engine serves desktop, CLI,
  headless, and web modes, with providers, MCP, memory, hooks, permissions, and
  JSONL session resume. Its GUI is React/Vite, so it is not a line-by-line
  Rust port of every surface. The [v0.138.0 release](https://github.com/thClaws/thClaws/releases/tag/v0.138.0)
  is dated 2026-09-25 and provides Apple Silicon, Intel, and universal macOS
  builds. Its [JSONL append path](https://github.com/thClaws/thClaws/blob/main/crates/core/src/session.rs)
  uses an OS advisory cross-process lock, but source review found no
  external-effect intent/result journal or idempotency
  guarantee. It is useful for product-code comparison; session resume alone
  does not establish safe recovery from a crash during a tool call.
- [Agent Code](https://github.com/avala-ai/agent-code) is MIT-licensed and
  offers an embeddable Rust engine plus CLI with a fullscreen TUI, headless
  mode, HTTP API, ACP, MCP, permissions, and an evaluation crate. Its README
  advertises session persist/resume/fork/rewind/compact, but the public docs
  do not promise recovery for a process killed during a tool effect. The
  latest release is [v0.30.0](https://github.com/avala-ai/agent-code/releases/tag/v0.30.0)
  (2026-07-29); the repository remained active through 2026-09-23, with 58
  commits since that release. Session storage uses a per-session cross-process
  lock and atomic temp-file replacement, but normally saves at process exit or
  session switch rather than after each tool effect. Its desktop/web client is
  Flutter. It is the most useful full coding-agent product source to inspect,
  though it is a narrower product than Canopy.
- [Pi's upstream harness](https://github.com/earendil-works/pi) is MIT but is
  implemented in TypeScript; its current repository includes a separate
  `pi-durable` package for durable conversation/task/document workflows. The
  [nktkt Rust port](https://github.com/nktkt/pi) provides provider, agent, and
  CLI crates with per-turn session persistence and resume, but its latest
  [release](https://github.com/nktkt/pi/releases/tag/v1.2.0) was 2026-05-12 and
  the repository has had no commits since. Its [session code](https://github.com/nktkt/pi/blob/main/crates/pi-coding-agent/src/session.rs)
  rewrites JSON directly with `std::fs::write`, rather than atomically
  replacing it. The README lists the TUI and web UI as unported and reports
  ten offline tests.
  It is a useful porting reference, but lacks the upstream durable runtime and
  all shipped surfaces.
- A separate, much larger [Pi Rust implementation](https://github.com/Dicklesworthstone/pi_agent_rust)
  is a native coding-agent product with streaming, many tools, providers,
  sessions, and extensions. Its license is titled “MIT License (with
  OpenAI/Anthropic Rider)” and the rider removes rights for OpenAI, Anthropic,
  their affiliates, and parties acting for or benefiting them. That is not a
  clean MIT option and needs license review before any use. Its README also
  currently marks its performance budgets as unmeasured, so its Rust design
  claims are not evidence that it meets Canopy's macOS reliability gates.
- [Tau](https://github.com/tau-agent/tau) is MIT and documents a long-running
  server/TUI split, Unix-socket reattachment, plugin tools, and task worktrees.
  Its [latest release](https://github.com/tau-agent/tau/releases/tag/tau-agent-v0.1.1)
  is 0.1.1 (2026-05-05), with no later repository changes found and no macOS
  CI. Its [SQLite database](https://github.com/tau-agent/tau/blob/main/crates/tau-agent-lib/src/db.rs)
  uses WAL and writes messages as they arrive, with a per-session server lock.
  On startup, its [server state](https://github.com/tau-agent/tau/blob/main/crates/tau-agent-lib/src/server/state.rs)
  warns about persisted non-idle sessions and does not restore their
  interrupted agent loops.
  Reattachment helps when a UI disconnects but does not provide process-crash
  recovery, so Tau is an architecture reference rather than a proven Canopy
  base.
- [Codex](https://github.com/openai/codex) is Apache-2.0, like this repository,
  and remains a valid option given the maintainer's familiarity. Its
  `codex-core` targets Codex's Rust UIs and documents a macOS
  `/usr/bin/sandbox-exec` assumption, so reuse would require adapting its
  workspace and platform contracts.

Ratatui is an MIT TUI toolkit; Candle and ORT are inference runtimes. Those are
useful components, but they do not replace the agent/session runtime. The
practical MIT shortlist has distinct roles: use `rust-genai` for provider
transport while Canopy owns orchestration, evaluate Rig in a pinned runtime
spike, and inspect Agent Code or thClaws as broader product-code references.
No candidate reviewed here meets Canopy's recovery requirement for a process
terminated during an external tool effect. Retaining Canopy's own write-ahead
effect journal and idempotency policy appears safer than adopting a product
runtime wholesale. For Rig, test restart during a provider request and each
kind of external tool effect before considering its newer main-branch runtime.
Codex is not MIT, but its Apache-2.0 license matches Canopy's; its core is built
for Codex's Rust UIs and has a documented macOS `sandbox-exec` environment
assumption, so familiarity helps but does not remove adaptation work. The
current Rust implementation keeps provider conversion and bounded transport
separate from the runtime choice.

## macOS crash report findings

The screenshot shows a native Node/V8 stack. A report captured on 2026-09-24
at 00:12 local is consistent with that failure class, though it is later than
the screenshot timestamp and cannot be tied to that exact process. Its arm64
Node process aborted with `SIGABRT` from V8's fatal out-of-memory handler. The
Canopy fatal report identifies the command as
`packages/cli/dist/index.js` under Node 22.14.0. It measured 4.21 GB of live JS
heap against a 4.35 GB limit, 4.47 GB resident memory, and about 88 MB available
on the 16 GB Mac. V8's old space alone held 4.13 GB with only 29 KB available.
The launch environment had `QWEN_CODE_NO_RELAUNCH=true`, which disables the
CLI's startup path for adjusting the V8 heap limit; the report cannot show
whether a different limit would have avoided the failure, and available
system memory was already low. The native stack reaches
`v8_inspector::V8Console::runTask`, but neither report has a JavaScript
function name or heap snapshot to identify the allocation site.

Canopy's local process metadata links that PID to a long-lived CLI session.
Daemon logs show its SSE client disconnecting at the OOM after 18,738 event
frames, three backpressure events, and no slow-write warnings. The daemon
completed the pending prompt about five minutes later, so the server process
survived the client failure. The retained session transcript is about 1.64 MB;
that file alone does not account for the heap size. These logs narrow the
failure to the Node CLI's live session, but do not prove which retained object
or operation caused the growth. Three currently retained Canopy fatal reports
from 2026-09-22 through 2026-09-24 show the same Node CLI heap-exhaustion event.
The top-level `DiagnosticReports` directory contained no later Node/Canopy
`.ips` report when checked on 2026-09-25.

A separate `CanopyChat-2026-09-18-223902.ips` report is not the same failure:
its stack aborts in `ggml_metal_rsets_free` from the app's bundled
`llama.framework`. No available report ties that native teardown failure to
the later Node OOMs. No V8 heap profile was found in the inspected CanopyChat
log directory.

## Migration order

1. Rust workspace and canonical transcript/session contracts.
2. Provider adapters, context construction/compaction, tool execution,
   permissions, hooks, and the main agent loop.
3. Durable session store, daemon, ACP/MCP surfaces, and process lifecycle.
4. CLI commands and interactive terminal UI.
5. Channels, browser/computer control, voice, and native integrations.
6. Desktop, browser, editor surfaces, and SDKs.
7. Build/release cutover; remove the old runtime only after parity gates pass.

This order establishes the memory and recovery invariants before replacing
frontends. It does not reduce the final port scope.

## Acceptance gates

- Every shipped feature and public interface has a Rust implementation and a
  parity check against the existing behavior.
- Existing sessions, settings, extensions, MCP servers, and tools remain
  readable or have a tested migration.
- Repeated long-running tool sessions plateau within documented memory
  budgets; oversized events and tool results are rejected or summarized
  without retaining unbounded copies.
- Killing the process during a turn does not corrupt the journal; the session
  can resume without duplicating completed tool side effects.
- Native Apple Silicon and Intel macOS builds launch, stream, execute tools,
  recover a session, and complete a repeated long-session stress workload
  without an OOM abort.

## Current Rust workspace status

`canopy-core::agent_runtime` now acquires the shared native sleep inhibitor
while a prompt is active. The macOS, Linux, and Windows helpers mirror the
current `SleepInhibitor` commands and release by RAII on normal completion,
cancellation, or errors. Native CLI and ACP load the existing
`general.preventSystemSleep` setting through their settings scopes and pass it
to the runtime, defaulting to `true`; other hosts can override the runtime
builder. Spawn failures and unsupported platforms remain best-effort no-ops.

`canopy-core::memory` ports the auto-memory paths, scaffold persistence,
managed-entry text contracts, topic scanning, index generation, prompt
construction, status aggregation, managed-memory forgetting, model-backed
recall/forget selectors, write refresh, pending skill staging, team-memory
secret protection, and Git shareability checks from `packages/core/src/memory`.
It also ports strict channel-memory document parsing and legacy migration from
`channel-memory-document.ts`. The path
context accepts injected runtime/base directories, local mode, and project
scope while also providing a process adapter backed by Canopy `Storage` and the
corresponding environment keys. It retains git-root/workspace partitioning,
worktree roots, private/user/team scopes, the team-write exclusion, trusted
anchors, symlink-aware retention checks, metadata and extraction-cursor JSON
shapes, exclusive create-if-missing semantics, ENOENT-only missing reads,
UTF-8 replacement behavior, legacy entry parsing, CRLF frontmatter, scan caps,
unreadable-file isolation, safe index-field rendering, addressable path links,
team-description grouping, atomic private index writes, and rejection of
redirected team-memory roots. The prompt builder covers full/condensed protocol
text, tier routing, and JavaScript-compatible UTF-16 index truncation; sample
outputs were compared byte for byte. Deterministic recall ranking, active-tool
usage suppression, memory-age/freshness rendering, model selection request
validation, prompt formatting, resolver exclusions, model ordering, and
optional telemetry are implemented and covered at the service boundary. The
native `run` CLI and ACP runtime now resolve matching project and user memories
for each new prompt and append them after the user system instruction. If
scanning fails, the run continues without recall. These callers currently use
the core model selector through the OpenAI-compatible side-query adapter, using
`fastModel` when it resolves on the active endpoint and credential route, then
the session model. Selector errors fall back to deterministic ranking. ACP
cancellation reaches the side query. The callers do not yet pass recent-tool
exclusions or recall telemetry, and CLI resumes without a new prompt skip
recall.
Workspace/global context writes now
use per-path locking, a 30-second lock deadline, a 16 MiB existing-file cap,
section-aware append handling, UTF-16 offsets, and commit guards. Channel-memory
documents retain strict duplicate-key/property validation and legacy hashing;
the filesystem store adds safe channel/thread paths, deterministic migration,
stable revisions, duplicate and secret checks, compare-and-swap updates,
bounded locking, and atomic writes. The remember path builds managed-memory
prompts and policies, validates touched scopes, and rebuilds affected indexes
through an injected agent runtime boundary. Actual agent execution and
host-enforced permissions remain runtime responsibilities.
Private filename tie-breaking uses a stable common-English approximation where
Node uses ICU `localeCompare`; exact collation for other locales is not
reproduced. The forget selector ports prompt/schema validation, main-model
injection, cancellation, heuristic fallback, and persisted removal/index
updates. Status aggregation uses an injected task-listing interface implemented
by the new task manager. Write refresh classifies successful
file writes into private project/user memory, rebuilds affected indexes, and
invokes runtime callbacks. Secret scanning preserves ordered rule labels and
never returns secret text; the team-write guard checks paths before scanning.
Git shareability checks probe both the index and a representative topic file.
Pending skills move only new direct-child skills into task-specific staging
directories. The team-sync port reconciles with `pull --ff-only` before
committing only the team-memory path, then pushes a single explicit refspec
only when its own commit is safe to publish; it is not yet wired into session
startup. Extraction now ports cursor handling, history repair, scoped agent
requests, touched-topic attribution, metadata, index rebuild, and refresh
sequencing; dream ports trigger and cancellation gates, metadata, transcript
prompt construction, and its protected-memory policy. Skill review ports
prompting, history repair, scope decisions, archive reservation, and skill
discovery. `/learn` ports video classification and skill/video request
construction, while context filename selection preserves the process-wide
settings contract. `manager.rs` now owns in-process task records, subscriptions,
queued extraction, dream scheduling/cancellation, skill review, and pending
skill resolution through an injected runtime. The host still needs to create
this manager and supply agent executors. `memory_scoped_agent_config.rs` now
provides the scoped policy, but the host still needs to bind its permission and
shell-parser adapters into the agent tool loop and wire native callers; recall
resolution remains a separate slice.

The Rust workspace is an in-progress implementation, not a replacement
executable yet. The `canopy` binary has native `--help`, `--version`, `--acp`,
`run`, `sessions list` (including `--archived`), `sessions archive`,
`sessions unarchive`, guarded `sessions delete --yes`, `--resume`, and read-only
`recovery-check` modes; the
diagnostic command is capped at a 32 MiB transcript. ACP serves
newline-delimited JSON-RPC and
supports initialization, new/load/resume/prompt/cancel/close, durable session
restore, and paged `session/list` summaries with persisted worktree metadata.
Organized `session/list` also supports all, pinned, ungrouped, and valid custom
group views, including pin/group/color metadata from the v1 organization store
and cursors bound to the active filters. It merges live sessions on the first
page and reports when its 10,000-file scan cap truncates results. ACP
`session/set_mode` updates the session-local approval mode and publishes a
`current_mode_update`; MCP-over-HTTP remains unavailable. `run` streams
OpenAI-compatible responses and records a
durable session. Provider response bodies are capped at 16 MiB,
individual SSE events at 1 MiB, model-turn output at 8 MiB, accumulated API
history at 12 MiB, and each tool result at 50 MiB (64 MiB per batch). A clean
session can resume with a new prompt;
interrupted prompts and tool turns require confirmation and continue from the
recovered provider history. Native `run` and ACP resolve model-scoped
`generationConfig` field by field, with the selected provider entry taking
precedence over `model.generationConfig`. They apply sampling, reasoning,
custom headers, extra request bodies, cache controls, schema mode, context
window, input modalities, media splitting, tool-result format, retry limits,
retry delays, and retry error codes. `CANOPY_CODE_API_TIMEOUT_MS` overrides
settings unless the selected provider explicitly supplies `timeout`; zero or
negative timeout values use the source-compatible 2,147,483,647 ms disabled
timeout sentinel. Provider transports default to the source's 120-second
request timeout. Retry overrides currently cover request setup failures; Rust
does not yet replay a stream after an in-band rate-limit event. It registers
four read-only, workspace-only tools
(`read_file`, directory listing, glob search, and grep), plus workspace-only
`notebook_edit`, `edit_file`, and `write_file` mutations. It also supports
managed background shell processes with `task_list` and `task_stop`; shutdown
cancels owned shells and records terminal status after process teardown and
output drain. These task controls are shell-only and are not exposed over ACP.
Mutation tools show a complete diff and require explicit CLI approval unless
an applicable user or trusted-workspace allow rule applies; deny rules block
execution.
Existing files must have an unchanged prior read, with notebooks requiring a
full structured read. Writes
use a synced temporary file with atomic replacement, and changes whose full
diff cannot fit in the approval prompt are refused. `run_shell_command`
supports bounded foreground commands with a timeout and the same user and
trusted-workspace command rules. `todo_write`
validates task IDs and dependency graphs, keeps per-session plan JSON, and
returns a separate display payload that is saved outside provider history.
The agent runtime updates bounded active-todo reminder context from that
display payload. An injectable hook runtime validates created todos before
completed todos, skips persistence when validation blocks, then runs
post-write hooks sequentially in source order; post-write failures retain the
successful write result and add a reminder. Interactive editor flow remains
unported. `ask_user_question` validates one to four questions and
supports terminal single-choice, multi-choice, and custom answers with
bounded input. ACP now asks the connected client through
`session/request_permission`; answers are validated and size-capped, and
client cancellation and timeout remain distinct errors. The fullscreen Rust
TUI still uses its line-based question prompt rather than an inline question
card. PTY interaction, background tasks, live output updates, the rest of
Canopy's tools and permission flows, the interactive agent UI, and the
remaining shipped CLI commands also remain unported. The CLI now loads
system-default, user, trusted-workspace, and system settings through the Rust
config loader, including migrations, JSONC parsing, trust gating, warnings,
environment-file activation, and `$VAR`/`${VAR}` resolution. Its effective
environment snapshot supplies provider credentials and shell commands without
mutating process-global environment state. It also reads legacy
`tools.allowed`, `tools.exclude`, and `tools.core` settings. Workspace settings
are skipped for an explicitly untrusted folder. Rust now reads
`trustedFolders.json` as JSONC
and applies the deepest matching rule,
`TRUST_PARENT` behavior, canonical symlink aliases, and untrusted tie
precedence. IDE RPC ingestion, full tool registry parity, settings writes,
and shell virtual-operation analysis remain unported. The Rust CLI filters
its currently registered core-tool subset with legacy `tools.core` and
`tools.exclude` rules.
The new `session_catalog.rs` ports the active and archived `listSessions`
metadata contract: workspace filtering, worktree-root matching, first-prompt
and creation metadata, title/source recovery from bounded head/tail reads,
exclusive mtime cursors, and JSON Lines plus human-readable `sessions list`
output. It retains only the newest 10,001 candidate paths per scan, reads at
most ten records per transcript, and caps runtime-status sidecars at 64 KiB.
Runtime-output-directory settings and the settings-selected runtime root are
applied before listing.
`canopy sessions ps [--json]` now reads the cross-process live-session
registry, emits the source JSON Lines shape, and sanitizes terminal table
fields with source-compatible age buckets and display-width truncation. A
native `canopy run` now registers its session for the duration of execution
and removes only its own process record on normal exit. Registration remains
best-effort, and the full interactive UI lifecycle is still unported.
`services/session_lifecycle.rs` now ports single and bulk removal, archive and
unarchive, and linked custom-title records, with project/worktree ownership
checks, symlink-resistant transcript access, sidecar cleanup, and best-effort
usage salvage. The CLI exposes `sessions archive` and `sessions unarchive`,
including JSON summaries, and refuses to archive sessions reported live by
the shared process registry. `sessions delete --yes` requires explicit
confirmation, refuses IDs reported live, and removes linked organization
entries. Bulk removals currently run sequentially rather than concurrently.
`session_organization_store.rs` ports the v1 group, pin, color, snapshot, and
removal store with bounded reads and private atomic writes. The Rust CLI adds
group list/create/rename/color/delete and session pin/unpin/color commands;
group deletion requires `--yes`. These commands map the service/API contract
because the TypeScript CLI has no organization subcommands. Group assignment
and ordering are not exposed by this CLI slice.
`services/session_registry.rs` ports the shared live-session index and process
liveness checks: validated schema-1 records, captured registration paths,
private atomic writes, Linux boot/start-time and PID-namespace isolation,
stale-record rechecks, and temp-file cleanup. The source has only `kill(pid, 0)`
liveness on macOS, so PID reuse cannot be distinguished there; Rust preserves
that limitation. Windows liveness is not implemented, the Rust entry points
use synchronous filesystem operations, debug-log wording is absent, and the
shared atomic writer lacks the source's foreign-UID in-place fallback.
The `mobile-mcp` Rust workspace member provides bounded stdio and single-client
SSE MCP transports, the 26 standard tool schemas plus three opt-in remote tools,
device helpers, and bounded subprocess execution. Full Android/iOS/simulator
driver behavior, package-relative binary lookup, logging, and telemetry remain
incomplete; the Android actions now include orientation, process/app helpers,
and direct UI hierarchy dumps for connected ADB devices. See
`rust/crates/mobile-mcp/PORT_STATUS.md` for per-source gaps.
The `canopy-audio-capture` member wraps the vendored miniaudio header through a
small C++ device-lifecycle boundary and exposes bounded PCM/WAV capture,
silence detection, waveform level, and macOS authorization status. Its six
tests and native build passed on Apple Silicon, but no microphone, permission
prompt, Intel Mac, Linux, or Windows runtime was exercised; the Node addon and
package-install integration remain in place. Details are in
`rust/crates/canopy-audio-capture/PORT_STATUS.md`.
The Rust CLI TUI now links the capture crate for Space-key hold/tap dictation,
uses SoX and Linux `arecord` fallbacks, and sends completed WAV input to the
configured Qwen3-ASR Flash batch endpoint. It also supports trusted keyterms
and best-effort fast-model transcript cleanup through the existing
OpenAI-compatible client. Qwen/DashScope realtime ASR, non-TUI voice clients,
hardware capture behavior, and cross-platform permission behavior remain
unvalidated; see the same status note.
The ACP bridge now separately bounds its in-flight turn-compaction accumulator
at the baseline 10,000-event/8 MiB limits, even if adaptive live-journal caps
grow. When reconnect replay loses earlier turn events it emits a
`history_truncated` marker; live SSE delivery is unchanged and the full
transcript remains authoritative. This mitigates a second unbounded retained
copy found during review, but it does not establish the allocation source of
the foreground Node/V8 OOM described above.
The exported Rust `channels` module now contains the base package's sanitizers,
DM/sender/group authorization gates, persistent pairing state, workspace path
helpers, JSONL group history, and deterministic memory intent/recall selection.
It also has bounded webhook prompt and display-text construction, while webhook
receipt and execution remain outside the port.
`ObservedChannelContactStore` validates and bounds the versioned user/group/topic
registry and builds freshness-filtered contact graphs. The native Weixin host
now records authorized direct-message senders in the workspace-keyed registry;
the shared `hash_daemon_workspace` helper matches the TypeScript daemon key.
The Rust host does not yet expose the workspace HTTP read route, and this Weixin
message source has no names or group/topic labels to hydrate.
`channel_loop_store.rs` persists loop definitions with validated reads,
target-based listing and caps, and same-directory atomic updates.
`channel_loop_tools.rs` ports the
three tool schemas and JSON-RPC request behavior behind a runtime-neutral
handler trait; the daemon transport registration remains unported.
`channel_loop_scheduler.rs` adds due-loop dispatch, bounded concurrency,
recovery/failure handling, and startup reconciliation against the persisted
loop store. Cron interpretation and channel-runner execution remain injected.
`block_streamer.rs` ports progressive splitting, idle emission, ordered
delivery, and flush/stop behavior with UTF-16-aware thresholds. It uses Tokio
and rounds a split that would bisect a JavaScript surrogate pair to a whole
Rust scalar value.
`proactive_delivery_error.rs` provides the typed permanent/transient delivery
error and source chain; adapter production/handling remains to be connected.
`session_router.rs` ports scope-aware route keys, in-flight coalescing,
eager/lazy session recovery, route invalidation and death handling, bridge
replacement, and insertion-ordered private persistence. The bridge event
subscription and daemon lifecycle adapter are not connected yet.
`channel_polling.rs` adds the reusable adapter polling loop, cursor restore and
atomic save, single-loop lifecycle, interruptible stop, and capped exponential
backoff. Concrete platform pollers remain to be ported.
`channels/inbound_commands.rs` adds adapter-neutral `/help`, `/new` (with
`/clear` and `/reset` aliases), `/cancel`, `/status`, `/who`, `/approve`,
`/approve-always`, and `/deny` handling behind host callbacks. `/who` preserves
shared-session authorization, basename-only workspace display, and scope
wording. Permission replies preserve pending-request visibility, option
selection, and stale/failure responses. Native Weixin and Telegram hosts wire
the shared command set after authorization; the QQ host currently wires
permission commands only. ACP `session/request_user_input` delivery remains a
separate host gap. The GitHub/GitLab mention parsers and DingTalk/Feishu
Markdown render helpers are also ported as isolated utilities; their concrete
API adapters remain.
`feishu_media.rs` adds ID validation, timeout and bearer handling, bounded
streamed downloads, MIME fallback, and awaited cancellation behind a transport
seam; concrete Feishu client wiring remains.
`dingtalk_media.rs` ports download-code resolution followed by a bounded,
streamed file download with awaited cancellation through the same kind of
injectable seam.
`dingtalk_outbound_image.rs` adds image-marker handling, canonical path and
file-content validation, and token-redacted multipart upload without enabling
an additional HTTP dependency feature.
`weixin_accounts.rs` ports credential loading and private atomic replacement,
including cleanup of orphaned temporary credentials.
`weixin_api.rs` adds iLink request contracts, retry/cancellation behavior,
upload URL selection, and strict CDN validation with injectable HTTP, timing,
and random-byte seams.
`weixin_types.rs` contains the optional snake_case message/media wire models
and protocol constants; the API wrapper keeps raw JSON values to preserve the
source's runtime type assertions.
`weixin_login.rs` ports QR start and polling, expiry refresh, request aborts,
and the eight-minute default deadline through injected transport and clock
seams.
`feishu_question_controller.rs` adds reservation and lifecycle handling, action
claims, deferred responses, timeout and cancellation, and serialized terminal
card projection. Adapter context and Card API callbacks remain a local seam.
The DingTalk interactive-card type/default/callback helpers and Feishu
question-card builder/parser are also ported. QQ Bot token/gateway HTTP,
protocol/config types, routing-state persistence, and outbound delivery policy
are now separate Rust helpers; the QQChannel caller, gateway runtime, and QR
login integration remain. Weixin monitoring, text/image sending, AES media
handling, and outbound marker orchestration are integrated; adapter lifecycle
and context-token wiring remain.
`qqbot_message_projection.rs` ports group-message prompt/display projection,
including per-group bot OPENID extraction intents and the source's UTF-16
mention-tag boundary. `qqbot_cron.rs` ports cron-flow gating, chunk buffering,
route-aware sends, and bounded transient retries behind adapter hooks.
`weixin_typing.rs` ports the shared ticket cache and typing lifecycle with a
reconnect generation guard. Native QQ and Weixin CLI hosts now wire those
platform helpers into their direct message paths; QQ group media and several
adapter services, plus daemon worker integration, remain open.
`telegram.rs` ports the Telegram Bot API and long-poll seams, message/entity and
reply projection, photo/document/voice downloads, command menu and `/start`,
typing state, topic-aware response/proactive routing, HTML formatting/splitting,
and rejected-HTML fallback. Its native handler must still connect the shared
ChannelBase authorization, `/help`/`/new`/`/cancel`/`/status` command path,
SessionRouter, lifecycle subscriptions, configuration, and signal handlers.
Long-poll cursors remain in memory and temporary media needs a host retention
policy. See `telegram_PORT_STATUS.md` for the complete adapter gap list.
`channel_prompt.rs` ports the security-sensitive inbound prompt projection:
speaker and mentioned-member attribution, quoted replies, attachment paths,
inline image selection, metadata sanitization, and bounded display text. The
main ChannelBase routing, command, and dispatch state machine remains unported.
Pairing writes are atomic and private on Unix, workspace-scoped approvals do
not cross workspace boundaries, and storage errors propagate through the gate
API. Memory recall uses full NFKC normalization. The pairing mutation lock is
process-local, group history reloads the full JSONL file per operation, and
both areas still need cross-process concurrency review. Channel adapters,
webhook receipt/execution, daemon transports, and end-to-end application wiring
remain to be ported. See
`rust/crates/canopy-core/src/channels/PORT_STATUS.md` and the adjacent per-slice
status notes for exact coverage and compatibility limits.
The exported channel modules pass 449 focused tests after integrating QQ Bot
group-message projection, cron text buffering, and Weixin typing lifecycle on
top of the earlier helper slices. A previous full locked offline workspace run
passed 2,202 tests: 2,155 core, 25 CLI, 9 audio-capture, and 13 mobile-MCP, on
Apple Silicon macOS; that run predates the latest CLI, audio, mobile, extension,
and memory-diagnostic changes, which have not had tests run. The latest locked
workspace compile check passes. After adding native CLI and ACP memory-recall
wiring, the focused runtime suites pass, and local mock-provider requests confirmed that a
matching project memory reaches both system prompts. The previous
mock-provider stress run completed 80 tool calls across 81 requests in 2.17 s;
it wrote a 146,260-byte transcript, sent 2,806,049 request bytes in total, and
sampled at 24,816 KiB peak RSS. The run checked that recalled project memory
was present in each provider request. These are short synthetic CLI checks,
not a long soak or the shipping Node process. These results do not establish
parity for unported adapters and product surfaces.
Three Node fatal reports dated September 22–24 record `Allocation failed -
JavaScript heap out of memory` on Node v22.14.0, arm64 macOS. At each failure,
V8 had used 4.17–4.21 GB of a 4.345 GB heap limit; old space was nearly full,
and system available memory was 83–109 MiB. The matching macOS report,
`node-2026-09-24-001220.ips`, ends in `node::OOMErrorHandler`,
`V8::FatalProcessOutOfMemory`, and `SIGABRT`. No heap snapshot was captured,
and the reports have no JavaScript stack, so they establish repeated heap
exhaustion but do not identify the retaining allocation. The screenshot
filename is timestamped 23:15 on September 23, while the macOS report says
00:12 on September 24; I cannot tie that report to the screenshot as the same
incident. The transcript linked to the PID 51490 report is 1.64 MB and contains
412 browser-control calls, but no inline image payloads or `computer_use__`
calls. A separate OOM-linked session for PID 1205 contains 18 browser
screenshots and about 4.26 million base64 characters (roughly 3.2 MB decoded),
also with no `computer_use__` calls. These transcript sizes do not explain
multi-gigabyte heap use. They make raw CUA screenshot payloads an unlikely main
cause of the PID 51490 crash; retained browser/page state or another long-lived
session allocation remains possible, but the reports contain no heap snapshot
to establish the cause. The live Node ACP process was at 75 MiB RSS after 52
minutes when inspected; that single sample is not a long-session trend. The
Rust mock run's RSS sample is not comparable evidence for the shipping Node
process. Source review found a separate plausible CUA-adjacent retention path:
`GeminiChat` replaces historical inline images with references but keeps the
base64 payloads in a per-chat `Map` for later reattachment. That TypeScript map
had no eviction. It is now bounded to 8 MiB and 64 entries with LRU eviction,
matching the native Rust runtime's image cache. An image too large for that
cache still reaches the current request, but later references to an evicted
image remain text-only. This removes unbounded image-cache growth; it does not
attribute the reported OOMs to CUA. The focused cache suite passes 17 tests;
core TypeScript typecheck, ESLint, and formatting also pass.
The full TypeScript `Config` class, live environment reload tracking, and
consumers outside the native CLI remain unported.
The standalone IDE context store is ported but is not connected to the CLI or
daemon trust-notification flow yet.
Because shell analysis is incomplete, any active path/domain deny rule blocks
shell commands, path/domain ask rules keep them interactive, and recognized
wrapper/expansion forms are blocked when command deny rules exist.

At this checkpoint, `packages/core/src/tools` contains 92 non-test TypeScript
source files and `packages/core/src/providers` contains 17; Rust has 10 tool
implementation files and 11 provider implementation files. These counts only
show module coverage and do not measure behavioral parity.
Current modules cover these parts of the source:

- `transcript.rs`: record validation, active-branch projection, fragment
  aggregation, user-display projection, and history-gap diagnostics.
- `compression.rs`: context-fit budgets, token estimates, truncation, and
  emergency history fitting.
- `genai_compat.rs`: string and PartList conversion to model Content values.
- `tool_display.rs`: bounded tool-result previews, including file diffs,
  terminal output, task lists, and subagent metadata.
- `utils/xml.rs`: source-compatible XML metacharacter escaping and linear-time
  detection/escaping of spoofed `<system-reminder>` tags, including hidden
  Unicode formatting characters. `todo_write` now uses this shared helper.
- `utils/request_tokenizer.rs`: UTF-16 text estimates, MIME support and
  supported-format warnings, base64 image metadata for PNG/JPEG/WebP/GIF/BMP/
  TIFF/HEIC, dimension-based token scaling, and JSON request grouping for text,
  images, audio, and other provider parts. Malformed image payloads use the
  source's 512x512 fallback; zero dimensions return the six-token minimum.
  Text is counted incrementally, and base64 metadata decoding is capped at
  64 MiB. The shared text estimator feeds native OpenAI reasoning-token
  accounting and PDF output guards; the full request estimator accepts
  `serde_json::Value` but has no runtime caller yet.
- `jsonl.rs`: tolerant JSONL recovery, synced append, atomic replacement,
  bounded record serialization and line reads.
- `jsonc.rs`: comment stripping that preserves JSON strings, UTF-8 bytes, and
  original line positions; JSONC object parsing with BOM/trailing-comma
  support; recursive deep merge and sync; exact replacement of a selected
  nested path; and nested content edits that retain formatting and comments,
  remove comments attached to deleted keys, normalize duplicate root keys,
  and verify the final semantic value. The settings-file backup wrapper,
  recursive duplicate-key normalization, and exact `jsonc-parser` diagnostic
  parity remain unported.
- `ide_context.rs`: the shared IDE context schema and process store, stable
  newest-file ordering, active-file cleanup, the ten-file/16,384 UTF-16-unit
  caps, bounded retained snapshots, and removable change subscriptions. The
  CLI/ACP `ide/contextUpdate` protocol endpoint and extension UI remain
  unported.
- `session_writer.rs`: local stale-lock recovery, transcript identity and hash
  checks, durable append, release, sealed handoff, and certified takeover.
- `turn_interruption.rs`: clean/interrupted prompt/interrupted tool-turn
  classification from persisted history, with reminder handling, detached
  continuation parts, provider-required tool-result ordering, and synthetic
  tool-error parts.
- `tool_repair.rs`: dangling tool-call repair with synthetic errors, late real
  result hoisting, duplicate removal, and preservation of non-tool user parts.
- `session_api_history.rs`: reconstruction of provider history from transcript
  records, including compression checkpoints, realtime exclusion, mid-turn
  message merging, and thought stripping. It consumes owned JSON records to
  avoid copying large tool results during reconstruction.
- `session_paths.rs`: project hashes, sanitized project folders,
  active/archive transcript locations, and UUID-like session ID validation.
- `storage.rs`: global and project settings/data paths, `QWEN_HOME` versus
  `CANOPY_RUNTIME_DIR` resolution, captured per-instance runtime roots,
  task-local configurable and pinned contexts with an explicit child-task
  propagation helper, workspace plan path validation through existing
  symlinks, and project temp, workflow, skill, debug, OAuth, and extension
  path helpers. The Rust CLI now uses this layer for project-scoped tool-result
  spill files and the config loader for settings. The full `Config` class,
  process-environment mutation/reload bookkeeping, debug logger, compile cache,
  IDE trust override, and non-CLI consumers remain unported.
- `skills/{paths,symlink_scope,types,skill_load,activation,manager,curator}.rs`:
  project, archived, and pending skill roots; typed metadata; extension
  frontmatter parsing and discovery; path-pattern activation with sticky
  activation state; level-scoped discovery and cache precedence; extension
  hooks, change listeners, and injectable watcher lifecycle; auto-skill
  curation, archive/restore rollback, pinning, and state persistence;
  symlink-target validation; and project containment checks. The loader uses a
  full YAML 1.2 parser with aliases, flow/block values, null removal, and the
  source's malformed-frontmatter fallback. It limits value conversion depth
  to 128 and has a compatibility retry for colon-space plain scalars.
  Activation uses `globset`, whose edge syntax can differ from picomatch.
  Watcher and curator runtime effects are injected; skill writes and tool
  integration remain unported.
- `extensions.rs`, `extension_variables.rs`, `extension_activation.rs`,
  `claude_converter.rs`, `gemini_converter.rs`, `qoder_converter.rs`,
  `extension_preferences.rs`, `extension_setting_helpers.rs`,
  `extension_install_source.rs`, `extension_marketplace_fetch.rs`,
  `extension_marketplace_source_service.rs`, and `agent_plugins.rs`: built-in
  extension variable validation/hydration and command-hook root substitution,
  standalone Claude agent/config/MCP conversion, Gemini manifest projection,
  detection, symlink-confined package copying, TOML-command-to-Markdown
  conversion and temporary-directory cleanup, Qoder manifest/MCP/context
  resolution, and extension activation override
  matching,
  URL/upload-identity redaction, the favorites/scope/per-extension MCP
  preference JSON store with atomic writes and corrupt-file quarantine,
  extension setting validation/change classification/dotenv formatting,
  marketplace source classification and install-source parsing/resolution,
  sanitized Discover metadata projection and an atomic marketplace source
  registry, public-network extension URL/address checks with a reqwest DNS-pin
  helper, plus Agent Plugins v1 root-manifest validation with symlink-aware
  path containment. The marketplace fetch helper uses per-hop public URL/DNS checks,
  a pinned no-proxy reqwest transport, cross-origin credential stripping,
  bounded streamed bodies, and a shared timeout; it supports GitHub API/raw
  fallback and direct HTTP JSON sources behind injected resolver/transport
  interfaces.
  `extension_marketplace_source_service.rs` loads local/remote JSON, persists
  source metadata, and refreshes entries while retaining `addedAt`.
  Gemini package conversion returns a temporary converted package, but it is
  not wired into the installed-extension transaction store or
  installer/update lifecycles. Claude and Qoder package-level copy/conversion
  and the installed-extension transaction store remain unported.
  `agent_plugin_skills.rs` adds SKILL.md discovery/frontmatter validation;
  `agent_plugin_mcp.rs` adds mcp.json validation, per-server normalization,
  and runtime path revalidation. These helpers are not yet connected to
  extension install, update, conversion, settings, or MCP lifecycle flows.
  Marketplace install/update callers still need to consume configs and
  complete plugin installation and update lifecycles.
- `config_resolver.rs`: ordered configuration layers, source metadata
  serialization, absent-value handling, optional/default resolution, and
  environment-layer constructors.
- `services/cron_tasks_file.rs`: durable per-project cron-task path resolution,
  corruption-failing record validation, unknown-field preservation, bounded run
  history, atomic no-follow writes, and cross-process read/modify/write locking.
  `services/cron_tasks_lock.rs` adds scheduler-owner acquisition, stale-lock
  recovery, process liveness checks, and ownership-checked release. Cron
  calculations in `services/cron_scheduler_primitives.rs` add wakeup delay
  normalization, deterministic jitter, JavaScript Date millisecond conversion,
  next-fire calculation, and a bounded cache. The scheduler lifecycle,
  recurring-task cleanup, catch-up behavior, task management routes, and
  delivery integration remain unported.
- `config/{loader,migrations,schema,environment,warnings}.rs`: four-scope
  settings loading and merge behavior, the current migration chain, schema
  metadata and validation, trust-gated environment snapshots, corruption
  recovery, and source-compatible unknown-setting warnings. The CLI consumes
  the loader for permissions, model credentials, and shell environment. The
  checked-in schema is a snapshot; live environment reload tracking, debug
  logger, compile cache, IDE trust override, and non-CLI consumers remain.
- `env_var_resolver.rs`: `$NAME` and `${NAME}` interpolation in strings and
  nested JSON trees, with custom values taking precedence over process
  variables. The CLI passes the loader's effective environment snapshot to the
  provider and workspace shell tools. Rust JSON values cannot represent the
  JavaScript cycle cases.
- `resources/resource_registry.rs`: MCP resource identity by
  `(server_name, uri)`, collision-free pair keys, replacement on rediscovery,
  server filtering, clearing, and deterministic listing. Its ordering uses
  Rust string order; TypeScript's `localeCompare` depends on the runtime
  locale.
- `output/json_formatter.rs`: JSON response/stat/error projection, JavaScript
  truthiness for optional stats and error codes, pretty output, and ANSI
  stripping. Statistics are passed as JSON values; `format_error` receives
  the error type name from its caller.
- `services/workflow_snapshot.rs`: terminal-run projection, best-effort JSON
  persistence, newest-first tolerant listing, 30-file mtime retention, and
  guarded deletion of matching journal directories. It is still separate
  from the in-memory workflow registry and has no storage adapter.
- `services/workflow_run_registry.rs`: workflow state registration, guarded
  pause/resume transitions, bounded phase and log histories, dispatch and
  per-phase token accounting, terminal settlement, cancellation, ordered
  listing, 10-entry terminal retention, and attached runner handles with
  identity-safe release plus cancellation/pause/resume delegation. Approval
  event bridges, callbacks/notifications, todo-chain context capture, and
  binding the handle trait to a concrete runner remain.
- `services/worktree_pin.rs`: canonical path resolution and fail-closed
  validation of caller-owned linked worktrees behind an async Git runtime
  interface. The concrete Git adapter and agent/workflow callers remain
  unported.
- `tools/prior_read_enforcement.rs`: structured mutation decisions for
  missing, stale, unverifiable, non-text, and non-regular targets, including
  the source's partial-text-read policy. Write and edit now use it for
  pre-read, post-read, and pre-write checks; notebook edits keep their separate
  full-read policy. The current tool API carries the raw message but drops the
  helper's structured error code and display message.
- `acp_bridge/tool_write_origin.rs`: validated ACP metadata for write origin
  (`write_file`, `edit`, `notebook_edit`, or `shell_sed_edit`), with caller
  marker replacement and empty-metadata omission. Bridge call sites remain to
  be connected.
- `utils/dotenv.rs`: common dotenv assignments, exports, comments, quoted and
  multiline values, and the source's global-directory-before-home fallback
  with process variables taking precedence. Colon assignments are supported;
  backslash-newline continuations remain unsupported.
- `utils/yaml.rs`: full YAML parsing, recursive null removal, explicit
  timestamp/binary sanitization, malformed-document fallback, and nested YAML
  serialization. `yaml_serde` formatting may differ from `eemeli/yaml`; custom
  line wrapping is approximate, and keep-chomping (`|+`/`>+`) parity is not
  verified.
- `utils/cancellation.rs`: cancellable tokens with parent propagation,
  cancellation futures, combined sources, timeout, wakeup, and explicit
  listener cleanup. It uses Rust polling/wakers rather than JavaScript abort
  events and does not model the source listener-count warning cap.
- `utils/async_message_queue.rs`: synchronous FIFO enqueue and drain behavior
  matching the source message queue.
- `utils/lru_cache.rs`: bounded least-recently-used eviction, recency updates,
  and the source's cache operations. Rust key identity uses `Eq`/`Hash`; it
  cannot reproduce JavaScript `Map` object-reference identity for arbitrary
  keys.
- `utils/part_utils.rs`, `message_inspectors.rs`, and `encoding.rs`: provider
  part text projection and mutation, message-type predicates, and UTF-8/ASCII
  label normalization. The JSON boundary preserves unknown provider fields;
  TypeScript SDK union types and top-level `undefined` have no direct
  `serde_json::Value` representation.
- `utils/formatters.rs`: binary KB/MB/GB display with JavaScript-compatible
  fixed-decimal rounding and unit selection after one-decimal rounding.
- `utils/binary_content.rs`: MIME classification, filename and URL extension
  selection, RFC 5987 decoding, magic-byte sniffing, bounded text detection,
  private file persistence, and byte-size formatting. The Rust helper is not
  connected to the web-fetch caller yet.
- `utils/cron_parser.rs`: five-field syntax, Vixie day-of-month/day-of-week
  matching, and bounded next-fire lookup. It matches ECMAScript whitespace
  handling, including FEFF and excluding NEL; its elapsed-minute scan can
  differ from JavaScript local-calendar stepping across a fall-back DST hour.
  Channel scheduler wiring remains open.
- `utils/cron_display.rs`: truthful minute/hour/day step labels with raw-text
  fallback for malformed and misleading schedules. It is not wired to a Rust
  UI caller yet.
- `utils/context_length_error.rs`: bounded recursive error-text collection,
  context-window overflow detection, fragment-local timeout veto, and token
  count extraction. Its explicit value model handles Error causes and
  throwing accessors; plain JSON cannot represent JS accessors or cycles.
- `utils/osc8.rs`: OSC 8 hyperlink framing and support detection with
  per-call environment reads, stream-TTY checks, terminal-version rules, and
  tmux/screen wrappers. Rust callers pass TTY state for non-stdout streams.
- `utils/error_parsing.rs`: provider API error parsing, nested error-message
  extraction, 429 guidance by auth path, quota passthroughs, and
  already-formatted message detection. JavaScript cause cycles and lone
  UTF-16 surrogate truncation are outside its typed error representation.
- `utils/internal_prompt_ids.rs` and `utils/runtime_model_prefix.rs`:
  background-prompt recognition and nested runtime-model-prefix removal.
  These are exported helpers; call-site wiring is still open.
- `utils/safe_json_parse.rs`: strict JSON parsing followed by `jsonrepair-rs`
  fallback, with caller-provided and empty-object defaults. Repair edge cases
  may differ from the npm `jsonrepair` package, and source debug logging is
  omitted.
- `utils/safe_json_stringify.rs`: compact and pretty JSON formatting with
  numeric/string indentation handling. Its typed `serde_json::Value` boundary
  cannot represent cycles, undefined properties, or custom `toJSON` hooks.
- `utils/read_text_range.rs`: bounded line ranges, handle-bound reads,
  byte-cursor windows, encoding metadata, cancellation checks, and byte/output
  budgets. `ReadFileTool` now uses the handle-bound UTF-8 path for paged reads
  and falls back to its existing decoder for non-UTF-8 files. Attachment
  reading and cursor-window integration remain open; synchronous file reads
  cannot be interrupted mid-call.
- `utils/bare_mode.rs`: exact `QWEN_CODE_SIMPLE` truth-token and CLI-flag
  behavior with injectable environment lookup. The native CLI has not wired
  this mode into its command/config flow yet.
- `utils/safe_mode.rs`: reads `CANOPY_CODE_SAFE_MODE` through the same
  truth-token parser; native CLI caller wiring remains open.
- `utils/shell_pager_env.rs`: uses `cat` on non-Windows targets, supports an
  explicit pager and optional `GIT_PAGER`, and clears inherited pager values
  when disabled. Child-shell wiring remains open.
- `utils/startup_event_sink.rs`: supports thread-safe sink set/clear, no-op
  dispatch without a sink, and panic isolation through an injected logger.
  This Rust boundary uses an instance-scoped registry and has no startup
  profiler caller wired yet.
- `utils/runtime_status.rs`: validates the schema-versioned snake_case
  session sidecar, writes it through the shared atomic writer, supports
  cancellable reads, and provides best-effort removal. `canopy run` and ACP
  publish per-session sidecars atomically, while `sessions list` validates
  them as a work-directory fallback. The Rust preview has no live session-ID
  switch flow to refresh a sidecar mid-run.
- `utils/folder_structure.rs`: recursive breadth-first directory context,
  ignore filtering, filename regex filtering, a combined 20-item default, and
  tree truncation markers. It is distinct from the direct `list_directory`
  tool and is not connected to native context or attachment reading yet.
- `utils/sanitize_child_env.rs`: shared removal of Canopy's three internal
  child-process secrets, wired at user shell and stdio MCP spawn boundaries.
  Third-party credentials are preserved, and MCP configuration cannot add an
  internal token back after sanitization.
- `utils/tool_result_cleanup.rs`: sequential age-based cleanup of project
  tool-result files and legacy `.output` artifacts, with lstat-style symlink
  handling and source-compatible counters. Both CLI and ACP startup schedule
  the cleanup in the background; source debug/warn logging is omitted.
- `utils/terminal_safe.rs`: OSC/CSI and remaining control-sequence removal,
  bidi-control filtering, and notification-label whitespace normalization
  with an 80-code-point cap. CLI and notification call sites remain unwired.
- `utils/atomic_file_write.rs`: sibling-temp atomic replacement, data and
  parent-directory sync, mode preservation/override, symlink policy, and
  permission-error retries. EXDEV fallback and ownership-preserving direct
  writes are still absent; existing async artifact writers remain separate.
- `utils/json_string_byte_projection.rs`: JSON-escaped UTF-8 byte accounting
  and bounded head/tail projection. Rust strings cannot contain lone UTF-16
  surrogates, though the internal counter covers their JavaScript escaping.
- `settings_merge.rs`: recursive settings merge with replace, concatenate,
  union, and shallow-spread strategies, including prototype-sensitive key
  filtering. JSON cannot represent `undefined`, and UTF-16 lone-surrogate
  string spreading has a narrow representation difference.
- `settings_migration.rs`: legacy `tools.allowed`, `tools.exclude`, and
  `tools.core` array migration into permission arrays, without mutating the
  input. The standalone `config/migrations.rs` module additionally carries
  the current versioned migration chain. Malformed non-object `permissions`
  values are normalized to an object by this utility; the TypeScript
  migration can throw for truthy primitives.
- `services/compaction_input_slimming.rs`: compaction-query history slimming,
  image/document removal statistics, MIME placeholder sanitization, tuning
  values, and environment-over-settings precedence. Environment parsing now
  matches ECMAScript whitespace and rejects blank integer settings. The no-op
  path borrows the original history instead of deep-cloning large values.
  Rust strings cannot represent a lone surrogate when the 128 UTF-16-unit MIME
  limit cuts an emoji.
- `services/image_payload_references.rs`: SHA-256-backed image references,
  nested tool-image replacement, ordered recent-image reattachment, explicit
  reference restoration, and base64 byte estimates. `agent_runtime.rs` now
  applies it to a request-only history copy, with payload stores scoped by
  session ID and final user content preserved. Transformations avoid cloning
  the original image-bearing part arrays, and recent payload selection borrows
  stored entries. The runtime retains at most 8 MiB of estimated payload and
  metadata bytes and 64 entries per session, with a four-session LRU per
  runtime. Current explicit references and reattachments are kept first; the
  cap can evict an image needed by a later reference. Request preparation can
  temporarily hold the previous bounded cache plus images from the 12 MiB
  history. Chat-compression settings are not injected yet; malformed values
  outside the TypeScript SDK's `Content`/`Part` types may be treated
  differently.
- `services/post_compact_attachments.rs`: recent successful file-call and
  image extraction, bounded file restoration, workspace and symlink checks,
  analysis-block cleanup, summary trailer, plan-mode and subagent reminders,
  and pending tool-call preservation. The filesystem is injected, but session
  compaction and resume do not yet call this builder.
- `services/token_estimation.rs`: shared character-based content and prompt
  estimates, conservative scaling of only newly added content, and usage
  metadata handling when candidate and thought counts overlap. Exotic
  JavaScript numeric-string coercions remain different.
- `tool_utils.rs`: canonical and legacy tool aliases, allow/exclude matching,
  and exact shell-command invocation matching.
- `services/loop_detection.rs`: consecutive and global duplicate calls,
  alternating call patterns, repeated text and thoughts, read-only churn,
  same-tool stagnation, overview-style git inspection stagnation, retry
  rollback, and the configurable adaptive call cap. `agent_runtime.rs` now
  checks streamed content, thoughts, tool calls, retries, and finishes before
  emitting each event or running a tool. A detected loop is persisted as a
  `loop_detected` system record. The CLI runtime currently uses default loop
  settings rather than Canopy's full `Config` values, and the Rust JSON
  argument model cannot represent JavaScript cycles or object identity.
- `services/attribution_trailer.rs`: bounded 30 KiB JSON notes and a literal
  argv-form `git notes add` command targeting the caller-captured commit ID.
  It avoids shell quoting and the source's symbolic-`HEAD` race.
- `services/session_file_history_state.rs`: retained file-history snapshots,
  duplicate suppression, atomic batch updates, backup metadata, and ISO-millisecond
  serialization. Its date parser handles common ISO and numeric forms, not all
  permissive JavaScript `Date` strings. Resume now rebuilds this state from the
  recovered transcript and feeds it to the file-history service.
- `services/file_history.rs`: stateful file-history support, with
  the JSONL snapshot schema, generated backup paths constrained to a session
  directory, pre-edit backups, unchanged-file inheritance, per-file failure
  markers, restore validation, rewind/apply, memory-bounded comparisons and
  diff summaries, capped turn-diff hunks, and 100-snapshot/orphan cleanup. The
  writer and its edit/notebook wrappers track pre-edit state; the
  CLI creates a per-session service for terminal and ACP runs; prompt start
  snapshots and tool updates are recorded into the session transcript; resume
  restores and validates persisted backups. File restore exists at the service
  level, but the rewind selector, conversation rewind, and ACP/CLI callers are
  not ported; descriptor-based protection against same-user path replacement
  also remains open. Tool updates are persisted when control returns to the
  runtime after a tool batch, leaving a crash window after file mutation.
- `services/usage_history.rs`: the persisted usage-record schema, metrics
  conversion, namespaced telemetry event replay, incremental live merge,
  dedupe, salvage, time-range aggregation, top-tool/skill lists, and dashboard
  totals. `services/usage_dashboard.rs` exposes the dashboard API over that
  implementation. Neither module is connected to the Rust session lifecycle
  or HTTP usage-dashboard route.
- `services/session_title.rs` and `services/tool_use_summary.rs`: provider-neutral
  title and tool-summary query shaping, bounded history/result cleanup, and
  injected query interfaces. They are not connected to the CLI session or UI
  flows yet.
- `services/session_recap.rs`: best-effort recap prompt shaping, hidden-thought
  and reminder filtering, the 30-message turn-aware window, and `<recap>` tag
  extraction. The history adapter supplies Canopy's startup-context offset and
  the side-query adapter owns model fallback; this service is not wired into
  session resume yet.
- `services/runtime_sample_ring.rs`: the 60-entry RSS/heap/CPU sample ring,
  same-millisecond behavior, and CPU-delta normalization. Clock and CPU
  sampling are injectable; `native_memory_probe.rs` now supplies host RSS and
  optional process CPU readings on macOS/Linux.
- `services/daemon_memory_budget.rs` and `child_heap_policy.rs`: ACP host/cgroup
  budget arithmetic and the observation-only child heap partition. Memory
  measurements are caller-supplied; neither child limits nor spawn refusals
  are applied.
- `services/memory_pressure_policy.rs`: memory-pressure thresholds, cleanup
  recommendations, cooldown/escalation rules, and repeated-diagnostic cadence.
  It does not probe process memory, execute cleanup steps, or emit telemetry.
- `services/memory_pressure_monitor.rs`: provider-neutral pressure sampling,
  cleanup sequencing, coalesced checks, queued escalation, failure accounting,
  and generation-aware cancellation. A failed pre-cleanup RSS sample now
  retries any stronger escalation queued during cleanup startup, including
  when starting a previously queued action. `memory_pressure_runtime.rs`
  connects native RSS and CPU probes and computes the host/cgroup limit once.
  Native CLI and ACP sessions now sample memory at startup and every 15 seconds,
  and apply stale/cold/full file-cache cleanup to the same cache used by tools.
  `AgentRuntime` requests additional checks after tool calls. It reads
  `CANOPY_MEMORY_PRESSURE_*` threshold overrides. `CompactHistory` queues a
  request that the agent applies between provider turns through the native
  microcompactor, preserving configured recent results and managed-memory
  reads before clearing the corresponding file cache. These are live CLI and
  ACP integrations. Hard/critical pressure now writes a crash-surviving phase-one
  diagnostic using the monitor's latest RSS sample and adds that same value as
  `session.nativeMemory.processRssBytes`. The snapshot also records bounded,
  best-effort host memory totals, Linux available memory where reported, and
  the effective host/cgroup cap; unsupported or failed probes are marked
  unavailable. The native report marks V8 and
  session-history data unavailable, but now records the selected run/ACP
  session ID, process ID, native runtime, Canopy version, platform,
  architecture, and uptime. The monitor starts immediately after workspace
  tools select that session, takes one prompt startup sample, and retains the
  15-second periodic cadence. `AgentRuntime` also requests a check after each
  attempted tool execution through a capacity-one signal, so bursts coalesce;
  one task serializes checks and cleanup. The monitor's scheduled-check bit
  also coalesces requests.
  Linux full diagnostics report `/proc/self/status` `VmHWM` as peak RSS using a
  bounded read; the current safe macOS probes expose current RSS but not a
  lifetime peak. Source pressure-time telemetry and shared service wiring
  outside the CLI remain unported.
- `services/microcompaction.rs`: rule-based tool-result and media eviction,
  idle and size triggers, per-kind recent retention, UTF-16 accounting,
  read-file path preservation, and bounded eviction metadata. Read-only
  planning borrows history, and clear operations avoid cloning replaced
  response/media fields. The memory pressure path invokes this compactor at a
  safe provider-turn boundary using the source's synthetic elapsed-idle
  timestamp and negative-threshold opt-out; regular pre-send and manual
  compression flows are not connected.
- `services/native_memory_probe.rs`: bounded no-shell macOS/Linux RSS and CPU
  probes, bounded host-memory context for crash diagnostics, system swap
  totals, and validated cgroup v1/v2 limit selection. Linux snapshots include
  `MemAvailable` when present; macOS reports host total and effective limit,
  with `availableBytes` set to null. Swap reads are capped and unavailable on
  unsupported platforms or probe failure. Native Rust has no V8 heap
  statistics; macOS `ps` CPU counters have 10 ms resolution, and nested cgroup
  mount discovery is not implemented.
- `services/memory_diagnostics.rs` and `memory_diagnostics_dumper.rs`: the
  diagnostics JSON schema and risk analysis, bounded `ps` process-tree probe,
  optional `/proc` probes, and a two-phase crash-surviving dump. Node/V8 values
  and full diagnostic collection are injected because this Rust process has no
  Node APIs; failed optional probes become `null` and are not logged. The Rust
  native CLI connects this service on hard/critical pressure and records its
  process-level snapshot before the optional OS probes run, including system
  swap totals and an explicit unavailable marker for native heap counters.
- `services/tool_result_retention.rs`: aggregate retained tool-result counts
  and estimated sizes, plus over-budget results not marked with a truncation
  sentinel. It shares the compaction estimator, but callers still inject the
  actual global and per-tool output budgets. Parity review added edge coverage
  for raw newline-dense output length and JS nullish `output ?? error` choice.
- `services/tool_result_boundary_diagnostics.rs`: content-free size summaries,
  HMAC-SHA256 value identifiers, artifact normalization, byte thresholds, and
  rate-limited suppression counts across result boundaries. Callers inject the
  enable check, logger, context IDs, and key; runtime boundary hooks and
  async-local session/prompt context are not connected yet. Failed providers
  now leave quota and suppression counts available for the next event.
- `trusted_folders.rs`: JSONC rule loading, lexical and canonical path
  comparison, deepest-match precedence, `TRUST_PARENT`, untrusted tie wins,
  explicit trust-level reporting, trust-status serialization, the loaded
  cache and change subscriptions, and sync/update writes under the same
  `.lock` directory contract as `proper-lockfile`. Writes re-read under lock,
  validate every rule, reject symlinks/non-regular files, preserve JSONC
  comments/formatting, use owner-only atomic replacement, and update memory
  only after disk commit. Reads/writes are bounded to 16 MiB. The CLI applies
  file trust before loading workspace permissions; wiring the IDE context store
  into the full CLI/ACP trust notification flow remains.
- `session_recovery.rs`: clean/interrupted/degraded recovery plans, repair
  diagnostics, continuation payloads, visible notices, and explicit
  confirmation policy.
- `branch_points.rs`: v1 durable checkpoint parsing, active-chain validation,
  completed-turn detection across paired tool calls/results, and ordered branch
  point resolution. It is not yet connected to session recording or the CLI.
- `session_resume_token_counts.rs`: latest assistant usage or compression
  checkpoint recovery, including candidate/thought overlap handling and the
  estimated-versus-authoritative flag.
- `goals/`: the versioned Goal protocol and reducer, serialized recovery and
  legacy projection, the session-keyed `/goal` compatibility store, bounded
  evidence catalogs, checkpoint materialization, and the serialized Goal
  runtime for restoration, host binding, permits, continuations, terminal and
  checkpoint verification, and disposal. Provider verification stays behind
  host traits. `goals/turn_context.rs` scopes each executor tool call to its
  validated permit, explicitly clears absent permits, and provides child-task
  propagation. Runtime construction and notification wiring into the CLI/ACP,
  plus Goal hook/tool implementations, remain incomplete.
- `services/session_turn_state.rs`: prompt-turn recovery from current and
  telemetry prompt IDs, user-prompt counting for the matching session, eligible
  user parent-UUID collection, and insertion-ordered unique background
  notification task IDs. The transcript reader consumes this projection;
  session UI wiring remains unported.
- `services/session_transcript_reader.rs`: bounded transcript snapshots,
  fragment recovery and aggregation, active-chain and side-task replay
  filtering, signed frozen-prefix paging, turn-aligned forward/backward pages,
  artifact selection, and cold restore projections for API history and
  session metadata. Cold restore now rebuilds the artifact snapshot and
  warnings from the selected active-chain artifact records. Goal
  checkpoint/evidence recovery, branch-point projection, live restore
  projection, and the source reader's cooperative scan cache remain
  incomplete; non-Unix file identity is also less precise.
- `acp_bridge/session_runtime.rs`: concurrent cold restores with matching
  effective action, history replay transport/page size, inherited-history
  policy, and live replay mode share one factory restore. Waiters reserve
  attachment counts before awaiting, while mismatched requests stay fenced.
  Summary/full live-mode requests currently require an exact match because the
  Rust restore result cannot yet recompute each waiter's journal projection.
- `acp_bridge/replay_window_limits.rs` and
  `acp_bridge/journal_growth_policy.rs`: source-compatible replay/journal
  defaults and validation, exact limit error text, per-session baseline pool
  accounting, proportional event-cap growth, safe-integer clamping, and the
  256 MiB session hard cap. The existing daemon memory policy disables a
  derived pool when either journal cap was explicitly pinned. Rust event-bus
  reconnect replay has a separate byte budget and remains independent. ACP
  compaction/journal ownership and CLI-to-session growth-pool wiring remain
  unported; see `rust/crates/canopy-core/src/acp_bridge/PORT_STATUS.md`.
- `session_store.rs`: active/archive transcript locations, bounded secure
  reads, writer-lease acquisition, new session creation, and resume planning
  positioned at the projected active-branch leaf.
- `tool_effect_journal.rs`: synced write-ahead intent records and recovery of
  calls whose side effects may have run before their result was persisted.
- `notebook.rs`: bounded structured rendering for code, markdown, and raw
  notebook cells, including text output, ANSI stripping, UTF-16-compatible
  output budgets, and size truncation; cell replace/insert/delete operations
  preserve notebook formatting and IDs, and clear stale code outputs.
- `tools/read_file.rs`: workspace-contained text, notebook, PDF, and supported
  image/audio/video reads. Text reads use zero-based line paging, a 2,000-line
  default, a 2 MiB line cap, and an 8 MiB output cap; decoding covers UTF-8,
  UTF-16/32 BOMs, and detected legacy encodings. It rejects Canopy-ignored
  paths, tracks reads in the shared file cache, and can skip an unchanged full
  read while its prior content remains resident in history. Cache updates and
  this shortcut can be disabled through the tool API. `.ipynb` files are read
  in full up to 8 MiB and rendered as structured cells; paging is rejected.
  Supported media is attached only when the selected model supports its
  modality and encoded size stays under 10 MiB. PDF reads use `pdfinfo` to
  include small documents or require a page range for larger documents, with a
  file-size estimate when metadata is unavailable. Full text reads are capped
  at 100 MiB and explicit page-range reads at 512 MiB; ranges validate the
  20-page maximum and use bounded, timed `pdftotext` subprocesses. Native PDF
  parts, scanned-page rendering, external-path approval, microcompaction and
  application-level cache configuration remain to port.
- `pdf.rs`: strict single-page and closed-range parsing (pages 1 through
  1,000,000; at most 20 pages per request), explicit open-ended-range errors,
  `pdftotext` extraction with a 30-second timeout, drained and capped child
  output, a 100,000 UTF-16-unit limit, and the source 12,000-token safety
  threshold. It also enforces the source 100 MiB full-read and 512 MiB paged
  input caps. Process timeout, output bounds, real Poppler extraction, and
  argument construction have local regression coverage. PDF extraction still
  depends on Poppler being installed.
  The remaining PDF gaps are native PDF media handling for providers that
  accept it, scanned-page rendering, and permission-mediated reads outside the
  workspace.
- `tools/notebook_edit.rs`: cell-level notebook mutations require an unchanged
  full structured read, workspace containment, a reviewed diff, and explicit
  approval. Structural changes invalidate the read cache when fallback cell
  IDs can shift.
- `tools/ask_user_question.rs`: validates the 1-4 question and 2-4 option
  contract, single/multi-select flags, and canonical question-index answer
  formatting. The CLI prints a bounded terminal form with an `Other` text
  entry and clean decline handling; non-interactive ACP collection and the
  source UI's confirmation payload remain unported.
- `tools/computer_use/`: asset/path constants and screenshot-size precedence,
  all 35 pinned CUA tool schemas, high-risk call detection and schema-directed
  argument coercion, MCP permission-error classification, install-state
  parsing/serialization and approval-file I/O, the injected install/permission
  bootstrap state machine, streamed checksum-verified downloads with staged
  installation, a lazy MCP client with idle shutdown/reconnect, and MCP
  text/image/audio result projection. An isolated agent-tool adapter can append
  feature- and permission-filtered declarations and route calls through the
  Rust executor, with bounded arguments and fail-closed injected authorization
  and bootstrap interfaces. The Rust `canopy run` path now reads the existing
  Computer Use settings, registers enabled schemas through tool and permission
  filters, prompts for per-action approval, persists install approval, installs
  the verified driver, and wires macOS status and privacy-pane operations
  through a host adapter. ACP now exposes enabled schemas through the native
  runtime and requests `session/request_permission` for every action, offering
  only one-call allow or reject. Deny rules, core-tool filters, and exclusions
  still apply; an allow rule does not skip ACP consent. Permission content shows
  the exact arguments and first-use installer disclosure, and an accepted call
  authorizes that installation. ACP cancellation drops the local pending
  permission future and execution future, and the ACP bootstrap status daemon
  cleans up on drop. The protocol client API does not expose cancellation for
  an outbound permission request, so the host's dialog may remain until the
  client responds; the adapter also has no explicit per-call cancellation
  token or separate driver cancel operation. Deferred ToolSearch exposure is
  unavailable, and bootstrap progress still goes to stderr instead of an ACP
  tool-call update. The Node application remains the shipped live runtime, and
  the Rust host path has not yet had an end-to-end desktop run.
- `followup/` and `followup_state.rs`: the overlay file system and speculative
  tool safety gate, plus delayed follow-up suggestion display, acceptance,
  dismissal, and telemetry state. The full speculative loop and UI wrappers
  remain unported; shell safety is supplied through a fail-closed classifier
  interface.
- `tools/web/`: the source's 88-entry preapproved-host table, HTTPS post-fetch
  markdown gate, URL validation and GitHub blob-to-raw rewrite, bounded
  UTF-16 text projection, fetch display hints, and bounded search-result
  projection. WebFetch now performs pooled, timed requests with response-byte
  and redirect limits, rejects cross-origin redirects, persists binary files
  within the session budget, extracts PDFs through Poppler, and converts HTML
  through the MIT-licensed `html2md-rs` adapter while retaining links and
  omitting images/scripts/styles. Conversion parser behavior can differ from
  Turndown for malformed HTML. The native CLI registers WebFetch behind its
  permission gate and summarizes through the configured OpenAI-compatible
  side-query pipeline; ACP also registers it and fails closed when client
  approval is unavailable. WebSearch configuration is gated against the
  selected model and resolved backend, then dispatched through the bounded
  search executor in both hosts. JSON-mode/fail-closed/multi-attempt WebFetch
  side queries, full provider-native side-query parity, and macOS end-to-end
  validation remain open.
- `telemetry/`: hook-name sanitization, deterministic session-derived trace
  IDs, secure random span IDs, and WHATWG-style OTLP HTTP URL normalization.
  The OpenTelemetry SDK, exporters, telemetry configuration/runtime, and
  application call sites are not yet ported.
- `file_read_cache.rs`: bounded per-process file tracking keyed by Unix
  device/inode identity, with nanosecond mtime and size drift checks, partial
  versus full reads, text versus non-text reads, write records, invalidation,
  normalized-path invalidation, full cache clearing, age-based eviction, and
  read-before-mutation checks. `read_file` and `grep` share this cache in a
  live CLI run. `edit_file` and `write_file` use this cache to enforce the
  read-before-mutation guard and update the fingerprint after a write. The
  cache now exposes the cleanup operations used by the live CLI memory-pressure
  sampler.
- `secret_scanner.rs`: high-confidence credential patterns from Canopy's
  curated scanner and a repository `.canopy/team-memory` path guard. Messages
  expose only rule labels; a bounded marker scan handles private-key blocks.
- `file_discovery.rs`: shared filtering for `.gitignore`, `.git/info/exclude`,
  nested ignore files, `.canopyignore`, `.agentignore`, `.aiignore`, and
  configured custom agent-ignore filenames. It preserves parent-to-child
  precedence, skips nested rules under ignored directories, reports filter
  counts, and rejects paths outside its root.
- `tools/list_directory.rs`: the `list_directory` function declaration and
  workspace-only directory listing, including ignore counts, name glob
  filters, directories-first sorting, empty-directory output, configurable
  line limits, and the 100-entry ceiling. Canonical path checks prevent a
  symlink from listing an external directory. Canopy's display descriptions,
  workspace/memory/skills permission exemptions, and ask-before-external
  approval flow still need the CLI permission layer.
- `tools/glob.rs`: recursive hidden-file search, literal matching for existing
  filenames with glob metacharacters, Canopy ignore filtering and traversal
  pruning, macOS/Windows case-insensitive matching, recent-file sorting, and
  bounded result collection/display. The Rust CLI currently searches one
  directory inside the workspace; Canopy's multiple workspace roots, external
  search approval, cancellation, and exact Node `glob` traversal semantics
  remain to port.
- `tools/grep.rs`: native recursive case-insensitive regex search, file-glob
  filtering, Canopy and common directory exclusions, grouped line-numbered
  results, and Canopy-style line/character truncation. Reads are streamed with
  a 2 MiB per-line limit; at most 1,000 matching lines are retained and the
  default rendered output is capped at 25,000 UTF-16 characters. It uses Rust's
  regex syntax, skips overlong lines, searches one workspace path at a time,
  and does not yet implement external-path approvals, Canopy's multi-root
  search, or cancellation.
- `tools/write_file.rs`: bounded UTF-8 workspace writes, prior-read and
  changed-since-read checks, full unified-diff approval, BOM/line-ending
  preservation, team-memory secret checks, and synced atomic file replacement.
  It currently lacks the source tool's backup/history, richer encodings,
  telemetry, permission modes, and coordination with hooks or other
  application surfaces.
- `tools/edit_file.rs`: literal replacement with unique-match-by-default and
  `replace_all`, quote/dash/space and trailing-whitespace normalization,
  deletion newline handling, prior-read enforcement, and the same approval and
  atomic-write path as `write_file`. It is limited to UTF-8 files up to 8 MiB
  and does not yet include managed-memory permission exemptions, edit history,
  attribution, telemetry, snippets, or the full structured error and display
  contract.
- `tools/shell.rs`: approved foreground and managed background execution in a
  workspace directory, 120-second default and 10-minute maximum foreground
  timeout, 32 KiB output capture per stream, process-group termination, and
  removal of Qwen/Canopy's internal daemon credentials from the child
  environment. `task_list` and `task_stop` expose the managed-shell registry to
  the CLI. Command AST permissions, read-only fast paths, shell previews, PTYs,
  terminal rendering, output spill files, and cross-platform shell
  configuration and environment handling remain incomplete.
- `permissions.rs`: rule parsing and direct invocation matching for command,
  path, domain, literal, parameter, and MCP rules; tool aliases and read/edit
  categories; command wildcard matching; and deny/ask/allow precedence. The
  CLI applies these rules to file mutations and shell commands from user
  settings. Canonical path matching is used for ask/deny rules while allow
  rules remain lexical. The CLI merges system-default, user,
  trusted-workspace, and system rule arrays and skips workspace rules for
  explicitly untrusted folders. The CLI applies `tools.core` filtering to its
  current registered subset. IDE trust overrides, permission-manager-wide
  `tools.core` semantics, shell virtual operations, and full shell grammar
  handling are still pending; recognized wrapper and expansion forms fail
  closed when command deny rules are configured.
- `agent_runtime.rs`: OpenAI-compatible streamed model turns, durable user and
  assistant records, tool-execution intents, bounded/spilled tool responses,
  model continuation, per-turn and cumulative history limits, capped tool
  output batches, a 100-turn and 100-call limit, and repeated-call detection.
  Consecutive read-only calls are grouped and run concurrently up to a
  configurable bound (default 10; `CANOPY_CODE_MAX_TOOL_CONCURRENCY`). Tool
  handlers and permissions are injected. Image references are restored on a
  request copy using a bounded session-scoped payload store; chat-compression
  settings are not yet passed to this runtime. This is not yet Canopy's full
  scheduler, hook system, interrupt handling, retry/fallback loop, or UI event
  bus.
- `hooks/{env_interpolator,ssrf_guard,url_validator,trusted_hooks,stop_hook_cap,planner,aggregator,async_registry,async_command_runner,registry,session_manager,command_runner,http_runner,function_runner,prompt_runner,prompt_provider,event_dispatch,event_inputs,native_dispatch_executor}.rs`:
  whitelist-based variable expansion and header sanitization; IPv4/IPv6 SSRF
  ranges and metadata checks with an injectable all-address DNS seam; URL
  allowlist matching; project-scoped trusted-hook keys persisted with private
  atomic replacement; Stop/SubagentStop continuation-cap parsing and warnings;
  hook event matcher routing and plan deduplication; event-specific output
  aggregation; bounded admission/output/timeout state for asynchronous hooks;
  source-priority registry state, enabled toggles, agent-scoped entries,
  duplicate identities, and transactional configured reload; plus HTTP hook
  request projection, environment interpolation, URL/DNS preflight checks,
  timeout and cancellation, once tracking, response parsing, and bounded output.
  The command runner handles one process invocation, bounded output capture,
  environment projection, cancellation and timeout; the session manager stores
  per-session registrations and applies alias-aware matcher rules.
  `async_command_runner` adds capacity checks, background command execution,
  and completion/failure updates to the async registry. `event_dispatch` merges
  planned registry hooks and session hooks, applies sequential output to later
  inputs, and aggregates results with the Todo fail-closed policy.
  `event_inputs` builds all 22 public event payloads and planner contexts,
  taking base fields and Stop/SubagentStop snapshots from the host at call time.
  `native_dispatch_executor` routes the dispatch seam to the typed runners and
  resolves configured function callbacks through a host adapter.
  `config_loader` ingests user/project/active-extension hook definitions with
  structured validation issues and a function-callback resolver seam.
  `system` now provides a locked façade that snapshots configured and session
  hooks before async dispatch, and `hook_helpers` ports context-usage payload
  construction and todo-change detection. `skill_registration` registers
  command/HTTP skill hooks in the native session manager. `system_events`
  exposes all 22 typed event methods over the input builders and dispatcher.
  `instructions_callback` ports the memory loader's informational
  InstructionsLoaded callback. The live application lifecycle remains
  unwired, including host provider setup, tool events, and message streaming.
  The function runner invokes host callbacks with timeout/cancellation outcome
  policy; the prompt runner builds provider-neutral request/response payloads
  and validates JSON output. `prompt_provider` maps those requests through the
  native OpenAI-compatible, Anthropic, and Gemini clients while leaving model
  alias and auth selection behind a host resolver seam. The CLI still needs to
  supply that resolver and construct the hook system at its event call sites.
  HTTP transport and DNS resolution are injectable, with a
  reqwest/Tokio adapter. These modules compile and compose behind a native
  dispatcher, but the application event call sites are not yet connected.
  Config loading,
  base-field/snapshot access, schema validation, trust-folder policy,
  extension activation, and host process lifecycle remain host work;
  `trusted_hooks` models only fields used for trust identity. Async timeout
  scheduling and process termination remain host work.
- `transcript.rs`: user-prompt-submit context stripping now mirrors the hook
  helper that removes only a trailing injected context part when user content
  precedes it; a sole matching part remains untouched.
- `recording.rs`: synchronous user, assistant, tool-result, and system record
  construction through the lease. This is an initial slice of
  `chatRecordingService.ts`; title generation, all UI record types, rewind,
  branching, artifact handling, and other recorder behavior remain to port.
- `providers/openai_compatible.rs`: native Rust HTTP transport for OpenAI Chat
  Completions compatible services. It bounds request/response bodies and SSE
  frame sizes, applies stream idle/lifetime deadlines, and redacts credentials
  from diagnostics. Local HTTP tests exercise request authentication, event
  delivery, bounded error bodies, and oversized-response rejection.
- `providers/schema.rs`: Gemini and MCP function-declaration conversion,
  Gemini schema normalization, OpenAPI 3.0 conversion, and the optional-field
  relaxation used by OpenAI-compatible tool calling. Source parity cases
  cover JavaScript number-to-string thresholds, negative zero, union/const/
  enum conversion, and nested optional-field handling.
- `providers/openai_request.rs`: Gemini-shaped request conversion for system,
  user, assistant, reasoning, function-call/result, image/PDF/audio/video,
  duplicate-ID, split-tool-media, assistant-merge, and orphan-cleanup paths.
- `providers/openai_response.rs`: OpenAI/Gemini non-streaming response
  conversion, finish-reason mapping, reasoning-token estimates, usage-cache
  provenance, and a stateful tagged-thinking parser.
- `providers/streaming_tool_call_parser.rs`: stream-local tool-call identity
  routing, collision and late-ID handling, argument repair, completion, and
  explicit limits on IDs, call count, names, and aggregate argument bytes.
- `providers/streaming_converter.rs`: OpenAI-compatible chunk conversion,
  cumulative text-delta normalization, tagged reasoning, protocol-tag leak
  checks, usage metadata, tool preparation, and truncation handling. Retained
  delta and pending-part buffers have 16 MiB limits; the converter now feeds the
  runnable core agent loop through `openai_stream.rs` and `agent_runtime.rs`.
- `providers/openai_stream.rs`: connects the bounded SSE client to the stream
  converter and maintains separate state per live provider request. It merges
  usage-only terminal events into the finish response, applies end-of-stream
  checks, maps embedded provider errors, and can feed converted chunks through
  the Canopy turn-event adapter.
- `turn.rs`: provider-independent Canopy response-to-event state for thoughts,
  visible text and images, tool requests, citations, finish reasons, retry and
  fallback resets, compression notifications, and cancellation. The adapter
  preserves the OpenAI reasoning marker across conversion and caps pending
  tool calls and citations; `agent_runtime.rs` owns the initial provider/tool
  continuation loop.
- `tool_response_finalizer.rs`: JavaScript-compatible UTF-16 batch budgeting,
  equal-share allocation, head/tail previews, astral-character-safe slicing,
  plan-mode lifecycle-prefix protection, artifact references, and distinct
  spill names for duplicate call IDs. Its file store uses atomic replacement,
  owner-only file permissions, a per-session byte ceiling, and bounded spill
  size. Tool output JSON-byte estimates now use a streaming counter so
  rejecting an oversized media payload does not first allocate a duplicate
  serialized buffer. Hooking it into `Config`, adding the source's fallback
  truncation path, and boundary diagnostics remain.
- `providers/prefix_caching.rs`: OpenAI authentication and endpoint checks,
  model-version support, cache keys, and the two explicit message breakpoints.
- `providers/retry_policy.rs`, `retry_error_classification.rs`, and
  `retry_adapter.rs`: capped exponential delays, jitter, fractional and
  date-form `Retry-After`, provider payload/status classification, quota
  handling, and conversion into the generic retry facts.
- `utils/retry.rs`: bounded and persistent retries, cancellation-aware waits,
  attempt context, heartbeat callbacks, content retries, and retry telemetry
  hooks. `agent_runtime.rs` retries provider stream establishment before any
  SSE chunk is consumed. Midstream restart is not enabled because it could
  duplicate already emitted events; retries currently have no Rust log sink,
  and the preview supplies no auth type or custom retry-code context.
- `providers/openai_pipeline.rs`: request parameter precedence, output-budget
  clamping, strict JSON schema formatting, stream options, cache policy,
  compression budgets, and official-endpoint reasoning opt-out.
- `providers/openai_profiles.rs`: non-DashScope profile selection, default
  output caps, DeepSeek's temperature default, MiniMax stream parsing options,
  and compatibility rewrites for default Qwen, DeepSeek, Mistral, MiMo,
  ModelScope, and Z.ai requests. DashScope selection and request shaping live
  in the following provider module.
- `providers/dashscope.rs`: endpoint selection, request headers,
  message/tool cache-control markers, GLM tool-less text fallback, layered
  thinking-knob selection/conflict resolution, and request assembly with
  metadata, vision flags, and user override precedence. The request builder
  is wired into `openai_pipeline.rs`; selecting provider-specific clients and
  telemetry remains.
- `providers/presets.rs`: the ordered 12-provider static catalog, including
  regional base URLs, documentation links, model metadata, display prefixes,
  headers, and UI labels. Credential resolution, key validation, custom-model
  setup, and install-plan callbacks remain dynamic host responsibilities.
- `token_limits.rs`: model ID normalization, input/output limits, output-window
  clamping, safe environment integer parsing, and max-token reconciliation.
- `modalities.rs`: ordered model-family image, PDF, audio, and video defaults,
  plus Qwen/Canopy, GLM, and tiered-effort wire-family predicates.
- `mcp/config_hash.rs`: recursively canonicalized SHA-256 approval fingerprints
  that exclude only top-level MCP provenance/display fields.
- `mcp/oauth_utils.rs`: RFC well-known URL construction and ordered OAuth
  metadata discovery, including protected-resource scope precedence,
  `WWW-Authenticate` parsing, canonical resource URIs, and bounded metadata
  downloads. The Rust discovery client uses 10-second connect and 20-second
  request deadlines and rejects metadata bodies over 1 MiB.
- `mcp/token_storage/{types,base}.rs`: OAuth credential JSON contracts, storage
  traits, required-field validation, keychain account sanitization, and the
  source five-minute token-expiry buffer.
- `mcp/token_storage/{plain_file,encrypted_file,macos_keychain,configured}.rs`:
  legacy plaintext OAuth credential arrays, source-compatible AES-256-GCM
  encrypted storage, macOS generic-password Keychain storage, and hybrid
  backend selection. The encrypted file uses private atomic replacement and
  bounded reads; Keychain access is macOS-only. Linux/Windows native credential
  stores and extension-settings use of `SecretStorage` remain.
- `mcp/oauth_provider.rs`: authorization-code/PKCE, metadata discovery,
  bounded callback listener, token exchange/refresh, and credential persistence.
  CLI and ACP trigger it on eligible OAuth challenges; product UI and
  non-loopback redirect integration remain.
- `tools/mcp/resource_content.rs`: framed MCP resource content, cumulative
  text/blob caps, summaries, and inline media parts. Rust cannot preserve a
  lone surrogate if a text cap splits an emoji, and its per-call blob budget is
  an integer `usize` rather than any JavaScript number.
- `tools/mcp/{pool_key,session_config,retry,prompt_registry}.rs`:
  source-compatible server fingerprinting (including the TypeScript SHA-256
  fixture), session config projection, bounded retry classification/backoff
  with cancellation, and an in-memory MCP prompt registry with collision
  renaming and server-scoped removal.
  Tool, prompt, and resource discovery now share the retry helper and preserve
  cancellation during the 200 ms exponential backoff. Tool calls remain
  unretried because a lost response can follow a completed external side
  effect; retrying those calls still needs an explicit host idempotency policy.
- `tools/mcp/{status,errors}.rs`: process-wide connection status snapshots with
  stable listener dispatch and the three typed server-add errors, codes,
  optional spawn details, and source-compatible messages. Client connection
  transitions and transport-pool listeners now update the status registry;
  the typed add-error values remain helper types and are not yet emitted by
  the CLI host.
- `tools/mcp/workspace_budget.rs` and `client_manager.rs`: ordered workspace
  server reservations, enforce/warn/off behavior, hysteresis, nested
  discovery-pass refusal batches, event serialization, and manager admission
  and release across discovery/removal/stop. Acquisition tickets invalidate
  late results after release/shutdown, and dropped discovery futures release
  their own leases. `transport_pool.rs` aborts uncommitted handshakes and
  schedules bounded disconnect cleanup; stdio also kills an unattached child
  synchronously. `McpCliHost` creates one workspace-scoped budget from
  `CANOPY_SERVE_MCP_CLIENT_BUDGET` and `CANOPY_SERVE_MCP_BUDGET_MODE` and
  injects the shared instance into its session managers. Other hosts must
  inject a shared budget themselves. If the Tokio runtime has already stopped,
  async disconnect cannot be scheduled, and injected SDK adapters remain
  responsible for cleaning up resources created internally.
- `tools/mcp/agent_tool_adapter.rs`: per-session MCP tool/resource declarations
  and routing composed with the agent executor, behind a host authorization
  trait. `McpCliHost` wires the adapter, manager, and authorization policy for
  native run and ACP sessions; resource reads use cumulative prompt/response
  budgets. Tool calls remain unretried because a disconnected call may have
  completed remotely, so retry safety needs an explicit host idempotency
  policy.

The record size cap is currently 16 MiB. This keeps one serialized or decoded
JSONL line from creating an unbounded temporary allocation; callers must
surface oversize records rather than treating them as successfully persisted.

Native Apple Silicon development checks currently run with:

```sh
cd rust
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The suite covers 1,000 sequential durable session writes, live-writer conflict,
stale local-owner reclamation, external transcript edits, certified handoff,
malformed and oversized JSONL handling, tool-result compaction, transcript
projection, provider HTTP bounds, request/response conversion, turn-event
conversion, tool-output batch budgeting, a local agent exchange that verifies
tool-result persistence before continuation and after clean-session resume,
workspace-bounded file reads, notebook rendering and editing, bounded PDF page
counting and text extraction through Poppler, OAuth URL and metadata discovery,
token-file persistence, XML and JSONC comment handling, trust-rule precedence,
task-local runtime-path isolation, symlink-safe atomic replacement,
environment-file precedence, LRU and queue semantics, cancellation cleanup,
JSON-string byte budgets, loop detection, MCP resource formatting and workspace
budgets, ACP memory arithmetic, compaction input slimming, token estimates,
tool-use summaries, session-title shaping, session recap, memory diagnostics,
branch checkpoint validation, memory-pressure orchestration and native probes,
provider retry classification and startup retry behavior, usage-history replay,
tool-result retention analysis, and privacy-safe boundary diagnostics.
The Rust count and verification results are refreshed at each integration
checkpoint; they show scale, not behavioral parity. At the preceding full
test/release checkpoint on Darwin 25.6 arm64 with Rust 1.98.0, formatting,
strict all-target Clippy, and the workspace suite passed (868 core tests and 6
CLI tests); the locked release build and native `--version`/`--help` launch
checks also passed. After the latest shell, ACP, MCP, and Goal runtime slices,
`cargo fmt --all -- --check` and `cargo check --workspace --locked` pass. No
tests, strict Clippy run, or release build were run after these slices. A
source-volume audit counted about 136,500 production Rust LOC in
`rust/crates/*/src` against about 864,400 active TypeScript/JavaScript LOC in
`packages/**/src` (roughly 15.8%), across 397 Rust files and 3,297 TypeScript/
JavaScript files. This is a scale estimate, not a behavioral completion
percentage: files do not map one-to-one, and large CLI, desktop, and UI
surfaces remain incomplete. A separate CUA Rust workspace was excluded. A
local two-turn SSE stub also exercised the earlier optimized TTY
`ask_user_question` path.
These checks do not prove the requested long-session memory plateau or
recovery after killing a live process. Full Canopy tool execution, those
stress/recovery gates, and shipped UI surfaces remain unported or unverified.
Since that checkpoint, `cargo check -p canopy-cli` has passed after adding
WebFetch HTML conversion and host wiring, WebSearch host wiring, live-session
registration in native `run`, and the standalone extension helpers. This is a
compile check only; no tests were run for those changes, and runtime behavior
has not yet been exercised.
The native CLI now also supports a basic line-oriented multi-turn session when
started without a prompt in a terminal. It rebuilds provider history from the
active transcript between prompts, refreshes auto-memory per prompt, and accepts
`/help`, `/exit`, `/quit`, or Ctrl-D to finish. TTY sessions use a bounded
Ratatui chat UI with a fullscreen help guide and live usage view; the complete
Ink UI, command palette, approval dialogs, and most slash commands remain
unported. The Rust `skill` tool now has a static schema, parameter validation,
and the source-compatible skill-content projection. Native CLI and ACP both
construct a `SkillManager`, advertise available skills, activate path-gated
skills after filesystem calls, and route approved invocations through it.
Both hosts now apply skill `allowedTools` as session-scoped grants, with ACP
also applying them to MCP calls. ACP maps named and positional arguments and
falls back to discovered MCP prompts when no active file skill matches. The
CLI still lacks referenced-file hydration, skill hooks, model overrides,
telemetry, direct `/<skill>` commands, MCP prompt fallback, and live refresh.
ACP still lacks skill hooks, model overrides, extra-file hydration, telemetry,
bundled skills, and live refresh. See
`rust/crates/canopy-cli/src/skill_runtime_PORT_STATUS.md` and
`rust/crates/canopy-cli/src/acp_server_PORT_STATUS.md`. `ExtensionInventory`
retains parsed configs for validated active Canopy and Agent Plugins skills,
which both host managers now consume.
Android Robot now includes process
listing, orientation, UI hierarchy dumps, and screen-element extraction;
process listing has no tool in the current TypeScript mobile catalog. The
native CLI also exposes session group list/create/rename/color/delete,
pin/unpin, and session color commands, mapped from the organization service/API;
the TypeScript CLI has no corresponding subcommands. Mobile coordinate scale
parsing now matches JavaScript's decimal `parseInt` prefix behavior, and
mobilecli binary lookup better matches package-relative search while retaining a
PATH fallback for native Rust binaries. Latest `cargo fmt --all -- --check` and
`cargo check --workspace --locked` pass. The check reports three existing
warnings in `mcp_host.rs`; no tests or device runtime interactions were run for
these additions. The Node/V8 CLI remains the shipping runtime, so these Rust
checks do not resolve its reported OOM crash.
Since then, native `canopy run` and ACP load hierarchical instruction files
and baseline context rules at startup and include them with user system
instructions. Both register conditional rules in `WorkspaceTools`; the
post-tool hook matches argument paths and paths returned by `glob`/`grep`,
deduplicates in source order, and appends escaped rule reminders after output
truncation. This is connected only in the native CLI and ACP executors; other
hosts and instruction-loaded hook delivery remain open. The latest
`cargo fmt --all -- --check` and `cargo check -p canopy-cli --locked` pass,
with the same three existing `mcp_host.rs` warnings. No tests were run for this
integration. The last source-volume audit recorded about 15.8%; it predates
later Rust additions and is not a current behavioral completion estimate. The
Node/V8 CLI remains the shipping runtime, so these Rust changes do not resolve
its reported OOM crash.
Subsequent slices wire model-based auto-memory recall into native `run` and
ACP through the OpenAI-compatible JSON adapter, with deterministic fallback and
ACP cancellation; recent-tool exclusions, recall telemetry, and cross-auth or
model-specific endpoint routing remain open. The shared channel dispatcher now
handles `/who`, `/approve`, `/approve-always`, and `/deny` through generic host
callbacks. The native Telegram host invokes its supported command subset; it
now relays bounded ACP tool-permission requests through `/approve`,
`/approve-always`, and `/deny`; interactive user-input cards remain open. The CUA status note now reflects
native CLI and ACP approval wiring, with per-call ACP permission and first-use
installer disclosure. Deferred ToolSearch exposure, ACP-associated bootstrap
progress, outbound request cancellation in the protocol client, and an
explicit adapter cancellation token remain open.
Memory-pressure diagnostics already retain
the first RSS sample; the native monitor still measures RSS only while the
TypeScript monitor also considers V8 heap pressure. Latest `cargo fmt --all --
--check`, `cargo check -p canopy-cli --locked`, and `git diff --check` pass,
with three existing `mcp_host.rs` warnings. No tests were run for these latest
slices. The last source-volume audit recorded about 15.8%; it predates the
latest Rust additions and is not a current behavioral completion estimate. The
Node/V8 CLI remains the shipping runtime, so neither the full port nor the
reported OOM resolution is complete.

## Latest integration checkpoint (2026-09-26)

- `AgentRuntime::new_anthropic` now routes model turns through the native
  Anthropic Messages adapter. ACP selects it explicitly with
  `--provider anthropic` and resolves `ANTHROPIC_MODEL`,
  `ANTHROPIC_BASE_URL`, and `ANTHROPIC_API_KEY`. Interactive `canopy run` also
  accepts `--provider openai|anthropic|gemini` and resolves the matching model
  and endpoint environment. Native Gemini uses `GEMINI_MODEL` and
  `GEMINI_API_KEY`; it supports API-key access, not Vertex ADC. Provider
  settings/auth resolution, ACP auxiliary model calls, and Telegram provider
  selection remain incomplete.
- `canopy channel telegram [configured-name]` starts a native Telegram host,
  runs sender/DM/group/pairing gates before host commands, persists routes via
  `SessionRouter`, and forwards prompts to a managed ACP child. It relays ACP
  tool permission requests using chat-scoped `/approve`, `/approve-always`,
  and `/deny` commands. Image prompt support, durable polling offsets,
  interactive user-input cards, and the other channel platform hosts remain
  open. See
  `rust/crates/canopy-cli/src/telegram_host_PORT_STATUS.md`.
- The Ratatui CLI editor now has bounded multiline input, prompt history with
  draft restoration, and conversation paging. Most Ink screens and slash
  commands remain unported; see `docs/design/rust-native-tui-status.md`.
- `services::commit_attribution` is wired into native file-edit tools, shell
  commits, and session snapshot/restore. Safe foreground Git commits receive
  the configured co-author trailer and SHA-pinned attribution notes; complex
  shell syntax, redirected repositories, and arbitrary shell edits remain
  gaps. See `commit_attribution_PORT_STATUS.md`.
- The Rust CLI and ACP both read `general.preventSystemSleep` and pass it to
  the RAII sleep inhibitor. CUA tools are available through the native CLI and
  ACP consent path, subject to the gaps in the CUA port status note.
- A Rust CUA memory review found bounded tool arguments and several avoidable
  copies. Structured results now serialize the elements tree by reference,
  successful calls do not construct an unused error string, JSON-RPC results
  move out of their envelope instead of being deep-cloned, and the stdio reader
  releases line-buffer capacity above 1 MiB after large responses. Window
  screenshot capture now reuses the owned PNG when no resize is needed and
  releases the compressed source after decoding when a resize is needed. These
  changes reduce transient duplication without changing image output. Desktop
  screenshots remain full resolution, and the reports do not identify CUA as
  the OOM cause. The
  local `node-2026-09-24-001220.ips` report confirms a `SIGABRT` from
  `V8::FatalProcessOutOfMemory`; its faulting stack is in V8 garbage collection
  and allocation, without an application-level allocation site. The snapshot
  lists 269 MB resident across writable regions, which does not establish total
  process RSS or explain the failed allocation.
- Native memory-pressure monitoring now takes a prompt startup sample, keeps
  its 15-second periodic cadence, and receives coalesced checks after each
  attempted tool execution in CLI and ACP. A capacity-one signal bounds queued
  requests, and one task serializes checks and cleanup. This improves detection
  timing but does not establish an OOM fix; host-wide pressure and process-tree
  RSS are still not used to trigger cleanup.
- Native CLI and ACP now wire the `skill` tool to `SkillManager` and validated
  active extension skill configs. Full TypeScript skill parity remains
  incomplete; see the per-host skill status notes above.
- `--provider gemini` now constructs the native Gemini REST/SSE adapter in
  both `canopy run` and ACP. Request and stream sizes, deadlines, retries, and
  cancellation are bounded; Vertex credentials, token counting, and auxiliary
  model calls remain open. See `providers/gemini_PORT_STATUS.md`.
- The Rust TUI now supports batch Qwen ASR and realtime Qwen/DashScope voice
  transcription. Realtime capture uses bounded PCM queues, DNS-pinned
  connections, request limits, and cancellation. Interim text is not rendered;
  hardware and cross-platform capture behavior remain unverified. See
  `canopy-audio-capture/PORT_STATUS.md`.
- `canopy-core::browser_launch` ports the secure browser-launch policy, and
  `mobile-mcp` covers native Android plus physical/simulator iOS paths. The
  browser helper still has no Rust CLI caller, and mobile device interactions
  have not been exercised on hardware. See the browser and mobile port notes.
- Saved `modelProviders` settings now resolve in native ACP and `canopy run`,
  including custom `providerProtocol` IDs, exact model IDs paired with their
  base URLs, and `envKey` credentials. Explicit `--provider`, `--model`, and
  `--base-url` values retain precedence. Provider setup/auth UI remains open.
- Ordinary `@path` file references now add bounded, modality-aware file context
  to interactive, one-shot, and clean-resume CLI prompts. `@{path}` custom
  command wiring, multiple roots, and success cards remain open. A new
  host-neutral resolver now covers active `@ext:` contexts, `@mcp:` server
  context, and discovered `@server:uri` resource reads. The native CLI now
  wires `@mcp:` and `@server:uri` into interactive, one-shot, and clean-resume
  prompts. The bounded, read-only extension inventory now loads installed user
  extensions from the QWEN_HOME-aware directory, validates supported Canopy
  and Agent Plugins manifests, and applies activation, trust, safe/bare-mode,
  and CLI override gates. CLI and ACP now supply active descriptors to `@ext:`
  prompt resolution. Native CLI and ACP also use the same bounded active
  Canopy extension `mcpServers` and normalized Agent Plugins root `mcp.json`
  sources in MCP settings assembly. Settings and project entries keep
  precedence, extensions fill missing names, and ACP session entries override
  extensions. Agent Plugins data directories are not created during inventory,
  so a stdio server that needs a missing `${PLUGIN_DATA}` working directory is
  unsupported.
  Workspace extension directories, unsupported manifest layouts, and the
  broader extension install/management/runtime remain open.
- Native interactive `/stats` and `/usage` open a four-tab live usage screen
  in the full-screen TUI; model and tool details read bounded runtime counters,
  while skills report unavailable until native skill events exist. Command
  arguments, daily/monthly summaries, and bounded CSV/JSON export keep their
  text path. ACP intercepts plain-text stats commands before model work and
  returns bounded session-scoped replies. The full TypeScript efficiency
  dashboard and detailed stats for `canopy run --prompt` remain incomplete.
- The native interactive CLI registers the rich HTML `ArtifactTool` with
  configured publisher settings and permission checks. ACP exposes
  `record_artifact` metadata only; it does not provide live artifact-list
  updates.
- Native runtime turns record token usage through a best-effort blocking write
  and honor `privacy.usageStatisticsEnabled`, provider auth type, and source
  labels. Background prompt-ID propagation remains incomplete.
- The native CLI resolves and refreshes stored MCP OAuth credentials for
  servers with `oauth.enabled: true` and reuses saved credentials on network
  transports when OAuth is not explicitly configured, while preserving an
  explicit `Authorization` header. Interactive `canopy run` now handles MCP
  401 challenges with metadata discovery, PKCE browser authorization, token
  persistence, and server rediscovery. ACP now gates browser OAuth behind
  `session/request_permission`, bounds consent and authorization waits, and
  connects session cancellation to the flow. Clients must support permission
  requests while `session/new` or restore is still being prepared; remote ACP
  clients also cannot complete the host's loopback callback. The `/mcp` setup
  UI and source auth-URL events remain open.

## Latest integration checkpoint (2026-09-26)

- The native Ratatui conversation view now enables terminal mouse capture and
  maps each wheel event to three conversation rows. Capture is disabled during
  tool prompts and terminal cleanup; the fullscreen UI remains a partial Ink
  replacement. See `docs/design/rust-native-tui-status.md`.
- The native QQ Bot host now handles ACP permission requests by selecting
  `reject_once` or `cancel`, preventing an unanswered client request from
  hanging a prompt. Interactive QQ approval cards and `/approve` relay remain
  open. See `rust/crates/canopy-cli/src/qqbot_host_PORT_STATUS.md`.
- `canopy channel weixin [configured-name]` now covers configured channel
  selection, QR login and account persistence, DM/sender gates, persisted ACP
  session routing, polling, and text replies. Polling waits for the message
  handler and reply before saving the next cursor. Inbound image/file handling
  and broader `ChannelBase` behavior remain open. See
  `rust/crates/canopy-cli/src/weixin_host_PORT_STATUS.md`.
- The new `canopy-sdk` Rust crate provides a process-backed stream-json query
  API, bounded JSON-lines transport, initialization, stream input, and common
  control requests. Bundled CLI lookup, typed messages, callbacks, hooks,
  SDK-hosted MCP, and abort-controller parity remain open. See
  `rust/crates/canopy-sdk/PORT_STATUS.md`.
- ACP compaction now stores separate full and summary journals with individual
  truncation accounting and transcript record anchors. The ACP runtime reader
  also bounds each input line to 16 MiB, queues at most two pending lines, and
  runs no more than 16 request tasks at once while continuing to process
  cancellation input. The executable still does not derive an adaptive journal
  pool or feed persisted transcript replay through the compaction owner;
  transcript metadata grouping, subagent reassembly, and replay telemetry also
  remain open. See the ACP bridge and server status notes.
- `cargo fmt --all -- --check`, `cargo check --workspace --locked --offline`,
  and `git diff --check` pass. The compile reports three existing warnings in
  `mcp_host.rs`; tests were not run.
- A physical source-line audit counted 264,389 lines in 445 files under
  `rust/crates` and 1,438,389 non-Rust lines in 4,012 `packages` files. The
  latter includes TypeScript, JavaScript, Python, Java, C/C++, CSS, and HTML;
  separate test directories/files and generated/build paths were excluded.
  This gives a **15.5%** Rust source-volume proxy. Counting the separate,
  pre-existing CUA Rust workspace adds 183,612 lines and gives **23.8%**.
  Co-located Rust unit tests may be included, and neither ratio measures
  behavior or feature parity.

The Node/V8 executable remains the shipped CLI, and the reported OOM cause is
unresolved. No long-session memory plateau, process-kill recovery, live channel,
or device-runtime workload was run for this checkpoint.

At this checkpoint, `cargo fmt --all -- --check`, `git diff --check`, and
`cargo check -p canopy-cli --locked --offline` pass. The compile reports three
existing warnings in `mcp_host.rs`; tests were not run. No new source-volume
audit, long-session memory workload, or process-kill recovery workload was
run. The last source-volume audit was about 15.8% and predates these additions;
that is not a behavioral completion estimate. The Node/V8 executable is still
the shipped CLI, and the reported OOM cause remains unresolved.

## Follow-up integration checkpoint (2026-09-26)

- Rust ACP session restore now seeds its compaction owner from persisted
  transcript records and replays the compacted snapshot for bulk or streamed
  `session/load`. Top-level `liveReplayMode: full|summary` is honored; update
  metadata absent from the Rust transcript model remains unavailable.
- The Rust SDK now supports the `can_use_tool` callback with typed permission
  suggestions, a 60-second default timeout, panic/error handling, and
  fail-closed response validation. Abort signals and several SDK host features
  remain open; see `rust/crates/canopy-sdk/PORT_STATUS.md`.
- Native QQ group `all` and `keyword` policies now route unmentioned group
  messages, with NFC normalization, source-style keyword boundaries, ordered
  handling, and the configured `requireMention: false` gate. QR login, media,
  shared channel commands, and interactive permission relay remain open.
- Weixin, Telegram, and QQ ACP subprocess readers now cap each JSONL output
  line at 16 MiB. Oversized frames fail pending requests and terminate the
  child. ACP stdin remains capped at 16 MiB with two queued lines and at most
  16 request tasks.
- MCP stdio JSON-RPC frames are now bounded incrementally at 64 MiB while
  reading. Daemon ACP child stderr retains at most a 256 KiB line and omits
  oversized lines, avoiding unbounded log-line buffers.
- The CUA audit found that inline screenshots can remain full-resolution and
  be copied through the model history, session recorder, and live tool event.
  The runtime's history cap is 12 MiB, while default image externalization
  begins at 20 images, so a few large captures can fail the history limit
  first. This is a plausible transient RAM spike, but the available evidence
  does not prove it caused the reported crash. No long-session memory plateau
  or paired Canopy/CUA RSS capture has been collected.
- `cargo fmt --all -- --check`, `cargo check --workspace --locked --offline`,
  and `git diff --check` pass. The check reports three existing `mcp_host.rs`
  warnings; tests were not run.
- The source-line audit counts 265,137 lines in 447 Rust crate files and
  1,440,228 non-Rust lines in 4,023 package files, excluding separate test
  files/directories and generated/build paths. This gives a **15.55%**
  Canopy-only source-volume proxy. Including the separately maintained CUA
  driver's 187,829 Rust lines across 324 files gives **23.93%**. These figures
  are not feature-parity percentages.

The Node/V8 executable remains the shipped CLI. The OOM cause remains
unconfirmed and the repository-wide port is incomplete.

## Current integration checkpoint (2026-09-26)

- The native CLI and ACP now schedule managed auto-memory extraction after
  successful turns through the existing planner and `MemoryManager`. ACP shares
  one manager per workspace and drains queued tasks on shutdown. The native
  extraction host uses temporary fork transcripts, project/user memory roots,
  permission rechecks, pinned-file protection, and memory-pressure gating. Its
  foreground shell tool passes the Bash AST read-only check, rejects background
  execution, and resolves its starting directory inside the workspace; dream
  and skill-review scheduling remain open.
- ACP now exposes discovered MCP prompts through the `skill` tool, parses
  named/positional arguments, prefers active skills, preserves path gates,
  propagates cancellation, and bounds prompt listing, arguments, and returned
  text. Skills and MCP prompts are session snapshots; the bundled TypeScript
  skill tree and several skill execution semantics remain unported.
- The Rust SDK now supports query-wide cancellation, a bounded stderr callback
  path, a configurable/default 60-second MCP request callback timeout, typed
  subagent configs in `initialize`, and SDK-hosted MCP server configs with
  per-query JSON-RPC routing. Typed nested content blocks and usage metadata
  retain their raw JSON for unknown variants. SDK hook registration and the
  remaining MCP resource/prompt methods are still open.
- Native Weixin now dispatches `/help`, `/status`, `/who`, `/clear`, `/reset`,
  and `/new` after sender/scope checks. Clear removes the scoped route and sends
  ACP cancel/close notifications. The serial poller still cannot process a
  second command while waiting on an active prompt, and Weixin permission
  approval relay remains unavailable.
- Native QQ now relays ACP permission requests to the originating chat through
  `/approve`, `/approve-always`, and `/deny`. Pending state is bounded and
  process-local; timeout, cancellation, disconnect, queue saturation, and
  delivery failure reject the request. Telegram media rejects declared
  oversize files before transfer and enforces 8 MiB photo and 32 MiB document/
  voice caps while streaming, including for custom API implementations.
- A CUA image-path review found concrete peak-memory multipliers, but no proof
  that CUA caused the reported crash. Window screenshots default to a 1568-pixel
  maximum dimension; desktop screenshots remain native resolution and bypass
  resizing. The Rust image utility avoids one compressed-PNG copy and drops
  compressed input earlier during resize, but still decodes the full image
  before resizing. In Node, base64 image data can remain in model history and
  active transcript records; the 8 MiB image-reference cache does not bound
  those owners. A local macOS diagnostic report dated 2026-09-24 records
  `node::OOMErrorHandler` followed by V8 `FatalProcessOutOfMemory` during GC,
  ending in `SIGABRT`. Its VM summary lists 269.2 MiB resident in writable
  regions, but it contains no preceding RSS/heap timeline, workload details,
  or CUA attribution. This confirms a Node/V8 allocation failure, not its
  trigger; no long-session plateau or paired Canopy/CUA RSS capture has been
  collected.
- `cargo fmt --all -- --check`, `cargo check --workspace --locked --offline`,
  and `git diff --check` pass for the main Rust workspace. The separate CUA
  workspace also passes `cargo check --workspace --locked` after fetching
  dependencies missing from the local offline cache. Existing warnings remain
  in `mcp_host.rs` and the platform overlay implementations. Tests were not
  run.
- The refreshed source-volume estimate counts 272,480 Rust lines across 449
  files under `rust/crates/**/src`, against the last measured 1,445,378
  non-Rust lines across 4,077 package source files. This is **15.86%** for
  Canopy. Counting 183,420 current CUA Rust lines under its crate `src`
  directories gives **23.98%** combined. These are scale estimates, not
  behavioral completion percentages, and the package denominator predates the
  latest additions.

The Node/V8 executable remains the shipped CLI, so these Rust additions do not
change its crash behavior. The diagnostic confirms V8 ran out of memory, but
the allocation trigger remains unknown and the report does not implicate CUA.

## Follow-up integration update (2026-09-26)

- `canopy run --prompt` now handles `/stats` and `/usage` for fresh sessions
  and clean resumes by using the active runtime metrics and skipping a model
  call. A one-shot run still has no historical model/tool counters when none
  have been collected; the interactive dashboard remains separate.
- ACP prompts can now carry supported inline audio. Gemini accepts its
  supported audio MIME types; OpenAI-compatible models accept WAV/MP3 only
  when the configured modality profile enables audio. The decoded payload cap
  is 10 MiB, validation and conversion observe cancellation, and unsupported
  combinations return explicit errors. The text-only-model transcription
  bridge is still absent.
- The SDK receive API now exposes typed user, assistant, system, result, and
  stream event/delta variants, nested content blocks, and usage metadata while
  retaining raw JSON for unknown variants. The current TypeScript SDK exposes
  no hook registration API and sends `hooks: null`; Rust preserves that
  contract. SDK-hosted MCP configs and lifecycle are implemented, including
  context-aware tool/resource/prompt handlers with request IDs, `_meta`, and
  cancellation. Resource templates and server-originated MCP requests remain
  open; the external `mcp_message` callback remains as a fallback.
- The native interactive CLI now schedules memory extraction, dream, and skill
  review through its memory manager after successful prompts, with settings,
  safe/bare-mode gates, scoped tools, cancellation, and configured limits.
  ACP dream and skill review remain disabled; CLI managed-memory status and
  pending-skill resolution commands are still missing. Dream can now search
  session transcript JSONL through foreground shell commands that pass the
  read-only classifier; output, command size, and runtime are bounded.
- ACP skill invocation now applies declared `allowedTools` as session-scoped
  grants to both the CLI tool host and MCP authorizer. Configured deny/ask
  rules retain precedence. Weixin permission prompts now use bounded per-session
  FIFO queues, allowing `/approve`, `/approve-always`, and `/deny` while another
  prompt is active; expiry, disconnect, relay failure, saturation, and shutdown
  fail closed.
- CUA capture paths now transfer owned RGBA/BGRA buffers directly into PNG
  encoding. BGRA channel conversion is in-place, and Linux X11 source bytes are
  dropped before encoding. The macOS debug screenshot path now transfers its
  owned PNG through resize; existing Linux and Windows screenshot tools use
  the same owned resize API. These changes remove avoidable full-buffer copies
  but do not bound native-resolution desktop capture or establish the cause of
  the reported OOM.
- `cargo fmt --all -- --check`, `cargo check --workspace --locked --offline`,
  and `git diff --check` pass for the main Rust workspace after these
  integrations. The separate CUA workspace also passes its locked check.
  Existing `mcp_host.rs` and platform overlay warnings remain. Tests were not
  run.
- A fresh source-volume scan counts 274,892 Rust lines across 449 files under
  `rust/crates/**/src` and 2,366,147 TypeScript/JavaScript lines across 5,237
  files under `packages/**/src`, giving **10.41%** for Canopy. It counts test
  sources and Rust inline test modules, and excludes dependency/build output
  directories. The separate CUA source contributes 183,420 Rust lines across
  285 crate source files, giving **16.23%** combined. This broader count
  supersedes the stale 15.9%/24.0% estimate above; these figures measure source
  volume only, not behavioral completion.

The Node/V8 executable remains the shipped CLI. The Rust port is incomplete,
and the reported OOM cause remains unconfirmed.

## Follow-up integration update (2026-09-26)

- The native CLI now implements `zoom_image` for static PNG, JPEG, and WebP
  images and exposes it through both native CLI and ACP tool declarations.
  Decoded buffers are capped at 96 MiB, each image dimension at 32,768 pixels,
  and a process-wide semaphore allows one image decode at a time. Workspace
  path checks, configured ignore files, symlink-safe Unix opens, and
  path-specific Ask/Deny rules are wired, and JPEG output uses 4:4:4 sampling.
  Outside-workspace behavior, telemetry, cancellation, and ancestor-path race
  gaps are listed in
  `rust/crates/canopy-core/src/tools/image_view_PORT_STATUS.md`.
- Native ACP workspaces now share an adaptive journal-growth budget derived
  from host/cgroup memory, with a single registry across workspace runtimes.
  Its growth pool follows the TypeScript 5% policy. The probe's supported
  cgroup paths and native budget flags remain narrower; see
  `rust/crates/canopy-cli/src/acp_server_PORT_STATUS.md`.
- `cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked --offline`
  passes after these integrations. Existing warnings remain in `mcp_host.rs`.

## Current source-volume estimate (2026-09-26)

For a comparable count, TypeScript/JavaScript includes source files under
`packages/**/src`; main Rust includes `.rs` files under `rust/crates/**/src`;
the separately maintained CUA Rust source includes `.rs` files under
`packages/cua-driver/rust/crates/**/src`. Counts include package test sources
under `src` and Rust inline test modules, and exclude dependency and build
directories. The current totals are 2,366,146 TypeScript/JavaScript lines
across 5,237 files, 294,282 main Rust lines across 469 files, and 183,545 CUA
Rust lines across 285 files. By this line-volume measure, Rust accounts for
11.1% of the main package source volume, or 16.8% when the CUA driver is
included. These are scale estimates, not behavioral completion percentages.
The Node/V8 CLI remains the shipped executable.

## Follow-up integration update (2026-09-26)

- Rust MCP OAuth storage now includes the source-compatible encrypted-file
  backend and a macOS Keychain backend. CLI and ACP retain the plaintext
  default; `CANOPY_CODE_FORCE_ENCRYPTED_FILE_STORAGE=true` selects Keychain
  when its set/get/delete probe succeeds and otherwise falls back to the
  encrypted file. `CANOPY_CODE_FORCE_FILE_STORAGE=true` skips Keychain.
  Keychain operations use Apple's Security Framework through the dual
  MIT/Apache-2.0 `security-framework` crate. The protected selector also
  implements `SecretStorage`, but extension settings do not use it yet. The
  native keychain implementation is macOS-only and has compile coverage but no
  live keychain interoperability check; Windows/Linux native secret stores
  remain unported. See
  `rust/crates/canopy-core/src/mcp/token_storage_PORT_STATUS.md`.
- `canopy-sdk::AutoReconnectTransport` now has ACP HTTP/WebSocket adapters and
  route dispatch. Closed errors retry once for REST fetch, route dispatch, and
  event streams; ACP cancellation is forwarded. Raw fetch remains unavailable
  for ACP adapters, and WebSocket reconnect cannot replay missed events. See
  `rust/crates/canopy-sdk/PORT_STATUS.md`.
- `canopy-core::channels::gitlab_adapter` now ports GitLab notification
  polling, cursor persistence, ordered todo processing, note handling, and
  acknowledgement lifecycle through an injectable API and bounded reqwest
  client. Daemon/CLI registration, auth resolution, and prompt dispatch remain
  host integration work. The larger GitHub adapter port is in progress.
- Locked offline `cargo check` passed for `canopy-core` after the Keychain
  addition and for `canopy-sdk` after ACP reconnect adapter work. No tests were
  run.

## Follow-up integration update (2026-09-27)

- `canopy channel wecom [configured-name]` now has a native foreground host
  connected to the WSS client, sender/group/DM gates, pairing, persisted ACP
  sessions, media lifecycle, and shutdown cleanup. The host bounds work to
  four active callbacks plus 32 queued frames, caps each inbound message at 16
  media references and 32 MiB aggregate media, and bounds retained ACP updates
  and final response text. Full shared `ChannelBase` behavior and daemon
  registration remain open.
- `canopy channel gitlab [configured-name]` now has a foreground native host
  wired to the polling adapter, shared authorization gates, pairing,
  `SessionRouter`, ACP, thread-aware notes, and shutdown cancellation. It also
  remains outside the TypeScript daemon lifecycle and lacks shared
  `ChannelBase` command/memory/loop surfaces.
- The Rust SDK now exposes its daemon REST/SSE facade and a growing set of
  typed endpoint wrappers, preserving raw JSON while adding route helpers.
  At this checkpoint, the separate workspace-prefixed `WorkspaceDaemonClient`
  wrapper was the next SDK slice. No tests were added or run.
- Locked, offline `cargo check` passes for `canopy-core`, `canopy-cli`, and the
  current `canopy-sdk` snapshot. CLI compilation reports three existing
  warnings in `mcp_host.rs`; workspace-wide formatting has not yet been rerun
  after the active SDK edits.
- A fresh raw source-volume snapshot counts 2,366,147 TypeScript/JavaScript
  lines across 5,237 files under `packages/**/src`, 314,286 Rust lines across
  478 files under `rust/crates/**/src`, and 183,545 CUA Rust lines across 285
  files under `packages/cua-driver/rust/crates/**/src`. This is **11.7%** for
  the main Rust workspace and **17.4%** when the separate CUA source is
  included, using Rust lines divided by measured TypeScript/JavaScript plus
  Rust lines. These figures include package test sources and Rust inline test
  modules. They are source-volume proxies, not functional completion scores;
  the in-progress SDK workspace wrapper is not in this snapshot.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- ACP session artifacts now revalidate persisted IDs and locators against the
  current workspace, restore tombstone and sticky-ephemeral markers, preserve
  live ephemeral entries when requested, and roll back incomplete restores.
  `SessionRecorder` retains the restore warnings. Rust session media now has a
  helper to preserve unaffected blocks while degrading only missing media
  refs, though its mid-turn serve caller is not ported yet. ACP compaction now
  rejoins interleaved subagent text and thought chunks by parent tool and
  source-record identity.
- Native Feishu/Lark dispatch is wired. The adapter covers authenticated
  webhooks, ACP session routing, cards, media, memory, and observed contacts;
  callback-frame delivery and several channel command/lifecycle details remain
  open.
- Rust `serve` now has an explicit `--transport-smoke` preview on loopback with
  bounded HTTP/1 handling, graceful shutdown, and shallow/bootstrap health
  responses. Plain `canopy serve` reports that daemon parity is incomplete;
  workspace and ACP routes, auth, Web Shell, channel services, and full deep
  health are not implemented.
- The standalone updater now syncs staged files and directories around Linux
  and macOS atomic directory exchange. The release signing key still needs to
  be embedded at build time; Windows directory-sync guarantees are unavailable
  through this implementation.
- Locked offline workspace `cargo check` passes. Four existing dead-code
  warnings remain in CLI MCP, review, and update modules. No tests were added
  or run.
- The latest `packages/**/src` source-volume snapshot is 2,367,864
  TypeScript/JavaScript lines across 5,250 files, versus 342,709 Rust lines
  across 504 files under `rust/crates/**`, and 209,898 CUA Rust lines across
  361 files under `packages/cua-driver/rust/crates/**`. That is 14.47% main
  Rust source volume, or 23.34% including CUA. These are LOC comparisons, not
  behavior-parity percentages.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- The external-context integration now has a Rust `UserPromptSubmit` auto-recall
  binary build script, managed Hook settings examples for POSIX and Windows,
  and setup documentation. The Rust Hook preserves submitted-prompt
  provenance, bounds input and query sizes, and applies provider and wall-clock
  cancellation. The integration crate is outside the source-volume count below.
- The native WeCom host now dispatches the shared `/help`, `/new`, `/clear`,
  `/reset`, `/cancel`, `/status`, `/who`, and permission command paths after
  sender/group/DM authorization and before session resolution. The clear
  operation keeps shared-session confirmation and sends replies to the
  originating chat.
- GitHub final-response publication recovery is still in progress; its
  implementation is not counted as complete here. Extension marketplace
  source management remains the only standalone Rust extension CLI group;
  install, uninstall, enable, and disable still need the native manager layer.
- A refreshed source-volume snapshot counts 2,367,840 TypeScript/JavaScript
  lines across 5,249 files under `packages/**/src`, 317,622 main Rust lines
  across 480 files under `rust/crates/**/src`, and 183,545 CUA Rust lines
  across 285 files under `packages/cua-driver/rust/crates/**/src`. Rust is
  **11.8%** of the measured main source total, or **17.5%** when the separate
  CUA source is included. This is a line-volume proxy; there is no reliable
  behavior-weighted completion percentage yet.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- `canopy-sdk` now exports the workspace-prefixed daemon client across the
  TypeScript `WorkspaceDaemonClient` route catalog. The primary daemon client
  also implements cancellation-aware extension-operation polling and validated
  workspace/session generated-content SSE streams. Rust keeps open JSON
  endpoint payloads; browser `fetch`/`Response`, XHR upload progress, and raw
  ACP fetch responses remain outside the port. See
  `rust/crates/canopy-sdk/PORT_STATUS.md` and
  `rust/crates/canopy-sdk/src/workspace_daemon_client_PORT_STATUS.md`.
- `canopy extensions sources add|remove|list|update` now ports marketplace
  source registry management on the Rust CLI using the existing core APIs.
  It preserves source names on update and redacts credentials in listed URLs.
- `cargo fmt --all -- --check`, `cargo check --workspace --locked --offline`,
  and `git diff --check` pass. The CLI compile retains three unrelated
  `mcp_host.rs` warnings. No tests were added or run.
- The refreshed source-volume count is 2,367,880 TypeScript/JavaScript lines
  across 5,253 files under `packages/**/src`, 316,878 main Rust lines across
  480 files under `rust/crates/**/src`, and 183,545 CUA Rust lines across 285
  files under `packages/cua-driver/rust/crates/**/src`. Rust is **11.80%** of
  the measured main source total, or **17.45%** when the separate CUA source is
  included. These counts include source tests and Rust inline tests; they are
  source-volume proxies, not feature-completion scores.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- GitHub final-response publication now has a durable audit and pending-delivery
  record, retry, and visible-comment reconciliation. GitLab, DingTalk, WeCom,
  and QQ Bot dispatch shared inbound commands after authorization; GitLab also
  has a bounded side poller for `/approve`, `/approve-always`, and `/deny` while
  a normal ACP prompt is waiting. DingTalk now relays bounded ACP permission
  requests through its per-chat webhook. QQ Bot routes the shared slash-command
  set and supports memory CRUD with scoped confirmations for classifier-based
  mutations. Other channel lifecycle and memory parity, plus agent-command
  discovery, remain incomplete.
- `canopy extensions list` includes disabled entries and user/workspace
  activation state. Native `enable` and `disable` mutate the v2 snapshot and
  legacy projection under the shared lock with atomic writes. Native uninstall
  now uses a recoverable journal transaction with rollback and bounded local
  inventory. Install is still unavailable until Rust download, extraction,
  conversion, consent, and staging validation are ported; runtime refresh and
  telemetry also remain gaps.
- Both external-context Rust binaries now have target-specific extension
  packaging, generated manifests, target metadata, README, and LICENSE. The
  TypeScript entrypoints remain unchanged. Exact Undici proxy parity and
  cross-target package validation remain open.
- MIT Rust framework alternatives were reviewed against Canopy's current
  provider/runtime boundary. Keep `AgentRuntime`; Rig would overlap the existing
  tool, permission, and session loops, while `rust-genai` is a possible narrow
  provider-client experiment with a reqwest 0.13 versus 0.12 compatibility cost.
  Neither framework establishes lower RSS or crash risk. See
  `docs/design/rust-llm-framework-evaluation.md`.
- Main, external-context, and CUA Rust format checks pass. The main workspace
  and external-context crate pass locked offline `cargo check`; the CUA
  workspace also passed earlier in this session. The package scripts built
  both external-context profiles on `aarch64-apple-darwin` and passed Node
  syntax/Prettier checks. The CLI compile reports three existing `mcp_host.rs`
  warnings. No tests were added or run.
- The refreshed source-volume snapshot counts 2,366,107 TypeScript/JavaScript
  lines across 5,233 files under `packages/**/src`, 323,362 main Rust lines
  across 483 files under `rust/crates/**/src`, and 183,545 CUA Rust lines
  across 285 files under `packages/cua-driver/rust/crates/**/src`. Rust is
  **12.02%** of the measured main source total, or **17.64%** when the separate
  CUA source is included. These counts include source tests and inline Rust
  tests; they are source-volume proxies, not feature-completion scores. Counts
  use the currently visible source paths and may shift when ignored or generated
  files enter or leave the workspace.

## Follow-up integration update (2026-09-27)

- The Rust CLI now installs extensions from local paths, Git/GitHub, HTTPS
  archives, and npm sources. It checks workspace trust, bounds and streams
  downloads, converts supported Agent Plugins/Gemini/Claude/Qoder packages,
  shows a sanitized consent preview, validates staged inventory, then commits
  through the recoverable activation journal. Update commands, npmrc
  interpolation and legacy auth formats, programmatic settings callbacks,
  exact Git transport behavior, runtime refresh, and install telemetry remain
  open.
- DingTalk and WeCom now manage channel memory with scoped confirmations and
  guarded updates/removals. DingTalk and Telegram retain bounded ACP command
  catalogs per session and forward recognized names and aliases after channel
  authorization. The Rust ACP server still does not publish a command catalog
  on session creation/load; catalogs stay empty until a later skill refresh
  update arrives.
- Native `canopy run` now accepts up to five stacked direct skill commands in
  one prompt. It clears stale per-skill argument files, applies each skill's
  declared tool permissions, submits the combined skill content and trailing
  prompt text, and warns when further recognized skill tokens exceed the
  limit. Dynamic skill command completion, slash-command usage telemetry,
  hooks, and per-skill model overrides remain open.
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check` and
  `CARGO_BUILD_JOBS=2 cargo check --manifest-path rust/Cargo.toml --workspace
--locked --offline` pass after these changes. Three existing
  `mcp_host.rs` warnings remain. No tests were added or run.
- The refreshed source-volume snapshot counts 2,366,106 TypeScript/JavaScript
  lines across 5,233 visible files under `packages/**/src`, 328,354 main Rust
  lines across 484 files under `rust/crates/**/src`, and 183,545 CUA Rust lines
  across 285 files under `packages/cua-driver/rust/crates/**/src`. Rust is
  **12.19%** of the measured main source total, or **17.79%** when the separate
  CUA source is included. The CLI and core contain 1,721,262 TypeScript/
  JavaScript lines versus 301,782 main Rust lines. These figures include
  source tests and Rust inline tests; they measure source volume, not behavior
  parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- Native ACP now publishes each session's `available_commands_update` after a
  successful `session/new`, `session/load`, or resume response has been
  flushed. The notification is scoped to the returned session ID and includes
  that session's visible skills and connected MCP prompts. Telegram, DingTalk,
  Weixin, WeCom, and QQ Bot retain bounded per-session catalogs, restore them
  from load replay, and route only validated recognized names and aliases.
- `canopy extensions update <name|--all>` is wired into the Rust CLI. It reuses
  bounded source acquisition and conversion, validates staged inventory and
  stable identity, preserves unchanged settings, and commits with the
  recoverable update journal. TypeScript's lightweight no-update probes,
  interactive settings reconfiguration, live extension-manager refresh, and
  parallel all-update behavior remain open.
- Native direct skills can be stacked up to five per prompt, and the TUI
  completes eligible skill names both at the root and in subsequent stack
  positions. The Rust package helper now copies `bundled/skills` beside the
  release binary; npm still launches the Node/V8 CLI and release automation
  has not switched to this package.
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`, locked offline
  workspace `cargo check`, and `git diff --check` pass. Three existing
  `mcp_host.rs` warnings remain. No tests were added or run.
- The refreshed source-volume snapshot counts 2,367,840 TypeScript/JavaScript
  lines across 5,249 files under `packages/**/src`, 330,123 main Rust lines
  across 485 files under `rust/crates/**/src`, and 183,545 CUA Rust lines
  across 285 files under `packages/cua-driver/rust/crates/**/src`. Rust is
  **13.94%** of the measured main source total, or **21.69%** when the separate
  CUA source is included. These counts include source tests and Rust inline
  tests; they measure code volume, not behavior parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- The native CLI now wires `extensions new`, `extensions link`, and
  `extensions update`. Settings `list` reports user and workspace values with
  workspace precedence and redacts sensitive values. `set` prompts by setting
  name or env var, then stores sensitive values in keychain/encrypted storage
  or writes non-sensitive values through atomic `.env` updates.
- The native MCP CLI now wires `add`, `list`, `remove`, `approve`, `reject`,
  and `reconnect`. Add/remove preserve unrelated JSONC settings with atomic
  writes. List gates project/workspace probes on current config-hash approvals;
  reconnect requires a trusted workspace and current approvals for gated
  servers, then initializes and discovers tools with bounded connection and
  cleanup. `canopy auth` prints the existing migration notice.
- `canopy update` checks npm `latest`/`nightly` tags with a 5-second timeout
  and 128 KiB response cap, then prints update instructions. It does not run an
  installer; standalone download and replacement remain unported.
- The Rust package helper includes bundled skills and extension examples, but
  npm and release automation still launch the Node/V8 CLI.
- `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`, locked offline
  workspace `cargo check`, and `git diff --check` pass. The workspace check
  reports two unused-code warnings in `mcp_host.rs`. No tests were added or
  run.
- The refreshed source-volume snapshot counts 2,367,840 TypeScript/JavaScript
  lines across 5,249 files under `packages/**/src`, 333,707 main Rust lines
  across 495 files under `rust/crates/*/src`, and 183,545 CUA Rust lines across
  285 files under `packages/cua-driver/rust/crates/*/src`. Main Rust lines are
  **14.09%** of the TypeScript/JavaScript baseline; including CUA, Rust lines
  are **21.84%** of that baseline. These counts include source tests and inline
  Rust tests; they measure code volume, not behavior parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- The native CLI dispatches `canopy hooks` and its `canopy hook` alias. The
  interactive prompt also routes `/hooks` to a read-only TUI browser; line
  mode prints a concise listing. It reads user, project, and active-extension
  hook definitions without starting hook runners.
- A `serve` audit confirms the Rust CLI has no HTTP daemon. TypeScript's
  command, startup runtime, server, and route families span a much larger
  subsystem; the audit estimates 30–52 person-weeks for full behavior parity,
  with substantial uncertainty before a transport spike.
- `canopy review meta` is now dispatched as a Rust command and keeps usage
  errors at exit 2 and GitHub/auth/runtime errors at exit 1.
- `canopy update` now has a verified archive download and replacement path for
  bundled Node standalone installs. It requires signed checksums and a
  production public key embedded at build time; without that release key it
  fails closed. Native Rust installs still receive package-manager guidance.
  The release signing key is not wired yet, stale lock files need manual
  recovery, and sudden power-loss durability still needs file and directory
  fsync handling. Linux and macOS now use atomic directory exchange so a
  process crash does not leave the executable path missing between renames.
- ACP now has the bounded generation queue primitive and fatal-report support;
  the native CLI installs the sanitized panic-report hook at startup. The
  generation queue has no consumer yet because the HTTP `serve` path is still
  absent.
- `cargo fmt --check -p canopy-cli`, locked offline workspace `cargo check`,
  and `git diff --check` pass. Existing dead-code warnings remain in
  `mcp_host.rs` and adjacent CLI modules. No tests were added or run.
- The comparable `packages/**/src` snapshot is 2,367,635 TypeScript/JavaScript
  lines across 5,241 files, 338,270 Rust lines across 500 files under
  `rust/crates/*/src`, and 183,545 CUA Rust lines across 285 files under
  `packages/cua-driver/rust/crates/*/src`. That gives **14.29%** main Rust
  source volume, or **22.04%** including CUA. A broader whole-repository
  TypeScript/JavaScript count gives **12.67%** main Rust or **19.73%** including
  CUA. Both are line-volume comparisons, not feature-completion percentages;
  the missing `serve` subsystem alone remains a large parity gap.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- Native `/doctor memory` is available in the Rust interactive CLI and ACP. It
  reports bounded RSS, host/effective memory limits, available process metrics,
  and optional three-sample RSS movement as text or JSON. V8 heap counters and
  heap snapshots remain unavailable in the native runtime.
- Plain `canopy serve` starts a bounded loopback HTTP/1 daemon with
  `/health`, `/capabilities`, `/daemon/status`, and a `GET`/`HEAD`
  `/session/:id/status` route. Session status uses bounded direct catalog
  lookup and runtime-sidecar/PID liveness checks. `--token` overrides
  `QWEN_SERVER_TOKEN`; a configured token gates every route, and `--require-auth`
  requires a nonempty token at startup. Live bridge activity counters are
  placeholders; session streaming and other route families remain unported.
- Linked extension settings now flow into matching Rust MCP stdio servers in
  session discovery and `mcp list` probes. User/project precedence is retained,
  explicit server and inherited environment values keep their priority, and
  secrets are not added to shared settings or logs.
- Feishu ACP permission requests use bounded action cards with sender, chat,
  session, route, and message checks. `/approve`, `/approve-always`, and `/deny`
  resolve matching tool permissions; `/deny` also cancels one exact pending
  user-input question after sender, chat/thread, run, and route checks. Delivery
  failures and timeouts reject permission requests. Remaining question-command
  and adapter lifecycle parity is incomplete.
- Full workspace formatting, locked offline `cargo check`, `git diff --check`,
  and scoped Rust whitespace checks pass. The build reports five dead-code
  warnings. No tests were run.
- The refreshed source-volume snapshot counts 2,367,864 TypeScript/JavaScript
  lines across 5,250 tracked files under `packages/**/src`, 352,409 Rust lines
  across 508 current files under `rust/crates/*/src`, and 183,545 CUA Rust lines
  across 285 files under `packages/cua-driver/rust/crates/*/src`. Main Rust is
  14.88% of the TypeScript/JavaScript baseline, or 22.63% including CUA. These
  counts include source tests and inline Rust tests; they measure code volume,
  not feature parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- Native `canopy serve` now exposes `GET`/`HEAD /workspace/git/log` and
  `/workspace/git/log/commit`. It rechecks workspace trust before Git access,
  validates ranges and commit IDs, and caps subprocess output, time, and
  concurrency. Workspace-qualified log routes, some TypeScript range cases,
  and higher TypeScript output limits remain unported.
- The native Ratatui conversation view renders common Markdown headings,
  emphasis, lists, quotes, links, rules, inline code, and fenced code. It caps
  inline style spans at 128 per source line and visible history at 16,384 rows,
  retaining the newest content. Older rows remain in the session transcript.
  Tables, nested block constructs, and interactive links remain open.
- The audio-capture package no longer ships its C++ addon or node-gyp build
  path. Its Node compatibility facade loads the Rust N-API addon; absent a
  matching prebuild or source-checkout Rust toolchain, installation remains
  nonfatal and voice input can use the SoX/arecord fallback.
- Physical iOS mobile MCP screen recording retains the existing `mobilecli`
  path and now matches TypeScript output-validation order and duration
  rounding. No physical-device run was performed.
- QQ's experimental cron-message buffer is wired to ACP text chunks, current
  QQ routes, and the existing bounded delivery retry policy. TypeScript has no
  `runCronFlow()` call site and QQ does not enable proactive send, so no
  scheduled-prompt trigger was added.
- Locked offline `cargo check --workspace` passes for the main Rust workspace;
  the audio `node-addon` feature, external-context Rust crate, and CUA workspace
  also compile. The main workspace reports six existing dead-code warnings;
  CUA's Linux and Windows builds report platform-specific unused-code
  warnings. Rustfmt, scoped whitespace checks, and `git diff --check` pass. No
  tests were run.
- The refreshed source-volume snapshot counts 2,367,838 TypeScript/JavaScript
  lines across 5,249 tracked files under `packages/**/src`, 354,214 Rust lines
  across 509 current files under `rust/crates/*/src`, and 183,545 CUA Rust
  lines across 285 files under `packages/cua-driver/rust/crates/*/src`. Main
  Rust is **14.96%** of the TypeScript/JavaScript baseline, or **22.71%** when
  the existing CUA Rust is included. These are code-volume ratios, not
  behavioral completion percentages.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- The native Ratatui transcript formats pipe-delimited Markdown tables as
  bounded cell rows, displaying at most 16 columns.
- Native TUI slash completion now discovers command filenames from the
  runtime-selected local extension inventory. It applies safe/bare mode,
  `slashCommands.disabled`, extension-name conflict handling, and bounded
  traversal and file reads. These are suggestions only; Rust does not yet
  execute extension command actions.
- Rust MCP settings now admit `mcp.serverCommand` only for trusted workspaces
  outside safe and bare modes. The value becomes a synthetic stdio `mcp`
  server after source merging, with bounded argv parsing and no shell
  invocation; shell operators, comments, glob patterns, malformed quotes,
  and oversized commands are rejected.
- ACP tool permission prompts now offer scoped Allow always choices where an
  exact rule can be represented safely. Project rules require a trusted
  workspace; writes use the atomic JSONC settings updater and take effect in
  the current session. Explicit ask/deny rules retain precedence.
- Targeted `rustfmt --check` and locked offline workspace `cargo check`
  pass. The workspace reports six existing dead-code warnings. No tests were
  run.
- The refreshed working-tree source count is 2,366,104 TypeScript/JavaScript
  lines across 5,233 tracked files under `packages/**/src` and 569,607 Rust
  lines across 889 `.rs` files in the repository. This broad count is **24.07%**
  by line volume. Within `rust/crates/*/src`, Rust is 355,159 lines across 509
  files (**15.00%**); including the 183,545-line CUA workspace raises that
  subset to **22.77%**. These counts include source tests and inline Rust
  tests. They measure code volume, not behavioral completion.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- Native `canopy serve` now has read-only `GET`/`HEAD /workspace/git/diff` and
  `/workspace/git/diff/file` routes. Both recheck workspace trust and bound
  Git subprocesses, route time, concurrency, and response size. The summary
  route reports tracked and untracked line counts with up to 50 file rows; the
  file route returns bounded unified hunks and can synthesize additions for
  safe untracked text files. Workspace-qualified variants and the rest of the
  `serve` session/runtime APIs remain unported.
- Feishu `blockStreaming: "on"` now streams bounded text blocks through the
  shared Rust `BlockStreamer`; completion flushes queued sends, while clear,
  cancellation, and ACP errors discard buffered text. The Feishu direct ACP
  client still lacks the bridge response-boundary signal and broader question
  and lifecycle parity.
- The GitLab foreground host now accepts bounded `/answer` commands for ACP
  `ask_user_question` requests, validating the originating sender, project,
  issue/MR thread, and live session. Requests expire after four minutes and
  are cancelled in ACP; each question answer is capped at 8 KiB and total
  answers at 32 KiB.
- The WeCom host now supports `steer`, `followup`, and `collect` dispatch modes
  with bounded per-session queues and attachment cleanup on drain, drop,
  cancel, and clear. Its ACP prompt lock still serializes work across sessions;
  source SDK callbacks can accept busy messages differently.
- Locked offline `cargo check --workspace`, targeted rustfmt checks, and
  scoped whitespace checks pass. The CLI reports six existing dead-code
  warnings. No tests were run.
- The refreshed working-tree count is 2,367,838 TypeScript/JavaScript lines
  across 5,249 files under `packages/**/src` and 572,027 Rust lines across 890
  `.rs` files in the repository. That is **24.16%** by source volume. Within
  `rust/crates/*/src`, Rust is 357,579 lines across 510 files (**15.10%**);
  including the existing CUA Rust source gives 541,124 lines across 795 files
  (**22.85%**). These counts include source tests and inline Rust tests and do
  not measure feature parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- Native `canopy serve` now exposes `GET`/`HEAD /workspace/git/branches` with
  the TypeScript v1 local/remote branch, tag, detached-head, and recent
  checkout projection. The route rechecks workspace trust and bounds Git
  subprocess time/output, route time, concurrency, and response size.
- Native TUI extension prompt commands now load active local extension `.toml`
  and `.md` command files and expand `{{args}}` or the default invocation
  suffix. Discovery respects workspace trust, safe/bare mode, and disabled
  commands; resource and output sizes are capped. Workspace-confined `@{path}`
  injection uses the native reader, selected-model modalities, existing ignore
  rules, and visible diagnostics. Shell `!{}` still fails visibly without
  execution. User/workspace command directories, live reload, and shell
  permission/confirmation handling remain unported.
- QQ now handles channel and per-group `dispatchMode` values `steer`,
  `followup`, and `collect`. It drains per-session FIFO prompts, bounds queued
  prompts to 32 / 16 MiB and collect buffers to 128 / 1 MiB, and applies
  shared-session authorization before steering. `/cancel` clears collected
  work; `/clear` invalidates stale queued work. Streaming, proactive delivery,
  and session recovery remain open.
- The Weixin status note now records that the TypeScript buffered/drained
  callbacks have no effective behavior to port: ChannelBase's defaults are
  no-ops, while Weixin only overrides typing start/end. Background-task
  lifecycle events remain a separate gap.
- Locked offline `cargo check --workspace`, targeted rustfmt checks, and
  scoped whitespace checks pass. The CLI reports six existing dead-code
  warnings. No tests were run.
- The refreshed source-volume snapshot counts 2,367,838 TypeScript/JavaScript
  lines across 5,249 files under `packages/**/src` and 573,318 Rust lines
  across 891 `.rs` files in the repository (**24.21%**). Within
  `rust/crates/*/src`, Rust is 358,870 lines across 511 files (**15.16%**);
  including the 183,545-line CUA Rust subtree gives 542,415 lines across 796
  files (**22.91%**). These counts include source tests and inline Rust tests;
  they measure code volume, not feature parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up integration update (2026-09-27)

- Native TUI extension prompt commands now expand workspace-confined `@{path}`
  injections after command argument substitution. They use the selected model's
  modalities and configured Canopy ignore files, retain ordered text/media
  parts, and show read diagnostics. Shell `!{}` remains fail-closed.
- Rust extension updates now probe Git refs, GitHub release tags, and npm
  dist-tags before downloading. A confirmed unchanged source skips acquisition
  and conversion; failed or inconclusive probes follow the existing
  transaction path. Archive validators, credential-required npm probes, and
  TypeScript network-policy parity remain open.
- The general native interactive `/doctor` command now reports runtime,
  provider/model/settings, MCP, tool declarations, Git, and platform checks
  from observed Rust runtime state. Missing auth validation and MCP connection
  data are surfaced as unavailable/warnings. `/doctor memory` and
  `/doctor rollback` retain their existing paths; CPU profiling is not ported.
- Native `canopy serve` now exposes token-gated POST routes for Git checkout,
  branch creation, push, pull/fetch, and commit. They cap request bodies at
  64 KiB, recheck trust before Git runs, serialize mutations, bound subprocess
  work, and redact workspace paths from errors. This is single-workspace only;
  workspace-qualified routes and generation guards remain unported.
- The shared Git mutation layer validates refs before argv use, bounds each
  child to 30 seconds and 10 MiB per output stream, protects existing branches
  during failed branch-creation rollback, and restores the index when
  `commit --all` fails.
- `cargo check -p canopy-core --locked --offline`,
  `cargo check -p canopy-cli --locked --offline`, targeted workspace formatting,
  and scoped whitespace checks pass. The CLI build reports six dead-code
  warnings. No tests were run.
- The refreshed source count is 2,367,838 TypeScript/JavaScript lines across
  5,249 tracked files under `packages/**/src`, and 575,384 Rust lines across
  893 non-generated `.rs` files in the repository (**24.30%** by line
  volume). The dedicated `rust/` workspace contains 360,983 lines across 514
  files (**15.25%**); the separate CUA Rust tree contains 209,973 lines across
  364 files. These counts include test source and measure code volume, not
  feature parity.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.

## Follow-up concurrency update (2026-09-27)

- The native WeCom host preserves FIFO prompt queues within each session while
  allowing different sessions to issue ACP prompts concurrently. JSON-RPC
  replies remain correlated by request ID and streamed updates by session ID;
  the bounded ACP event relay still reports lag explicitly.
- `cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked --offline`
  passes with six pre-existing dead-code warnings. Targeted Rust formatting and
  whitespace checks pass. No tests were run.

The Node/V8 CLI remains the shipped executable; the Rust port is incomplete.
