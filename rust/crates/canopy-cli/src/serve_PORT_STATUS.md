# Native `serve` command port assessment

## Current status

`canopy-cli` now has a loopback-only
`canopy serve [--port] [--token] [--require-auth]` command backed by Hyper. It
implements `GET`/`HEAD /health`, `/capabilities`, `/daemon/status`,
`/workspace/memory`, `/workspace/git`, `/workspace/git/branches`,
`/workspace/git/log`,
`/workspace/git/log/commit`, `/workspace/git/diff`,
`/workspace/git/diff/file`, plus `POST /workspace/git/checkout`,
`/workspace/git/branch`, `/workspace/git/push`, `/workspace/git/pull`, and
`/workspace/git/commit`, `/session/:id/status`,
`/session/:id/transcript`, and the native-only
`/session/:id/transcript/records`, with bounded HTTP/1 handling, host/origin
checks, bearer auth, and graceful shutdown. The capabilities endpoint
advertises health, daemon status, session status, transcript events and
pagination, the bounded `workspace_memory_read`, `workspace_git_read`, and
`workspace_git_log_read` features, the `workspace_git_mutation` feature, plus
the native-only record-page feature.
The workspace-memory route returns only bounded metadata for context files at
the workspace root and global memory directory; it does not return file
contents. The workspace Git route returns branch and working-tree counts only,
without per-file paths. No
agent-session runtime or other daemon routes are started. Session status is a
bounded projection of the native catalog and live-process sidecar, without
prompt/client state. The
TypeScript-shaped transcript endpoint returns bounded replayed ACP events;
`/transcript/records` remains a separate bounded raw JSONL page. Because the
runtime sidecar does not report whether a prompt is active, replay conservatively
leaves dangling tool calls unresolved rather than marking an in-progress call
failed. `--token` overrides `QWEN_SERVER_TOKEN`, and a configured token protects
every route including `/health`; `--require-auth` rejects startup when no token
is set. Unsupported daemon flags are rejected explicitly. This is a usable
transport, read-only diagnostics, and targeted Git mutation slice, not `serve`
behavior parity. The
native `--acp` command remains a separate newline-delimited JSON-RPC process on
stdin/stdout.

The workspace-memory route uses the TypeScript `GET /workspace/memory` trust
policy and filename settings where the native serve process can determine
them. It reads bounded system, user, system-default, workspace, and trusted
folder settings without migration writes; the selected `context.fileName`
string or string list is captured at daemon startup, with `CANOPY.md` and
`AGENTS.md` as fallback. Before each request stats any memory file, it reloads
the bounded trust settings and trusted-folders file and denies access with the
source-shaped `403 untrusted_workspace` response for an explicit denial,
unknown decision, unreadable/invalid trust policy, or invalid trust setting.
This recheck makes trust revocation apply while the daemon remains running.
Rust serve has no IDE trust context, so an enabled policy without a matching
explicit trusted-folder rule remains denied. The listener also applies its
loopback host/origin checks and configured bearer token.

The workspace Git route applies the same fresh trust check before any Git
probe. It runs a direct read-only `git status --porcelain=v1 --branch -z`
process with bounded output, timeout, and concurrency, then reads only bounded
Git metadata for stash count and operation markers. `wait=1` allows a five
second status budget; ordinary requests use a two second budget. The Rust
listener has no `WorkspaceGitState` watcher/cache or workspace event stream, so
it computes a fresh status on each request and cannot match the TypeScript
route's cached fast response or publish `git_status_changed` and
`git_branch_changed` events. A missing repository, failed/timed-out Git
command, malformed status output, or oversized status output returns the v2
branch-only shape; unavailable or oversized stash logs count as zero.

The single-workspace Git mutation routes use the existing bounded core
operations for checkout, branch creation, push, pull/fetch, and commit. They
accept at most 64 KiB of request data, require a configured bearer token even
when ordinary loopback reads are open, and serialize mutations through one
permit. Each route rechecks workspace trust immediately before Git runs. The
shared Git helper bounds each child process to 30 seconds and 10 MiB of stdout
and stderr; the daemon caps the whole operation at 225 seconds. Known Git
failures preserve the TypeScript status/error codes, and error bodies redact
both the workspace and discovered Git-root paths. There are no multi-workspace
Git mutation routes or Git state change events in the native daemon.

The single-workspace Git log routes also apply a fresh trust check before any
Git command and return the TypeScript v1 list and commit-detail shapes. They
use argument arrays, accept only bounded Git revision ranges and 7–40
character hexadecimal commit IDs, cap log output at 1 MiB, numstat output at
4 MiB, serialized responses at 8 MiB, each Git process at five seconds, and
concurrent lookups at two. Log pages default to 50 entries and clamp to 200;
commit details retain at most the first 50 file paths while totals include all
parsed numstat records. The stricter range validation returns a 400 for
malformed or duplicate range values, and native output limits can report an
unavailable log or a bounded 413 where TypeScript's larger process buffer may
still return data. The routes do not add workspace-qualified paths or Git log
change events.

The TypeScript command is not just a listener wrapper. Its command definition
is 1,043 lines with about 50 options. The runtime and server entrypoints add
another 10,956 lines (`run-canopy-serve.ts`: 7,960; `server.ts`: 2,996).
Across production TypeScript files under `packages/cli/src/serve`, there are
47 files with direct Express route registrations, about 40,000 lines in those
files, and 298 direct `app.get/post/put/patch/delete/...` registrations. This
count excludes WebSocket route handling and indirection through mounted
handlers; it is an inventory indicator, not a complete endpoint count. The
ACP HTTP transport (`acp-http/index.ts`, 2,644 lines) and session routes
(`routes/session.ts`, 6,456 lines) are among the largest individual areas.

## TypeScript boundaries

The relevant source is `packages/cli/src/commands/serve.ts`,
`packages/cli/src/serve/run-canopy-serve.ts`, `packages/cli/src/serve/server.ts`,
and the modules in `packages/cli/src/serve/`.

| Boundary                     | Responsibilities                                                                                                                                                                                                                                                                                                                                                                                             |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Command and options          | Bind host/port, token and auth policy, TLS, browser launch, Web Shell, Local/Remote Control, workspaces, ACP bridge mode, session and replay limits, memory budget, channel selection, timeout and rate-limit settings. Validates incompatible options before startup.                                                                                                                                       |
| Daemon startup and shutdown  | Resolve and canonicalize workspaces; freeze and scrub child environments; load settings and trust state; configure provider/ACP child arguments; start or preheat runtimes; lazily start secondary workspace runtimes; start channel workers; publish the listener only after bootstrap; own signal handling, bounded drain, child cleanup, PID reservations and daemon logs.                                |
| Listener-wide policy         | HTTP/HTTPS binding; host allowlist; CORS and browser-origin restrictions; listener-specific bearer credentials; loopback/auth exceptions; rate limiting; bounded JSON bodies; access logs and telemetry; consistent errors and graceful shutdown.                                                                                                                                                            |
| ACP/session transport        | HTTP ACP session new/load/resume, prompt and cancellation; attach and disconnect; replayable SSE; WebSocket ACP and optional reverse-MCP/CDP paths; permission and session-shell calls; admission limits and session restoration.                                                                                                                                                                            |
| Workspace API                | Legacy-primary and workspace-qualified APIs for status, session catalogs, workspace registration/removal, trust, settings and permissions; files and uploads; git and GitHub PRs; memory and agents; tools and skills; extensions and MCP; voice and live voice; scheduled tasks and channel controls. These routes resolve a runtime and must retain that runtime's trust, generation and ownership checks. |
| Other daemon services and UI | Health, capabilities, diagnostics and metrics; usage/goals; webhooks and notifications; Local Control listener/pairing; static Web Shell assets and SPA fallback; background maintenance and task keepalive.                                                                                                                                                                                                 |

The static shell is an independent `packages/web-shell` artifact, but its API
contract spans these routes. Keeping `--web` working requires both serving the
assets and implementing the APIs the UI calls.

## Rust components that can be reused

- `rust/crates/canopy-cli/src/acp_server.rs` already hosts native sessions and
  tools over stdio ACP. It builds native ACP agents, restores sessions, handles
  cancellation and permissions, and uses the Rust session store. Its transport
  and host lifecycle are process/stdio scoped, not HTTP daemon scoped.
- `rust/crates/canopy-core/src/acp_bridge/` contains Rust event-bus,
  replay/journal limits, session runtime/factory, process registry and workspace
  path primitives. These are useful building blocks, but do not implement the
  TypeScript HTTP bridge routes or daemon workspace registry.
- Native domain logic already exists across core for settings, permissions,
  files/tools, MCP, extensions, memory, sessions, git-adjacent operations,
  channels, voice, and memory budgeting/diagnostics. The coverage and active
  ACP/CLI wiring differ by feature; each route family needs a parity check
  before treating a core module as a drop-in implementation.
- `canopy-cli` uses Hyper 1 and hyper-util for bounded HTTP/1 serving; its
  previous transport preview is now wired to plain `canopy serve`. The Rust
  memory diagnostics already include an RSS-only `NativeMemoryProbe`, which
  backs the process field in the native daemon-status response. These pieces do
  not provide a session/workspace router, WebSocket stack, TLS, or daemon
  lifecycle parity.

## Phased parity plan and estimate

Estimates are rough **person-weeks** for implementation plus integration, with
the existing Rust domain components reused where their behavior is already
compatible. They assume an engineer familiar with the codebase and omit
schedule time for external UI/API redesign. They are original phase ranges, not
remaining-work estimates; phase 0's transport spike is complete and phase 1 is
partially implemented. Parallel work can reduce elapsed time after module
interfaces and route ownership are agreed.

| Phase                                         | Scope and completion gate                                                                                                                                                                                                                                                                                                         | Estimate |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------: |
| 0. Contract and transport spike               | **Partially complete:** Hyper HTTP/1 selection, bounded loopback listener, shutdown, host/origin checks, and health/capabilities/status bootstrap contracts. TLS, WebSocket choice, ACP exchange, and full route/error/SSE contract inventory remain.                                                                             |      2–3 |
| 1. Daemon foundation                          | **Partially complete:** plain `serve` dispatch, port parsing, loopback binding, bounded transport, basic host/origin policy, bearer token auth, health/capabilities/status, signals and bounded drain. Remaining: configurable bind/TLS/CORS, structured logs, real runtime startup/readiness, and full daemon option validation. |      4–7 |
| 2. ACP-over-HTTP                              | Port session transport, bridge/session ownership, prompt/cancel/restore, replayable SSE and WebSocket behavior. Reuse native ACP/runtime primitives where possible; define and verify the HTTP-to-session adapter.                                                                                                                |     6–10 |
| 3. Workspace runtime and core APIs            | Implement workspace registry/admission and lifecycle, legacy-primary plus workspace-qualified routing, trust/generation guards, and parity for the core UI APIs (status, sessions, files, git, settings, permissions, memory, tools and skills).                                                                                  |     7–12 |
| 4. Remaining API families and daemon services | Extensions/MCP controls, agents, voice/live voice, scheduled tasks/goals, channel workers/webhooks, Local/Remote Control, uploads, reverse-MCP/CDP and remaining diagnostics. Keep feature gates and per-workspace service ownership aligned.                                                                                     |     7–13 |
| 5. Web Shell, packaging and cutover           | Serve the built Web Shell with secure headers and correct SPA fallback; package assets; port `--open` and supported flags; run compatibility and security acceptance checks against the TypeScript daemon; switch distribution only after release parity.                                                                         |      4–7 |

**Full parity planning range: 30–52 person-weeks**, plus any deferred feature
work discovered by route-by-route acceptance. A smaller headless/API-only
milestone that omits the Web Shell, secondary workspaces, channels and advanced
routes would be materially less work, but must be labeled as a subset rather
than a completed port.

## Main blockers and risks

1. **Transport contract and scope:** TypeScript has both ACP HTTP and many
   custom REST routes. The Rust stdio ACP server does not define those REST
   response shapes. First decide which surfaces are in the initial Rust
   milestone and preserve wire compatibility for any surface the current Web
   Shell uses.
2. **Workspace isolation:** route ownership varies by process, primary
   workspace, selected runtime, and live session. Runtime resolution,
   trust, draining/removal, and generation fences are security boundaries;
   porting handlers without their route ownership model risks cross-workspace
   reads or writes.
3. **Startup parity:** TypeScript has deferred readiness, optional ACP child
   preheat, lazy secondary runtimes, channel workers and careful environment
   scrubbing. Native ACP currently starts its own session runtime directly.
   Embedding it, supervising child processes, or designing a new native host is
   an architectural decision, not a route-level substitution.
4. **WebSocket and streaming behavior:** SSE replay, cancellation, connection
   admission and WebSocket subprotocols need a server-side implementation and
   client compatibility checks. Hyper is wired for basic HTTP/1, but there is
   no session streaming or WebSocket route implementation.
5. **Security-sensitive parity:** host and origin checks, listener-scoped
   credentials, TLS key/certificate handling, strict mutation gates, bounded
   uploads, session-bound shell execution and secret-safe diagnostics need
   explicit acceptance coverage before a Rust listener is exposed beyond
   loopback.
6. **Domain gaps:** existing Rust modules do not imply route parity. Confirm
   each route's side effects, settings persistence, service lifecycle,
   response schema, and TypeScript-specific behavior before reusing a core API.
7. **Release cutover:** package scripts still launch the Node/V8 CLI. Porting
   the daemon alone does not move the shipped application to Rust; assets,
   launch selection, versioning and rollback also need a coordinated release
   change.

## Recommended work order

Keep Node as the reference daemon while the Rust command grows. Continue from
the current bounded loopback server by designing Rust session/runtime
ownership, then port ACP session transport and one workspace-qualified route
end to end before parallelizing route families. Do not claim `serve` parity
until the route and flag matrix, Web Shell acceptance, shutdown behavior, and
cross-workspace security checks pass against the TypeScript implementation.

For the current source delta, no tests were added or run. Rust formatting and
whitespace checks are recorded in `serve_transport_PORT_STATUS.md`.
