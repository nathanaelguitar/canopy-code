# Remote Control (Tailscale-based daemon attach)

## Summary

Give the interactive TUI a "just like Claude Code" remote-control
experience: every interactive session is (by default, opt-out) a live
session on the local `canopy serve` daemon from the moment it starts.
`/remote-control` exposes that already-live session over Tailscale with a
pairing QR and fires a webhook so the CanopyChat iOS app can push a
notification with a deep link. The terminal and the phone are both live
clients of the _same_ daemon-managed session — either can send a prompt or
answer a permission request, and both see everything happen in real time.
This is co-driving, not a fork and not a pause/handoff.

Scope: `packages/cli` only. No changes to `packages/sdk-typescript`
(actively being modified by unrelated in-flight work — see Landmines) or to
the CanopyChat iOS app itself (out of repo; a separate Codex prompt covers
that side, written after this side is proven — see Stage D).

## Motivation

The user's request evolved twice during design, both times toward more
capability, not less:

1. First ask: fix `canopy serve --local-control`'s LAN-only QR pairing
   (it deliberately excludes Tailscale/VPN interfaces and CGNAT addresses —
   see `packages/cli/src/serve/local-control/lan-interfaces.ts`), which is
   why a phone off the home network got nothing. Landed as
   `--remote-control`, a CLI flag that reuses the same
   `LocalControlService` but selects a Tailscale address instead
   (`packages/cli/src/serve/local-control/tailscale-interface.ts`).
2. Second ask: make it a `/remote-control` slash command inside the
   interactive session, working like Claude Code's mobile remote control —
   get a push notification, immediately commandeer the session from
   anywhere. Initial design was a strict pause/handoff (terminal stops,
   daemon takes over, terminal reclaims later) to dodge dual-writer and
   tool-approval-arbitration problems. **The user explicitly rejected
   this**: "it doesn't pause it. it's simultaneous co-driving just like
   with claude code remote-control." Then: make remote-control **opt-out**
   — every session is automatically an RC session, not something you turn
   on.

That second correction is why the daemon-attach architecture below reads
as "opt-out from the start" rather than "attach mid-session." A mid-session
swap (turn a locally-running interactive session into a daemon-attached
one on demand) would require either rewriting the TUI into an ACP client
of the daemon, or building new plumbing to stream a live in-process
transcript into the daemon's journal format — neither exists today, and
both are real rearchitecture. Starting the daemon attach at process
startup instead avoids the swap entirely: there is nothing to migrate,
because the session was born on the daemon.

## Validated facts (do not re-derive these — they were confirmed by direct

testing or by reading the implementation, not assumed)

1. **The daemon already supports N simultaneous co-driving clients on one
   session.** Verified live: started `canopy serve`, created a session,
   attached two independent SSE consumers (`clientId=phone`,
   `clientId=terminal`) to `GET /session/:id/events`, sent a prompt from
   `terminal` — both SSE streams received the identical broadcast event.
   Sent a second prompt from `phone` while the first was still in flight —
   it was **accepted and queued** (`202`, its own `promptId`), not
   rejected. This is the core assumption the whole feature depends on, and
   it already holds with zero new server code.

2. **The interactive TUI's chat recording and the daemon's session
   restore read/write the exact same file.** `ChatRecordingService`
   (interactive TUI) and `SessionService.loadSession` /
   `POST /session/:id/resume` (daemon) both resolve to
   `storage.getProjectDir()/chats/<sessionId>.jsonl`
   (`packages/core/src/services/sessionService.ts:506`,
   `packages/core/src/services/chatRecordingService.ts:853`). "Seed the
   daemon with the current conversation" is not new plumbing — it is
   exactly what `--resume`/`--continue` already do.

3. **`POST /session/:id/resume` and `POST /session/:id/load` already
   exist** (`packages/cli/src/serve/routes/session.ts:3369-3370`,
   `restoreSessionHandler`) and restore a session by id, including
   worktree sidecar handling.

4. **`ChatRecordingService.close({ handoff: true })` already exists**
   (`packages/core/src/services/chatRecordingService.ts:1363`) as a
   first-class "hand write-ownership to a successor" primitive, with fail-
   closed semantics if the final flush doesn't durably land. This matters
   for the _legacy_ case — see Non-goals.

5. **Tool-approval-over-HTTP already exists**:
   `POST /session/:id/permission/:requestId`
   (`packages/cli/src/serve/routes/permission.ts`). The gnarliest product
   question in a two-live-client design — who answers a permission prompt
   when both a phone and a terminal are attached — already has a server-
   side answer; it does not need to be invented.

6. **The rendering seam is `HistoryItem` + `context.ui.addItem`, not
   `useGeminiStream`.** `useGeminiStream`
   (`packages/cli/src/ui/hooks/useGeminiStream.ts`, 5,753 lines) is the
   _execution_ engine — tool scheduling, compression, vision bridging,
   goal-turn binding. In the daemon-attach architecture the daemon-spawned
   `canopy --acp` child does all of that execution; the terminal only
   needs to display the resulting event stream and forward input. So the
   terminal-side adapter (`useDaemonStream`, not yet built) does not need
   to replicate the execution engine — but it does need to match
   `useGeminiStream`'s call signature and return shape closely enough that
   the huge amount of `AppContainer.tsx` code downstream of it
   (`packages/cli/src/ui/AppContainer.tsx:2129`, `streamingState`,
   `pendingHistoryItems`, `thought`, `pendingToolCalls`,
   `loopDetectionConfirmationRequest`, `activePtyId`, and more) keeps
   working. This is real integration work, not a quick shim — see Stage A.

7. **`POST /workspace/local-control/enable` does not yet accept a
   `network` param.** `packages/cli/src/serve/routes/workspace-local-
control.ts:150-160` only reads `address`/`target` from the body. The
   `LocalControlService.enable()` method itself already accepts
   `network: 'lan' | 'tailscale'` (added for the `--remote-control` CLI
   flag). This route needs the same passthrough before `/remote-control`
   can enable Tailscale pairing on an already-running attached session via
   HTTP instead of only at daemon boot via CLI flag.

## Non-goals for v1

- **True live mid-turn handoff for a session that started in local-only
  mode.** If a user is already mid-conversation in a plain (non-attached)
  interactive session and wants to switch to daemon-attached, the correct
  primitive is `close({ handoff: true })` + `POST /session/:id/resume` +
  process restart into attached mode — not an in-process swap. Punted
  until default opt-out (Stage C) ships and this becomes rare.
- **Full `useGeminiStream` parity in `useDaemonStream`.** Ship the pieces
  the acceptance test needs (message rendering, prompt submission,
  permission answering); thoughts, artifacts, voice, and other long-tail
  event types are iteration.
- **Daemon reuse across machines / discovery beyond one workspace.** One
  daemon per workspace, reused across terminal tabs in that workspace.
  No cross-workspace or cross-machine daemon registry.
- **CanopyChat iOS app changes.** Out of repo. Stage D produces a written
  brief for Codex to implement app-side; this repo's job ends at firing a
  webhook with a documented payload.

## Architecture

```
┌─────────────────────────┐         ┌──────────────────────────┐
│ Terminal (interactive)  │◄───────►│  canopy serve daemon      │
│ ink UI, useDaemonStream │  HTTP/  │  (spawned or reused,       │
│ SSE-driven HistoryItems │  SSE    │  one per workspace)        │
└─────────────────────────┘         │                            │
                                     │  ┌──────────────────────┐  │
┌─────────────────────────┐  HTTP/  │  │ canopy --acp child    │  │
│ Phone (CanopyChat app)  │◄───────►│  │ (does all execution:  │  │
│ over Tailscale          │  SSE    │  │  tool calls, model    │  │
└─────────────────────────┘         │  │  calls, compression)  │  │
                                     │  └──────────────────────┘  │
                                     └──────────────────────────┘
```

The daemon-spawned `canopy --acp` child is the sole execution engine and
sole writer of `chats/<sessionId>.jsonl`. The terminal is a client, exactly
like the phone is a client — this symmetry is what makes co-driving free
instead of something to build.

## Build stages

Tracked as tasks #9-#12 (created 2026-08-26) so this plan survives a
context reset. Each stage commits independently on the `remote-attach`
branch (off `canopy-rebrand`, itself ahead of `origin/canopy-rebrand`).

### Stage A — daemon-attached TUI startup mode (opt-in flag first)

The multi-day item. Land behind an opt-in flag/env var before Stage C
flips the default, so a slip here doesn't block shipping nothing.

1. **Daemon lifecycle: one per workspace, reused.** Probe
   `127.0.0.1:4170/health` (falling back through the port-autoincrement
   sequence observed live: 4170 → 4171 → 4172...). No existing daemon
   registry/lockfile was found for "is a daemon already running for this
   workspace" (checked `packages/cli/src/serve/daemon-logger.ts`'s stable-
   log lock — that's log-file contention, not a discovery registry; and
   `packages/core/src/ide/ide-client.ts`'s port lockfiles — that's the IDE
   companion's own unrelated MCP discovery). Build one: on health hit,
   confirm workspace match (the existing `workspace_mismatch` 400 response
   shape already signals this); on miss, spawn detached
   (`node .../dist/index.js serve --port 4170 --workspace <cwd>`,
   `detached: true`, `.unref()`), parse stdout for the actual bound port
   (auto-increment case), poll `/health` until ready. Leave the daemon
   running on TUI exit — the existing session reaper (30 min idle
   threshold) already reaps it; do not add new lifecycle teardown.
2. **Session creation/resume at startup.** New session:
   `POST /session` as normal. Resumed session (`--resume`/`--continue`):
   `POST /session/:id/resume` instead of the local
   `sessionService.loadSession` path.
3. **`useDaemonStream`.** New hook in `packages/cli/src/ui/hooks/`,
   matching `useGeminiStream`'s consumed return shape (see Validated Fact
   6). SSE consumption: hand-rolled parser in `packages/cli` (do **not**
   depend on `@qwen-code/sdk-typescript`'s SSE transport — see Landmines).
   Map observed event types (`user_message_chunk`, `agent_message_chunk`,
   `tool_call`/`tool_call_update`, `extensions_changed`, permission
   requests) to `HistoryItem` variants via `addItem`. Prompt submission →
   `POST /session/:id/prompt`. Permission answers →
   `POST /session/:id/permission/:requestId`.
4. **Wiring.** `gemini.tsx` gains a startup branch parallel to the
   existing `isAcpMode` branch (~line 396): when attached, construct
   `useDaemonStream` inputs instead of a local `GeminiClient` turn loop.
   `AppContainer.tsx:2129` calls one hook or the other based on this flag.

**Acceptance test** (run on this box, real model): from a real interactive
session in attached mode, send a prompt from the terminal; separately curl
the daemon "as the phone" (attach SSE, send a prompt) — confirm both
streams show both turns live, and confirm a permission request raised by
either turn is answerable from either side.

**Expectation to set**: cold start pays the daemon spawn cost once per
workspace (~1.5s measured via `processToListenMs` in the daemon's own
startup timing log) — near-zero for the second and later session in the
same workspace since the daemon is reused.

### Stage B — `/remote-control` command (Tailscale + webhook)

Small, depends on Stage A already being attached.

1. Add `network` passthrough to `POST /workspace/local-control/enable`
   (Validated Fact 7).
2. Command calls that route with `network: 'tailscale'`, gets the pairing
   URL back.
3. Fire-and-forget webhook POST (never blocks/fails the command on
   webhook error) to a configured URL — env var or settings key, `CANOPY_`
   prefixed since it's ours. Payload: `{url, sessionId, workspaceName,
title}`. No config → no fire; config presence is the opt-in for the
   webhook specifically (see Stage C's three-layer default).
4. Print QR + status via `ui.addItem` in the existing chat log.

### Stage C — flip the default to opt-out

Deliberately last and smallest diff, so if Stage A or B slip, a working
opt-in feature is still shipped and usable.

Three independent layers, each with its own default — daemon-backed does
**not** imply network-exposed:

| Layer                                 | Default                                | Why                                                                                                                                                                               |
| ------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Daemon-backed session (loopback only) | **On**                                 | No new exposure — same machine, same user; loopback is already the daemon's existing auth-free trust boundary.                                                                    |
| Tailscale listener                    | **On when a tailnet interface exists** | Gated by the pairing token, confined to the user's private tailnet. Skips silently (no error, no prompt) when `NoTailscaleInterfaceError` — i.e., no Tailscale installed/running. |
| Webhook                               | **Off unless configured**              | Firing a push notification to a third-party app is real action; config presence is the opt-in.                                                                                    |

Single escape hatch that kills all three: `--no-remote-control` /
`settings.remoteControl.enabled = false`.

**Scope restriction, enforced in code, not just documented**: this default
applies to interactive TUI sessions only. `-p` (non-interactive), ACP mode
(`--acp`/`--experimental-acp`), and `canopy serve` itself must never auto-
spawn a daemon — otherwise scripts/automation break and `canopy serve`
recurses into spawning another `canopy serve`.

### Stage D — Codex prompt for the CanopyChat app side

Written after A-C are proven on this side, not before (the payload schema
isn't final until Stage B is real code). Lives at a stated path (default:
home dir). Contents: webhook payload schema and firing conditions; what
the app backend must do (receive POST → APNs push with deep link → app
opens the pairing URL over Tailscale); constraints Codex can't discover on
its own — the pairing URL's fragment _is_ the secret granting full session
control (must never be logged), iOS needs the Tailscale VPN active for the
tailnet URL to resolve at all, and the URL targets the Web Shell so a
wrapped webview is the fast path for a native-feeling first version. A
quick read-only `ls` of the CanopyChat app repo (mentioned as
`aether_local_ai` or similar, on this machine, exact path TBD) to name its
real path/stack in the prompt — not deeper than that; app-side
implementation is explicitly Codex's job, not this session's.

## Landmines (standing, not resolved by this doc — re-check before each

stage)

- **`packages/sdk-typescript/src/daemon/{RestSseTransport,sse,transports,
index}.ts` and `packages/web-shell` (which depends on it) have real,
  unrelated, uncommitted work in progress** (SSE transport / idle-timeout
  changes) as of this doc's writing. Do not modify, depend on, or build
  on top of those files. `useDaemonStream`'s SSE client is hand-rolled in
  `packages/cli` for exactly this reason — small enough (~50-100 lines)
  that duplicating instead of depending is the safer call here.
- **Another session/agent ("Muse Spark") is actively pushing to
  `canopy-rebrand`** (e.g. commit `b5bce13` landed mid-session, ChatGPT
  OAuth support). This work happens on the separate `remote-attach`
  branch. Do not merge/push to `canopy-rebrand` or `main` without asking
  first.
- **`git add -A packages/` is unsafe right now** — it would sweep the
  uncommitted sdk-typescript WIP into an unrelated commit. Stage explicit
  file paths only.
