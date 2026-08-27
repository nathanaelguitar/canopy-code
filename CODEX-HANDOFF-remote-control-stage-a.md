# Codex handoff: finish Stage A (daemon-attached TUI)

Branch: `remote-attach` (based on `canopy-rebrand`, in the `qwen-code`/
`canopy-code` repo). Full design doc, read it first:
`docs/design/2026-08-26-remote-control.md` — it has the validated facts,
architecture rationale, and all four build stages. This file is the
narrower, tactical handoff for finishing Stage A specifically.

## What this feature is

Every interactive `canopy` session should (eventually, opt-out) run as a
live session on the local `canopy serve` daemon instead of a local-only
loop, so a phone (the CanopyChat iOS app) can co-drive the same session
over Tailscale — both the terminal and the phone are simultaneous live
clients of one daemon-managed session, not a fork and not a pause/handoff.
That distinction was an explicit user correction mid-design; don't
reintroduce a handoff model.

## What's already built, tested, and committed on this branch

Three pieces, each independently verified against a real running daemon
(not just type-checked), each its own commit:

1. **`packages/cli/src/serve/ensure-workspace-daemon.ts`** —
   `ensureWorkspaceDaemon(workspaceCwd)`: probes `127.0.0.1:4170/capabilities`
   for a matching-workspace daemon; reuses it if found, spawns
   `canopy serve --port 4170 --workspace <cwd>` detached otherwise. Verified
   live: fresh spawn, reuse (confirmed `spawned: false`, one process),
   mismatch spawns a second daemon on the auto-incremented port. Known v1
   limitation, documented in the file: only the default port is probed, so
   a daemon that landed on a non-default port (because 4170 was taken) won't
   be found by a later probe — no discovery registry exists in this
   codebase and building one is out of scope. **Read the top comment on
   `spawnDaemon` before touching this file** — it explains a real bug that
   was found and fixed (piping the child's stdout/stderr and then
   destroying the parent's read end let the caller exit promptly, but
   crashed the still-running daemon on its next log write; fixed by
   redirecting to a real log file instead).

2. **`packages/cli/src/ui/daemon-attach/daemon-session-events.ts`** —
   hand-rolled SSE client for `GET /session/:id/events` (parses `id:`/
   `event:`/`data:` frames, reconnects with `Last-Event-ID`), plus
   `submitDaemonPrompt` (`POST /session/:id/prompt`) and
   `answerDaemonPermission` (`POST /session/:id/permission/:requestId`).
   Hand-rolled deliberately — **do not** depend on
   `packages/sdk-typescript`'s SSE transport (see Landmines below).
   Verified live: event count/ids matched the daemon's own connection-close
   log line exactly (`eventFramesWriteSettled=4, lastEventIdWritten=4`).

3. **`packages/cli/src/ui/daemon-attach/use-daemon-stream.ts`** —
   `useDaemonStream(baseUrl, sessionId, clientId, addItem)`. This is the
   piece that matters most for what's left: it's designed as a drop-in
   replacement for `useGeminiStream` at the call site in
   `packages/cli/src/ui/AppContainer.tsx:2129`. Its return type is checked
   by the TypeScript compiler against `ReturnType<typeof useGeminiStream>`
   and passes with zero errors — this is a _real_ compiler-verified
   structural match, not an assumption. It was also mounted in a real ink
   render tree (via `tsx`, against a live daemon session, using a `script`-
   wrapped pty since ink needs a real TTY to render) and confirmed to
   connect and reflect live `streamingState` correctly — see the commit
   message on `5de610116f` for exact verification method if you want to
   repeat it.

   Fields that are real: `streamingState`, `submitQuery`,
   `pendingHistoryItems` (accumulates `agent_message_chunk` text into one
   pending item while streaming, matching the local hook's pending-item
   pattern), `clearPendingState`, `isReceivingContent`,
   `streamingResponseLengthRef`, permission handling (see below).

   Fields that are inert stubs (typed correctly, do nothing yet — this is
   explicitly fine per the design doc's Non-goals, "iteration not v1"):
   `thought` (always null), `preemptGoalTurn`, `retryLastPrompt`,
   `handleApprovalModeChange`, `pendingToolCalls` (always `[]`),
   `activePtyId` (always undefined), `loopDetectionConfirmationRequest`
   (always null), `cancelOngoingRequest` (documented no-op — daemon has a
   `session_cancel` capability per `/capabilities` but it isn't wired here
   yet).

   Extra fields beyond the `useGeminiStream` shape (present because
   TypeScript allows excess properties on values, just not on object
   literals assigned directly to a narrower type):
   `answerPermission(requestId, outcome)` and `pendingPermission`. The
   `permission_request` SSE event shape
   (`{requestId, sessionId, toolCall, options}`) was confirmed by reading
   `packages/acp-bridge/src/bridgeClient.ts` around line 878 (the actual
   `entry.events.publish({type: 'permission_request', ...})` call) — not
   guessed from a live capture, because triggering a real tool-approval
   round-trip against the model available on this box was too slow to wait
   out reliably (30-90s+ per turn on the local SGLang model under a heavy
   skills/extensions config). If you get a live capture, it should match;
   if it doesn't, trust the source over this note.

## What's NOT done — this is the actual remaining work

Two things, both inside files with hundreds of other things depending on
them. Read before editing, don't rush.

### 1. Find the submission-dispatcher seam in `AppContainer.tsx`

The text input's submit handler is not a simple call to `submitQuery`.
Trace from `packages/cli/src/ui/AppContainer.tsx` around the `onSubmit`
prop wiring (search for where `submittedValue` is built, roughly lines
2550-2760 as of this writing) — it's a large callback that handles slash
commands, `?btw`/`/btw` side-questions, workflow-keyword detection,
streaming-state checks (whether a slash command can run during an active
response), speculation/prediction results, and admission control, _before_
it ever calls `submitQuery`. In attached mode:

- Slash commands (anything starting with `/`) must still run **locally**
  — `/remote-control` itself, `/theme`, `/model`, etc. are harness
  commands, not turns for the remote agent. Don't route these to the
  daemon.
- Plain user text (not a slash command) is what should route through
  `useDaemonStream`'s `submitQuery` instead of `useGeminiStream`'s.

The cleanest seam is probably conditional construction: build
`useDaemonStream(...)` instead of `useGeminiStream(...)` when attached
(see #2 below for how "when attached" gets decided), and since both hooks'
`submitQuery` land in the same destructured variable name at the call
site, most of the huge dispatcher function _should_ keep working
unmodified — it already funnels non-slash-command input to whatever
`submitQuery` currently is. Verify this assumption carefully rather than
trusting it; the dispatcher has several branches (BTW commands, workflow
keywords, speculation) that call `submitQuery` with rich `metadata` objects
(`onAdmissionFailed`, `claimGoalTurn`, `steerInput`, etc.) that
`useDaemonStream`'s stub `submitQuery` currently ignores — confirm each
such call site degrades gracefully (i.e., the daemon-mode prompt still
gets submitted, just without the advanced local-execution behavior) rather
than silently dropping the user's input.

Also check `packages/cli/src/remoteInput/` (`RemoteInputWatcher.ts`,
`RemoteInputContext.tsx`) before designing new plumbing — it's an existing,
different remote-input mechanism (`--input-file`-based JSONL commands from
an external process, calling the same `addMessage`/`submitFn` path used by
normal typed input) that already proves "inject a message into the normal
submission path from outside the keyboard" is a solved problem in this
codebase. It's a different transport (file, not daemon HTTP) but may be
useful precedent for how `addMessage`-level injection is expected to work,
particularly for how confirmation responses are threaded
(`ConfirmationHandler`, `confirmation_response` command type) — there may
be a reusable pattern for permission-answer wiring here instead of building
something bespoke.

### 2. `gemini.tsx` startup branch

`packages/cli/src/gemini.tsx` already branches on `isAcpMode` around line
396 (`argv.acp || argv.experimentalAcp`) to decide whether to run
`runAcpAgent` instead of the normal interactive path. Add a parallel
branch — gate it behind an explicit opt-in for now (a flag or env var; the
design doc's Stage C is a _separate_, later step that flips this to
opt-out by default, don't do that yet) — that:

1. Calls `ensureWorkspaceDaemon(cwd)`.
2. Creates a new session (`POST /session`) or resumes
   (`POST /session/:id/resume`) depending on whether `--resume`/`--continue`
   was passed — the daemon route already exists for this
   (`packages/cli/src/serve/routes/session.ts:3369-3370`).
3. Passes the resulting `{baseUrl, sessionId}` down to wherever
   `AppContainer` gets constructed, so it can choose `useDaemonStream` over
   `useGeminiStream` at the `AppContainer.tsx:2129` call site.

## Acceptance test (do this for real, on this box, before calling it done)

From a real interactive session in attached mode: send a prompt from the
terminal. Separately, `curl` the daemon "as the phone" — attach SSE
(`GET /session/:id/events?clientId=phone`), send a prompt
(`POST /session/:id/prompt`). Confirm both streams show both turns live,
and confirm a permission request raised by either turn is answerable from
either side (`POST /session/:id/permission/:requestId`). This exact test
(minus the terminal side, which didn't exist yet) was already run
successfully against the raw daemon API — see the design doc's Validated
Fact 1 for the command sequence to adapt.

## Landmines — check these are still true before proceeding, don't assume

- **`packages/sdk-typescript/src/daemon/{RestSseTransport,sse,transports,
index}.ts`** had real, unrelated, uncommitted work in progress as of this
  writing (SSE transport / idle-timeout changes). Check `git status` — if
  still uncommitted and unrelated to remote-control, do not modify, depend
  on, or build on top of those files. This is _why_ the SSE client in this
  feature is hand-rolled instead of using the SDK.
- **`git add -A packages/` is unsafe** if that WIP is still present — it
  would sweep those files into an unrelated commit. Stage explicit file
  paths only.
- **Another session/agent has pushed directly to `canopy-rebrand`
  mid-development before** (e.g. ChatGPT OAuth support landed while this
  feature was being built). Check `git log canopy-rebrand -5` and
  `git fetch origin && git log HEAD..origin/canopy-rebrand` before assuming
  the branch you're building on is still what you expect. Do this work on
  `remote-attach`; don't merge/push to `canopy-rebrand` or `main` without
  the user's explicit go-ahead.
- The daemon backend's own multi-client co-driving support does **not**
  need to be built — it already works (Validated Fact 1 in the design
  doc). If something about co-driving seems broken, the bug is almost
  certainly in the new `packages/cli` client code, not the daemon.
- Filenames under `packages/cli/src` must be kebab-case
  (`check-file/filename-naming-convention` ESLint rule via
  `eslint.legacy-filenames.mjs` exceptions for pre-existing names only —
  new files don't get added to that exception list, they just get named
  correctly). Both new files in this feature already follow this
  (`ensure-workspace-daemon.ts`, `daemon-session-events.ts`,
  `use-daemon-stream.ts`) — keep doing so.
- This repo went through a qwen→canopy rebrand scoped to `packages/cli` +
  `packages/core` only. Real backend/model identity (`qwen*` model-name
  regexes, OAuth/asset URLs, the `QwenLM` GitHub org, cross-package
  contracts with ~20 out-of-scope sibling packages still named
  `@qwen-code/*`) is deliberately untouched — don't "fix" `qwen` strings
  you encounter outside `packages/cli`/`packages/core` without checking
  whether they're one of those real external references first.

## After Stage A lands

Stages B (`/remote-control` command: Tailscale enable + webhook + QR),
C (flip default to opt-out), and D (Codex prompt for the CanopyChat iOS
app side — a _different_ handoff doc than this one, written after Stage
B's webhook payload shape is real code) are tracked as tasks and described
in the design doc. Don't start them until Stage A's acceptance test passes
for real.
