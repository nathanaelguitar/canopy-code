# Claude handoff: Canopy on Codex CLI

Updated: 2026-09-28

## Product direction

The user chose the open-source **Codex CLI** (`openai/codex`) as Canopy's
agent runtime. They mean the CLI, not the Codex desktop app. Keep Canopy's
existing CLI, workspace daemon, CanopyChat remote control, pairing/Tailscale,
and session API. The daemon was already functional; repeated crashes were
reported, with OOM/high RAM as a hypothesis rather than an established cause.
Do not resume the repo-wide Rust rewrite. Use the Rust work as a behavior
inventory where it helps preserve Canopy-specific features.

The intended boundary is Canopy's daemon and ACP channel -> private local
`codex app-server` subprocess -> Codex thread execution. Canopy remains the
public session/event/auth boundary. Codex thread IDs and JSON-RPC must not be
exposed to CanopyChat. Keep the Codex runtime opt-in until compatibility,
remote co-driving, permissions, and memory behavior are verified.

Read first:

- [`AGENTS.md`](AGENTS.md) for repository rules.
- [`docs/design/codex-canopy-integration.md`](docs/design/codex-canopy-integration.md)
  for the architecture and migration sequence.
- [`docs/design/2026-08-26-remote-control.md`](docs/design/2026-08-26-remote-control.md)
  for validated daemon and CanopyChat contracts.

[`CODEX-HANDOFF-remote-control-stage-a.md`](CODEX-HANDOFF-remote-control-stage-a.md)
is historical. Its daemon/co-driving evidence is useful; its plan to make a
`canopy --acp` child the agent runtime has been superseded.

## Current implementation

The opt-in entry point is:

```sh
CANOPY_AGENT_RUNTIME=codex canopy serve
```

Set `CANOPY_CODEX_CLI_PATH` if `codex` is not on `PATH`. The default remains
Canopy's current runtime. The daemon uses its existing ACP channel factory;
the ACP helper starts `codex app-server` over private stdio. The adapter is in
[`packages/cli/src/codex-runtime/acp-agent.ts`](packages/cli/src/codex-runtime/acp-agent.ts).
The internal `--codex-acp` dispatch is in `packages/cli/src/cli.ts`, and the
daemon runtime selection is in `packages/cli/src/serve/run-canopy-serve.ts`.

The current slice supports session create/load/resume, text and image prompt
input, text resources, assistant streaming, basic shell/file-change/MCP and
web-search tool updates, cancellation, model and reasoning selection, Canopy
mode mapping, and shell/file-change permission prompts. It serializes
translated notifications to preserve event order. A sidecar in
`$QWEN_HOME/codex-sessions/` (default `~/.canopy/codex-sessions/`) maps Canopy
session IDs to Codex thread IDs and persists selected mode, model, and
reasoning effort.

Approval mapping is approximate: `default` maps to `on-request`; `auto` and
`auto-edit` map to Codex `untrusted`; `plan` maps to read-only; `yolo` maps to
full access without approval prompts. Review these semantics carefully before
changing or recommending a default.

## Important gaps and unverified behavior

- This is an experimental adapter slice, not a full migration or parity port.
- No live Codex app-server session or CanopyChat co-driving verification has
  been run. The existing daemon routes are retained, but their behavior with
  this runtime is not yet proven.
- No RSS comparison or crash diagnosis was performed as part of this change.
  Do not claim a memory reduction or an OOM fix.
- Extra permission grants and MCP elicitation are not translated; unrecognized
  Codex server requests fail closed. Canopy-owned skills, memory, extension
  tools, CUA, audio, and workspace MCP configuration are not integrated.
- The ACP bridge currently sends `mcpServers: []` for new sessions, so the
  adapter's MCP-config conversion does not connect Canopy workspace MCP
  settings today. Codex's own MCP configuration is separate.
- The app-server JSONL transport is hand-written. Implementation was compared
  against locally generated protocol types for Codex CLI 0.157.0, but those
  scratch schemas are not checked in. Recheck the protocol against the actual
  installed CLI version. The installed CLI version was observed as 0.157.0;
  no runtime handshake smoke test was performed.
- Abrupt parent termination, app-server cleanup, auth failures, multiple live
  ACP sessions, and all input types used by CanopyChat need explicit checks.

## Next steps

1. Review the full adapter against the installed Codex CLI's current
   app-server protocol. Confirm the initialize handshake, thread lifecycle,
   turn events, tool item shapes, approval responses, and shutdown behavior.
   Consider using a maintained official SDK or generated types where that
   reduces protocol drift; keep the daemon-facing ACP boundary.
2. Add focused tests for JSONL request correlation/errors and line limits,
   session sidecar resume/config persistence, notification ordering, prompt
   cancellation, and approval mapping. Follow `AGENTS.md`; do not rely on
   typecheck alone for behavioral changes.
3. Run a live smoke test in a disposable workspace: create a session, stream a
   reply, trigger and answer both an approval and rejection, cancel a turn,
   restart the adapter/daemon and resume the same thread, and check failure
   behavior when Codex is not authenticated or exits unexpectedly.
4. Verify terminal and CanopyChat can co-drive one session: both receive the
   same ordered events, both can submit prompts, and either can answer a
   permission request. Keep Canopy's pairing/Tailscale and auth path intact.
5. Measure baseline Canopy and Codex-runtime startup, idle RSS, peak RSS, and
   long-session growth with representative CUA/image and remote-control
   workloads. The original screenshot alone does not establish that CUA caused
   the crash.
6. Only after behavior is verified, port required Canopy-specific features as
   narrowly scoped MCP tools/extensions, retaining Canopy policy and data
   contracts. Keep opt-in until acceptance criteria are met.

## Verification and repository state at handoff

Commit `16109e986` (`feat(cli): add opt-in Codex runtime`) is pushed to
`origin/feature/browser-captcha-solver`. The CLI package typecheck, targeted
ESLint, and package build passed. The commit hook ran Prettier and ESLint.
No tests or live integration checks were run.

At handoff, `git status --short` shows this new handoff file as uncommitted and
a pre-existing modified `package-lock.json`; the lockfile was intentionally
not included in the Codex commit. Inspect its diff before touching it, and do
not sweep it into a follow-up commit unless the user asks.
