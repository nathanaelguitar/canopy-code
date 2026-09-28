# Codex runtime for Canopy

Status: direction decision, 2026-09-27. This replaces the repo-wide Rust
rewrite as the primary migration goal. It does not claim that the Codex adapter
or CanopyChat integration is implemented.

## Decision

Build Canopy's agent runtime on Codex and keep Canopy as the product layer:
Canopy owns its CLI, workspace daemon, CanopyChat connection, session metadata,
and Canopy-specific tools and workflows. Codex owns the agent turn loop and
its thread execution. Stop adding Rust ports solely to reach line-count parity.

For a product UI that must display events and handle approvals, use Codex App
Server as the runtime boundary. First validate the official TypeScript SDK as
the shortest local integration path; use the documented app-server JSON-RPC
protocol where the Canopy daemon needs explicit control of event streaming,
approval routing, cancellation, or thread lifecycle. Do not expose the Codex
app-server transport to CanopyChat. Keep it local to the Canopy daemon and
translate between it and Canopy's existing authenticated/pairing-aware session
API. The current app-server docs describe stdio as the default transport and
WebSocket as experimental and unsupported for production.

References: [Codex as a platform](https://developers.openai.com/blog/codex-as-a-platform),
[Codex App Server](https://developers.openai.com/codex/app-server), and
[Codex TypeScript SDK](https://developers.openai.com/codex/sdk).

## Preserve CanopyChat remote control

CanopyChat remains the key product requirement. A Canopy session must still be
usable concurrently from the terminal and phone, with both clients seeing the
same streamed output and being able to submit prompts or answer a pending
permission request. The phone continues to connect through Canopy's daemon and
existing pairing/Tailscale flow; it must not need to know Codex's internal
thread identifiers or app-server protocol.

The intended ownership boundary is:

- One Canopy workspace daemon owns the local Codex app-server process.
- The daemon maps a Canopy session ID to a Codex thread ID and persists only
  Canopy-specific metadata alongside that mapping.
- Codex is the single writer for its thread history. Canopy projects Codex
  events into its current session event stream rather than running a second
  agent loop or maintaining a competing transcript.
- Existing Canopy clients keep using Canopy session IDs, event IDs, prompt
  submission, cancellation, and permission routes. The adapter translates
  those operations to Codex and translates the resulting events back.
- Codex's local process transport stays private to the daemon. Remote access
  continues through Canopy's own auth, pairing, and network controls.

This is a target architecture, not a claim that current Canopy daemon routes
are already Codex-backed. The current remote-control design and its live-tested
co-driving behavior are documented in
[`2026-08-26-remote-control.md`](./2026-08-26-remote-control.md). Its Stage A
plan to run a `canopy --acp` child as the execution engine is superseded; retain
the validated daemon and CanopyChat contracts, but redo runtime wiring for
Codex.

## What the Rust work contributes

The Rust work is a reference implementation and a source of portable behavior,
not a second runtime that needs to be completed before Canopy can ship on
Codex. Reuse only pieces that close a specific Canopy product gap:

- **CUA image bounds:** carry the explicit decode and capture limits into the
  CUA path used by Codex. These limits reduce oversized-image risk; they do not
  establish the cause of the historical Node/V8 crashes or prove the memory
  issue is fixed.
- **Mobile MCP:** preserve the iOS 17+ physical-device tunnel and WebDriverAgent
  setup behavior where it is useful to CanopyChat. Hardware validation is
  still required.
- **Audio capture:** retain the Rust N-API addon only if the existing Canopy
  voice workflow needs it; preserve graceful optional fallback behavior.
- **Canopy-specific services:** expose desired memory, external-context,
  browser, device, and workflow capabilities as Codex MCP tools or other
  supported Codex extensions. Keep their Canopy policy and data contracts.
- **Parity notes:** use the Rust port status files as behavior inventories and
  edge-case references. Do not transplant the standalone Rust CLI, provider
  loop, TUI, or session runtime into the Codex integration by default.

The Rust port's source-line percentages are historical code-volume snapshots,
not percentages of product completion or parity. The repo-wide Rust rewrite is
paused. No crash fix or memory reduction is claimed from porting code alone.

The current branch also contains Rust prototypes for core/CLI behavior,
daemon SDK transports and event normalization, external-context MCP programs,
audio capture, mobile MCP, and several native TUI commands. The most directly
portable pieces are the bounded CUA image handling, mobile device setup,
Canopy-specific MCP tools, and daemon/event contract notes. The cron scheduler
is only an in-memory prototype: durable schedules and CLI delivery into the
interactive turn loop are not wired. The native TUI and independent Rust agent
runtime should not be treated as the Codex product path. Prior targeted Rust
compiles and formatting checks are recorded in the implementation notes; this
direction update does not claim fresh test results or hardware validation.

## Migration sequence

1. **Prove the runtime boundary locally.** Start a Codex session from Canopy;
   stream assistant and tool events; approve and reject a tool request; cancel
   a turn; resume the same Codex thread after closing the adapter; and exercise
   text, image, and voice inputs that Canopy supports. Record the exact
   app-server/SDK version and protocol behavior.
2. **Build the daemon adapter behind an opt-in.** Map Canopy create/resume,
   prompt, event replay, cancellation, and permission-answer operations to
   Codex. Keep Codex process output bounded and private. Make thread ownership
   and failure recovery explicit before enabling concurrent access.
3. **Retain remote-control behavior.** Verify terminal and CanopyChat can
   co-drive one session, receive the same ordered events, and answer a
   permission request from either client. Preserve Tailscale pairing and
   notification behavior. CanopyChat's app-side work is outside this
   repository and needs its own handoff once the daemon event contract is
   stable.
4. **Move Canopy-only capabilities.** Register the required features as
   MCP-backed tools or supported extensions. Port a feature only when its
   behavior, permissions, and persistence requirements are clear.
5. **Measure before changing defaults.** Compare startup, idle RSS, peak RSS,
   and long-session growth against the current CLI using representative CUA,
   image, audio, and remote-control workloads. Keep the Codex path opt-in until
   persistence, approvals, remote co-driving, and memory behavior meet the
   existing acceptance criteria.

## Open questions and risks

- Which Codex thread/session operations are stable enough for Canopy's durable
  resume and event-replay contract?
- Can the app-server process serve all sessions for one workspace daemon, or
  should it be scoped more narrowly? Decide from measured resource use and
  isolation requirements.
- How should existing Canopy transcripts be imported or resumed? Avoid two
  writers for one conversation and define a one-way migration if needed.
- Which approval policies can be represented in Codex and mirrored safely to
  CanopyChat without weakening Canopy's current permission checks?
- Which Canopy voice and provider behaviors map cleanly to Codex, and which
  require separate MCP or product work?
- Codex's process, sandbox, auth, and data-storage behavior must be reviewed on
  each supported desktop platform before making it the default.
