# ACP bridge journal and replay port status

`replay_window_limits.rs` ports the ACP bridge's compacted replay and journal
defaults, safe-integer validation, JavaScript-shaped validation messages, and
per-session growth baseline record. `journal_growth_policy.rs` ports the
stateless shared-pool accounting: each session is charged above its own
baseline, a request includes its current cap, grants are bounded by the
remaining pool and the 256 MiB per-session hard cap, and event caps scale
proportionally.

`compaction_engine.rs` is now connected to `SessionEventBus`. Admitted events
feed a separate bounded in-flight journal and a turn accumulator; `turn_complete`
and `turn_error` compact accumulated text chunks, tool updates, and latest-wins
updates into replay segments. The compacted replay window has its own 4 MiB
default and evicts whole segments. `replay_snapshot`, `live_journal_snapshot`,
`journal_limits`, and `compaction_memory_stats` expose that state to ACP
consumers. Seeded replay is stored in the compaction owner but still clears the
reconnect ring, as before.

The reconnect ring, per-subscribe replay burst, compacted replay window, and
live journal remain separate limits. `replay_budget_bytes` and ring settings
were not changed. Adaptive growth runs only when a pool is configured and no
journal cap was explicitly pinned. `BridgeSessionRuntimeOptions::with_memory_budget`
derives the pool in bytes through `services::daemon_memory_budget`; one
registry is shared by all sessions in a runtime, and callers can pass one
registry to multiple workspace runtimes. Every session registers its own byte
baseline and releases its grant when its bus closes or drops.

The configured Rust ACP CLI still constructs default runtime options without
calling `with_memory_budget`, so that executable path keeps adaptive growth
disabled. Rust `session/load` now seeds an empty restored event bus from the
active transcript's text-bearing user and assistant records, then uses the
compaction snapshot for both bulk replay and ordered `session/update`
notifications. The bulk response keeps the versioned
`qwen.session.loadReplay.updates` envelope and sets `partial: true` when the
compacted snapshot reports truncation. The request's top-level
`liveReplayMode` accepts `full` or `summary`; the existing `_meta` replay mode
still selects bulk versus streamed delivery.

The transcript projection preserves each available transcript UUID as
`qwen.session.recordId` and `qwenTranscript.sourceRecordIds`, and carries a
parseable transcript timestamp. The transcript API does not retain the
original ACP update's prompt ID, originator client ID, parent-tool metadata,
or arbitrary per-update `_meta`; the current projection also remains limited
to text from user and assistant records, as before. Tool results, system
records, and non-text media are not reconstructed as ACP updates. Truncation
markers are represented by `partial: true` in the bulk envelope; streamed
ACP `session/update` notifications have no compatible field for that marker,
so the marker itself is omitted from streamed updates.

This is a functional first pass, not complete `compactionEngine.ts` parity.
Full and summary live journals now retain separate event queues, byte totals,
truncation counts, and latest record-id anchors. They use the same current
per-session caps; a useful grant raises those caps for both journals, while
each journal independently asks and evicts against its own retained state.
The summary filter follows the source's parent-tool rules and only treats
numeric `usage.inputTokens` / `usage.outputTokens` values as usage records.
Each live-journal truncation marker carries that journal's latest record-id
anchor when available. Anchors survive turn-boundary journal resets and are
seeded from persisted replay events, matching the TypeScript lifecycle.

Text compaction now follows the source's grouping rules: top-level chunks merge
only while adjacent, while subagent chunks reassemble across interleaved slots
by update kind, parent tool-call ID, and normalized
`qwenTranscript.sourceRecordIds`. A new tool-call slot clears that parent's
text index so later chunks start a fresh segment; index entries are rebuilt
after slot eviction. Discrete message chunks remain separate. Merged turn
events concatenate text, merge update metadata field by field, shallow-merge
nested `qwenTranscript` values, union source record IDs in encounter order,
and preserve the latest available event ID, envelope metadata,
prompt/originator fields, and string session ID.
The live snapshot only groups text events whose data/update/content keys and
metadata match the source's modeled-field rules; it permits timestamp envelope
metadata and requires matching attribution, session, parent, and source IDs.
Tool-call updates now shallow-merge update fields, ignore null update values,
merge transcript metadata, merge envelope metadata, retain attribution
independently when an incoming field is absent, and normalize the compacted
update type to `tool_call`.

Remaining compaction differences: Rust's `BridgeEvent` has fixed top-level
fields, so it cannot preserve arbitrary unknown event-envelope keys that
TypeScript object spreads retain; `data` and update `_meta` remain arbitrary
JSON values. Compacted replay pagination anchors, replay eviction callbacks,
degraded-state reporting, and status/telemetry integration are not wired yet.
The turn accumulator is bounded at the baseline limits and does not receive
adaptive growth.

`generation_stream.rs` ports the bounded, request-scoped single-consumer queue
used by generated side content. Producers retain synchronous non-blocking
`push`; consumers receive asynchronously or adapt it into a Rust stream. It
preserves direct handoff, bounded FIFO backpressure, drain-before-close/failure,
and repeated failure behavior. There is no Rust `serve` request route using it
yet, because the HTTP daemon remains unported.

`fatal_diagnostic_reports.rs` ports the diagnostic directory controls and
Node-child startup flags, and adds sanitized private reports for Rust panics.
The native CLI installs the panic hook at process startup. Panic reports omit
payloads, environment variables, and network data. Abort/OOM failures can
bypass the hook, and local paths can appear in captured backtraces; non-Unix
platforms lack a standard owner-only directory permission API.

Verification for this slice: `rustfmt --edition 2024` and
`cargo check -p canopy-core --locked --offline` passed. The CLI package also
passed a locked offline build after module wiring. No tests were added or run.
