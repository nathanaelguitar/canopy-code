# Session-reference port status

Source behavior is in `packages/core/src/services/session-reference-service.ts`
and the session-reference branch of
`packages/cli/src/ui/hooks/atCommandProcessor.ts`.

## Implemented

- `session_reference.rs` reads active project sessions, validates and rebuilds
  the transcript through shared Rust services, projects user-visible text,
  drops thought parts, derives the current title, and summarizes each tool
  response by name and status without exposing result bodies or arguments.
- Active title lookup streams the project chat directory without a 10,000-file
  cap. It keeps only a match count and one session ID; title reads use bounded
  64 KiB head/tail windows and matching transcripts use a bounded first-record
  read for project ownership validation.
- It applies the 8,000-token tail budget, omission marker, newest-line
  retention, metadata, and missing/malformed/foreign-project handling. Project
  ownership checks include direct cwd hashes, worktree roots, and runtime
  status sidecars.
- Native `canopy run` resolves `@session:<uuid|custom title>` for interactive
  prompts, new one-shot prompts, and clean `--resume` prompts. Title lookup is
  active-session only, exact and case-insensitive after trimming, and reports
  missing or ambiguous titles. Duplicate mentions are skipped both by
  mention-key and resolved session ID.
- Resolved reference blocks are extra parts of the same user turn. The typed
  prompt remains visible in the TUI and transcript `displayText`; memory recall
  uses the processed query. The TUI shows one success/error card for each
  unique session mention and surfaces error/duplicate debug messages; line mode
  prints the corresponding cards and messages to stderr. Filesystem `@path`
  references are processed by `AtFileProcessor` and use separate per-file
  success/error cards in both modes.
- Multi-part user-turn recording support is in
  `canopy-core/src/agent_runtime.rs`.

## Remaining gaps

- Filesystem `@path` references now have a native resolver with workspace and
  global-temp containment, modality-aware content projection, and per-path
  feedback. Native mixed-reference context remains grouped by kind rather than
  interleaved according to the original `@` token order.
- Extension, MCP resource, and MCP server mentions are handled by the separate
  native resource-reference resolver; they are outside this session service's
  scope. Multiple configured workspace roots are not yet supported for file
  references.
- Title lookup checks all active session files, while archived-session
  references remain unsupported, matching the TypeScript title lookup and
  active-session loader.
- The Rust token estimator is the shared character-based estimator, so
  approximate token counts can differ from the TypeScript estimator.

`cargo fmt --all -- --check` passed. `cargo check -p canopy-cli --locked
--offline` passed; it reported three existing warnings in `mcp_host.rs`. No
tests were added or run for this port.
