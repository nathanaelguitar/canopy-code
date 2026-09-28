# Persisted session lifecycle port status

Source: `packages/core/src/services/sessionService.ts` methods
`removeSession`, `removeSessions`, `archiveSessions`, `unarchiveSessions`,
and `renameSession` (around lines 1539–1840).

## Covered in `session_lifecycle.rs`

- UUID-like ID validation, active/archived project-head checks, worktree-root
  ownership recognition, and runtime-status ownership fallback.
- Session removal, duplicate elimination, per-ID outcomes, best-effort usage
  salvage, worktree sidecar removal, and file-history backup cleanup.
- Usage salvage reads a no-follow snapshot capped at 64 MiB to bound memory
  before calling the existing summary writer; larger transcripts skip salvage
  and still proceed to deletion.
- Archive/unarchive conflict checks, per-ID errors, sidecar moves, and known
  location fast paths.
- Custom-title JSONL records with source, timestamp, original cwd/version, and
  the recovered tail UUID as `parentUuid`.
- Unix transcript opens and title appends refuse leaf symlinks; transcript
  reads are bounded by the existing JSONL record limit.

## Integration dependencies and remaining gaps

- The parent crate must export this module from `services/mod.rs` and connect
  `SessionLifecycle` to its callers. No existing core session-organization
  cleanup implementation exists yet; provide a `SessionOrganizationCleanup`
  implementation to preserve metadata cleanup after deletion.
- `remove_sessions` currently processes filesystem work sequentially; the TS
  implementation starts removals concurrently and then reports in input order.
- The existing Rust usage-salvage helper returns only success/failure and
  swallows read/write errors, so this module cannot emit the TS warning for a
  failed salvage attempt.
- `rename_session` uses the source's 64 KiB tail strategy. When the final
  record exceeds that window, it may not find its UUID, matching the source
  behavior; JSONL parsing itself is capped at the existing 16 MiB record
  limit.
- Seven focused module tests pass from an isolated harness. Registering the
  module in `services/mod.rs` will allow the same tests to run in the workspace.
