# Session artifact port status

`session_artifacts.rs` ports stable artifact identity, locator validation,
ownership checks, bounded live collections, source-aware eviction, JSONL event
and snapshot records, tombstones, sticky ephemeral markers, transcript replay,
and live snapshot restore.

Live restore now normalizes every persisted locator against the active session
and workspace, verifies that stable IDs still match, rechecks workspace file
presence and size/mtime metadata, rejects workspace escapes, sanitizes
persisted metadata, downgrades persisted `pinned` retention to `restorable`,
and marks unavailable or changed entries as metadata-only restores. It restores
tombstone and sticky-ephemeral markers, can preserve already-live ephemeral
entries, rolls back when no persisted artifact can be restored or the replay
contains completeness warnings, and prunes overflow to the configured live
limit.

The TypeScript implementation still has broader integration and lifecycle
coverage. Rust restore is synchronous, does not hash workspace contents, and
does not expose the TypeScript warning-detail/durability model. The recorder
rebuilds and restores the transcript projection at construction and exposes
those warnings through `SessionRecorder::artifact_restore_warnings()`; a
runtime path that replaces a live store from a later rebuilt projection has
not been connected. Full client tombstone-owner bookkeeping, adaptive snapshot
policy parity, and persistence warning telemetry remain open. The convenience
`from_snapshot` constructor still discards restore warnings; callers that need
diagnostics should use `restore_snapshot` with the rebuilt projection.

No tests were added or run for this slice. Rust formatting was applied; Cargo
verification is pending because the shared lockfile is being updated by the
parallel HTTP transport work.
