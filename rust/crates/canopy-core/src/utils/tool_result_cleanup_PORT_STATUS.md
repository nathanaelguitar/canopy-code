# `toolResultCleanup.ts` Rust port status

Source inspected: `packages/core/src/utils/toolResultCleanup.ts`. The named
`packages/core/src/utils/toolResultCleanup.test.ts` is absent from this
checkout, so the six Rust regression tests are based on the implementation's
observable rules and edge cases.

No equivalent Rust cleanup routine existed in the audited tree. Existing
`tool_response_finalizer` code writes artifacts and `storage` resolves their
directory, but neither performs this age-based cleanup.

Implemented in `tool_result_cleanup.rs`:

- Sequential project enumeration and sequential cleanup of each project's
  `tool-results` entries followed by legacy top-level `.output` files.
- `symlink_metadata`/`lstat` behavior: only regular files are candidates;
  project, file, and directory symlinks are skipped.
- Source-compatible missing-path and error handling: unreadable global or
  nested directories return without incrementing `errors`; failed project
  stats increment it; candidate `ENOENT` during stat/unlink is ignored; other
  candidate stat/unlink failures increment it.
- The exact age comparison (`now - mtime < max_age` keeps the file), one
  integer-millisecond time snapshot, deletion counts, and byte totals.
- Seven focused in-file regression tests for cleanup thresholds, deleted byte
  totals, missing paths, and symlink safety.

Debug logging is not mapped: this workspace slice has no matching logger
available to the isolated utility. Cleanup results and error counts are
preserved; the TypeScript debug/warn messages are omitted.

## Integration and verification

The module is exported from `rust/crates/canopy-core/src/utils/mod.rs`, and
both the native CLI and ACP runtime schedule it as a detached startup task.
The caller resolves the configured temp directory before spawning, preserving
the active runtime root. Cleanup counters remain available to awaited callers;
the CLI startup path intentionally ignores the result like the TypeScript
fire-and-forget caller.

Verified on macOS ARM64 (Darwin 25.6.0) as part of the exported utility suite:

- `cargo test -p canopy-core utils:: --locked --offline` — all seven cleanup tests passed within 182 utility tests.
- `cargo fmt --all -- --check`.

No manifest or dependency changes were needed.
