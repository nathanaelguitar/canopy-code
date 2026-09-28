# Standalone updater port status

`standalone_update_command.rs` provides the async helper
`perform_standalone_update(standalone_dir: &Path, new_version: &str) ->
Result<StandaloneUpdateResult, String>`. `update_command.rs` invokes it for
verified installs that are running through their bundled Node executable.

## Ported behavior

- Validates the version and target, downloads the target archive and
  `SHA256SUMS` from the OSS release host with GitHub release fallback, and
  streams the archive to disk with a 512 MiB limit.
- Requires `SHA256SUMS.sig` and verifies the Ed25519 signature over the exact
  checksum-file bytes. Release builds must embed a production key with the
  compile-time `CANOPY_RELEASE_PUBLIC_KEY_DER_B64` setting; builds without it
  fail closed. Unsigned archives and the TypeScript source's test key are not
  trusted.
- Caps manifests at 64 KiB; limits archives to 100,000 entries and 64 MiB of
  cumulative path bytes; and caps extracted content at 2 GiB. Strict
  `canopy-code/` root paths, file/directory-only zip support, tar
  regular-file/directory/symlink support, and post-extraction symlink checks
  are enforced. Hard links, special files, absolute paths, parent traversal,
  and unsafe symlink targets are rejected.
- Rejects downgrade requests and requires the archive manifest and the new
  bundled Node CLI's `--version` output to equal the requested version. The
  smoke test has a ten-second timeout and captures at most 64 KiB from each
  output stream.
- Uses a PID lock and random private staging directories. A stale lock is not
  deleted automatically; the error asks the user to verify no updater is
  running before removing it. Linux and macOS stage the candidate at `.old`,
  atomically exchange it with the current install, and retain the former
  install at `.old`. Downloaded archive data is `sync_all`'d before extraction.
  Before exchange, regular staged files and directories are synced bottom-up;
  the staging and install parent directories are synced after staging rename.
  The install parent is synced again after exchange. Rollback metadata is also
  synced on a best-effort basis. Filesystems without atomic directory exchange
  or directory sync support fail closed.
  Windows stages to `.new` and starts a detached helper that waits for the
  current CLI and optional launcher PID before moving directories; it logs the
  swap, writes rollback metadata after promotion, and attempts rollback if
  promotion fails.
- `/doctor rollback` detects the running Node standalone install and calls the
  rollback helper on non-Windows platforms. The helper refuses a live update or
  deferred-swap process, validates the preserved manifest's package and target,
  moves the current install to `.failed`, restores `.old`, and attempts to put
  the current install back if promotion of `.old` fails. Cleanup of `.failed`
  is best effort. Non-standalone installs receive the informational
  standalone-only message. Windows follows the TypeScript manual-recovery
  message rather than renaming a running installation; helper errors are shown
  with a `Rollback failed:` prefix.

## Gaps and release constraints

- The upstream artifact remains a Node standalone package (`node/`,
  `lib/cli.js`, and `manifest.json`). Detection requires the running process
  to be the bundled Node executable, so native Rust installs are not replaced
  with a Node bundle. Native Rust update assets and their version source are
  not defined in this slice.
- First-time npm-to-standalone migration, PATH wrapper creation/repair, shell
  rc edits, and update event/i18n plumbing are not ported. Rollback validation
  is stricter than TypeScript: malformed lock files fail closed, and a
  preserved manifest for another package or platform is rejected. The previous
  `.old` directory remains available for manual recovery, and the automatic
  promotion step handles errors it observes.
- On Linux or macOS, a sync failure before exchange aborts while the current
  install is still active; cleanup of the staged candidate is attempted and
  any leftover path is included in the error. If the exchange succeeds but
  syncing its parent fails, the update is already visible and `.old` retains
  the previous install; the command reports uncertain crash durability and
  leaves both trees for manual inspection/recovery instead of deleting either.
- Directory synchronization uses the host `sync_all` implementation. Sudden
  power-loss guarantees still depend on the filesystem and storage stack.
  Windows syncs the downloaded archive file but does not fsync the extracted
  tree or directory entries before its deferred batch swap. Other Unix targets
  do not implement atomic directory exchange and fail closed.
- The release pipeline must provide the production public key at build time
  and publish signatures with the archives. Until both are wired, the updater
  intentionally refuses to update.
- Windows deferred swaps run through a batch helper and cannot be exercised
  from this macOS worktree. Targets follow the upstream list; Windows ARM and
  other targets are unsupported.

## Dependencies and verification

- The module is included as a private child module of `update_command.rs`; no
  `main.rs` edit is needed. `canopy update` invokes it only after checking the
  latest npm tag and only for the detected Node standalone layout.
- Direct `canopy-cli` dependencies added for the port are `ring`, `flate2`,
  `tar`, `walkdir`, and `zip`. They were already present in the workspace
  lockfile transitively, so only the `canopy-cli` lock dependency list needed
  updating.
- `cargo fmt --check -p canopy-cli`, locked offline workspace `cargo check`,
  and `git diff --check` pass. No tests were added or run.
- For the `/doctor rollback` routing slice, only `rustfmt` and whitespace checks
  were run; no tests or Cargo commands were run.
