# Rust single-file Git diff hunks

`git_diff_hunks.rs` adds the reusable native equivalent of
`fetchGitDiffHunksForFile`. Its API is
`fetch_git_diff_hunks_for_file(cwd, file_path, old_path)`, returning an
optional `{ hunks, truncated }` value with camelCase-serializable hunk fields.
The helper is synchronous and should run on a blocking worker when called by
an async HTTP route.

For tracked files it runs `git diff HEAD` with literal pathspecs, rename
detection when a valid `old_path` is supplied, and both `--no-ext-diff` and
`--no-textconv`. It refuses merge, rebase, cherry-pick, and revert states.
Each Git command has a five-second deadline and a 64 MiB stdout ceiling. Hunk
blocks above 1,000,000 bytes are omitted; returned tracked hunks retain at most
400 content lines. The `truncated` field is set when the line cap cuts the
diff. No parsed hunk is returned for unchanged or binary tracked files.

Untracked files are synthesized only when `git ls-files --others
--exclude-standard` finds the exact literal path. The helper accepts regular
text files only, checks the first 8 KiB for NUL bytes, reads at most 1,000,001
bytes to detect growth past the 1 MB cap, and emits at most 400 added lines.
Empty untracked files return an available helper result with an empty hunk
list. Ignored files, symlinks, special files, unreadable files, and detected
binary files return `None`.

Relative paths reject empty values, drive prefixes, absolute prefixes, and any
`..` segment. Absolute paths must be lexically inside the canonical Git root.
Unix untracked reads walk each directory through `openat` descriptors using
`O_NOFOLLOW`, then open the final regular file with `O_NOFOLLOW` and
`O_NONBLOCK`; this prevents both static symlink escapes and ancestor replacement
from redirecting the read. The non-Unix fallback rejects observed symlink
components and checks the canonical parent, but cannot provide the same
descriptor-based race protection with the current implementation.

`GET` and `HEAD /workspace/git/diff/file` are wired through the native serve
transport. The route rechecks workspace trust, shares the two-slot diff
concurrency budget, runs the helper on a blocking worker, applies the 20-second
route timeout, preserves the TypeScript `{v, workspaceCwd, path, available,
hunks, truncated?}` shape, and caps serialized output at 8 MiB. Missing or
repeated `path` query parameters return the source-shaped 400 parse error;
duplicate `oldPath` values are ignored. Absolute paths containing `..`
components are rejected rather than normalized, which is stricter than the
TypeScript helper; ordinary `.` components are normalized by Rust path
handling. An invalid optional `old_path` is ignored to preserve the TypeScript
rename fallback.

The helper and route passed `rustfmt --check`, scoped whitespace checks, and
the locked offline Rust workspace `cargo check`. The CLI reports six existing
dead-code warnings. No tests were run.
