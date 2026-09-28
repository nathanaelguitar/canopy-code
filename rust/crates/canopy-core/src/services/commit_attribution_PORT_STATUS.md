# Commit attribution service port status

`commit_attribution.rs` ports the reusable state and payload logic from
`packages/core/src/services/commitAttribution.ts`:

- Per-file AI contribution counters, AI-created flags, and post-write SHA-256
  hashes, including BOM/CRLF canonicalization and divergence resets.
- Prefix/suffix diff contribution counts using UTF-16 code units, matching
  JavaScript `String.length` semantics for supplementary Unicode characters.
- Canonical path capture with missing-leaf parent resolution, repo-relative
  matching, committed rename movement, partial cleanup, and prompt windows.
- Versioned session snapshots with defensive value coercion, schema reset, and
  path-collision merging on restore.
- Staged-file note calculation, generator-name sanitization, generated-file
  exclusions, a 50-path generated-file sample cap, and the source's approximate
  `(added + removed) * 40` / binary-1024 diff-size inputs.
- `commit_attribution_git.rs` parses captured `--name-only`, `--name-status`,
  and `--numstat` output into `StagedFileInfo`, including deleted paths,
  rename mappings, normalized numstat rename paths, and the binary-size
  fallback. It returns a distinct empty-commit result only when all command
  outputs are available and the name list is empty; unavailable commands or
  missing numstat entries for changed files are analysis failures.

## Integration seam and remaining parity limits

- `WriteFileTool` optionally receives a host-owned shared service and updates
  it after a successful write. `EditFileTool` and `NotebookEditTool` forward
  the same handle to their shared writer. The native CLI attaches the same
  session service to these tools and `ShellTool`; ACP uses that shared tool
  executor. Prompt counts and snapshots flow through `AgentToolExecutor` for
  runtime persistence after prompt setup and tool batches, and resumed
  sessions restore the latest projected transcript snapshot.
- `ShellTool` now detects safe, foreground `git commit` commands, preserves
  the configured `general.gitCoAuthor.commit` toggle, and adds the default
  Canopy `Co-authored-by` trailer to inline `-m`/`--message` commits when the
  active shell supports safe POSIX quoting. It skips editor-driven messages,
  existing matching trailers, redirected repositories, and commands whose
  repository context cannot be established. Background commits are refused
  because they cannot be attributed synchronously.
- After a foreground commit moves `HEAD`, `ShellTool` verifies that exactly
  one commit landed, captures SHA-pinned file/status/numstat diffs (including
  root commits and amends), projects them through `commit_attribution_git`,
  applies rename mappings, validates tracked hashes against committed blobs,
  and adds a size-capped `refs/notes/ai-attribution` note to the captured
  commit. Successful notes, or commits with attribution disabled, partially
  clear only tracked files that landed in that commit. Analysis and note-write
  failures preserve file states and advance only the commit prompt window.
- Shell edits made outside the tracked write/edit/notebook tools are not
  recorded as AI contributions. Complex shell syntax, custom Git wrappers,
  and environment-directed repositories are handled conservatively and can
  skip note attribution; unsupported shells skip trailer injection. Non-UTF-8
  paths are still represented lossily in snapshots, unlike Node string paths.
- `SessionRecorder` can append deduplicated `attribution_snapshot` records,
  and `session_attribution_state::restored_attribution_snapshot` reads the
  latest object snapshot from branch-ordered transcript records. The host
  remains responsible for attaching the shared service and settings when it
  constructs the workspace tools.
- `services::attribution_trailer::build_git_notes_command` remains the argv
  builder and enforces the source's 30 KiB serialized-note byte limit by
  returning `None`; this service preserves the source behavior of capping only
  the excluded-generated sample rather than truncating the files map.
- Rust `String` cannot contain unpaired UTF-16 surrogates. Valid Unicode text
  counts exactly in UTF-16 units; malformed JS strings are outside this API's
  representable input domain. Non-UTF-8 filesystem paths are represented
  lossily in snapshots, unlike Node's string paths.
- When multiple snapshot keys canonicalize to one path, insertion order picks
  the freshest nonempty hash as in TypeScript. `serde_json` is configured with
  ordered object preservation so restore maintains the serialized key order.
