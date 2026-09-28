# Rust Git branches helper

`git_branches.rs` provides `fetch_git_branches(cwd: &Path) ->
Result<GitBranches, GitBranchesError>`, the read-only native equivalent of
`fetchGitBranches`. The serializable DTOs preserve the TypeScript camelCase
fields: local and remote branch metadata, tags, recent checkout targets, the
current head, and detached state. Missing upstream values are omitted from
JSON. The initial `rev-parse --git-dir` probe reports `NotRepository` for the
usual Git diagnostic; other probe failures distinguish command failure,
timeout, and output-limit overflow.

Ref rows use NUL-delimited `for-each-ref` fields and reject malformed rows and
symbolic refs. The helper queries local branches, remote branches, tags, the
symbolic head, and at most 200 reflog entries concurrently. Optional query
failures produce empty data, matching TypeScript behavior. Recent history
keeps up to 20 unique checkout destinations and omits the current head and
object IDs.

Every Git process has a 10-second deadline and a 10 MiB ceiling for stdout and
stderr. Over-limit streams are drained without retaining more data, and
timed-out processes are killed. The helper removes inherited repository and
object-store selectors, global/system config redirects, `GH_REPO`, and all
numbered `GIT_CONFIG_KEY_` / `GIT_CONFIG_VALUE_` variables before starting
Git; it sets `LC_ALL` and `LANG` to `C`.

The `/workspace/git/branches` route applies a 35-second overall deadline and
an 8 MiB serialized response cap. Like the TypeScript implementation, optional
ref and reflog errors are intentionally suppressed, so those errors do not
appear in the result. Non-UTF-8 ref names and subjects are decoded with
replacement characters for JSON compatibility.
