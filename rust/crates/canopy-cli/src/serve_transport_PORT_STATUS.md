# Rust `canopy serve` daemon slice

## Implemented

`serve_transport_command.rs` now runs as
`canopy serve [--port <0-65535>] [--token <token>] [--require-auth]`.
It binds to `127.0.0.1` by default on port `4170`; `--port 0` asks the OS for
an ephemeral port. `--transport-smoke` remains accepted as a no-op for scripts
that used the earlier transport preview. `--help` lists this slice's routes.
Other daemon flags, including `--hostname`, TLS, and session/runtime options,
fail explicitly because the Rust command does not implement them.

Bearer token resolution follows the TypeScript CLI: an explicitly supplied
`--token` takes precedence over `QWEN_SERVER_TOKEN`; the selected value is
trimmed and an empty result is treated as unset. When a token is configured,
every request that passes the host/origin guards must include
`Authorization: Bearer <token>`, including `/health`. Missing or invalid
credentials return `401 {"error":"Unauthorized"}`. With no token and no
`--require-auth`, the loopback developer default remains open. `--require-auth`
fails before binding when neither token source resolves to a non-empty value.
Bearer values are parsed directly from the bounded HTTP header bytes and
compared as SHA-256 digests; the configured token is not logged or retained in
plain text by the request context.
The daemon-status `security.tokenConfigured` and `security.requireAuth` fields
reflect the effective token and CLI flag. Supplying `--token` also prints a
warning that the value is visible in the process command line.
The TypeScript loopback startup path currently exposes `/health` before its
bearer middleware unless `--require-auth` is set. This Rust slice deliberately
uses the stricter requested contract and requires the token on `/health`
whenever one is configured.

The listener retains the bounded Hyper 1 / hyper-util HTTP/1 transport: at most
128 active connections; 16 KiB and 64 header fields with a five-second header
read timeout; a 64 KiB request-body limit and five-second body timeout; and a
five-second graceful shutdown on Ctrl+C or Unix SIGTERM. It only accepts a
loopback-bound listener. Host checks permit the matching loopback names and
`host.docker.internal`; same-origin browser requests are permitted while other
`Origin` values receive the TypeScript CORS denial response.

The implemented routes are:

| Route                                                                         | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /health`, `HEAD /health`                                                 | TypeScript shallow response `200 {"status":"ok"}`. Deep probes (`deep=1`, `deep=true`, or `deep=`) return the bootstrap `503 {"status":"degraded","reason":"bootstrap"}` with `Retry-After: 1`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `GET /capabilities`, `HEAD /capabilities`                                     | TypeScript v1 capabilities envelope shape. It advertises `health`, `daemon_status`, `capabilities`, `session_status`, `session_transcript`, and `session_transcript_pagination`, the read-only native `workspace_memory_read`, `workspace_git_read`, `workspace_git_branches_read`, `workspace_git_log_read`, and `workspace_git_diff_read` slices, and the explicitly native-only `session_transcript_records`; it does not advertise the full TypeScript `workspace_memory` read/write feature.                                                                                                                                                                                                                                                                                           |
| `GET` / `HEAD /daemon/status`                                                 | TypeScript v1 daemon-status summary; `?detail=full` includes the full section. Invalid or repeated `detail` parameters return the TypeScript `400 invalid_detail` response. RSS is sampled through Rust's bounded `NativeMemoryProbe`; native heap subdivisions are zero because Rust has no V8 heap.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `GET /workspace/memory`, `HEAD /workspace/memory`                             | Read-only filesystem metadata for the configured `context.fileName` string/list (or `CANOPY.md` and `AGENTS.md` by default) at the bound workspace root and global Canopy directory selected by the settings paths. Returns the TypeScript v1 status shape, absolute paths, byte counts and per-file stat failures; it never reads or returns file contents. Bounded settings/trust inputs are parsed read-only. Trust is re-evaluated for each request; denied, unknown, unreadable, or invalid policy returns `403 untrusted_workspace`. The serialized response is capped at 64 KiB, configured filename lists at 8 entries, and each lookup at five seconds, with two concurrent lookups.                                                                                               |
| `GET /workspace/git`, `HEAD /workspace/git`                                   | Single-workspace read-only v2 status projection. Rechecks the same bounded trust policy before probing Git, recognizes `wait=1`, and returns branch, staged/unstaged/untracked/conflict counts, upstream counts, stash count, optional operation, and `computedAt`; it does not include per-file paths. The porcelain output is capped at 8 MiB, Git process time at 2 seconds by default or 5 seconds for `wait=1`, and concurrent lookups at two. Failed, timed-out, oversized, malformed, or non-repository status returns the TypeScript branch-only shape.                                                                                                                                                                                                                             |
| `GET /workspace/git/branches`, `HEAD /workspace/git/branches`                 | Read-only TypeScript v1 branch inventory: local/remote refs, tags, current head, detached state, and up to 20 recent checkout branches. Rechecks workspace trust and uses sanitized Git environment, bounded subprocesses, concurrency, route time, and serialized output.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `GET /workspace/git/log`, `HEAD /workspace/git/log`                           | Single-workspace TypeScript v1 log-list shape: `{v, workspaceCwd, available, entries, hasMore}` with each entry containing SHA, abbreviated SHA, author identity/date, subject, optional refs, and parent SHAs. `limit` defaults to 50 and clamps to 1–200; `skip` is nonnegative; a supplied `range` must be at most 256 bytes and match the safe revision-character allowlist. Malformed or repeated ranges return 400. Checks fresh workspace trust before Git. Uses argv without a shell, 1 MiB stdout, five seconds per process, a 12-second route budget, two concurrent log lookups, and an 8 MiB serialized response cap.                                                                                                                                                           |
| `GET /workspace/git/log/commit`, `HEAD /workspace/git/log/commit`             | Single-workspace TypeScript v1 commit-detail shape with metadata, body, up to 50 file paths, and total file/line counts. Requires one 7–40 character hexadecimal `sha` parameter; missing, repeated, or malformed values return the TypeScript 400 `parse_error` body. Checks fresh workspace trust before Git. Uses argv without a shell, 1 MiB metadata and 4 MiB numstat output caps, five seconds per process, a 12-second route budget, two concurrent log lookups, and an 8 MiB serialized response cap. A failed or oversized numstat command preserves commit metadata with zero file stats, matching the TypeScript helper's fallback.                                                                                                                                             |
| `GET /workspace/git/diff`, `HEAD /workspace/git/diff`                         | Read-only TypeScript v1 workspace diff summary with totals and up to 50 file rows, including rename paths, binary/deleted/untracked flags, and bounded line counts for untracked files. Rechecks workspace trust before Git, refuses transient merge/rebase/cherry-pick/revert states, and caps Git output, subprocess time, route time, concurrency, untracked-file reads, and response size. Workspace-qualified diff routes remain unported.                                                                                                                                                                                                                                                                                                                                             |
| `GET /workspace/git/diff/file`, `HEAD /workspace/git/diff/file`               | Bounded TypeScript v1 per-file unified hunk projection with traversal checks, rename-aware diffing, and synthesized added hunks for safe untracked text files. Trust and concurrency are rechecked; binary/unreadable/unchanged files produce `available: false`. Workspace-qualified hunk routes remain unported.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `GET /session/:id/status`, `HEAD /session/:id/status`                         | Reads one active transcript directly through `SessionCatalog` and requires a valid, same-host runtime sidecar with a live PID. Returns the `BridgeSessionSummary` JSON shape with runtime start time, transcript ownership metadata, and bounded strings; invalid native transcript IDs return `400 invalid_session_id`, and missing, archived, inactive, or foreign-workspace sessions return the TypeScript `404 session_not_found` shape.                                                                                                                                                                                                                                                                                                                                                |
| `GET /session/:id/transcript`, `HEAD /session/:id/transcript`                 | Returns the TypeScript `{v, sessionId, events, nextCursor?, hasMore, startTime, lastUpdated, partial?, replayError?}` contract using `replay_transcript_record_page`. It shares the bounded `limit` (default 100, max 500), signed `cursor`, or exclusive `beforeRecordId` query parser with `/records`, checks live sidecar/catalog/file/session ownership before and after reading, redacts skill details from returned event updates, and sets `Cache-Control: no-store`. Source snapshots/pages stay capped at 256 MiB/4 MiB; serialized event responses are capped at 8 MiB of converted events plus 64 KiB for the signed cursor and a 64 KiB envelope allowance. Oversize output returns the source-compatible `413 transcript_page_too_large` body with `pageBytes` and `maxBytes`. |
| `GET /session/:id/transcript/records`, `HEAD /session/:id/transcript/records` | Native-only bounded record-page endpoint. Accepts TypeScript-shaped `limit` (default 100, max 500), signed `cursor`, or exclusive `beforeRecordId` (max 200 UTF-16 units); cursor and boundary are mutually exclusive. Reads only an active transcript whose live sidecar and catalog entry still match before/after the read. Uses the native reader's 256 MiB snapshot ceiling and 4 MiB page/serialized-response ceiling, returns `Cache-Control: no-store`, and signs continuation cursors with the project-local HMAC key.                                                                                                                                                                                                                                                             |

Paths are case-insensitive and accept a trailing slash, matching Express routing.
`HEAD` responses preserve the corresponding `GET` content length while omitting
the body. The status response reports `warning` with the
`daemon_runtime_starting` issue because this milestone does not create an
agent-session runtime; its process and listener startup fields remain useful.

The session lookup is capped at two concurrent blocking lookups, reads a
runtime sidecar of at most 64 KiB, rejects transcript metadata fields above
their per-field limits, and caps the serialized response at 16 KiB. It reports
`clientCount: 0`, `hasActivePrompt: false`, no pending interactions, and no
turn error because this daemon does not share the live bridge's client/prompt
state. Those fields are schema-compatible placeholders, not live activity
measurements. The route does not return archived sessions and does not expose
the full set of optional bridge metadata such as worktree/branch, pin/group, or
turn-error details. Valid session IDs whose metadata exceeds the field limits
return `422 session_status_metadata_too_large`.

The workspace-memory endpoint uses the same host/origin and optional bearer
middleware as the other routes. Per-file metadata failures are returned in an
`errors` array as `memory_file`/`stat_failed`; unexpected discovery failures
return `500 memory_discovery_failed`, excessive serialized output returns
`413 memory_response_too_large`, and saturated lookups return a bounded `503`.
Before each filesystem stat, the native listener re-reads bounded trust
settings and the trust-rules file without performing migrations or writes; a
revocation takes effect while the listener remains up. System/user/system
defaults determine `security.folderTrust.enabled`, and the trust rule resolver
applies the same path precedence as core. Since this CLI process has no IDE
trust context, an enabled trust policy with no explicit matching trust rule is
denied. Context filenames are read from merged settings at daemon startup and
then held for the listener lifetime; invalid/unreadable settings fall back to
the default filenames. Settings/trust inputs are capped at 4 MiB/1 MiB. The
read route advertises only `workspace_memory_read`, not the full read/write
`workspace_memory` feature.

The workspace Git route runs the TypeScript porcelain-v1 branch/status probe
directly after the fresh trust check. It reads at most 8 MiB of command output,
allows two concurrent probes, and kills Git after two seconds by default or
five seconds for `?wait=1`. Git metadata reads for worktree `gitdir` /
`commondir` files and the stash log are bounded and reject symlinks; stash logs
over 4 MiB count as zero. The response contains only the source-shaped
`workspaceCwd` plus branch/status fields, never changed-file paths. Native
serve has no watcher/cache or bridge event fan-out: it computes each result
fresh and cannot publish `git_status_changed` or `git_branch_changed`. Thus
`wait=1` is bounded but the default response does not have TypeScript's
branch-only cached fast path. The read route advertises `workspace_git_read`.

The `/workspace/git/log` routes use separate process and concurrency budgets
so they do not change the existing status endpoint. Log list output is capped
at 1 MiB; commit metadata is capped at 1 MiB and `diff-tree --numstat` output
at 4 MiB. Both enforce a five-second timeout per Git command, a 12-second
overall route timeout, two concurrent requests, and an 8 MiB serialized JSON
response ceiling. The source also caps the list at 200 and commit file details
at 50; Rust retains those limits. Unlike TypeScript's `range` helper, which
silently omits unsafe strings, Rust rejects invalid or repeated ranges with a
400 parse error. The 256-byte native range cap and output limits can make some
otherwise valid large Git histories unavailable. There are no
workspace-qualified log routes or Git-log event notifications.

Transcript record reads have a separate one-request concurrency gate because
the indexed snapshot can be as large as the reader's 256 MiB source-file cap;
the page is then limited to 4 MiB before serialization and after skill-detail
redaction. The cap is on source bytes and response bytes, while parser/index
structures can use more memory than either byte count.

The native-only records response remains `{v, sessionId, records, nextCursor?,
hasMore, startTime, lastUpdated, direction, branchPointsByAssistantUuid?}`.
It stays on the separate `/transcript/records` path because the TypeScript
`/session/:id/transcript` contract is replayed ACP events, not raw JSONL rows.
The event converter limits replay to 20,000 events, 8 MiB of serialized event
data, and 64 KiB cursors; the event route accepts that full converted event
budget, plus the maximum cursor and a 64 KiB envelope allowance, after
skill-detail redaction. This is explicitly capped at 8.125 MiB and remains
below TypeScript's 32 MiB serialized response ceiling. Converter-limit pages
are marked partial and do not advance their cursor. Output exceeding the Rust
route cap receives the source-compatible `413 transcript_page_too_large`
response with measured and maximum byte counts. The separate records route
retains its 4 MiB serialized-response cap.

There is no active-prompt field in the native runtime sidecar. The route
therefore passes `finalize_dangling=false` for all event pages: an unmatched
tool call remains unresolved, avoiding a false failure while another live
process may still be running it. The TypeScript route finalizes dangling calls
only when active-prompt checks before and after the read both report idle, so
the Rust endpoint can omit the corresponding terminal error event for a
completed transcript. Native `/session/:id/status` also reports
`hasActivePrompt: false` as a placeholder; it must not be used to infer this
state. Adding a reliable prompt-state sidecar or sharing the live runtime is
needed to match that finalization behavior.

## Integration

`main.rs` dispatches plain `canopy serve` to `serve_transport_command::run` and
the top-level help advertises the regular command. The embedding API remains
`bind_loopback(port)` plus `serve_until_shutdown(listener, future)`. It uses
only already-declared Rust workspace crates and the Hyper dependencies added
for the earlier transport spike; no manifest or lockfile edits were needed for
this slice.

## Remaining architecture and parity

This is a real loopback daemon process, but it is not full `serve` parity. No
agent-session runtime starts; `/session/:id/status` only projects metadata for
sessions whose native sidecar reports a live process. Rust serve does not
provide session creation, prompt/client state, or the rest of the ACP/session,
workspace, settings, tools, Web Shell, channel, TLS, configurable-host,
CORS-allowlist, rate-limit, logging, and other TypeScript routes. Unknown
routes currently return `404`; unsupported daemon options return a CLI error.
The advertised features include `session_transcript` and
`session_transcript_pagination` now that the event replay route is wired, along
with the separate native record-page feature and the narrower
`workspace_memory_read` feature. The memory reader does not yet load daemon
settings or workspace trust. Event replay still cannot match
the TypeScript dangling-tool-call finalization rule until the runtime exposes
active-prompt state. The daemon-status values for sessions and ACP remain empty
bootstrap values. Sharing a Rust session runtime with the HTTP daemon is the
remaining architectural blocker for the rest of the session API.

## Verification

The transcript and workspace-memory follow-ups ran formatting and whitespace
checks only; no tests or Cargo commands were run for those slices. The earlier
session-status slice was checked with Cargo separately.
