# GitLab adapter Rust port

`gitlab_adapter.rs` is exported as `canopy_core::channels::gitlab_adapter` and
ports the polling and note behavior in
`packages/channels/gitlab/src/GitlabAdapter.ts` behind `GitlabApi` and
`GitlabInboundHandler` traits.

The native module includes source-named GitLab config/cursor JSON fields,
`baseUrl` normalization, bot identity lookup, `allowedUsers` lowercase
normalization, connect warnings, the persisted `PollingChannelBase` loop, and
manual single-poll access for host schedulers. It drains existing todos on the
first poll without dispatching and initializes the cursor to their maximum ID;
later polls clean up stale IDs, process new todos in ascending ID order, and
preserve the TypeScript cursor advance-before-cleanup behavior. Unsupported
targets, missing IIDs, and unconfigured actions are marked done and advance the
cursor. Processing errors emit the source error note best effort, then advance
the cursor and mark the todo done best effort.

Todo dispatch includes issue/MR description lookup and per-poll caching,
`directly_addressed` fallback to the `mentioned` template, `%%` escaping and
known-variable expansion, GitLab note-ID detection, mention stripping, and
source-shaped group envelopes. Note mentions only fetch descriptions when the
template contains `%description%`; those lookup failures continue with empty
metadata, while issue/MR description failures on non-note todos use the
best-effort error-note path. Prompt start/end callbacks award and remove the
`eyes` emoji, including waiting for a pending award before removal.

`ReqwestGitlabApi` implements GitLab REST calls for current user, paginated
pending todos, todo completion, issue/MR descriptions and notes, and note
emoji. Requests time out after 30 seconds, response bodies are capped at 8 MiB,
and todo pagination is capped at 500 pages / 50,000 items. The injectable API
and request cancellation token let a host or future tests substitute a
controlled backend.

## Remaining host integration and compatibility boundaries

- Daemon/CLI channel config loading, secret/environment token resolution,
  authorization gate setup, ChannelBase registration, and dispatch into the
  shared prompt runtime remain outside this core module. `GitlabInboundHandler`
  receives the built envelope; the host must run its normal authorization and
  ChannelBase path and call the exposed prompt start/end hooks.
- `disconnect` cancels active HTTP requests as well as stopping the shared poll
  loop. TypeScript stops its poll loop but does not pass the abort signal into
  Gitbeaker requests, so an in-flight request may settle differently.
- The concrete reqwest list operation deliberately bounds pagination and
  memory use; a pending backlog above 50,000 items fails the poll rather than
  loading an unbounded response set. The source Gitbeaker `.all()` has no such
  adapter-level cap.
- GitLab IDs are represented as Rust `u64` API identifiers and cursor values as
  finite JSON `f64`; JavaScript `Number` can represent values outside that
  integer domain, though GitLab IDs are normally far below the exactness limit.
- Rust strings cannot represent lone UTF-16 surrogates. The already-ported
  GitLab mention helper otherwise follows the source regex boundary rules.

`rustfmt --edition 2024` and
`cargo check --manifest-path rust/Cargo.toml -p canopy-core --locked --offline`
completed successfully for this slice. Tests were not run.
