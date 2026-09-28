# GitHub channel adapter Rust port

github_adapter.rs is exported as canopy_core::channels::github_adapter. It
ports the polling, inbound-task, and final-response publication portions of
packages/channels/github/src/GithubAdapter.ts behind GithubApi,
GithubAuthorization, and GithubInboundHandler traits. The handler returns its
final response and ACP session ID to the adapter, which owns publication and
the matching inbound-task transition.

The module includes source-named config and cursor JSON fields, cursor
validation/normalization, the chat_thread session-scope default, final-only
streaming configuration, publication-policy instructions, GitHub login
resolution, allowed-user lowercase normalization, reason-filter validation,
issue-thread reply validation, and a durable PollingChannelBase loop.
with_configured_reqwest supports either an explicit token or a local gh
credential; local CLI output is time- and size-bounded.

Polling requests notifications from one second before the persisted cursor,
orders them by updated_at, retains the pre-advance comment window, and marks
the batch read only after processing. A failed mark keeps the pending timestamp
for the next poll and does not advance lastProcessedAt. Per-thread API errors
are logged and skipped while remaining notifications continue. The source
reasonFilter allowlist and routing are preserved for mentions, review
requests, assignments, author/comment aggregation, and other notification
reasons.

Comment processing includes source window filtering, bot-comment suppression,
mention matching/removal, authorization-aware synthetic mentions in directed
pairing flows, first-contact issue/PR bodies, latest direct-event selection,
bounded comment aggregation, metadata and display projection, and the 500-key
cursor dedupe lists. Accepted inbound work is written to a workspace-scoped,
private task file before dispatch. Startup recovery replays accepted/running
tasks and failed tasks up to three attempts; it persists cancellation as a
terminal state, posts the source error comment best effort, and blocks cursor
commit while recoverable work or task-state persistence errors remain.
Prompt start/end hooks manage best-effort eyes reactions on real comment IDs.

Final publication suppresses empty text and the exact `<no-reply/>` sentinel,
including supported fenced output, and writes `posting`, `posted`,
`suppressed`, and `failed` records to the private workspace-scoped GitHub audit
log. Definite rate-limit responses that guarantee no write persist a
deduplicated pending delivery atomically. The connect-time retry worker checks
for a prior posted audit and for a matching bot-authored comment before making
another POST; successful reconciliation records comment IDs/URLs, removes the
pending entry, and cleans up its inbound task. A queued delivery moves its task
to `reply_pending` and clears the saved envelope so prompt recovery cannot
rerun the work. Recovery consults pending deliveries and source-keyed audit
records before replaying any accepted or running task.

ReqwestGithubApi implements authenticated-user lookup, notification and
comment/event pagination, issue/PR metadata, notification reads, issue comments,
and reactions. Requests have a 30-second timeout, response bodies are capped
at 8 MiB, pagination is capped at 500 pages / 50,000 items, API retries are
cancellable, and retry cooldowns are capped at 15 minutes. Pagination links
must retain the configured API origin.

## Remaining host and compatibility boundaries

- A foreground native CLI host now loads channel config, resolves token/proxy
  environment references, connects the shared sender/group/DM gates and
  pairing store, routes sessions through ACP, and calls prompt reaction
  hooks. Final text returns through the adapter's durable publication path; see
  `rust/crates/canopy-cli/src/github_host_PORT_STATUS.md`. The CLI host does
  not reproduce all `ChannelBase` commands, memory/loop/webhook behavior, or
  interactive permission relay. The TypeScript daemon worker and channel
  registration are not connected. Per-message task cancellation is not
  forwarded.
- The injected API abstraction can be used with any backend. with_reqwest
  requires an explicit token; with_configured_reqwest handles the optional
  local gh flow. GitHub CLI diagnostics do not reproduce every config-dir
  hint in the TypeScript adapter.
- Rust cancellation aborts active polling HTTP requests and retry sleeps when
  disconnect is called; the TypeScript polling calls do not pass their abort
  signal through Octokit requests. A host inbound handler itself is not
  cancelled by this API trait.
- Native request limits are stricter than Octokit's unbounded pagination and
  do not use GitHub's response error body. Retry cooldowns have a 15-minute
  cap. Cursor timestamps require RFC 3339; JavaScript Date accepts additional
  parseable forms. GitHub IDs use u64, while JavaScript stores numbers.
- Workspace task-state replacement is private and atomic within this process,
  but it does not coordinate mutations from a second process. Rust strings
  cannot represent lone UTF-16 surrogates.

- GitHub comments have no idempotency key. Audit records suppress task replay,
  and pending retries scan for an already visible matching bot comment before
  posting again; a comment not yet visible through GitHub's list endpoint could
  still race that reconciliation.

Formatting and locked offline compilation for canopy-core and canopy-cli were
verified after the publication port. Tests were not added or run.
