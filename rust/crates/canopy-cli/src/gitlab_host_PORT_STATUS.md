# Native GitLab CLI host status

`gitlab_host.rs` now contains the foreground native host entrypoint for
`canopy channel gitlab [configured-name]`. It selects `channels.gitlab` by
default, accepts an explicit configured name, or requires a unique configured
GitLab channel. Token and `baseUrl` values support `$ENV_NAME` references and
the `$$` literal-dollar escape. The host resolves the workspace, applies
GitLab's `chat_thread` session-scope default, and starts the native
`GitlabAdapter` with its bounded REST client and persisted todo polling loop.
The module, help text, and `canopy channel gitlab` dispatch are registered in
`main.rs`.

The host wires the GitLab username and project path through the shared sender,
group, DM, and workspace pairing gates. It routes accepted todos through a
persisted `SessionRouter` and an ACP child process, invokes the adapter's note
reaction hooks around each prompt, and publishes nonempty final text with the
adapter's issue/MR thread-aware note method. Shutdown cancels active ACP
sessions and the adapter's poll/API requests before disposing the router and
child process. Accepted sender/project/topic contacts are persisted under the
workspace-scoped daemon channel directory.

After authorization and before session resolution, the host dispatches shared
`/help`, `/new`, `/clear`, `/reset`, `/cancel`, `/status`, `/who`, and
permission command syntax through `inbound_commands`. Clear removes the
sender/chat/thread route, cancels the ACP session, and closes it. Replies are
sent to the originating GitLab issue or merge-request thread. This host does
relay ACP `session/request_permission` requests to the active GitLab issue or
merge-request thread. Pending requests retain the ACP request ID, available
options, sender/chat/thread owner, and user-input marker. `/approve`,
`/approve-always`, and `/deny` answer only a matching request visible to the
authorized sender. The request comment tells the user to mention the bot with
the command, since GitLab delivers those replies through todos. Requests that
present an `ask_user_question` payload now accept one
`/answer <request-id> <question-number> <answer>` note command per question.
Answers stay bounded to 8 KiB each and 32 KiB total; the final answer submits
the ACP request with its original submit option and indexed `answers` object.
The route must match the
originating sender, project, issue/MR thread, and still-active session. The
request expires after four minutes and is cancelled in ACP; `/deny` and session
cancellation also close it. Unrecognized or malformed user-input requests
remain deny-only.

GitLab's normal poll callback waits for a full ACP prompt, so a permission
command posted after the request would otherwise wait behind that prompt. The
foreground host therefore runs a separate permission control poller before
starting the adapter. It snapshots the current maximum todo ID to ignore old
commands, polls every two seconds only while permission requests are pending,
and routes note todos containing `/approve`, `/approve-always`, `/deny`, or
`/answer` through the same `GitlabHost::handle_inbound` authorization gates.
It dispatches at most 16 matching commands per poll and uses a 512-entry todo-ID
dedup window against the normal adapter path. Only command todos are marked
complete by this poller; the adapter cursor, normal todo order, and note-reaction
lifetime remain adapter-owned. The ACP reader retains up to 128 permission
requests and rejects new requests when that bound is reached. Shutdown cancels both pending
ACP permission requests and the control poller.

## Remaining work

- The control poller is foreground-only and uses GitLab's pending todo list;
  permission commands are limited to 16 per two-second cycle, and all other
  messages still wait for the normal sequential adapter poll to finish.
- The standalone host does not register the full shared `ChannelBase` command,
  memory, loop, webhook, or daemon worker lifecycle surfaces.
- This is a foreground CLI host and is not wired into the TypeScript daemon's
  channel manager or configuration lifecycle.

No tests were added or run. `rustfmt --edition 2024 --check` and
`cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked --offline`
pass; the compiler reports six existing dead-code warnings elsewhere in the
CLI crate.
