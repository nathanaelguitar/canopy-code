# WeCom Native Host Port Status

The native command is available as `canopy channel wecom [configured-name]`.
It resolves `botId`, `secret`, and `wsUrl` from settings or environment
references, authenticates the WeCom WebSocket client, and routes authorized
messages into persisted ACP sessions.

The host currently covers sender, DM, and group gates; pairing notices;
five-minute message deduplication; quoted text; guarded inbound media download
and decryption; inline image and temporary-file attachments; lease cleanup;
final Markdown chunking; image-marker upload; authentication; reconnect after a
server kick; SDK reconnect fallback; a five-minute activity watchdog; and
Ctrl-C shutdown that invalidates, cancels, drains, and then cleans active work.

Inbound work is bounded to four active handlers and a queue of 32 callback
frames. Each callback can reference at most 16 media items and retain or stage
at most 32 MiB of downloaded media. When the host queue fills, it drops newly
received callback frames and logs the drop. The WebSocket event broadcast is
also finite; lag is logged and frames already evicted by the broadcast cannot
be recovered by the host. ACP updates larger than 256 KiB are rejected for the
active prompt, the event relay retains at most 32 updates, and collected final
response text is capped at 4 MiB. These limits keep bursts and oversized model
output from growing host memory without a bound.

Reconnect and shutdown invalidate the connection generation, cancel active ACP
prompts, drain or abort and join handler tasks, then clean attachment leases.
Authentication waits observe Ctrl-C and retain the source adapter's 30-second
authentication timeout.

After sender, DM, and group authorization, the host sends `/help`, `/new`,
`/clear`, `/reset`, `/cancel`, `/status`, `/who`, and the shared permission
commands through the core inbound-command dispatcher. Shared-session clearing
still requires `/clear confirm`; `/who` exposes only the workspace basename;
all command replies use the existing WeCom Markdown sender and the originating
chat. These commands run before session resolution, so read-only commands do
not create sessions. The host does not expose pending ACP permission requests,
so `/approve`, `/approve-always`, and `/deny` report that no request is pending.

Channel memory operations run after the sender, DM, and group gates pass and
before session resolution. Entries are scoped to the configured WeCom channel
and chat. Deterministic memory requests support remember, paged list, inspect,
update, and removal. Other natural-language requests use a short-lived ACP
classifier session; low-confidence and invalid classifications continue to
the agent. Natural updates and removals show the selected entry and require
confirmation from the same sender in the same chat within 60 seconds. The
handler compares the saved text before changing it. Clearing always requires
that scoped confirmation. A classifier-detected remember saves as a side
effect and sends the original message to the agent. Relevant entries are
added as untrusted context to normal prompts except when the configured
session scope is `single`. Classifier sessions are closed after use and
participate in shutdown prompt cancellation; memory replies use the existing
WeCom Markdown sender.

The host does not yet reproduce all `ChannelBase` behavior. ACP
`available_commands_update` notifications and session-load replay snapshots
populate bounded, per-session agent-command catalogs. `/help` lists canonical
commands for the routed session, and canonical names and aliases are matched
case-sensitively so recognized commands do not receive a speaker prefix.
Channel-level and group-level `dispatchMode` now accept `steer`, `followup`, or
`collect`, defaulting to `steer`. Messages are ordered per session; distinct
session queues can now run ACP prompts concurrently. The ACP client routes
JSON-RPC replies by request ID and filters streamed updates by session ID,
while each session's dispatcher remains the single prompt owner for that
session. Its event broadcast remains bounded, so a sufficiently lagged session
stream can still fail with an explicit overflow error.
Follow-up and steer messages queue behind the session's current prompt. An
authorized steer cancels the current ACP prompt and adds the source adapter's
cancellation note. Shared session steer requires membership in `allowedUsers`
when that list is nonempty.
Collect mode coalesces buffered prompt text after the active prompt and invokes
the WeCom attachment lease's buffered, drained, and dropped callbacks. `/cancel`
drops the collect buffer, and `/clear` drops queued and collected prompts while
invalidating the previous session queue owner.

Each session queue retains at most 32 queued messages and 32 MiB of prompt text
and image payload. Overflow is logged and dropped; dropped collect items run
attachment cleanup immediately. Collected image payloads are omitted, matching
the source `ChannelBase` collect re-entry behavior. These limits supplement the
four active inbound handlers and 32 callback-frame backlog described above.
WeCom group metadata is limited to the provider's chat ID, and unrecognized or
over-capacity callbacks are logged and dropped. The source adapter's SDK
callback listener can accept messages while the model is busy; this native host
instead uses its explicit bounded queue policy above.

The prompt-buffer slice is formatted, and the integrated Rust workspace passed
locked offline `cargo check` after the concurrent Git diff route was wired.
The CLI reports six existing dead-code warnings. No tests were added or run.
Platform-specific slash-command registration and typed channel lifecycle events
remain open.
