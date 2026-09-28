# Native Rust Telegram host status

The native CLI exposes `canopy channel telegram [configured-name]`. It reads a
Telegram channel entry from merged settings, resolves the bot token from a
literal or `$ENV_VAR`, applies sender, DM, group, and pairing gates before
dispatching local commands, and sends prompts to the native ACP runtime in a
managed child process. `SessionRouter` persists sender/chat/thread routes in
`<QWEN_HOME>/channels/sessions.json`. The host supports `/help`, `/new`,
`/cancel`, `/status`, and `/who`; text and local file references reach the ACP
session and responses are sent back to the originating chat/topic.
ACP tool permission requests are retained with a bounded queue and routed to
the originating chat. `/approve`, `/approve-always`, and `/deny` resolve the
request through the shared channel command dispatcher; requests with no live
chat context are rejected or cancelled.

The operator can manage workspace-scoped Telegram pairing state with
`canopy channel pairing list <name> [--cwd <dir>]`,
`canopy channel pairing allowlist <name> [--cwd <dir>]`,
`canopy channel pairing approve <name> <code> [--cwd <dir>]`, and
`canopy channel pairing revoke <name> <user|group> <id> [--cwd <dir>]`.
These commands use the same `FilePairingStore` as the host and default
`--cwd` to the current directory. `allowlist` prints dynamically pairing-
approved user and group IDs under separate headings; it does not show static
`allowedUsers` or configured group rules. `revoke` requires an explicit
`user` or `group` type, so identical IDs in both stores remain distinct; it
reports a useful error if that approval does not exist and leaves configured
channel access rules untouched. Negative IDs are accepted for Telegram group
targets. Request-controlled names and IDs are sanitized before terminal
output. `list` and `approve` retain their pending-request behavior, including
a nonzero CLI error when a code is missing, expired, or belongs to another
workspace.

The ACP child reader bounds each JSONL output frame to 16 MiB. An oversized
frame or read failure terminates the child and fails pending requests instead
of retaining an unbounded line buffer.

## Scope and gaps

- This is a single-process Telegram CLI host, not the general channel daemon or
  a port of every channel platform. It handles the Rust Telegram adapter's
  supported Bot API message updates only.
- Telegram photos use the largest Bot API photo variant, are downloaded only
  after the host's group, DM, and sender gates pass, and are capped at 8 MiB.
  The image is passed to ACP as an inline image content block. Documents and
  voice messages also run through those gates before download and are capped
  at 32 MiB each. Declared message and `getFile` sizes are rejected before
  transfer when they exceed the cap; the HTTP adapter also rejects an oversized
  Content-Length and stops streaming before appending a chunk that would cross
  the limit. Custom `TelegramApi` implementations must provide bounded file
  downloads; the trait's default fails closed instead of calling its legacy
  full-buffer download method. Accepted document and voice local paths reach
  ACP through the shared prompt projection.
- The TypeScript adapter downloads media without a local byte limit. The native
  limits are intentional resource bounds. Photos are sent inline to ACP
  without resizing.
- Telegram group-history backfill is enabled when `groupHistoryLimit` is
  positive. Limits resolve by exact group, wildcard group, then channel; a
  missing limit disables history. The host records non-empty messages rejected
  only because a mention is required, after checking sender authorization
  (except for approved pairing-policy groups, matching ChannelBase). Entries
  persist under `<QWEN_HOME>/channels/<encodeURIComponent(channel)>-group-history.jsonl`
  and are keyed by the JSON tuple `[channel, chat, thread|null]`. A later
  non-recognized-command prompt drains that key, rechecks paired-group approval
  and sender authorization, then adds sanitized history lines around the
  projected current message. Stored sender metadata is capped at 256 UTF-16
  units and text at 1000; the shared store retains at most 1000 keys and
  compacts after 1000 records. `/clear` clears the matching group/thread key,
  or all history for `single` session scope. Store I/O failures are logged and
  do not block message handling.
- Telegram dispatch mode resolves exact-group override, then wildcard-group
  override, then channel `dispatchMode`, then the TypeScript default `steer`.
  An exact group object without a mode shadows the wildcard object, matching
  ChannelBase. `followup` queues behind the active turn; authorized `steer`
  marks the active turn cancelled and requests ACP cancellation before queuing
  its replacement; unauthorized senders in a shared session are downgraded to
  follow-up. `collect` coalesces projected prompt text after the active turn,
  and drops media/reference objects after their text projection, matching the
  TypeScript synthetic-envelope behavior. Per-session prompts remain serialized.
  Native admission caps queued work at 32 prompts and 16 MiB, and collect buffers
  at 128 prompts and 1 MiB; when a collect buffer fills, the message falls back
  to the bounded follow-up queue. TypeScript currently has no corresponding
  queue or collect-buffer cap. `/clear` increments the session generation,
  discards collected text, requests cancellation, waits up to three seconds,
  and suppresses late output from an evicted turn. The Rust handler surface now
  exposes the shared `on_prompt_buffered` and `on_prompt_buffer_drained` hooks;
  buffered prompts invoke the former after insertion, while drain notification
  precedes collect re-entry and is skipped when there are no message IDs. The
  TypeScript Telegram adapter inherits both hooks as no-ops and does not set
  `Envelope.messageId`, so its current buffered payload is `undefined` and its
  drained callback is skipped for an empty ID list; native behavior matches
  that seam without substituting Telegram update IDs. Rust also logs the
  TypeScript-compatible steer watchdog diagnostic after three seconds if the
  exact predecessor run is still active, then disarms the watchdog as soon as
  the queued turn acquires the session lock. The separate buffer-dropped hook
  is not ported. Channel memory management now uses the shared native store for
  remember, paginated list, inspect, update, remove, and two-step clear. The
  ACP classifier follows ChannelBase's trigger phrases, 0.7 confidence floor,
  strict JSON fields, and known-ID validation; classified remember saves are
  silent side effects while the original prompt continues to the agent. Natural
  update/removal and clear require a confirmation scoped to channel, chat,
  thread, and sender. The 60-second window starts after the confirmation prompt
  is delivered, and update/removal confirmations compare the stored text before
  mutation. TypeScript's recall-index cache and recall telemetry are not ported.
  The observed-contact graph is persisted after inbound authorization succeeds,
  at `channels/daemon/<workspace-hash>/observed-contacts.json`; observations
  include sender IDs/labels and, for groups, chat and topic IDs/labels.
  Persistence failures fail open. The Telegram core transport exposes
  proactive-send support for valid chat/topic targets, but this CLI host is not
  connected to the daemon's general `deliverChannelMessage` dispatch API. The
  host does relay ACP background notification responses to persisted Telegram
  session targets when the update is a text `agent_message_chunk` marked
  `qwenDiscreteMessage`, with source `background_notification_response` and
  `rewritten !== true`; events with a string `parentToolCallId` are ignored.
  It uses the same HTML formatting, splitting, plain-text fallback, and numeric
  topic validation as other Telegram sends, and logs delivery failures without
  terminating the host. In the TypeScript path, `ChannelBase.deliverProactive`
  is called by `daemon-worker.ts`; `TelegramAdapter` enables proactive sends
  and validates numeric thread IDs. ChannelBase's structured question
  presenter is driven by `session/request_permission` metadata, and Telegram
  inherits its unsupported presenter, so it falls back to generic permission
  text commands. There is no
  `session/request_user_input` method in the TypeScript Telegram/ChannelBase
  contract. Incoming prompts do include relevant channel memory when
  the configured session scope is not `single`; recall is scoped by
  channel, chat, and Telegram thread, skips recognized slash commands, uses
  the shared bounded selector, marks facts as untrusted context, and fails
  open on storage errors. Other ACP requests that need a Telegram host handler
  remain unimplemented outside permission handling.
- Telegram retains each ACP `available_commands_update` catalog under the
  exact ACP session ID. `/help` reads the catalog for the current route, with
  the most recently updated catalog used only when no route exists, matching
  ChannelBase's global fallback. Inbound command attribution recognizes a
  session's canonical names and `_meta.altNames` aliases with case-sensitive
  first-token matching; agent-command matching keeps any `@bot` suffix intact,
  while local commands remain case-insensitive. Unknown and malformed slash
  text keeps sender attribution in shared prompts, and recognized commands are
  forwarded unchanged to ACP. Catalogs accept at most 128 command entries,
  16 aliases per command, 128-byte ASCII command names, 512-character
  sanitized descriptions, and 16 KiB per session; at most 256 session catalogs
  are retained. Closing or clearing a session and session-death cleanup evict
  its catalog. The Rust ACP server publishes the current catalog after a
  successful `session/new`, `session/load`, `unstable_resumeSession`, or
  `session/resume` response, and again on workspace skill refresh. The initial
  update is built from the active session's `AcpSkillSnapshot` and MCP prompts
  by `CliAcpAgent::publish_available_commands` in `acp_server.rs`; dynamic
  command discovery therefore does not depend on a later refresh.
- Pairing requests and dynamic approvals use the shared file store. The native
  CLI can inspect and revoke those approvals; it does not edit static channel
  configuration such as `allowedUsers` or configured group access rules.
- The ACP subprocess receives configured model and instructions. Telegram bot
  tokens written as `$VAR` resolve through the loaded effective environment,
  whose precedence is process environment, eligible `.env` entries, then
  `settings.env`. When `identity` or `memoryScope` is configured, the host
  appends ChannelBase's
  identity/memory boundary block after operator instructions, including the
  TypeScript fallback values and quoted-value limits. `/who` reports the
  resolved identity display name and memory namespace when that boundary is
  enabled, and omits them otherwise. Telegram Bot API proxy resolution follows
  TypeScript normalization and precedence: CLI `--proxy` (also `--proxy=`),
  settings `proxy`, then effective-environment `HTTPS_PROXY`, `https_proxy`,
  `HTTP_PROXY`, and `http_proxy`; scheme-less values receive an `http://`
  prefix and SOCKS schemes are rejected. The native CLI accepts `--proxy <url>`
  before `channel` or after the Telegram configured name. Channel
  `approvalMode` is connected through `SessionRouter` and forwarded in ACP
  session metadata, including classifier sessions. Model profile selection and
  the Node host's bridge-restart supervisor are not connected.
- Long-poll offsets and active prompt state are process-local. The Telegram
  adapter drops pending updates at connection start, as its TypeScript source
  does; durable update deduplication and process restart policy remain outside
  this slice.
- The adapter handles `/start` with its static welcome message before calling
  the host, matching the existing TypeScript adapter; this command therefore
  does not pass through the host's authorization gates.

## Verification

`rustfmt --edition 2024` completed for the changed Rust files, and
`cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked --offline`
passes with existing warnings in `mcp_host.rs`. Tests were not run.
