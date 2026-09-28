# Native Rust QQ Bot CLI host status

`canopy channel qq [configured-name]` now starts a runnable, native Rust QQ Bot
channel path. It selects a `type: "qq"` entry from merged settings, accepts
`appID` and `appSecret` literals or `$ENV_VAR` references, and falls back to the
existing per-channel credentials file. When neither source supplies both
credentials, it reports that QR-code login is unavailable in this host.

The host obtains and refreshes access tokens on demand, validates/fetches the
QQ gateway URL, and owns the WebSocket loop around
`canopy_core::channels::qqbot_gateway::QQGatewayProtocol`. It applies
IDENTIFY/RESUME, READY/RESUMED, sequence/heartbeat, invalid-session, close-code
and reconnect decisions; READY has a 30-second timeout. Cold READY restores
QQ route state and the managed-session routes. QQ state uses the existing
debounced/flush persistence helper under `<QWEN_HOME>/channels/`; session routes
use a per-channel `*-sessions.json` file there.

The implemented inbound path handles `C2C_MESSAGE_CREATE`,
`GROUP_AT_MESSAGE_CREATE`, and text from `GROUP_MESSAGE_CREATE`. It validates
chat IDs, drops bot-authored messages, deduplicates replayed message IDs for
five minutes, strips reserved prompt tags, projects group messages through
`prepare_group_message`, and persists chat type, reply ID, message sequence,
and group bot OPENID state. Group active-message toggle events update the
persisted active-message flag. @-bot messages from either group event use the
passive reply path even when active messages are disabled. Unmentioned
`GROUP_MESSAGE_CREATE` messages follow `groupAllPolicy`: `all` dispatches them
and `keyword` dispatches only NFC-normalized, case-insensitive keyword matches
with the source's ASCII word-boundary behavior. Empty keyword entries are
ignored. The default `log` policy does not forward unmentioned messages.
Unmentioned group messages still carry `is_mentioned: false`; the default
`GroupGate` `requireMention` therefore drops them unless the matching group
configuration sets `requireMention: false`. The configured `all` and `keyword`
policies also use the source's single-session scope.

Group gateway events enter a FIFO preparation queue so projection side effects,
policy checks, reply IDs, and duplicate suppression follow gateway order. The
queue holds at most 256 raw events and the host caps finishing prompts at 16;
when the raw queue fills, new events are dropped with a log message. Sender,
DM, group, allowlist, and pairing gates run before the prompt reaches a managed
`SessionRouter` session. A native ACP child handles prompts serially per
session; its completed response is returned to the originating QQ chat through
`qqbot_send`, including the existing markdown/text fallbacks and reply
sequence behavior. If the bounded ACP event relay reports dropped events, the
host refuses to send the resulting incomplete text as a reply.
The ACP JSONL reader uses the shared 16 MiB frame limit. An oversized frame,
stdout read error, or failed client-request response fails all pending ACP
calls with the recorded reason and terminates the child process.

After group/DM and sender authorization pass, the host records the user ID and
sanitized display label in the shared observed-contact store before command or
session routing. Group observations include the group OPENID, used as its label
because the QQ message payload has no group display name; QQ events currently
provide no topic/thread ID. Persistence failures are logged and fail open, so
they do not block an authorized inbound message. State is shared per daemon
workspace at `<global Canopy dir>/channels/daemon/<workspace hash>/observed-contacts.json`.
The hash uses the daemon workspace root (`default_cwd`), even when QQ's
configured channel `cwd` differs.

Incoming prompts include relevant channel memory when the effective QQ session
scope is not `single`. Recall uses the shared selector, is scoped by channel
and chat, skips recognized ChannelBase commands, wraps selected facts as
untrusted context, and fails open on storage errors. The selector limits the
injected entries to three and their combined text to 1,200 code points. When
`identity` or `memoryScope` is configured, the host appends ChannelBase's
sanitized identity and memory-isolation boundary after operator instructions;
both sections use `channel:<name>` defaults and the source's 128/256-character
limits. The boundary is omitted when neither object is configured. The same
resolved display name and memory namespace populate `/who`; `/status` exposes
the resolved identity ID and memory mode. `/status` reports the TypeScript
sender-policy values (`open`, `allowlist`, or `pairing`) rather than Rust enum
debug names.

Authorized inbound QQ messages now also run the shared deterministic
ChannelBase memory parser before session routing. The host supports remember,
paginated list, inspect, direct ID update/removal, and clear requests. Clear
requires a confirmation; natural-language update/removal classified by the ACP
runtime also require confirmation and compare the selected entry's original
text before mutating it. Pending confirmations expire after 60 seconds and are
scoped to channel, chat, and sender. Ambiguous natural-language matches are
shown as candidates. Messages classified as a memory save are stored silently
and continue to the agent; explicit memory commands receive a direct reply.
Natural-language classification reuses the QQ ACP process with a short-lived
session, and ambiguous or invalid classifier output falls through to the
normal prompt path.

## Shared inbound commands

After QQ sender, DM, and group authorization, the host invokes the shared
dispatcher before session routing for `/help`, `/new`, `/clear`, `/reset`,
`/status`, and `/who` (as well as `/cancel` and permission commands). `/help`
lists shared/platform commands and the routed session's ACP command catalog,
falling back to the latest catalog when no route exists. `/who` reports route
state, session-sharing scope, workspace basename, and optional configured
identity/memory namespace. `/status` reports route state, the normalized sender
policy (`open`, `allowlist`, or `pairing`), and optional identity ID/memory
mode. Shared clears require the shared confirmation and `allowedUsers` checks;
they remove only the caller's route or matching shared route, cancel and close
its ACP session, and clear pending permission state. ACP command catalogs are
bounded, keyed by session ID, restored from bulk `session/load` replay, and
cleared on close, session death, load reset, or ACP shutdown. Agent command
names and aliases are matched case-sensitively against the routed session's
catalog; shared local commands are matched case-insensitively. Shared-session
speaker attribution is omitted only for locally recognized commands or an
exact agent command/alias match.

## Prompt dispatch modes

The native host accepts channel-level and per-group `dispatchMode` values of
`steer`, `followup`, and `collect`. Exact group settings override the `*`
group setting; an absent or empty mode falls back to the channel value, then
to the TypeScript default, `steer`. Follow-up prompts run in FIFO order per
session. Collect buffers prepared prompts arriving during an active turn and
sends their combined text as one next prompt.

Per session, queued prompts are capped at 32 and 16 MiB; collect buffers are
capped at 128 prompts and 1 MiB. A full collect buffer falls back to the
bounded follow-up queue, and a full queue receives the existing retry message.
Steer cancels an active shared-session turn only for an authorized sender
(unshared sessions and empty `allowedUsers` are authorized); other senders are
queued and logged. `/cancel` discards collected prompts, while `/clear` bumps
the session generation and drops queued and collected prompts. Owner and
generation checks prevent an old prompt from replying or draining work after a
clear or session death.

## Experimental QQ cron message buffering

When `cron-msg-experimental` is true, the host subscribes to ACP
`agent_message_chunk` events and connects them to the shared QQ cron buffer.
The adapter checks READY state, active prompt ownership, and the session's
current QQ route; it only accepts targets owned by this QQ channel without a
thread ID. Sends use persisted chat type/reply context, the five-minute reply
TTL, group active-message state, and the existing markdown/text fallback
sender. Missing targets, routes, or tokens are dropped like the TypeScript
adapter; transient delivery and transport errors use the source's bounded
5-second/10-second retry sequence, while its permanent delivery codes stop
retrying. The buffer is created only when the experimental config flag is on,
and shutdown cancels its timers and discards pending text.

The TypeScript `runCronFlow()` API has no call site in this repository, so it
does not currently schedule or invoke QQ prompts. The Rust adapter preserves
that gate: ordinary ACP prompt chunks are ignored by the cron buffer. QQ also
does not enable ChannelBase proactive sending, so the shared
`ChannelLoopScheduler` cannot run QQ loops without changing the source
channel's policy. There is no native scheduled-prompt trigger in this host.

## Remaining QQ channel work

- This is not a port of the roughly 2,800-line TypeScript adapter. There is no
  QQ QR login flow, timer-based token refresh, initial `maxGwRetries` policy,
  or exact TypeScript reconnect backoff. The Rust host uses
  `maxReconnectAttempts` and bounded exponential reconnect delays.
- The group all-message path currently handles text projection and routing.
  QQ group media, history/trigger behavior, and user-facing policy diagnostics
  remain unported.
- Long-lived stream chunk delivery, proactive delivery, active-stream recovery,
  session backup repair, and bridge restart supervision remain unported. The
  experimental cron buffer is wired, but no scheduled-prompt trigger exists in
  the source or native host.
- Rich inbound QQ media and rich outbound message blocks are not projected;
  the first path handles text content and sends ACP's completed text response.
- ACP `session/request_permission` requests are retained in bounded
  process-local state and relayed to the originating C2C or group chat.
  `/approve`, `/approve-always`, `/deny`, and `/cancel` use the shared inbound
  command semantics, including shared-session `allowedUsers` checks. Unanswered
  permission requests expire after five minutes and are rejected; cancellation,
  ACP disconnect, queue saturation, and relay failure also fail closed. Other
  ACP client requests receive JSON-RPC method-not-found. Permission requests are
  not persisted across host restarts; the ACP child is restarted and old
  request IDs cannot be resumed. Thread-specific QQ routing is also not
  connected.
- Configured or saved app credentials must be non-empty strings in the native
  host. The TypeScript account helper accepts any JavaScript-truthy JSON value
  before passing it to its API client.

For the shared-command compatibility update, `rustfmt --edition 2024 --check`
and scoped whitespace checks passed for the QQ host and status note. No tests
or Cargo commands were run.
