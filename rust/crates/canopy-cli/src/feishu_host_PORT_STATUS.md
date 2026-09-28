# Native Feishu/Lark Host Port Status

## Scope and reference

This host is a native Rust implementation slice for
`packages/channels/feishu/src/FeishuAdapter.ts`, with shared channel behavior
from `packages/channels/base/src/ChannelBase.ts`. It uses the existing
`canopy-core::channels::feishu_*` card, media, question-controller, Markdown,
and message helpers. It is not a complete behavioral replacement for the
TypeScript adapter.

The host accepts `canopy channel feishu [configured-name]` and the `lark`
platform alias. It selects `channels.feishu`, `channels.lark`, or one named
`type: "feishu"` entry; resolves literal and `$ENV_VAR` credentials; applies
the channel workspace, sender/DM/group policies, pairing store, model,
instructions, approval mode, session scope, and webhook settings; then starts
an ACP child. Session routes persist under the global Canopy channel state
directory and restore on startup. Workspace settings are loaded through the
existing trusted settings loader.

## Implemented behavior

- **Transport startup and lifecycle:** defaults to Feishu long connection or
  binds the configured webhook host and port. Ctrl-C stops accepting events or
  reconnecting, disposes the question controller and session router, and shuts
  down the ACP child. WebSocket reconnect delay is bounded and interruptible.
- **Long connection:** discovers the endpoint with AppID/AppSecret, validates
  the secure URL and service/device identifiers, implements the bounded
  protobuf Frame wire format, sends protocol pings, observes the server ping
  interval, acknowledges event frames, and reassembles bounded out-of-order
  fragments with a 10-second expiry. Frames cap at 4 MiB, assembled events at
  8 MiB, and the fragment cache at 16 MiB across 16 message sets.
- **Webhook authentication:** caps headers at 32 KiB and bodies at 1 MiB;
  validates the Feishu timestamp/nonce/body signature using a constant-time
  comparison; validates URL-verification tokens; and decrypts encrypted event
  bodies with the existing WeCom AES-CBC primitive and Feishu's SHA-256 key
  derivation. Callback question/stop actions return their response in the
  webhook request. Full event queues return HTTP 503 instead of silently
  acknowledging a dropped event.
- **Authenticated Feishu APIs:** caches tenant access tokens; looks up bot,
  user, and group names; fetches quoted messages; downloads inbound images and
  files through `feishu_media`; sends bounded interactive Markdown cards;
  creates and patches streaming/question cards; and checks API-level error
  codes as well as HTTP status.
- **Inbound processing:** validates IDs and sender shape; ignores app-originated
  messages; deduplicates message IDs for five minutes; extracts text, rich
  posts, images, files, audio, video, and quoted interactive-card content;
  replaces mention placeholders and detects bot mentions; fetches parent
  message text and identifies bot replies; applies group, DM, sender, and
  pairing gates; and projects images/files into ACP prompts. Downloaded files
  use private temporary directories and are cleaned after prompt processing.
  The shared media downloader caps a resource at 50 MiB.
- **ACP and sessions:** persists user/chat/thread routes, opens and loads ACP
  sessions, serializes prompts per session, relays bounded ACP JSONL events,
  streams agent text into cards on the 1.5-second update cadence when block
  streaming is off, and cancels sessions from stop actions. Group replies
  include the Feishu `<at>` sender
  marker. User-input requests are normalized into the existing Feishu question
  card controller, scoped to their originating sender/chat, and expire after
  270 seconds. Ordinary ACP permission requests receive interactive cards with
  the supported allow-once, preferred allow-always, and deny choices. Each
  action returns the exact ACP option ID to its request; callbacks must match
  the originating message and chat and pass the sender, DM/group, shared-session
  and current-route checks. Cards expire after 270 seconds and automatically
  return the request's reject option, or a cancelled outcome when no reject
  option exists. Missing active routes, unsupported permission options, queue
  saturation, card-send failures, and callback timeouts fail closed. The same
  card action handler is wired to webhook callbacks and long-connection event
  frames. Inbound work queues up to 256 events and runs up to 16 handlers;
  retained response text is capped at 25,000 UTF-16 units.
- **Slash commands:** caches bounded ACP `available_commands_update` catalogs
  per session, including validated aliases and sanitized descriptions. `/help`
  lists the implemented local commands and current session's agent commands
  and aliases. Canonical names and aliases are matched case-sensitively against
  the current session snapshot;
  matched slash text is forwarded unchanged to ACP and skips sender attribution
  and memory recall. Unknown slash text remains attributed and can be handled
  as ordinary agent input. Catalogs cap at 256 sessions, 128 commands per
  session, 16 aliases per command, and 16 KiB of retained strings per catalog.
- **Shared local commands:** `/clear`, `/reset`, and `/new` share the base
  confirmation behavior: shared `single`, `chat_thread`, and group `thread`
  sessions require an exact `confirm` argument, and configured `allowedUsers`
  restrict shared clears. Clearing removes the route, cancels active ACP and
  user-question work, finalizes streaming cards, drains the prompt lock, and
  closes the ACP session. `/who` and `/status` use the same shared-session
  allowlist gate; `/who` reports the existing route, workspace basename, and
  scope note, while `/status` reports route state and sender access policy.
  When identity or memory scope is configured, `/who` also shows the sanitized
  display name and memory namespace, and `/status` shows the sanitized identity
  ID and memory mode. Unconfigured channels keep the shorter output. Both
  commands retain the TypeScript shared-session authorization gate.
  `/approve`, `/approve-always`, and `/deny` answer ordinary ACP tool
  permission requests. `/deny` also cancels a pending ACP user-input question
  when no ordinary permission request matches. Lookup is limited to the exact
  chat/thread and current session/run; question cancellation is restricted to
  the original requesting sender. Shared-session commands also require the
  shared-session allowlist and channel sender/group gates. Omitting the request
  ID works only when exactly one eligible request matches. Ambiguous requests
  list IDs and titles. Responses select the exact ACP option ID and finalize
  the matching permission card; question cancellation uses the controller's
  ACP cancellation and terminal-card path.
- **Memory and contacts:** deterministic remember/list/inspect/update/remove/
  clear intents use the shared channel-memory store. Mutating existing entries
  requires same-sender confirmation within 60 seconds and compares the saved
  text before changing it. Relevant memories are sanitized, identified, and
  marked as user-provided facts before prompt injection; recall is skipped for
  `sessionScope: "single"`. Observed-contact labels are loaded from the shared
  persisted graph and refreshed from Feishu APIs when absent.
- **Scheduled loops:** `/loop add "<cron>" <prompt>`, `/loop list`,
  `/loop inspect <id>`, and `/loop cancel <id>` use the shared Rust
  `ChannelLoopStore` and `ChannelLoopScheduler`; no adapter-specific scheduler
  or persistence format is introduced. Definitions persist in the global
  `channels/cron.json`, and the scheduler runs only when cron is enabled (not
  `experimental.cron: false` and not `CANOPY_CODE_DISABLE_CRON=1`). Add validates
  cron, caps prompts at 4,000 Unicode scalar values, and enforces the shared
  limit of 10 enabled jobs for the exact channel/sender/chat/thread target.
  List, inspect, and cancel first scope reads to that same target, so a guessed
  loop ID from another chat or sender cannot be inspected or disabled. Shared
  sessions require the configured `allowedUsers` entry. At fire time the host
  re-checks stored group, DM, sender/pairing, and shared-session authorization;
  it disables a target that has lost authorization. `sessionScope: "single"`
  also disables an existing job if it becomes incompatible. Feishu does not
  support proactive thread replies, so threaded targets are refused. Scheduled
  prompts share the session router and per-session prompt lock, reject ACP user
  input questions because no operator is present, collect the bounded ACP
  response, and cold-send it to the stored chat through the authenticated
  Feishu message API in both WebSocket and webhook modes. The Rust scheduler
  owns cron cadence, persisted run status, retry/backoff, and result previews.
  Before an unattended prompt, the host prepends a full channel-memory
  snapshot once per ACP session, capped and sanitized like the base adapter.
  It caches the snapshot's store revision, skips unchanged snapshots, and
  refreshes after Feishu memory mutations or an externally changed file. A
  read is accepted only when its revision stays stable and no local mutation
  invalidates its active read generation. On a read failure or write race the
  turn proceeds without the snapshot and a later loop can retry. When
  `identity` or `memoryScope` is configured, the first ordinary or scheduled
  agent prompt for the session receives the channel-boundary block after
  relevant or full-memory context.

## Remaining parity gaps and operational limits

- **Dispatcher registration** is in `rust/crates/canopy-cli/src/main.rs`.
  It routes both `Some("feishu")` and `Some("lark")` in the `channel` dispatch
  block to `feishu_host::run(&channel_args)` and rejects `--proxy`. `run`
  accepts `["feishu"]`,
  `["feishu", configured_name]`, and the equivalent `lark` arguments.
- Feishu callback delivery still depends on the active transport remaining
  connected. Long-connection events and webhook callbacks route through the
  same action handler; if a permission action does not arrive within 270
  seconds, the pending ACP request is explicitly denied or cancelled and its
  card is finalized when the API remains reachable.
- `blockStreaming: "on"` now uses the shared Rust `BlockStreamer` to emit
  trimmed blocks as Feishu messages. `blockStreamingChunk.minChars` and
  `maxChars` and `blockStreamingCoalesce.idleMs` use the TypeScript defaults
  (400, 1000, and 1500 ms) when omitted. Successful ACP prompt completion
  flushes and waits for queued sends; `/clear`, ACP cancellation, and ACP relay
  errors discard buffered text while already queued sends finish in order.
  The host's direct ACP client does not expose the bridge's separate
  `responseBoundary` signal, so it cannot reset the streamer at TypeScript
  output-segment boundaries (including follow-up/collect handoffs). The wider
  TypeScript output-segment lifecycle around questions, busy-wait/steer
  behavior, and full status/error terminal-state handling is also incomplete.
- ACP user-input question answers still require the interactive question card;
  `/deny` can cancel an unanswered question from its original requesting
  sender. `/who` caps and sanitizes the workspace basename at 128 code points;
  TypeScript emits the basename without this extra sanitization/cap. Identity
  and memory-scope command lines now match the TypeScript field choices and are
  emitted only when channel boundary configuration is enabled.
- Scheduled unattended prompts now use a full-memory snapshot once per ACP
  session and refresh it when the Rust channel-memory revision changes. Memory
  reads use the shared Rust store and revision API; local memory writes
  invalidate in-flight reads and cached snapshots. The configured
  channel boundary uses one instructed-session marker across ordinary inbound
  and scheduled prompts, so whichever arrives first claims it.
  `instructions` are already passed to the ACP child as its system prompt,
  rather than being repeated in the first user prompt. The shared loop
  lifecycle/task telemetry hooks are also not ported. The runner checks the
  shared scheduler continuation token before execution and delivery. A
  `/loop cancel` while ACP is already running lets that prompt settle and
  suppresses delivery, as the shared TypeScript runner does. `/clear` removes
  the session route and cancels
  its active ACP prompt.
  On loop timeout or a dropped ACP event stream, the host asks ACP to cancel;
  if the request does not settle within the five-second grace period, it removes
  the route and closes the ACP session. The native host has no bridge-recovery
  wait hook equivalent to TypeScript `waitForBridgeRecovery`.
  If there is no existing Feishu session, `/help` has no per-session ACP
  catalog to display; it does not fall back to a bridge-wide command list.
- Natural-language memory classification is not ported. The host implements
  deterministic memory phrases and explicit confirmations only; ambiguous
  memory requests go to the ACP agent.
- Webhook mode uses a small HTTP/1.1 listener. It rejects chunked request
  bodies, does not provide TLS, and does not enforce a callback path. Put it
  behind TLS termination when exposed outside localhost. The TypeScript Node
  server accepts the HTTP framing supported by Node.
- `webhookHost` and `webhookPort` are supported, but webhook listener tasks are
  not maintained by an SDK connection manager. The WebSocket host does not
  reproduce the SDK's exact reconnect nonce/count/interval policy or liveness
  watchdog; reconnect is a bounded exponential retry loop.
- The Rust host has bounded prompt/event output and attachment lifetimes, but
  it does not port the TypeScript stale-card timer, pending card-update drain,
  output-card segmentation around questions, reaction helpers, proactive
  delivery hooks, or all adapter disconnect races. Streaming card action and
  terminal patch races are therefore less hardened than the TypeScript
  controller paths.
- Incoming reply/thread IDs are used for session and memory scope. Outgoing
  sends use Feishu's chat message endpoint and do not preserve a thread reply
  target, matching the current base-class default but limiting thread-specific
  presentation.
- Contact-name lookup is best effort and depends on the app's Feishu contact
  scopes; IDs remain the fallback label when a lookup fails.

## Dependencies and verification

No new dependency is required. The host uses dependencies already present in
the Rust workspace: Tokio, Reqwest, tokio-tungstenite, serde_json, base64,
SHA-2, UUID, and the existing Feishu media/question helpers. The long-connection
protobuf envelope is implemented locally with strict bounds instead of adding
a generated-protobuf crate. No Cargo manifest or lockfile was changed.

`rustfmt --edition 2024 --check rust/crates/canopy-cli/src/feishu_host.rs`,
scoped whitespace checks, and the locked offline Rust workspace `cargo check`
passed after the parallel GitLab and WeCom slices were integrated. The CLI
reports six existing dead-code warnings. No tests were run.
