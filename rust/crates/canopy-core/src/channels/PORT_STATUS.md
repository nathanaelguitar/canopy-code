# `packages/channels/base` Rust port

## Implemented in `canopy-core::channels`

- `sanitize.ts`: code-point truncation and sender, quoted text, prompt, display,
  path, and log sanitizers.
- `DmGate.ts`, `SenderGate.ts`, and `GroupGate.ts`: typed policies, allowlists,
  mention/reply checks, passive authorization checks, and pairing requests.
  Pairing-store I/O errors propagate through the gates.
- `PairingStore.ts`: `FilePairingStore` persists user/group approvals and
  expiring requests, enforces pending caps, supports legacy migration, isolates
  workspace state, and atomically replaces private files. Its in-process
  mutex does not coordinate separate processes.
- `paths.ts`: tilde expansion, path resolution and realpath fallback, global
  Qwen home selection, and workspace-scoped directory names. On Unix, an
  unset `HOME` falls back to the temp directory; Node may find the home from
  the password database.
- `group-history-store.ts`: JSONL replay, malformed-record recovery,
  per-group limits and key eviction, drain/clear operations, and compaction.
- `channel-memory-intent.ts`: English and Chinese deterministic memory command
  parsing with confirmation intents and source-compatible match priority.
- `channel-memory-recall.ts`: reusable recall indexes, NFKC normalization,
  script-aware tokenization, deterministic ranking, fallback selection, and
  the source entry/code-point budgets.
- `observed-contact-store.ts`: validated, bounded observations for channel
  users, groups, and topics; freshness filtering; insertion-ordered graph
  aggregation; UTF-16-compatible label truncation; and owner-only atomic JSON
  persistence. The Rust store is exported as `channels::observed_contacts`.
- `ChannelWebhookTask.ts`: target lookup and bounded prompt/display text with
  sanitized fields and explicit untrusted-event instructions. Webhook receipt,
  secret resolution, and execution are not included.
- `ChannelLoopStore.ts`: validated JSON persistence, per-target listing and
  creation caps, legacy-field normalization, serialized updates, and private
  atomic replacement. Its update queue is process-local.
- `ChannelLoopTools.ts`: tool schemas and JSON-RPC behavior with a
  runtime-neutral async handler seam. Daemon transport registration is not
  included.
- `ChannelLoopScheduler.ts`: due checks through an injected cron resolver,
  five-run concurrency cap, persisted lifecycle updates, skip/failure policy,
  bridge-recovery handling, and startup reconciliation. Tokio timer and runner
  integration remain adapter seams.
- `BlockStreamer.ts`: progressive block splitting, idle emission, serialized
  sends, and flush/stop behavior. It requires Tokio; Rust cannot reproduce
  splitting inside a JavaScript surrogate pair, so thresholds preserve whole
  Unicode scalar values at those boundaries.
- `ChannelProactiveDeliveryError.ts`: typed permanent/transient delivery
  failures with an optional source error. The Rust predicate accepts this
  concrete type rather than arbitrary objects with matching fields.
- `SessionRouter.ts`: scope-aware channel identity routing, reservation and
  coalescing, eager/lazy recovery, bridge replacement, route invalidation,
  death handling, and ordered private persistence. The bridge event subscription
  and daemon lifecycle wiring remain adapter work.
- `PollingChannelBase.ts`: reusable cursor restore/save, single-loop lifecycle,
  interruptible waits, and capped exponential backoff. Polling APIs remain
  injected until concrete channel adapters are ported.
- `packages/channels/{github,gitlab}/src/mention.ts`: regex escaping, bot
  mention matching, and mention removal with each provider's boundary rules.
- `packages/channels/gitlab/src/GitlabAdapter.ts`: source-shaped config and
  cursor types, first-poll todo draining, ascending-ID processing, action
  template expansion, issue/MR description caching, note mention stripping,
  envelope creation, best-effort todo cleanup/error notes, and note emoji
  acknowledgement callbacks. The `GitlabApi` trait is injectable; the concrete
  reqwest API bounds each body to 8 MiB, each request to 30 seconds, and polling
  to 50,000 todos. Channel registration, token resolution, authorization gates,
  and wiring into the daemon/CLI ChannelBase remain host integration work; see
  `gitlab_adapter_PORT_STATUS.md` for exact differences and limits.
- `packages/channels/{dingtalk,feishu}/src/markdown.ts`: platform markdown
  normalization and bounded message chunking; adapter delivery is separate.
- `packages/channels/feishu/src/media.ts`: validated resource IDs, bearer-authenticated
  bounded streaming downloads, MIME fallback, request timeout, and awaited
  cancellation through an injectable HTTP seam.
- `packages/channels/dingtalk/src/media.ts`: two-step download-code resolution
  and bounded streamed file download with awaited cancellation behind an
  injectable HTTP seam.
- `packages/channels/dingtalk/src/outbound-image.ts`: UTF-16 marker projection,
  code masking, canonical path containment, file signature and size checks,
  and token-safe DingTalk image upload through an injected transport.
- `packages/channels/weixin/src/accounts.ts`: exact account JSON shape,
  private atomic persistence, corruption-tolerant loads, and orphan-temp
  cleanup using the shared atomic writer.
- `packages/channels/weixin/src/api.ts`: request contracts, error/retry rules,
  cancellation-aware polling, upload URL selection, and strict CDN validation
  through injected transport/runtime seams.
- `packages/channels/weixin/src/types.ts`: optional snake_case iLink message,
  media, request, and response models with numeric protocol constants.
- `packages/channels/weixin/src/login.ts`: QR start/poll flow, expiry refresh,
  confirmation, bounded request waits, and the overall eight-minute deadline.
- `packages/channels/qqbot/src/api.ts` and `types.ts`: token/gateway/message
  HTTP requests, strict gateway URL validation, protocol event/config types,
  opcode/intent constants, and documented config defaults.
- QQ Bot routing-state helpers from `QQChannel.ts`: ordered five-map JSON,
  validation and legacy restore, 500 ms debounced private writes, atomic rename,
  and explicit flush/disposal behavior. QQChannel map/path/lifecycle wiring
  remains separate.
- QQ Bot outbound send policy from `QQChannel.sendMessage`: passive markdown,
  reply sequencing and rollback, 429 handling, active markdown/text fallbacks,
  response-body draining, and `<noreply>` suppression. Route resolution and
  caller persistence remain separate.
- QQ Bot group-message projection from `QQChannel.prepareGroupMessage`:
  mention-aware display/prompt text, sender identity privacy, slash detection,
  bot OPENID extraction intents, and source-compatible UTF-16 mention limits.
  The caller still applies cache and warning intents.
- QQ Bot gateway protocol state from `QQChannel.ts`: IDENTIFY/RESUME selection,
  sequence and heartbeat tracking, READY versus RESUMED, invalid-session reset,
  reconnect decisions, and forwarding for the seven consumed gateway events.
  WebSocket lifecycle, reconnect timers, persisted-state restoration, and event
  handling remain host work; see `qqbot_gateway_PORT_STATUS.md`.
- QQ Bot cron text buffering from `handleCronTextChunk` / `runCronFlow`:
  cron-flow gating, UTF-16 thresholding, debounce, route lookup, and bounded
  transient retries. The native QQ CLI host connects ACP text events, persisted
  target/send state, and delivery-error mapping. No `runCronFlow` call site or
  QQ proactive-send support exists in the TypeScript source, so scheduled prompt
  invocation remains unavailable.
- `WeixinAdapter` typing tickets and lifecycle: shared ticket caching,
  best-effort typing requests, active-chat deduplication, and reconnect fencing.
  ChannelBase event wiring and context-token lookup remain adapter work.
- `packages/channels/telegram/src/TelegramAdapter.ts`: typed Bot API and
  long-poll seams, text/photo/document/voice projection, entity/reply metadata,
  command menu and local `/start`, typing lifecycle, topic-aware response and
  proactive sends, Markdown HTML formatting/splitting, and plain-text fallback.
  Native ChannelBase/session-router wiring, config/startup integration, signal
  handling, and attachment retention remain open; see
  `telegram_PORT_STATUS.md` for exact boundaries.
- `WeixinChannel.sendMessage` outbound orchestration: code-masked image marker
  parsing, marker removal, text-first/image-sequential sends, and the Chinese
  fallback message after image errors. Adapter lifecycle and context-token
  lookup remain separate.
- `packages/channels/feishu/src/question-card-controller.ts`: request
  reservation, authenticated action claims, response settlement, expiry,
  disposal, and FIFO terminal-card projection through local adapter callbacks.
- `ChannelBase.processInbound` prompt projection: speaker and member
  attribution, quoted replies, attachments, metadata, image selection, and
  bounded display text. Routing and prompt dispatch remain outside the helper.
- `ObservedChannelContactStore`: the persistent observed-contact registry and
  graph aggregation used by channel lookup enrichment. Native Weixin, QQ, and
  Telegram CLI hosts now persist authorized inbound observations into the
  same workspace-hashed registry path; their status notes record which sender,
  group, and topic labels each platform provides. The TypeScript daemon worker,
  HTTP route, and `ChannelBase` call sites remain on the legacy runtime, and
  other native channel hosts are not connected yet.

- GitHub notification polling from packages/channels/github/src/GithubAdapter.ts:
  reason routing, cursor dedupe, first-contact bodies, durable inbound task
  recovery, error comments, thread replies, and working reactions behind
  injectable API/authorization/handler traits. A foreground native CLI host
  now wires gates, pairing, ACP prompt/session routing, and thread-comment
  delivery. The final-response publication saga, full ChannelBase features, and
  TypeScript daemon-worker integration remain separate; see the core adapter
  and CLI host status notes.

The DingTalk interactive-card type/default/callback helpers and Feishu
question-card builder/parser are also ported. Weixin monitoring, text/image
send utilities, AES media handling, and outbound orchestration are integrated.
Platform adapter and channel-caller wiring remains outside this core slice.

The polling runtime, channel prompt projection, and platform utility helpers
are integrated in `canopy-core`; latest focused test totals are tracked in the
design ledger after validation.

## Behavior boundaries

- Rust scalar-value boundaries match JavaScript code points for valid Unicode;
  Rust strings cannot represent lone UTF-16 surrogates.
- NFKC comes from `unicode-normalization`; Unicode casing and script-property
  tables can differ from the Node version for recently assigned characters.
- Group history reloads the full JSONL file per operation, as the TypeScript
  store does. Neither it nor pairing state currently coordinates mutations
  across processes.
- Observed-contact read/modify/write is serialized within the Rust process but
  does not coordinate writers in other processes. Native host path selection
  uses `hash_daemon_workspace`; read-route wiring and persistence calls for
  other channel hosts remain.
- Remaining platform adapter classes, webhook receipt and execution, and
  daemon/CLI transport wiring still need ports or integration. GitHub and
  GitLab core polling adapters are present; the native GitHub CLI host wires
  caller/auth/prompt basics, while daemon integration and the GitHub
  final-response publication saga remain. Check
  adjacent `*_PORT_STATUS.md` notes for exact compatibility limits.

The modules are exported from `canopy-core`. The latest
`cargo test -p canopy-core channels:: --locked --offline` run passed all 449
channel tests. The latest complete `cargo test --workspace --locked --offline`
run passed 1,412 tests (1,386 core, 19 CLI, 6 audio-capture, and 1 mobile-MCP).
`cargo fmt --all -- --check` and `git diff --check` also passed. These results
cover the integrated helper modules; they do not establish parity for unported
adapters and product surfaces.
