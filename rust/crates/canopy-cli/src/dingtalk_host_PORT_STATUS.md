# Native Rust DingTalk host status

Source: `packages/channels/dingtalk/src/DingtalkAdapter.ts` and the existing
DingTalk helpers in `canopy-core::channels`.

`canopy channel dingtalk [configured-name]` now starts a native Stream host.
It selects `channels.dingtalk` by default or a named `type: "dingtalk"`
configuration, resolves `clientId` and `clientSecret` literals or `$ENV_VAR`
references, applies the channel cwd and session scope, and launches a native
ACP child. Session routes persist per configured channel under the global
Canopy `channels/` directory.

The host opens the DingTalk gateway with the robot callback subscription and,
when interactive cards are enabled, the card callback subscription. It answers
Stream ping frames, acknowledges event and robot callbacks before dispatch,
deduplicates message IDs for five minutes, and reconnects with bounded
exponential delay after disconnects. Ctrl-C stops reconnect waits and the live
socket. Sender, DM, group, mention, and pairing checks use the existing Rust
gates and workspace-scoped `FilePairingStore`. `atSender` is included in the
first Markdown reply chunk for group turns. Inbound quoted text, non-bot
mentions, and the first rich-text media code reach the ACP prompt. Images are
passed inline; files, audio, and video are saved to a unique temp directory.
The host uses the existing DingTalk media downloader, Markdown chunker, image
marker validator/uploader, card config parser, and card callback parser.

The shared inbound command dispatcher runs after sender, DM, and group
authorization and before session resolution. It handles `/help`, `/new`
(`/clear` and `/reset` aliases), `/cancel`, `/status`, `/who`, and the shared
permission response commands. Shared-session clear and cancellation use the
configured `allowedUsers` guard, and clear requires confirmation for a shared
session. Status and identity lookups do not create sessions. Replies use the
current DingTalk `sessionWebhook` for the chat; DingTalk messages in this host
do not carry a thread target.

Channel memory management also runs only after the sender, DM, and group gates
pass. Entries are scoped to the configured DingTalk channel and chat; pending
clear, natural-language update, and natural-language removal confirmations are
scoped to the originating sender and expire after 60 seconds. Explicit memory
commands support remember, paged list, inspect, update, and removal. Natural
language requests use a short-lived ACP classifier session; low-confidence or
invalid classifications fall through to the agent. Natural update and removal
show the selected entry and require confirmation, then compare the saved text
before changing it. Clearing always requires confirmation. A classifier-detected
remember action saves as a side effect and still sends the user's message to
the agent. Relevant chat memories are included as untrusted context for normal
prompts except when the configured session scope is `single`.

Normal prompts are serialized per session. ACP output uses a bounded JSONL
reader. Up to 128 `session/request_permission` requests are retained in
process-local state and relayed to the originating chat with `/approve`,
`/approve-always`, and `/deny` choices. Responses require the request's chat and
either its originating sender or a sender authorized for the configured shared
session. Requests expire after five minutes and are rejected; clear/cancel,
prompt completion, ACP disconnect, and host shutdown retire their pending
state. The ACP response is sent with the original JSON-RPC request ID. Relay
notices use the current `sessionWebhook` cached from the latest robot callback
for that conversation; DingTalk provides no persistent chat-send target in
this path. Valid ACP `available_commands_update` entries are cached by ACP
session ID. `/help` lists the catalog for the sender/chat's existing routed
session, and exact canonical names and aliases are forwarded without adding the
shared-session speaker prefix, matching the TypeScript channel's command
recognition. Names are restricted to the CLI slash-command character set;
descriptions and aliases are sanitized and the cache is bounded to 128 session
snapshots, 256 commands per update, and 64 KiB per catalog. Dynamic catalogs
are not shared across sessions or used before a route exists.

## Remaining parity gaps

- `/help` before a routed session exists has no session-scoped command catalog.
  After session creation, load, or resume, Rust ACP publishes the session's
  command list; skill refresh also updates it. ACP requests that include a
  user-input interaction cannot be presented as a DingTalk card; the shared
  permission dispatcher limits those requests to `/deny`.
- Replies are sent after a full ACP turn. There is no block streaming, message
  edit/update, reaction attach/recall, or proactive REST delivery. The current
  Rust core has no DingTalk edit or reaction transport helper.
- The Rust core contains interactive-card config/callback parsers only. The
  TypeScript status/question card controllers and card API client are not
  available to this host, so it acknowledges and parses enabled card callbacks
  but cannot present, update, cancel, or answer a card interaction.
- The source's `DingtalkConnectionManager` has socket health checks and client
  replacement; the Rust host reconnects with exponential backoff and pings but
  does not port its exact manager timing, `useConnectionManager: false` SDK
  behavior, or replacement-client lifecycle. `useConnectionManager` is type
  validated but does not change native behavior.
- Text reply references are projected. DingTalk webhook/session targets are
  process-local and are refreshed only by new inbound messages; proactive
  sends after restart cannot use a stale target.
- The host has not ported app-level message lifecycle hooks, prompt buffering,
  CUA, pending group-history collection, or the TypeScript bridge restart
  supervisor.

## Verification

`rustfmt --edition 2024 rust/crates/canopy-cli/src/dingtalk_host.rs` and
`CARGO_BUILD_JOBS=2 cargo check --manifest-path rust/Cargo.toml -p canopy-cli
--locked --offline` pass after the DingTalk agent-command update. The CLI check
reports four warnings in other CLI modules. No tests were added or run.
