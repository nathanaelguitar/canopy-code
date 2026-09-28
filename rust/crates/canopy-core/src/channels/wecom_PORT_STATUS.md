# Rust WeCom channel port status

The native Rust port is in progress. The TypeScript behavior oracle is
`packages/channels/wecom/src/WeComAdapter.ts`, with WebSocket protocol details
from the MIT-licensed `@wecom/aibot-node-sdk` dependency.

## Implemented in `wecom.rs`

- Config parsing for `botId`, `secret`, and optional `wss://` `wsUrl`; debug
  output redacts the secret.
- Inbound callback body projection, sender/chat/message metadata, bot-mention
  and quoted-reply fields, text/voice/mixed/file/image/video rendering, and
  recursive media-reference collection with URL deduplication and the source
  depth limit.
- Outbound image-marker parsing that leaves markers in fenced, inline, and
  indented code; markdown chunking at the source's 3,800 UTF-8-byte limit;
  source-shaped markdown payloads; filename and image MIME helpers.
- Inbound HTTPS media download with local/private-address rejection, DNS
  address pinning, redirects disabled, proxy use disabled, a 20 MiB streamed
  response cap, 10-second connect/read limits, a 60-second request cap, and the
  SDK's AES-256-CBC/PKCS#7 decryption format.
- Outbound marker-file reads are confined to the private `tmp/channel-files`
  directory, reject symlink escapes and non-regular files, use `O_NOFOLLOW` on
  Unix, and enforce the 20 MiB cap while reading.
- The five-minute message deduper exposes the source's in-flight/seen admission
  states and retry commit point. The attachment lease store ports message,
  route, session, buffer-drain, buffer-drop, and prompt-end ownership, with
  cleanup confined to the private staging root.
- Inbound attachment processing downloads refs in source order, returns images
  as base64 prompt attachments, writes other types as private files, maps voice
  to audio, and honors a host-provided connection-generation check before and
  after I/O. It caps each file at 20 MiB, each message at 16 attachment refs
  and 32 MiB aggregate media, and uses the source's media-only text placeholders.

## Implemented in `wecom_ws.rs`

- SDK-compatible WSS auth (`aibot_subscribe`), callback and event delivery,
  request-ID ack correlation, per-callback-ID serialized replies, 30-second
  heartbeats, server ping/pong, bounded JSON frames, manual shutdown, and
  exponential connection/auth-failure reconnects. A `disconnected_event` is
  delivered then stops automatic reconnect, matching the SDK behavior.
- Proactive `aibot_send_msg` markdown/media commands and the SDK's three-step
  media upload (`aibot_upload_media_init`, 512 KiB base64 chunks with retries
  and bounded concurrency, then `aibot_upload_media_finish`). The client caps
  frames at 1 MiB and uploaded bytes at the adapter's 20 MiB limit.
- Wire events and connection status are exposed as bounded broadcast/watch
  streams for a future native host to consume.

## Implemented in the native CLI host

- `canopy channel wecom [configured-name]` config selection and settings/env
  resolution for credentials and the optional secure WebSocket URL.
- Sender, DM, and group authorization, pairing notices, observed contacts, and
  persistent ACP session routing by configured channel and session scope.
- Authentication timeout, event handling, five-minute message deduplication,
  reconnect after server kicks, a five-minute activity watchdog, and
  Ctrl-C-driven cancellation, handler drain, and attachment cleanup.
- Inbound projection and authorization before media download; inline image and
  temporary-file attachments use the shared connection-generation guard and
  attachment lease callbacks. Outbound Markdown, image-marker file reads,
  upload, and send use the shared safe media helpers and WSS acknowledgements.
- Bounded processing: four active handlers, 32 queued callbacks, 16 media
  references and 32 MiB aggregate media per callback, 32 retained ACP events
  of at most 256 KiB each, and a 4 MiB accumulated final response cap. If the
  host queue is full, a new callback is logged and dropped; event-broadcast
  lag is logged because evicted callbacks cannot be recovered.

## Remaining work

- The CLI host now supplies shared inbound commands, per-session
  follow-up/steer/collect buffers, attachment buffer callbacks, scoped channel
  memory management and recall, and `/who`/`/status`; see
  `rust/crates/canopy-cli/src/wecom_host_PORT_STATUS.md` for host behavior and
  limits. Different sessions now run ACP prompts concurrently while each
  session stays ordered.
- Typed channel lifecycle events and more descriptive group labels remain open.
  Add end-to-end host acceptance evidence and preserve the Node implementation
  until package parity and acceptance are established.

The locked offline CLI check and targeted WeCom formatting check pass; no tests
were run.
