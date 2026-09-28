# Telegram adapter Rust port status

Source: `packages/channels/telegram/src/TelegramAdapter.ts` and
`packages/channels/telegram/src/TelegramAdapter.test.ts`.

## Implemented

- Typed Telegram Bot API update, message, entity, reply, photo, document,
  voice, file, user, and chat records.
- An injectable `TelegramApi` seam plus a production `reqwest` implementation
  for `getMe`, command menu registration, webhook removal, `getUpdates`, file
  lookup/download, `sendMessage`, and `sendChatAction`. Proxy and local Bot API
  endpoints can be supplied at construction.
- Startup retrieves bot identity, attempts the exact five-command menu, drops
  pending updates, and starts one cancellable long-poll task. Poll failures use
  bounded exponential backoff. Disconnect aborts polling and clears typing
  tasks.
- Inbound text, largest-photo selection, document, and voice handling. Media
  errors preserve the TypeScript explanatory text; successful document/audio
  payloads are written under unique directories in the OS temporary
  `channel-files` directory. Photo bytes become base64 JPEG data.
- Envelope projection preserves sender/chat names, group flags, forum topic,
  UTF-16 entity ranges, addressed bot commands, bot replies, and referenced
  reply text. The adapter maps its richer envelope into the existing gate and
  prompt-projection contracts.
- `/start` is handled locally. Other slash commands are forwarded unchanged
  through the shared inbound handler seam. Handler failures trigger the source
  generic reply. Long-running handler work is detached from update polling.
- Shared per-chat typing state, four-second repeats, terminal lifecycle and
  session-death cleanup, response and proactive topic routing, the source
  Markdown-to-HTML features, UTF-16-aware HTML splitting, and per-chunk
  plain-text fallback.

## Host integration and remaining gaps

- The native CLI host is implemented in
  `rust/crates/canopy-cli/src/telegram_host.rs` and wired through
  `canopy channel telegram [configured-name]`. It implements
  `TelegramInboundHandler`, loads settings and environment-backed tokens,
  performs authorization preflight before downloading media, routes shared
  commands and prompts through the native session router, and supplies the
  current inbound route or persisted target when sending responses. The host
  status note records its current parity and limitations:
  `rust/crates/canopy-cli/src/telegram_host_PORT_STATUS.md`.
- Telegram remains a single-process CLI host rather than an adapter registered
  with the general channel daemon's proactive-delivery API. The CLI does not
  use the Node host's bridge-restart supervisor or model-profile selection.
- Long-poll offsets and inbound tasks are process-local and are not persisted.
  The adapter drops pending updates at each new connection, as the source does;
  durable update deduplication and restart recovery remain unimplemented.
- Downloaded media files are written to unique OS temporary directories for
  agent access. A long-running process retention/cleanup policy is not wired.
- Telegram numeric forum-topic IDs are represented as signed 64-bit integers
  for outbound requests. The TypeScript adapter converts through JavaScript
  `Number`, so unusual IDs outside this range are not equivalent.
- This module handles Bot API message updates only; callbacks, edited
  messages, channel posts, and other Telegram update variants are not handled
  by the TypeScript adapter either.

## Verification

Focused parity tests cover startup order, update/payload mapping, photo size
selection, document/voice persistence, media-download failure, command routing,
typing lifecycle cleanup, topic-aware response/proactive delivery, formatter
syntax, HTML splitting, and fallback conversion. Tests were not run for this
follow-up. `cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked
--offline` passes with six existing dead-code warnings. This check does not
establish that the Node crash or macOS stress gate is resolved.
