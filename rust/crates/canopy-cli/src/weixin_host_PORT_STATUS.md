# Native Weixin CLI host status

The Rust CLI now exposes `canopy channel weixin [configured-name]`. It selects a
`type: "weixin"` entry from merged settings, resolves the workspace, model,
instructions, session scope, approval mode, direct-message policy, sender
policy, and allowed-user list, then starts a native ACP child process.

The host loads the account from the shared Weixin state directory. When no
account exists, it runs the existing QR login flow and saves the returned
credentials. The existing poll loop supplies cursor persistence, error
backoff, and user-message extraction. Direct messages pass through the Rust DM
and sender gates. Shared slash commands and channel-memory intents are
dispatched before session routing; ordinary messages are projected with
referenced-message context, routed to a persisted per-user ACP session, and
sent through the Rust text/image outbound orchestration using the message
context token. Regular prompts are queued and the polling callback returns so
polling can continue while a prompt is running. Per-session FIFO
queues keep prompts serialized, with limits of 16 queued messages per session
and 128 queued or active prompts across the host. Queue saturation sends a
visible retry notice. Prompt replies and delayed prompt errors are handled by
the queue worker after the poll cursor has advanced, matching the TypeScript
adapter's detached inbound handling.

Configured `identity` and `memoryScope` settings resolve with ChannelBase's
`channel:<name>` and `metadata-only` defaults. When either setting is present,
the host appends the bounded, sanitized channel isolation boundary after the
operator instructions so those instructions cannot override it. `/who`
reports the resolved display name and memory namespace under the same
configuration gate.

The host maps each queued ACP prompt's start and terminal outcome onto the
shared Rust Weixin typing lifecycle. It requests typing after media preparation
and before `session/prompt`, then sends a cancel after the response is delivered
or the prompt fails or is cancelled. `/clear` ends typing before cancelling its
session. Host shutdown sends cancel for each active chat before disconnecting
the typing lifecycle. Typing-ticket lookup and status requests are best-effort;
indicator API failures do not fail prompt handling.

Accepted inbound messages persist the sender as an observed channel contact
after the direct-message and sender authorization gates, before local commands
or agent routing, matching `ChannelBase.processInbound`. The registry uses
`<global Canopy dir>/channels/daemon/<workspace hash>/observed-contacts.json`;
the 16-character workspace hash matches the shared TypeScript daemon key.
Persistence errors are logged and do not block message handling. Weixin input
currently has no display name, group, or topic fields, so records contain the
sender ID as both ID and label with no group relationship.

Inbound images are downloaded and decrypted, checked by magic bytes, and
forwarded as ACP image content blocks after the projected text. Inbound files
are downloaded and decrypted to a per-message directory under the system temp
directory, then added through the shared file-attachment prompt projection.
Media download failures are logged; file failures replace the prompt text with
the same failure note used by the TypeScript adapter. The downloader bounds
native image payloads to 8 MiB and files to 32 MiB. It streams response chunks
up to those limits and decrypts the owned ciphertext buffer in place. For an
image, raw bytes are dropped after base64 encoding, before the ACP request is
serialized. The ACP server accepts standard image blocks and converts them to
the runtime's inline image part before model dispatch.

The ACP child reader bounds each JSONL output frame to 16 MiB. An oversized
frame or read failure terminates the child and fails pending requests instead
of retaining an unbounded line buffer.

## Remaining gaps

- The media limits are native-host safeguards: the TypeScript adapter does
  not impose the same 8 MiB image and 32 MiB file caps. Image content is sent
  inline over the ACP stdio protocol, rather than through the ACP session media
  store. Downloaded file directories follow the TypeScript temp-file lifetime
  and are not automatically cleaned up.
- Inbound video and voice items are not projected by the monitor or native
  host; the TypeScript adapter also only handles image and file items today.
- The host now dispatches the shared inbound slash-command set after DM and
  sender authorization but before resolving a session. `/help`, `/status`,
  `/who`, `/clear`, `/reset`, and `/new` use the shared Rust dispatcher;
  replies go through Weixin's normal context-token-aware outbound sender.
  Shared-session and `allowedUsers` checks follow the configured session scope.
  Clearing removes the scoped route, sends ACP `session/cancel` and
  `session/close`, and drops the prompt lock. The source Weixin adapter does not
  register `/cancel`, so that command remains forwarded to the agent.
- ACP available-command updates are retained in bounded per-session catalogs.
  `/help` uses the routed session's commands, with the latest catalog as the
  fallback only when no route exists. Names and aliases are validated and
  matched case-sensitively against the first slash token; recognized commands
  keep the slash at the start of shared-session prompts, while unknown slash
  text keeps speaker attribution. Catalogs are bounded to 256 sessions, 128
  commands per session, 16 aliases per command, 128-byte command names, 512
  description characters, 256-byte session IDs, and 16 KiB per catalog.
  Session clear/close/death, failed session loads, and ACP process exit clear
  stale entries; bulk session-load replay restores saved command updates.
- ACP `session/request_permission` requests are retained in bounded
  process-local state and relayed to the originating Weixin chat. `/approve`,
  `/approve-always`, and `/deny` use the shared command dispatcher and choose
  permission options by their ACP kind. Requests expire after five minutes and
  are denied; missing prompt origins, relay failures, queue saturation, and
  shutdown fail closed. Clearing a session cancels its pending permission
  requests. Pending requests do not survive host restarts.
- Channel-memory intents use the shared deterministic Rust parser before ACP
  routing. The host handles remember, paged list, inspect, explicit update and
  removal, and two-step clear. Unambiguous natural-language list, inspect,
  update, removal, clear, and remember requests use a temporary ACP
  classification session, matching the TypeScript channel classifier.
  Natural update/removal and clear confirmations are scoped to the configured
  channel, direct chat, and sender; they expire after 60 seconds and do not
  survive restart. Confirmed natural updates/removals use expected-text checks
  so changed entries fail safely. Memory uses the shared versioned channel
  memory document under that channel and chat. Candidate previews, duplicate
  reporting, page boundaries, and user-facing responses follow the TypeScript
  adapter. Classifier-detected remember requests save as a side effect and
  still continue to the normal agent prompt. For non-single session scopes,
  ordinary ACP prompts now include the shared Rust recall selector's relevant
  entries, formatted as untrusted user facts before the user prompt. Selection
  stays within the resolver's three-entry and 1,200-code-point limits. A recall
  read failure is logged and leaves the prompt unchanged.
- Channel-level `dispatchMode` now follows the shared default (`steer`) and
  accepts `steer`, `followup`, and `collect`. Follow-ups keep FIFO order.
  Steering requests ACP cancellation, prefixes the replacement prompt with the
  cancellation context, and keeps it serialized behind the active request; a
  three-second watchdog logs if that same request is still active. Senders that
  are not authorized to steer a shared session are queued as follow-ups.
  Collect mode buffers at most 128 messages and 1 MiB of text plus reply
  context per session, with host-wide caps of 512 messages and 4 MiB. It
  coalesces projected text after the active turn and drops collected image/file
  payloads. On buffer saturation, the new message falls back to the bounded
  FIFO queue. The TypeScript collect buffer has no matching native caps. The
  host does not expose prompt-buffered/drained callbacks because there is no
  effective callback behavior to port: ChannelBase's defaults are no-ops, and
  Weixin only overrides prompt start/end for typing. Lifecycle events for
  background tasks outside queued ACP prompts remain unimplemented.
- Configuration is a focused subset. It does not port dynamic settings reload
  or all shared channel options. The account store is the single shared
  account used by the current TypeScript adapter;
  it is not a per-configured-channel account registry.
- The native host only records observed contacts; Weixin has no adapter label
  cache to hydrate from the registry. Store mutations are serialized within
  one process, not across multiple processes.

## Verification

`rustfmt --edition 2024 rust/crates/canopy-cli/src/weixin_host.rs` passes. The
locked offline workspace check is pending the parent agent's consolidated
verification. Tests were not run. The host has not been exercised against a
live Weixin account or compared with the full TypeScript integration behavior.

The host is therefore a runnable first message path, not full Weixin channel
parity or completion of the repository-wide Rust port.
