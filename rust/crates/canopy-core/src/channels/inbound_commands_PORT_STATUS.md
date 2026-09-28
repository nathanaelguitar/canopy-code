# Shared inbound command dispatcher port status

Source: `packages/channels/base/src/ChannelBase.ts`.

## `/approve`, `/approve-always`, and `/deny` implemented

- The generic dispatcher enforces the same shared-session authorization gate,
  looks up explicit IDs or chat/thread pending requests, and filters request
  visibility by exact chat/thread, sender-input ownership, and shared-session
  scope. Ambiguous results list up to six sanitized IDs and tool titles.
- Approval selects `allow_once` (including the legacy `proceed_once` option),
  always approval prefers project then user scope, and denial selects
  `reject_once` (including legacy `cancel`) or sends a cancelled outcome.
- It preserves relay-unavailable, interactive-user-input, missing-option,
  bridge-failure, accepted, and stale-request replies from ChannelBase. The
  interactive question hint is sent at chat level without selecting a thread.
- The `InboundCommandHost` seam supplies pending request snapshots and the
  permission response operation. A responding host owns pending-entry cleanup
  on accepted, stale, and failed bridge responses.

## `/who` implemented

- The shared dispatcher now recognizes `/who` and rejects unauthorized access
  with the same reply used by the TypeScript command. Authorization stays in
  the host's shared-session policy, so per-user sessions and configured
  allowlists retain their existing semantics.
- The host provides the active-session flag for the current sender/chat/thread,
  workspace path, session scope, and optional channel identity/memory metadata.
  The contract requires a read-only lookup that does not create a session.
- The dispatcher reports the channel, only the workspace basename, active or
  absent session, and scope note. `single` is called out as channel-wide;
  group `thread` and `chat_thread` are called shared by the group; group `user`
  sessions are called private to the sender. Identity display name and memory
  namespace are included together only when the host enables the channel
  boundary prompt, and both use the source `sanitize_quoted_text` behavior.

## Native host integration

The native Weixin and Telegram CLI hosts implement `InboundCommandHost` and
invoke this dispatcher after successful platform authorization. Both expose
the shared `/help`, `/status`, `/who`, `/new` (`/clear` and `/reset` aliases),
and permission commands, subject to their source adapter's command policy.
Weixin intentionally leaves `/cancel` for the agent because its TypeScript
adapter does not register that command. Telegram's end-to-end ACP
`session/request_user_input` relay remains a separate host feature.

The QQ CLI host implements the permission lookup/response hooks and routes
`/approve`, `/approve-always`, `/deny`, and `/cancel` through the dispatcher;
its other shared commands remain unported. The shared `/who` contract and
permission visibility preserve the same per-channel/chat/thread and
shared-session checks across the wired hosts. Daemon worker integration and
the other channel adapters still need their own host wiring.

No tests were added or run for this slice.
