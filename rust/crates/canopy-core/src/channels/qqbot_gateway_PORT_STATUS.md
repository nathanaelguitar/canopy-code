# QQ Bot gateway protocol port

`qqbot_gateway.rs` ports the protocol state owned by
`packages/channels/qqbot/src/QQChannel.ts`:

- HELLO heartbeat negotiation with the source's 45-second default and
  5-second minimum.
- IDENTIFY versus RESUME selection, including token and sequence fields.
- READY versus RESUMED behavior, including cold-start restore signaling and
  sequence/session retention.
- Heartbeat acknowledgement tracking and reconnect after two missed intervals.
- INVALID_SESSION state reset and persisted-state flush signaling.
- Server-requested reconnect and close-code retry/resume decisions.
- Dispatch forwarding for the seven QQ events consumed by the TypeScript
  adapter.

The module returns effects to a host rather than owning the WebSocket. The
native CLI composition is in
[`qqbot_host.rs`](../../../canopy-cli/src/qqbot_host.rs): it connects to the
validated gateway URL, sets READY and heartbeat deadlines, performs reconnects,
restores channel and session-router state, and dispatches C2C and group-at
events. It does not dispatch `GROUP_MESSAGE_CREATE` or port the remaining
TypeScript channel features; see the host status file for exact residuals.

No tests were added or run for this slice. Formatting and compilation are
checked at the Rust workspace boundary.
