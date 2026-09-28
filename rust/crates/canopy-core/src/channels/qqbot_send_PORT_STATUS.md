# QQBot outbound send port status

Ported the delivery sequence from `QQChannel.sendMessage` in
`packages/channels/qqbot/src/QQChannel.ts` to `qqbot_send.rs`.

## Implemented

- Initial markdown request with optional passive reply `msg_id`/incremented `msg_seq`.
- Immediate 429 handling, sequence rollback on failed or errored passive sends, active-message permission checks, active markdown retry, reply-only active plain-text fallback, and pure-active plain-text fallback.
- `DeliveryError` codes, network error propagation, `<noreply>` suppression, and response-body consumption on every HTTP status. Failed body reads are ignored after the body has been taken, matching `.text().catch(() => '')`.
- Resolved route, reply-ID freshness, and sequence state are explicit inputs/state. The module performs no persistence or file I/O.
- The injectable tests inspect the outbound POST JSON, bearer authorization, content type, timeout, attempt order, sequence state, status handling, and response-body reads. `<noreply>` trimming follows ECMAScript whitespace rules.

## Remaining integration

`channels/mod.rs` needs to export `qqbot_send`. The channel caller must resolve the route and fresh reply ID, load the current message sequence, pass active-message permission, then persist the updated sequence after success or failure as appropriate.

## Verification

`rustfmt --edition 2024 rust/crates/canopy-core/src/channels/qqbot_send.rs` completed.

`cargo test --manifest-path /tmp/qqbot-send-harness/Cargo.toml --offline` passed:
34 passed, 0 failed (17 `qqbot_send` tests plus the included `qqbot_api` and
`sanitize` unit tests). The temporary harness compiles the actual source files;
it uses the injected local transport and performs no network requests.

The full `canopy-core` workspace suite was not run for this slice. Routing,
reply-ID TTL lookup, token refresh, persistence, and diagnostic logging remain
the channel caller's responsibilities.
