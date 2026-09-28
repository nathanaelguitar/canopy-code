# QQ Bot routing Rust port status

`qqbot_routing.rs` ports `isValidChatId` and the route-selection portion of
`QQChannel.resolveRoute` from `packages/channels/qqbot/src/QQChannel.ts`.

## Behavior covered

- Accepts only nonempty ASCII letters, digits, underscore, and hyphen, up to
  128 characters, before using the ID in a request path.
- Prefers runtime-learned chat types over configured `chatTypes` overrides.
- Returns no route for invalid IDs or IDs without a known route type.
- Uses `qqbot_api::get_api_base` for standard and sandbox hosts.
- Resolves C2C routes to `/v2/users/{chatId}/messages` and group routes to
  `/v2/groups/{chatId}/messages`.
- Leaves disposed-channel and token-refresh checks and their logging in the
  caller.

## Dependencies and integration

No manifest changes are required. The module reuses the existing
`QQChannelConfig`, `QQChatType`, and `get_api_base`. Export it from
`channels/mod.rs` with:

```rust
pub mod qqbot_routing;
```

## Verification

`rustfmt --edition 2024` completed. Seven focused tests cover allowed IDs,
invalid characters and length, runtime/config precedence, missing route types,
sandbox selection, and C2C/group paths. The QQBot package has no dedicated
`resolveRoute` test file, so these tests exercise the source decision rules
directly.
