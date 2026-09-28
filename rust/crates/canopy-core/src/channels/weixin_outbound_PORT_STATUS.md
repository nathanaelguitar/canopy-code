# Weixin outbound orchestration Rust port status

`weixin_outbound.rs` ports the message orchestration in
`WeixinChannel.sendMessage` from `packages/channels/weixin/src/WeixinAdapter.ts`.
Text formatting and image upload remain in the existing
`weixin_send_utils.rs` and `weixin_send_image.rs` modules.

## Behavior covered

- Masks closed fenced and inline code before parsing case-insensitive
  `[IMAGE: ...]` markers.
- Trims parsed image paths with ECMAScript whitespace rules, preserves marker
  order and duplicates, and removes only markers that match a parsed path.
  Markers inside code and non-matching marker spellings remain in displayed
  text.
- Collapses three or more LF characters to two and trims the cleaned text.
- Sends nonempty cleaned text first, then sends images sequentially with the
  caller-provided workspace allowlist and context token.
- Propagates text-send failures. Logs image-send failures and attempts the
  source's Chinese fallback text; logs fallback failures and continues to the
  next image.
- Injects the send and output boundaries, with a production sender that
  delegates to the existing Reqwest-backed text and image helpers.

## Dependencies and integration

No manifest or lockfile changes are required. Export the module from
`channels/mod.rs` with:

```rust
pub mod weixin_outbound;
```

The caller supplies channel name, chat ID, base URL, token, context token, and
workspace directories. This keeps channel configuration and context-token
lookup outside the orchestration module.

## Verification

`rustfmt --edition 2024` completed. An isolated offline harness compiled this
module with the existing Weixin API, types, media, text/image sending modules,
and passed 49 tests total: 7 outbound orchestration tests plus 42 tests from
the included existing Weixin modules. The TypeScript `WeixinAdapter.test.ts`
covers typing lifecycle, not `sendMessage`, so the outbound tests directly
exercise the source orchestration rules.
