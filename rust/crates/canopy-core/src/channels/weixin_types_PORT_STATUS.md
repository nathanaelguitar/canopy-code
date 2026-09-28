# Weixin wire types Rust port status

Source: `packages/channels/weixin/src/types.ts`. That source has no dedicated
types test file; the tests in this module pin the wire contracts directly.

## Implemented

- Rust structs for the base info, CDN media, text/image/voice/file/video items,
  recursive reference messages, Weixin messages, and all API request/response
  shapes.
- All source properties retain their snake_case JSON field names. Every
  optional property is omitted when absent, including nested media and
  referenced message items.
- `MessageType`, `MessageItemType`, `MessageState`, and `TypingStatus` expose
  the source's numeric constants as associated constants.
- Numeric protocol fields use `i64`; the source types are JavaScript `number`,
  but these protocol fields and constants are integer-valued.

Four focused tests cover all constant values, a nested media/reference message
round trip, omission of unset fields across every struct, and sparse request
serialization.

## Verification and remaining adapter gaps

The isolated offline Cargo harness passed **4/4** tests. `rustfmt --check` and
`git diff --check` passed. The harness is under `/tmp` and does not change the
workspace manifests or lockfile.

The module is exported from `canopy-core::channels`. `weixin_api.rs` currently
constructs and returns raw `serde_json::Value` to preserve the source's runtime
type-assertion behavior, so it does not consume these typed models directly.
The Rust channel adapter, message monitor, and send path remain separate
integration work.
