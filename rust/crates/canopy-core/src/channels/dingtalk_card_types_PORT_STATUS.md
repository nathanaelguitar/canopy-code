# DingTalk interactive card types Rust port status

Source: `packages/channels/dingtalk/src/interactive-card-types.ts` and
`packages/channels/dingtalk/src/interactive-card-types.test.ts`.

## Implemented

- Typed Rust models for the interactive-card config, status/question card
  options, callback, callback target, and accepted/forbidden/ignored results.
- `parse_dingtalk_interactive_card_config` preserves omitted-versus-null
  behavior, defaults, strict boolean/object checks, finite-positive timeout
  validation, exact error messages, and the Node timer maximum clamp.
- `parse_dingtalk_card_callback` accepts objects or JSON-encoded records,
  follows the TypeScript source search precedence for embedded `value`,
  `content`, `cardPrivateData`, and top-level fields, and normalizes action,
  track, actor, form, and cancel fields. Actor identity comes only from the
  top-level callback.
- `parse_dingtalk_card_actor_id` exposes the same trusted top-level identity
  lookup used by the callback parser.
- Config and callback structs serialize with TypeScript camelCase keys;
  omitted optional callback booleans are omitted from serialized output.

The parser operates on `serde_json::Value`. JSON itself cannot represent
`undefined`, so callers use `None` for a missing config value and
`Some(Value::Null)` for explicit `null`. Rust's callback result models deferred
execution as a boxed `FnOnce` returning a `Send` future.

## Verification and remaining integration

The file contains 18 focused tests for config defaults and validation, timeout
clamping, output serialization, embedded callback shapes, source precedence,
cancel truth values, malformed input, and top-level identity trust. All 18 pass
in an isolated offline Cargo harness importing this module by path with
`serde` derive and `serde_json` preserve-order enabled.

The module is not yet registered in `channels/mod.rs` or called from a DingTalk
Rust adapter. The parent port task owns shared module registration and status
ledgers.
