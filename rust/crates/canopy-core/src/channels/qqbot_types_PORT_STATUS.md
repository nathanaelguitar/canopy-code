# QQBot protocol types port status

Ported `packages/channels/qqbot/src/types.ts` to `qqbot_types.rs`.

## Implemented

- All gateway opcode values and C2C/group intent bit masks.
- Message author/event types, including optional legacy author fields, group-only fields, optional mention arrays, and the `all`/`single` mention-scope union.
- Optional QQ channel config fields with source JSON names and strict Rust enums for group policy and chat route unions.
- Config fallback constants/helpers for the documented policy, mention, buffer, reconnect, flush, and gateway retry defaults. Raw optional config fields remain absent during serialization.
- Group robot add/remove and active-message toggle event types.

## Verification

- `rustfmt --edition 2024 rust/crates/canopy-core/src/channels/qqbot_types.rs` completed.
- An isolated offline Cargo harness including the new module passed **7/7** unit tests.
- Tests cover opcode/intent values, optional author fields, group mention scopes, config JSON field names and unions, default values and invalid buffer lengths, and group event timestamps.

## Remaining integration

`channels/mod.rs` needs to export `qqbot_types`. No shared module, manifest, or ledger files were changed for this slice.
