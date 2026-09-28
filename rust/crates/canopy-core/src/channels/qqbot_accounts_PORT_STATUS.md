# QQBot accounts Rust port status

`qqbot_accounts.rs` ports `packages/channels/qqbot/src/accounts.ts`.

## Behavior covered

- Builds `<global Qwen directory>/channels/<safe name>-credentials.json`.
- Returns no credentials for missing/unreadable files, invalid JSON, missing
  fields, or values that are falsy in JavaScript (`null`, `false`, numeric zero,
  and the empty string).
- Keeps truthy parsed JSON field values intact, including non-string values,
  matching the source's runtime check without adding string validation.
- Creates the global `channels` directory recursively before saves and writes
  compact JSON with `appId` and `appSecret` keys.
- Uses the existing atomic file writer. New files receive mode `0o600`; existing
  permissions are preserved, consistent with the source's `writeFileSync`
  `mode` option. The writer follows destination symlinks and creates no parent
  directory for a custom credential path.

## Dependencies and integration

No manifest or lockfile changes are required. The module uses the existing
`paths`, `serde_json`, and `atomic_file_write` modules. Export it from
`channels/mod.rs` with:

```rust
pub mod qqbot_accounts;
```

## Verification

`rustfmt --edition 2024` completed. An isolated offline harness passed 14 tests:
7 QQBot account tests for path composition, missing/corrupt/partial data,
JavaScript truthiness, save shape and directory creation, file permissions,
and custom-path failure; plus 7 tests from the included atomic-write helper.
