# `cronDisplay.ts` Rust port status

Source: `packages/core/src/utils/cronDisplay.ts` and
`packages/core/src/utils/cronDisplay.test.ts`.

`cron_display.rs` ports the friendly labels for valid minute, hour, and day
step patterns. It keeps the source expression unchanged for malformed or
non-trivial schedules, including steps that do not divide the field range,
span a whole field, or misstate day intervals across month boundaries. Field
trimming and splitting use ECMAScript whitespace rules, matching the paired
cron parser.

The exported module has six retained unit tests derived from all source test
groups. All six passed on Darwin arm64. It is not connected to the channel
scheduler or another Rust UI caller yet.
