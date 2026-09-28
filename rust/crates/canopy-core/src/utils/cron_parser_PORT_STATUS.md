# `cronParser.ts` Rust port status

Source: `packages/core/src/utils/cronParser.ts` and
`packages/core/src/utils/cronParser.test.ts`.

The Rust module in `cron_parser.rs` ports the five-field parser, wildcard/range/
list/step syntax, `N/step` expansion, Sunday `7` normalization, Vixie DOM/DOW
matching, `matches`, and the bounded next-fire search. Parsed values use
`IndexSet<u8>` so iteration preserves JavaScript `Set` insertion order and
duplicates retain only their first occurrence. Parse errors keep the source
parser's message wording and day-of-week-first validation order. Expression
and field trimming plus field splitting use the ECMAScript whitespace code
points explicitly: FEFF is treated as whitespace and NEL (U+0085) is not.

The exported implementation has 13 in-file unit tests derived from the
TypeScript suite, including FEFF/NEL whitespace edge cases. No Cargo
dependency changes were needed (`chrono` and `indexmap` are already direct
dependencies). `cargo test -p canopy-core utils::cron_parser --locked --offline`
passed all 13 tests. Scheduler wiring remains open.

The generic date API evaluates wall-clock fields in the supplied Chrono time
zone. `next_fire_time` advances by elapsed one-minute instants; around a
fall-back transition this can visit both occurrences of a repeated local
minute, unlike the source's local `Date.setMinutes` stepping. Spring-forward
nonexistent minutes are skipped. Vixie cron daemon-specific DST catch-up
behavior is outside the source parser's scope and is not implemented here.
