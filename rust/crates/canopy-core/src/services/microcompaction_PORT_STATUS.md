# Microcompaction Port Status

Source: `packages/core/src/services/microcompaction/microcompact.ts` and `microcompact.test.ts`.

Static parity review found the recent-result budgets and ordering, idle/force/size trigger precedence, size threshold and low-watermark behavior, pending-result treatment, and output/token estimates aligned with the TypeScript source. Rust measures JavaScript string limits with UTF-16 code units, including output character totals and token estimates.

The audit fixed three issues:

- Keep-recent environment parsing now trims ECMAScript whitespace, including BOM (`U+FEFF`), without trimming additional Unicode whitespace that JavaScript `String.trim()` does not.
- Size planning now reads committed and pending history through a borrowed `HistoryView`; idle/force path lookup uses the same view. This removes full deep copies made before determining whether a clear is needed.
- Clear operations now clone only fields that survive. Clearing a tool result skips the large old `response` and nested-media `parts` fields that are replaced or removed; nested-media removal skips cloning the dropped `parts` array. Updating a touched history content also avoids cloning its old `parts` before replacement.
- Changed history is materialized only after a real clear saves tokens, so defensive no-op refs do not clone the complete history before the idle path returns unchanged.

Added focused regressions for ECMAScript trimming and clearing large replaced fields while preserving metadata and leaving the input untouched. `rustfmt --edition 2024 rust/crates/canopy-core/src/services/microcompaction.rs` passed. Cargo tests remain held for the parent agent's consolidated run.

Changed results still require an owned `Vec<Value>` under the current Rust API, so unchanged history entries and retained parts are cloned when an actual clear produces a changed history. Rust strings also cannot represent lone JavaScript UTF-16 surrogates; valid Unicode strings have matching UTF-16 length behavior.
