# Folder structure port status

`folder_structure.rs` ports `packages/core/src/utils/getFolderStructure.ts` as
a recursive breadth-first scan with the 20-item combined default, file-first
directory processing, default and custom ignored-folder names, optional
`FileDiscoveryService` filtering, filename regex filtering, and tree/truncation
formatting. Focused tests cover tree order, ignored directories, filtering,
custom case-sensitive ignores, unread queued folders, and missing-root output.

The module is exported from `utils/mod.rs`, but no native caller uses it yet.
Tests and builds have not been run during this slice, per the consolidated
Cargo window.
Rust's standard library has no equivalent to JavaScript's locale-aware
`String.localeCompare`, so entry ordering is deterministic Unicode scalar
ordering and can differ for mixed-case or accented names. JavaScript's
stateful `RegExp` flags (`g` and `y`) also have no direct counterpart in the
borrowed Rust `Regex` option. Missing-root framing matches; OS-specific error
details for other filesystem failures use Rust's `io::Error` text and may not
match Node's `code: message, syscall, path` wording exactly.
