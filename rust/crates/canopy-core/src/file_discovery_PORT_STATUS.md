# `canopy-ignore-parser` Rust port status

`file_discovery.rs` reads `.canopyignore` plus the default or configured custom
ignore files, applies their patterns independently in source order, reports the
source that matched a path, and exposes the configured filename list. It also
handles `.gitignore`, `.git/info/exclude`, nested Git ignore files, and filter
counts for callers.

Parity fixes cover ignored-parent precedence over negated descendants,
independent-source negations, directory descendants, POSIX backslash path
normalization, absolute paths through lexical and canonical roots, rooted
backslash rejection, and ECMAScript whitespace for custom names and blank
pattern lines. Six focused regressions were added; native workspace test
verification is pending.

The Rust implementation uses the `ignore` crate rather than npm's
`ignore` package. Edge behavior outside the covered matching and path cases
may differ. Debug logging and malformed-UTF-8 replacement details are not
ported.
