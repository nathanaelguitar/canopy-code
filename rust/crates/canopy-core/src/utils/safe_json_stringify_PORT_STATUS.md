# `safeJsonStringify` Rust port status

`safe_json_stringify.rs` serializes `serde_json::Value` and preserves the
source's compact/default and pretty-print forms, including numeric space
clamping, text indentation, and the distinction between absent input and JSON
null.

Rust's owned `Value` tree cannot represent object identity or cycles, so
`[Circular]` replacement and `toJSON` callbacks cannot occur. JavaScript
property insertion order, undefined object fields, functions, symbols,
`BigInt`, non-finite numbers, and custom `toJSON` values are outside the
typed JSON boundary. A supplementary scalar at the tenth UTF-16 indentation
unit is kept whole instead of emitting a lone surrogate.

Six focused tests cover compact values, nullish output, number/string
indentation, clamping, and repeated sibling subtrees. Native verification is
included in the exported `utils::` test filter.
