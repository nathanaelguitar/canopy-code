# `read-text-range` Rust port status

`read_text_range.rs` ports the fast path, bounded streaming line-range reader,
handle-bound reader, byte-cursor window reader, UTF-8 truncation, line-ending
metadata, BOM handling, cancellation checks, and the three typed reader errors.
Borrowed file handles are read with explicit positions through `ReadAt`; the
caller supplies the handle's captured `file_size`, and neither reader closes
the handle. A path read opens one descriptor and uses its metadata size as a
snapshot for the rest of the operation.

The temporary test harness includes the existing `encoding` and
`cancellation` modules and runs the 16 focused tests embedded in this source.
Coverage includes fast-path split-line behavior and BOM/CRLF metadata, large
deep ranges, bounded scans, scan-budget errors, snapshot bounds across appended
data, unchanged descriptor position, beyond-EOF counts, cursors after skipped
chunk-spanning lines, invalid UTF-8 after the detection sample, UTF-8 byte
truncation, cursor snapping and errors, whole-line output bounds, byte cursors
across BOM/CRLF, giant-line progress, cancellation, and large non-UTF-8
refusal.

This is a synchronous Rust API. Cancellation is polled before and after each
positional read, so it cannot interrupt a single blocking OS read. Encoding
detection and legacy decoding use `chardetng`/`encoding_rs`; labels and
misclassification edge cases may differ from Node's `chardet` plus
`iconv-lite`. UTF-16/32 fast-path decoding uses lossy scalar conversion, and
malformed trailing code units do not reproduce every `iconv-lite` replacement
detail. Rust's unsigned offsets and limits cannot represent negative or
non-finite JavaScript numbers. The path variant uses one opened-file snapshot,
which is more stable than the source's separate `stat` and path-based read but
can differ under replacement races.

The module is exported from `utils/mod.rs` and is connected to the Rust
`ReadFileTool` for UTF-8-compatible text. The tool keeps its bounded decoder
fallback for BOM-marked UTF-16/32 and detected legacy encodings. Other text
attachment paths do not yet use the range reader.
