# `zoom_image` port status

`image_view.rs` ports the bounded static-image crop path from
`packages/core/src/utils/image-view.ts` and the `zoom_image` tool in
`packages/core/src/tools/zoom-image.ts`. The native CLI and ACP advertise and
dispatch the tool, preserve its inline JPEG result, and apply path-scoped
permission rules. ACP also permits it in Plan mode as a read operation.

The decoder accepts static PNG, JPEG, and WebP. It rejects APNG and animated
WebP, applies image orientation before selecting normalized crop coordinates,
uses the source visual-patch budget and Lanczos3 resizing, flattens alpha onto
white, and returns JPEG at quality 92 with 4:4:4 chroma sampling. Encoding uses
[`jpeg-encoder` 0.7.1](https://docs.rs/crate/jpeg-encoder/0.7.1), dual
MIT/Apache-2.0 licensed. Input files are capped at 100 MiB,
decoded pixel buffers at 96 MiB, each source dimension at 32,768 pixels, and
the encoded response at 9 MiB. The CLI runs at most one image decode at a time.
The decoded-buffer check is explicit because the `image` crate's allocation
limit is not enforced by every supported decoder.

The path must resolve inside the workspace. Ignore checks cover both the
requested path and its canonical target, including configured custom ignore
files. On Unix, the file is opened with `O_NOFOLLOW | O_NONBLOCK` and then
validated through the open handle to reject final-component symlink swaps,
FIFOs, and other non-regular files.

Remaining differences: Rust does not yet record the TypeScript file-operation
telemetry event; an in-progress blocking decode cannot be interrupted by prompt
cancellation; ancestor-directory replacement races are not eliminated by the
final-component no-follow open; and the Rust tool rejects paths outside the
workspace, where TypeScript can request permission and read them.
