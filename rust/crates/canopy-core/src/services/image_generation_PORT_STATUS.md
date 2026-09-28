# Image generation service port status

Source: `packages/core/src/services/image-generation-service.ts`.

`image_generation.rs` provides a provider-neutral Rust service for the existing
image generation request contract. It normalizes and validates the configured
HTTPS base URL, posts the same model/prompt/size payload to the multimodal
generation endpoint, maps provider errors, extracts the first returned image
URL and request ID, and returns the downloaded PNG bytes.

The implementation caps generation responses at 1 MiB and downloaded PNGs at
10 MiB, verifies the PNG signature, uses 240-second generation and 120-second
download deadlines, and observes the shared cancellation token. Image URLs are
checked with the existing `ExtensionNetworkPolicy::Public` resolver and DNS
pin before each request. Redirects are manual, limited to three, and validated
and pinned again at every hop. The configured API base URL receives the same
HTTPS/credential/query/fragment checks as the TypeScript service; its host is
not subjected to result-image public DNS policy, matching the source behavior.

`generate_image` uses the system DNS resolver. `generate_image_with_resolver`
allows a caller to supply the resolver used by the shared network policy.
`services::image_generation` is exported from the crate.

Remaining integration and parity limits:

- Settings lookup and tool/UI call sites are not wired in this slice.
- The TypeScript `fetchFn` injection seam has no Rust HTTP-transport equivalent;
  only DNS resolution is injectable.
- The shared Rust public-address policy can reject additional reserved address
  ranges compared with the TypeScript hostname/IP prefilter.
- Rust maps response stream read failures to a stable service error instead of
  preserving the runtime-specific underlying fetch exception text.
- No live provider request or platform DNS behavior was validated here.

`cargo fmt --manifest-path rust/Cargo.toml --package canopy-core` and
`cargo check --manifest-path rust/Cargo.toml -p canopy-core --locked --offline`
passed. No tests were added or run, and no Cargo dependency or lockfile changes
were needed.
