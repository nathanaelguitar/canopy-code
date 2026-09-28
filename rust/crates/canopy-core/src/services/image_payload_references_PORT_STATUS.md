# Image payload reference parity status

Compared `image_payload_references.rs` with
`packages/core/src/services/image-payload-references.ts` and its focused tests.

The port preserves the 12-hex-character SHA-256 prefix identity, overwrite-on-ID
collision behavior, UTF-16-based base64 size approximation, case-insensitive
reference matching, session-store separation at the runtime caller, and
backwards deduplication followed by restoration in encounter order. Repeated
IDs are treated as the same reference in both implementations; the 48-bit
truncated digest can collide, so it is an identity key rather than a collision
proof.

Reduced avoidable payload copies in the Rust transformation path: source part
arrays are no longer deep-cloned before replacing their image parts; returned
payloads are moved into the collected/replaced lists; and recent-image
selection and reattachment borrow those payloads. The in-memory store still
retains its own copy, `ImagePayloadStore::get` still returns an owned value, and
outgoing JSON needs an owned base64 string. Preserved content and untouched
parts are cloned into the owned request result. A focused unit test checks
that recent selection borrows the latest entry per ID while preserving order.

Both implementations cap each image store at 8 MiB and 64 entries. Rust also
keeps a four-session least-recently-used cache per runtime (about 32 MiB of
retained cache by the same estimate). Each request carries the previous
bounded cache forward and adds images encountered in current history. The
retention priority is current explicit references, newest reattachments,
newly encountered history images in reverse encounter order, then previous
cache entries in their prior retention order. Entries that exceed the byte or
item cap are skipped deterministically. A later reference cannot restore an
image that was evicted at a cap.

The Rust runtime adds an intentional trigger difference from TypeScript:
besides the configured inline-image count threshold, it externalizes images
when encoded image strings in history reach 6 MiB, and when history is within
2 MiB of the 12 MiB serialized-history cap. If appending an image would cross
12 MiB, the pre-cap pass can externalize the latest user message too, retain
its image references in the bounded cache, and add a reference summary so the
next request can restore model-visible image context. Recent cached references
continue to be restored through subsequent tool turns. TypeScript currently
triggers this path by image count only. The working store may temporarily hold
the previous bounded cache and images from validated history while preparing a
request.

A single image whose estimated cache cost exceeds 8 MiB cannot be kept in the
bounded cache. The runtime returns `HistoryTooLarge` if such an image must be
externalized to get under the history cap, rather than silently sending a
broken reference. The same applies when all current-turn images and explicit
references together cannot fit the cache. If the history remains over 12 MiB
after image removal because of text or other fields, it still cannot proceed.

In the current Rust entry points, the main run creates one `AgentRuntime` per
run and the ACP factory creates one runtime per session agent. The per-runtime
four-session LRU also bounds custom callers that reuse a runtime across session
IDs. TypeScript scopes storage to a `GeminiChat` instance, which is replaced
when `Client.startChat()` creates a new chat.

The Rust byte-pressure trigger and pre-cap path were formatted and checked with
`cargo check -p canopy-core --locked --offline`; no tests were run per
instruction. No shared exports or manifests were changed.
