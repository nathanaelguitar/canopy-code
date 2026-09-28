# DingTalk Markdown Rust port status

Source: `packages/channels/dingtalk/src/markdown.ts` and
`packages/channels/dingtalk/src/markdown.test.ts`.

## Implemented

- `split_chunks` mirrors the 3,800-unit chunking algorithm, preserves line
  breaks, avoids splitting triple-backtick delimiters, and closes/reopens code
  fences when a fenced block spans chunks.
- `extract_title` reads only the first line, removes the source's heading,
  bold, whitespace, hyphen, and quote prefixes, and limits the result to 20
  UTF-16 units with `Reply` as the empty fallback.
- `normalize_dingtalk_markdown` uses the same chunking behavior and leaves
  ordinary Markdown, including tables, unchanged.
- JavaScript string limits are counted in UTF-16 code units. Rust split points
  stay on UTF-8 scalar boundaries, so a limit that falls inside a supplementary
  Unicode scalar rounds backward rather than producing an invalid lone
  surrogate.

## Verification and remaining integration

The module contains 18 unit tests corresponding to the TypeScript cases plus
UTF-16 and exact-limit coverage. It can be checked without Cargo using:

```sh
rustc --edition=2024 --test rust/crates/canopy-core/src/channels/dingtalk_markdown.rs -o /tmp/dingtalk_markdown_tests
/tmp/dingtalk_markdown_tests
```

The module is not registered in `channels/mod.rs`, and no DingTalk Rust adapter
calls it yet. The shared module registry and status ledgers are maintained by
the parent port task.
