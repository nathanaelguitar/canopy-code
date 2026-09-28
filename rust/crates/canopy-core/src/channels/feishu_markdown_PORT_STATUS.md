# Feishu markdown port

`feishu_markdown.rs` ports `packages/channels/feishu/src/markdown.ts` and the
behaviors covered by `markdown.test.ts`. It builds card JSON, applies streaming
and terminal status labels, adds optional stop buttons and headers, creates
collapsible long-content panels, splits final card content around markdown
tables while respecting code fences, extracts short first-line titles, and
chunks long messages with fence closure/reopening and hard line splitting.

The implementation follows JavaScript UTF-16 code-unit lengths for card
summaries, title truncation, collapsible split points, and 4,000-unit chunks.
Rust cannot represent a JavaScript string containing half of a surrogate pair,
so any such slice or chunk boundary rounds to a valid UTF-8 scalar boundary.
All markdown transformations and fallback labels remain the same for normal
Unicode text.

Focused coverage is in the module's 11 unit tests: default and streaming card
shapes, custom and terminal status, header and stop-button data, collapsible
content, table/code-fence separation, title cleanup and fallback, empty and
short chunks, long-line splitting, fence reserve/reopen behavior, near-limit
code preservation, and UTF-16/UTF-8 boundary handling.

Verification: the standalone offline Cargo harness passed **11/11** tests;
`rustfmt --check` and `git diff --check` passed. The standalone harness lives in
`/tmp` and does not modify the repository workspace manifest or lockfile.
