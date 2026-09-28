# Follow-up speculation Rust port status

This slice ports the copy-on-write overlay filesystem and the speculative tool
safety gate from `packages/core/src/followup/`. The gate allows the source's
read-only tools, redirects writes only for auto-edit/auto/yolo approval modes,
uses an injected fail-closed shell classifier, and blocks unknown tools. The
overlay maps writes into a private per-process temporary directory, supports
read redirection and applying accepted changes, and checks symlink containment.

Focused tests cover safety classifications, path rewriting, shell boundaries,
overlay copies, cleanup, and symlink escapes. The TypeScript speculation loop,
tool execution wiring, and acceptance/rejection UI remain unported.
