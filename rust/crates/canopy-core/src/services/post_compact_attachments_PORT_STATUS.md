# Post-compact attachment parity audit

Compared `post_compact_attachments.rs` with
`packages/core/src/services/postCompactAttachments.ts` and its focused tests.
The Rust path extraction preserves newest-first deduplication and excludes
failed calls; file restoration keeps the per-file byte precheck, decoded
character cap, binary sample threshold, aggregate embed budget, safe fences,
and reference/missing behavior. Workspace checks canonicalize file and root
paths before component-boundary comparison, and the Tokio adapter resolves
symlinks. Composition preserves the processed summary, plan/background
reminders, file and image payloads, merged attachment role, and pending final
function call in source order.

No confirmed behavior gap was found, so this audit made no code or test edits.
Existing Rust tests cover injected canonical paths including an outside-pointing
symlink, attachment budgets, nested tool images, reminder/summary content, and
pending-call preservation. The TypeScript suite additionally exercises an OS
symlink with the local filesystem; Rust's existing symlink boundary case uses
the injected filesystem surface rather than a real symlink.

Cargo checks were held for the consolidated integration run. Exact filesystem
error wording is platform-specific, and Rust strings cannot represent the
isolated UTF-16 surrogate that JavaScript `.slice()` could produce at a
subagent-description truncation boundary.
