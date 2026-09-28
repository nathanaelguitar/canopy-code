# Memory port status

## Managed auto-memory files

This slice covers `packages/core/src/memory/{paths,store,entries,scan,indexer,prompt,forget,relevanceSelector,status,refresh,secret-scanner,team-memory-git-status,team-memory-secret-guard,pending-skills,team-memory-sync,channel-memory-document,channel-memory,remember,extract,extractionAgentPlanner,dream,dreamAgentPlanner,skillReviewAgentPlanner,learn-skill-agent,const,manager,scopes,types,memoryAge}.ts`.
`AutoMemoryPaths` is immutable and can be created from explicit
`MemoryPathInputs` for tests or from the process environment and Canopy
`Storage` at runtime. It provides local `.canopy/memory`, runtime-rooted
git-root or workspace project memory, cross-project user memory, and tracked
team memory. The API preserves trusted write anchors, excludes team memory
from the private auto-memory permission predicate, and canonicalizes nearest
existing paths for managed-memory retention checks.

The store uses asynchronous Tokio filesystem operations. Scaffolding is
idempotent across concurrent processes through exclusive create-if-missing
writes; only `AlreadyExists` is ignored. Missing index reads map to `None`,
other filesystem failures propagate, and invalid UTF-8 is replaced as Node's
UTF-8 file reads do. Project scaffolds persist schema-version-1 metadata, an
empty index, and an extraction cursor using two-space JSON plus a trailing
newline. User scaffolds create only the root and empty index. Entry parsing
preserves the legacy bullet and newer per-entry formats.

Topic scanning covers CRLF frontmatter, the 200-file cap, unreadable-file
isolation, shared-tier deterministic ordering, and private-tier newest-first
ordering. Private filename tie-breaking uses a stable common-English
approximation of Node's ICU `localeCompare`; team order follows JavaScript
UTF-16 relative-path ordering.
Index generation sanitizes untrusted metadata, percent-encodes paths without
breaking links, groups duplicate team descriptions, and enforces source size
caps. Private writes use the shared crash-safe atomic writer; team writes reject
redirected roots and replace a symlink at the `MEMORY.md` leaf rather than
following it. `AutoMemoryIndexRead.stats` exposes `size` and `mtime_ms` rather
than Node's full `Stats` object.

The Rust prompt builder includes full and condensed instructions, user/project/
team routing, and JavaScript UTF-16-aware truncation. Representative prompt
outputs were compared byte-for-byte with TypeScript.

`write_context_file.rs` ports serialized workspace/global context writes. It
requires callers to pass the active context filename and global directory,
holds a per-path mutex for the full read-compose-write sequence, bounds lock
acquisition to 30 seconds and existing-file reads to 16 MiB, preserves
append/replace and no-op behavior, and recognizes section boundaries outside
backtick and tilde fences. It uses UTF-16 offsets for JavaScript-compatible
section insertion and checks the caller's commit guard before mutation.

`forget.rs` ports model and substring candidate selection, candidate validation,
per-file entry removal, metadata/index refresh, and `/forget` result reporting.
Its model client and main model are injected; the model request has an eight
second deadline and falls back to the deterministic selector. Per-file write
errors are best-effort, matching the source. `relevance_selector.rs` ports the
30-second model selector request, manifest, response validation, and returned
ordering. The resolver applies deterministic fallback, and both native CLI and
ACP callers now supply the OpenAI-compatible selector adapter.

`status.rs` aggregates the index, cursor, metadata, topic files, and task
records. `MemoryManager` implements the task-listing trait. The native
interactive `/memory` view displays the current auto-memory, auto-dream,
auto-skill, and confirmation toggle values; memory scope and project/user paths;
relative last-dream time; per-topic document counts; and recent
extraction/dream task records. When managed-memory availability is off (the
setting is disabled or the session is in bare mode), it shows legacy
user/workspace context-file locations, matching the TypeScript dialog;
otherwise it shows managed-memory folders, including when safe mode gates
automatic runtime work. It works in the full-screen TUI and line-oriented
interactive CLI. Its four toggle actions persist the
workspace-scoped settings through the comment-preserving JSONC updater and
atomic writer. Auto-skill changes also update the running session's review
gate; auto-memory and auto-dream runtime gates remain those captured when the
session started. Bare and safe mode initially display all four toggles off and
continue to gate their runtime behavior. Target selection creates and opens
managed user/project folders when managed memory is available, or the preferred
legacy context files otherwise. Managed folders open in the platform file
manager when a desktop is available; headless Linux creates and opens the
managed `MEMORY.md` index with the configured preferred editor. Task rows still
show only the manager's current-session records; task history is not loaded
from disk.

`refresh.rs` classifies successful write/edit candidates against private
project and user memory, excludes tracked team memory, rebuilds only affected
indexes, and invokes injected instruction-refresh callbacks. Tool-name
canonicalization, configured context filenames, availability, and live Config
callbacks remain runtime inputs.

`secret_scanner.rs` applies the source's ordered credential patterns and emits
only rule IDs and labels. `team_memory_git_status.rs` checks both the team
index and a representative topic path with `git check-ignore`, using a bounded
five-second probe and treating probe failures as not ignored. Team-memory
shareability warning text is preserved; source debug-only messages have no
Rust logging counterpart yet.

`pending_skills.rs` stages newly created direct-child skill directories under
task-specific pending roots, preserves description parsing and read-skip
behavior, and implements accept/reject handling. The native manager stages
reviewer changes when confirmation is enabled and exposes accept/reject APIs;
the CLI does not yet expose pending-skill resolution.

`team_memory_sync.rs` preserves reconcile-before-commit behavior: it records
preexisting local-ahead state, performs `pull --ff-only`, stages and commits
only `.canopy/team-memory`, unstages that path on commit failure, and pushes an
explicit branch refspec only when this sync created the commit. Its injected
runner supports safety tests; the process adapter adds noninteractive Git/SSH
settings and 15-second child limits. The sync is not yet wired to session
startup.

## Channel-memory documents

`channel_memory_document.rs` ports strict JSON parsing and validation, legacy
Markdown migration and SHA-256 marker, stable entry IDs, normalization,
serialization, and recall rendering from
`packages/core/src/memory/channel-memory-document.ts`. It rejects duplicate
JSON object keys, unexpected properties, invalid IDs, duplicate entry IDs,
oversized documents, and invalid migration markers. Rust `String` cannot
represent an escaped lone UTF-16 surrogate, so those values are rejected even
though JavaScript `JSON.parse` can construct them.

`channel_memory.rs` adds the filesystem-backed channel store: safe channel and
thread paths, deterministic reads and legacy migration, stable revisions,
duplicate and secret checks, compare-and-swap updates, bounded locking, and
atomic writes. Its seven focused tests cover migration, mutations, conflicts,
revisions, and concurrent writers. Failure injection from the TypeScript I/O
tests is not yet represented, and invalid-UTF-8 error text differs from Node.

`remember.rs` ports the remember prompts, context policies, path classification,
managed-memory setup, per-scope index rebuild, and result handling through an
injected `RememberAgentRuntime`. The host must still enforce its declared
tool/path restrictions and provide the actual agent execution loop; no Canopy
runtime caller is wired yet. User-memory index read/rebuild remains
best-effort, matching the source behavior.

`extraction.rs` and `extraction_agent_planner.rs` port cursor-based extraction,
history repair, prompt/request construction, touched-topic attribution,
metadata, index rebuild, and refresh sequencing through an injected runtime.
The native interactive CLI schedules extraction and dream after successful
user turns when managed auto-memory is enabled and the CLI is neither in safe
nor bare mode. Manager gates skip extraction when the turn already wrote
managed memory or memory pressure is active. Auto-dream also has its own setting
and remains gated by managed auto-memory. Tasks use temporary fork sessions
with the active provider/model, enforce configured turn/time limits, and
per-session scheduling is drained on a clean CLI exit. Native ACP does not
automatically schedule memory extraction, dreams, or skill reviews after
prompts.

`dream.rs` and `dream_agent_planner.rs` port trigger gates, cancellation,
metadata, index refresh, protected pinned-memory policy, transcript prompt
construction, and runtime limits. Native CLI wires this runtime with
project-memory-only file tools and a foreground shell tool for transcript
search. Shell commands must pass the Bash AST read-only classifier, use a
workspace-local working directory, respect configured tool and permission
rules, and cannot run in the background. The shared shell tool caps captured
stdout and stderr at 32 KiB each, limits command input to 64 KiB, defaults to a
120-second command timeout, and is cancelled when the dream is cancelled or
times out. The planner's narrow `grep` plus `tail -50` prompt now works against
session JSONL files; transcript files remain read-only and outside the
memory-write scope. ACP's workspace extension exposes an explicit manual dream
request with metadata recording and chat-recording suppression. It uses
managed-memory availability and scoped permissions, returns summary, touched
topics, and deduped-entry count, honors JSON-RPC request cancellation, and has
a 295-second timeout. ACP does not automatically schedule dreams after
prompts.

`skill_review_agent_planner.rs`
ports skill-review prompting, history closure, scoped permission decisions,
archive reservation, skill discovery, and injected agent execution. Native CLI
schedules review when auto-skill is enabled, after the 20-tool-call threshold,
unless skills were modified during that session. Confirmation mode stages newly
created skill directories as pending; neither host yet surfaces their
accept/reject flow. `const.ts` behavior is in `context_filenames.rs`,
including thread-safe process-wide filename selection. `learn_skill_agent.rs`
ports video parsing, local/remote/YouTube classification, skill prompts,
collision listing, and structured video request parts.

`manager.rs` ports task records and subscriptions, task draining, queued
extraction, memory-pressure and memory-write gates, dream scheduling and
cancellation, skill-review staging, pending-skill resolution, and status,
forget, and prompt forwarding. `scopes.rs`, `types.rs`, and `memory_age.rs`
provide the source-level module contracts while reusing the canonical path,
store, and recall implementations. The native interactive CLI constructs the
manager with extraction, dream, and skill-review runtimes, and drains its
tracked task handles on exit. ACP does not create a workspace memory manager
for prompt-triggered work; its explicit manual dream request calls the shared
dream runtime directly.

`memory_scoped_agent_config.rs` ports managed-memory path checks, pinned-memory
protection, tool decisions, and merging scoped rules with the base permission
manager. `shell_read_only.rs` ports the source Bash AST safety classifier and
`stripShellWrapper` token handling using native tree-sitter-bash. It preserves
the source's command allowlist and specialized Git, find, sed, awk, process,
output-option, expansion, redirection, and local Git-config checks. Syntax and
parser errors fail closed; scoped shell access allows only a `ReadOnly` result.
The native host enables foreground `run_shell_command` through the shared shell
tool and the same AST checker, using the effective runtime environment. Shell
working directories must resolve inside the workspace, background execution is
rejected, and base deny/ask rules remain enforced. File, search, and directory
tools are constrained to the manager's project and user memory roots, pinned
files remain protected, and active base permissions are rechecked. Background
permission asks are denied because there is no interactive approval path.
Native CLI extraction, dream, and skill-review forks use the current session's
provider/model and do not route through `fastModel`. Dream adds foreground
read-only shell access alongside its project-memory-scoped file tools; skill
review can inspect workspace files and writes under its scoped skill policy.
The source manager has no skill-review cancellation API or cancellation
signal, so an in-flight native CLI review can continue after its session closes.
ACP does not automatically schedule skill reviews. The native CLI lacks a
managed-memory task status command and pending-skill accept/reject affordance.

## Remaining memory work

The native CLI and ACP callers now inject the model selector through the
resolver. Their OpenAI-compatible host adapter selects a resolvable `fastModel`
on the active OpenAI endpoint, then falls back to the session model. The native
hosts do not yet route side queries to another auth type or to model-specific
endpoints, credentials, or custom headers. Selector errors and malformed
responses preserve the resolver's deterministic fallback. ACP prompt
cancellation is forwarded to that side query.

Native CLI and ACP callers now derive up to 16 distinct, most-recent function
names from prepared API history and pass them to model and heuristic recall.
New sessions pass an empty list. The native host still does not record recall
telemetry. Skill-nudge integration remains separate work. Native interactive
CLI exposes an editable `/memory` status view; ACP has no equivalent status
request. Neither host exposes the pending-skill accept/reject affordance.
Deterministic recall ranking, active-tool suppression, prompt formatting, and
memory-age/freshness rendering are implemented in `recall.rs`.
