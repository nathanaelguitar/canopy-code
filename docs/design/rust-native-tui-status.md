# Rust Native TUI Status

The native `canopy run` interactive path now has a bounded Ratatui chat screen
using Ratatui and Crossterm, both MIT licensed. It starts only when stdin,
stdout, and stderr are terminals and `TERM` is not `dumb`; the existing
line-oriented prompt remains the fallback. Prompted and non-TTY runs keep their
existing output behavior. In the full-screen UI, `/help` opens a native command
and keyboard guide, while bare `/stats` and `/usage` open the live usage view.

## Shipped interactions

- A full-screen conversation, message editor, session header, and status line.
- A full-screen help view covering supported interactive commands, editor and
  conversation controls, and the live usage view. Press Esc or q to return.
- `/doctor memory` reports native RSS, host and effective memory limits,
  optional process-tree and file-descriptor data, native pressure tier, and
  three-sample RSS movement with `--sample`. `--json` prints the bounded report
  as JSON.
- A live four-tab usage view for session, models, tools, and skills.
- Streaming assistant text, plus retry, model fallback, context-compaction,
  tool-start, tool-finish, and cancellation status updates.
- Conversation messages render common Markdown headings, emphasis, lists,
  block quotes, links, horizontal rules, inline code, and fenced code blocks.
  Pipe-delimited Markdown tables render as bounded cell rows, with separator
  rows displayed as rules. Table output is capped at 16 columns. Inline
  rendering is capped at 128 styled spans per source line, and the rendered
  conversation is capped at 16,384 rows while retaining the newest content.
  Unsupported constructs remain visible as text.
- Multiple turns, `/exit`, `/quit`, Ctrl-D, and Ctrl-C while editing.
- Bounded multiline editing with Ctrl+Enter to insert a newline and Enter to
  submit. Left/right, line-local Home/End, Up/Down between lines, Backspace,
  Delete, and bracketed multiline paste are supported. Input is capped at
  64 KiB and the editor shows up to five lines at a time.
- Restored user and model text is read from Gemini-shaped `parts` and text
  `content`, capped at 256 KiB; the live display is capped at 512 KiB.
- Up/Down recall a bounded history of up to 100 prompts and 64 KiB total;
  moving Down past the newest entry restores the draft that was present before
  history navigation. Individual editor input is capped at 64 KiB.
- Typing `/` at the start of the prompt, with or without leading indentation,
  shows fuzzy, case-insensitive suggestions for native commands, eligible
  user-invocable skills, and file prompt commands from `~/.canopy/commands`
  (or `$QWEN_HOME/commands`), the workspace `.canopy/commands`, and active local
  extensions. Workspace commands override user commands; extensions receive a
  display-name prefix on conflicts. User/workspace commands remain available in
  safe mode; bare mode and untrusted workspaces skip all file commands, while
  safe mode also skips extension commands. Discovery is bounded to 4,096
  walked entries, 256 total commands, 256 extensions, 64 roots per extension,
  16 MiB of command files, and 1 MiB per file. TOML and Markdown prompt
  definitions run with `{{args}}` substitution or the default raw-invocation
  suffix. `/stats` and `/usage` also complete subcommands and export options.
  Eligible skills can be stacked up to the runtime limit. Tab fills the
  selected command and adds a separating space; Up/Down cycle suggestions when
  multiple commands match; Enter fills an incomplete match and submits an exact
  command.
- Optional voice dictation follows the configured hold or tap mode. Esc or
  Ctrl-C cancels an active recording.
- PageUp/PageDown move through the visible conversation a page at a time. The
  view follows the newest content until the user scrolls upward, and returns to
  follow mode at the bottom. Mouse-wheel scrolling moves through the same view
  by three rows per wheel event.
- Existing permission and question prompts still work: the UI leaves the
  alternate screen around tool execution so line-based approvals can read
  stdin, then redraws the conversation afterward.
- Raw input mode and the alternate screen are restored on ordinary returns,
  errors, and unwinding. Ctrl-C during generation sets a cooperative
  interruption checked at the next model event; if execution does not return
  within two seconds, a cleanup watchdog restores the terminal before exiting.

## Remaining UI parity gaps

This is a chat shell, not a port of the complete Ink interface. Local extension
`.toml` and `.md` prompt commands are executable: invoking one expands its
static prompt and submits it as a model turn. Execution follows active
extension inventory, workspace trust, safe and bare mode, disabled command
names, and extension conflict naming. `{{args}}` uses the trimmed invocation
arguments; without that marker, the trimmed original invocation is appended.
Workspace-confined `@{path}` injection uses the selected model's media support,
Git and configured Canopy ignore rules, and bounded diagnostics. One processor
ordering difference remains: TypeScript injects `@{path}` content before
replacing `{{args}}`, while the native path replaces arguments first. Therefore
`{{args}}` markers inside text read from an injected file remain literal in the
native TUI. `!{...}` shell injection remains fail-closed because the TUI does
not implement its permission and confirmation flow. User/workspace file
commands with an exact primary-name match take precedence over native handlers,
following the rule that later loaders win in `CommandService`; aliases on the
replaced native command disappear unless a file command defines that alias.
Extension commands remain namespaced on native and local-name conflicts. MCP
command providers and mid-input command completion are not implemented.
Table rendering is a simple bounded pipe-cell view; nested block constructs
and interactive links are not implemented. Older rows beyond the
16,384-row display cap are hidden from the UI but remain in the session
transcript. The UI also lacks model/session/settings pickers, tool cards,
approval dialogs inside the TUI, screen-reader layouts, themes, or the full
keyboard shortcut system. Ctrl-C during a live tool can require the cleanup
watchdog and may terminate before normal session shutdown; session recovery
remains responsible for any interrupted work.

## Verification

`rustfmt` and a scoped trailing-whitespace check passed for the touched Rust
files. `cargo check -p canopy-cli --locked --offline` passed; the compiler
reported six warnings in existing CLI modules. Tests were not run.
