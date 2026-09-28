# Native TUI file prompt commands port status

The fullscreen TUI discovers user commands from `~/.canopy/commands` (or
`$QWEN_HOME/commands`) and workspace commands from `.canopy/commands`. It then
discovers local extension command definitions from the runtime-selected active
extension inventory and each extension manifest's `commands` paths, defaulting
to `commands/`. All three roots scan nested `.toml` and `.md` files; nested
paths become colon-separated slash-command names. Workspace commands override
user commands with the same name. Extension commands that conflict with native
commands, eligible skills, user/workspace commands, or earlier extension
commands receive the extension display-name prefix. `slashCommands.disabled`
is applied to the final case-insensitive command name.

Bare mode and untrusted workspaces skip all file commands. User and workspace
commands remain available in safe mode; extension commands are skipped there.
The extension loader excludes Agent Plugin v1 roots. Discovery follows
symlinks only when the file's canonical path stays inside its selected command
root and is bounded across all sources to 256 accepted commands, 4,096 walked
entries, 1 MiB per file, and 16 MiB total command file contents. Extension
discovery visits at most 256 active extensions and 64 command roots per
extension.

TOML definitions require a string `prompt` and accept an optional string
`description`. Markdown definitions accept optional YAML frontmatter and use
the trimmed body as the prompt. A matched `/name args` invocation replaces all
`{{args}}` markers with the raw arguments. Without that marker, non-empty
arguments append the original invocation after two newlines, matching the
TypeScript default argument processor. Expansion is capped at 2 MiB.

`@{path}` prompt injections now use the native `AtFileProcessor` after command
argument expansion. Reads are confined to the workspace, use the selected
model's text/image modalities and the processor's Git and configured Canopy
ignore rules, and retain failed placeholders while showing read, ignore, and
truncation diagnostics. Injected text and media parts preserve their order in
the submitted prompt. `!{...}` remains fail-closed because this TUI does not
yet implement the shell permission and confirmation lifecycle.

## Remaining semantic gaps

- `!{...}` shell execution and its permission and confirmation flow are
  unsupported.
- Command descriptions, `argument-hint`, and `when_to_use` are not shown in the
  completion menu. `disable-model-invocation` has no separate effect because
  this port only handles direct TUI slash invocations.
- User/workspace file commands expand before native TUI handlers. An exact
  match on a primary command name replaces the native command, matching the
  rule that later loaders win in `CommandService`; aliases on the replaced
  native command disappear unless a file command defines that alias. Extension
  commands remain namespaced when their names collide with native or local
  commands.
- The command inventory is loaded when the interactive loop starts and is not
  refreshed after extension files change.
- This path is specific to the fullscreen TUI; ACP and non-interactive command
  dispatch are unchanged.

No tests were added or run. Rust formatting is the verification scope for this
slice; the shared workspace Cargo check is being coordinated by the parent
agent.
