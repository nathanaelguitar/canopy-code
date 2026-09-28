# Native GitHub CLI host status

`canopy channel github [configured-name]` loads a GitHub channel from settings,
resolves token and proxy environment references, applies the GitHub-specific
`chat_thread` default, and starts the exported
`canopy_core::channels::github_adapter::GithubAdapter` with its bounded REST
client and polling loop.

The host wires the shared sender/group/DM gates and workspace pairing store,
the persisted session router, observed-contact recording after successful
preflight, and an ACP child process for prompts. It serializes prompts per
session and invokes the adapter's working-reaction hooks. A completed prompt
returns its session ID and final text to the adapter, which suppresses
`<no-reply/>`, posts the final issue/PR comment, writes publication audit
records, and coordinates durable delivery state with the inbound task. Pairing
notices continue to use the thread-aware issue/PR comment method. The CLI help
and `channel github` dispatch are registered in `main.rs`.

## Remaining parity gaps

- The standalone host does not register the full `ChannelBase` command,
  memory, loop, webhook, or shared-session authorization surfaces. ACP
  permission requests are answered with the reject/cancel option when present;
  there is no chat-based approval relay. Session cancellation is wired for
  shutdown but not to per-message task lifecycle events.
- This command is a foreground native host. It is not registered in the
  TypeScript daemon worker, channel manager, or daemon configuration lifecycle.
- The host uses its own ACP child/session bridge because the CLI's current
  Telegram, QQ, DingTalk, and Weixin bridges are private to their modules.
  ACP output lines are bounded, but session creation and prompt calls do not
  have an additional host-specific deadline.

Verification completed with:

- `rustfmt --edition 2024 rust/crates/canopy-core/src/channels/github_adapter.rs rust/crates/canopy-cli/src/github_host.rs`
- `cargo check --manifest-path rust/Cargo.toml -p canopy-core -p canopy-cli --locked --offline`
- `git diff --check` for the GitHub adapter, host, and GitHub status notes

The check passed. It emitted three unrelated warnings in `mcp_host.rs` for an
unused `json` import, unused `with_factory`/`budget` methods, and an unread
`session_id` field. No tests were added or run.
