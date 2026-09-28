# GitLab mention utility Rust port

`gitlab_mention.rs` ports the pure `escapeRegex`, `testBotMention`, and
`stripBotMention` behavior from `packages/channels/gitlab/src/mention.ts`.
Matching uses the source's permitted left boundaries and ASCII continuation
class, including GitLab's distinct rule that a period after a username is part
of the continuation and prevents a match. Unicode whitespace, case-insensitive
BMP matching, escaped username metacharacters, global removal, and empty
usernames are covered by focused unit tests.

The pure mention helper is exported from `channels/mod.rs` and is used by the
native core adapter in `gitlab_adapter.rs`. The daemon/CLI still need to
construct that adapter and connect its inbound and prompt callbacks to the
channel runtime. The Rust implementation cannot represent unpaired UTF-16
surrogates that JavaScript strings can contain, and its Unicode case table
follows the Rust toolchain's Unicode data rather than the Node version's table.
Supplementary case pairs are kept exact to match JavaScript's non-`u` regex
behavior.
