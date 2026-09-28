# GitHub mention utility Rust port

github_mention.rs ports the pure escapeRegex, testBotMention, and
stripBotMention behavior from packages/channels/github/src/mention.ts.
Matching uses the source's permitted left boundaries and ASCII continuation
class, including its deliberate allowance of a period after a username.
Unicode whitespace, case-insensitive BMP matching, escaped username
metacharacters, global removal, and empty usernames have focused helper
coverage.

The exported GitHub polling adapter now calls the mention matcher and stripper
for notification comment and first-contact body projections. The separate
github_adapter_PORT_STATUS.md records the remaining publication and host
integration gaps. Rust strings cannot represent unpaired UTF-16 surrogates,
and Unicode case tables may differ from the Node version for newer characters;
supplementary case pairs remain exact to match the source's non-Unicode regex.
