# sanitize-child-env.ts Rust port status

Added a crate-private shared helper that returns a copy of either a HashMap or
BTreeMap environment, removing exactly QWEN_SERVER_TOKEN, QWEN_DAEMON_TOKEN,
and QWEN_CODE_PRIVATE_ACP_CAPABILITY. It preserves all other keys, including
GH_TOKEN, GITHUB_TOKEN, AWS_ACCESS_KEY_ID, NPM_TOKEN, and the shell-only legacy
CANOPY_PRIVATE_ACP_CAPABILITY. Focused retained tests cover both map types,
non-mutation, exact key scope, and third-party credential preservation.

The user-command shell applies the helper to its captured environment and
continues removing CANOPY_PRIVATE_ACP_CAPABILITY at spawn as a compatibility
key. The stdio MCP config builder and actual process spawn both apply the
shared helper; this also prevents a server config env override from readding an
internal secret after the parent environment is sanitized.

The helper module is declared crate-private in `utils/mod.rs`, so shared
internal call sites use one policy without exposing it as public API. Cargo
tests have not been run because the parent task is holding consolidated
validation.
