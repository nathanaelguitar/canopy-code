# QQ Bot group-message projection Rust port status

`qqbot_message_projection.rs` ports the pure text and sender projection in
`QQChannel.prepareGroupMessage` from `packages/channels/qqbot/src/QQChannel.ts`.
It reuses `QQGroupMessageEvent`, `QQChannelConfig`, and the shared sender-name,
prompt-text, and Unicode code-point sanitizers.

## Behavior covered

- Uses the author username or the neutral `QQ User` fallback, and keeps sender
  identity out of the display-name position.
- Shows a full sender OPENID only when mentions are enabled and the chosen
  member/user OPENID is exactly 32 ASCII hexadecimal characters. Other
  nonempty identities use an eight-code-point fragment and ellipsis. Legacy
  `author.id` values can disambiguate identity but do not trigger OPENID
  format warnings.
- Strips all `<@...>` tags from prompt-clean text, removes only the tags matched
  to `mentions[].is_you` from display text, and strips user-forged
  `[atMention=...]`, `[botOpenId:...]`, and `[bot]` prompt tags.
- Applies the source mention-tag limit in JavaScript UTF-16 units, including
  astral characters.
- Preserves `allowMention` behavior, including raw member mentions in prompt
  text when enabled, removal of all mentions when disabled, and suppression of
  bot OPENID suffixes when disabled.
- Detects slash commands after mention and trusted-tag stripping, and honors
  an explicit `force_at_mention` override.
- Extracts and validates the first self-mention bot OPENID before the empty
  cleaned-text guard. A valid new value and malformed bot/sender OPENID
  warnings are returned as caller-owned intents.

## Dependencies and integration

No manifest changes are required. The only new module dependency is `regex`,
already used by the shared sanitizer and QQ Bot code. The module is exported
from `channels/mod.rs`.

Pass the existing per-group cache entry as `cached_bot_open_id`; `Some("")`
means that the key exists, matching the source's `Map.has` check. Apply
`RememberBotOpenId` and warning intents in the channel caller. Sender-warning
deduplication and logging remain caller-owned.

## Verification

Nine in-file tests based on
`packages/channels/qqbot/src/events.test.ts` and cover mention display/prompt
differences, OPENID privacy and suffixes, slash detection, force-at-mention,
empty-message side-effect ordering, malformed-ID warnings, and the UTF-16
mention length boundary. An isolated offline harness passed 24 tests including
shared types and sanitizers. The integrated channel suite passes 449 tests and
the full offline workspace suite passes 1,412 tests.
