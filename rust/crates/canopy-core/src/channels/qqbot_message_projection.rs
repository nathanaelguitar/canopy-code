//! Pure QQ Bot group-message text projection.
//!
//! Port of `QQChannel.prepareGroupMessage` from
//! `packages/channels/qqbot/src/QQChannel.ts`. Cache mutation and logging are
//! represented as intents for the channel caller to apply.

use crate::channels::qqbot_types::{QQChannelConfig, QQGroupMessageEvent};
use crate::channels::sanitize::{sanitize_prompt_text, sanitize_sender_name, truncate_code_points};
use regex::Regex;
use std::ops::Range;
use std::sync::OnceLock;

/// Projected values consumed by the QQ channel's inbound-message handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QQGroupMessageProjection {
    pub is_at_bot: bool,
    pub is_slash: bool,
    pub safe_name: String,
    pub clean_text: String,
    pub text: String,
    pub display_text: String,
    pub sender_name: String,
}

/// State changes and warnings that `prepareGroupMessage` requests from its
/// caller, without owning the channel's maps, warning deduplication, or logs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QQGroupProjectionIntent {
    RememberBotOpenId { chat_id: String, open_id: String },
    WarnInvalidBotOpenId { open_id: String },
    WarnInvalidSenderOpenId { chat_id: String, open_id: String },
}

/// A message may be empty after mention stripping while still carrying
/// bot-OPENID extraction intents, matching the source's extraction-before-
/// empty-message guard ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QQGroupMessageProjectionResult {
    pub message: Option<QQGroupMessageProjection>,
    pub intents: Vec<QQGroupProjectionIntent>,
}

fn at_mention_ranges(text: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut search_from = 0;
    while search_from < text.len() {
        let Some(relative_start) = text[search_from..].find("<@") else {
            break;
        };
        let start = search_from + relative_start;
        let inner_start = start + 2;
        let Some(relative_end) = text[inner_start..].find('>') else {
            break;
        };
        let inner_end = inner_start + relative_end;
        let utf16_length = text[inner_start..inner_end].encode_utf16().count();
        if (1..=64).contains(&utf16_length) {
            let end = inner_end + 1;
            ranges.push(start..end);
            search_from = end;
        } else {
            // The source's global regex resumes at the next character after a
            // failed candidate, so a valid nested `<@...>` can still match.
            search_from = start + 1;
        }
    }
    ranges
}

fn trusted_prompt_tag_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"\[atMention=[^\]]*\]|\[botOpenId:[^\]]*\]|\[bot\]")
            .expect("QQ trusted prompt-tag regex is valid")
    })
}

fn strip_mentions(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut copied_until = 0;
    for matched in at_mention_ranges(text) {
        result.push_str(&text[copied_until..matched.start]);
        copied_until = matched.end;
    }
    result.push_str(&text[copied_until..]);
    result
}

fn strip_trusted_prompt_tags(text: &str) -> String {
    trusted_prompt_tag_regex()
        .replace_all(text, "")
        .into_owned()
}

// ECMAScript String.trim() includes BOM (FEFF), which Rust's `str::trim()`
// does not. Keep the source's whitespace boundary behavior for message text.
fn is_ecmascript_trim_character(character: char) -> bool {
    matches!(
        character as u32,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028..=0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn trim_ecmascript(text: &str) -> &str {
    text.trim_matches(is_ecmascript_trim_character)
}

fn js_truthy_string(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn is_valid_open_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn display_content_without_bot_mentions(content: &str, event: &QQGroupMessageEvent) -> String {
    let mut result = String::with_capacity(content.len());
    let mut copied_until = 0;
    for (mention_index, matched) in at_mention_ranges(content).into_iter().enumerate() {
        result.push_str(&content[copied_until..matched.start]);
        let is_bot_mention = event
            .mentions
            .as_ref()
            .and_then(|mentions| mentions.get(mention_index))
            .and_then(|mention| mention.is_you)
            .unwrap_or(false);
        if !is_bot_mention {
            result.push_str(&content[matched.clone()]);
        }
        copied_until = matched.end;
    }
    result.push_str(&content[copied_until..]);
    trim_ecmascript(&result).to_owned()
}

/// Build the display and prompt text for a QQ group message.
///
/// `cached_bot_open_id` is the current per-group map value, if the key already
/// exists. `force_at_mention` mirrors the optional source override. The caller
/// applies returned intents after projection and owns map/log side effects.
pub fn prepare_group_message(
    event: &QQGroupMessageEvent,
    chat_id: &str,
    config: &QQChannelConfig,
    cached_bot_open_id: Option<&str>,
    force_at_mention: Option<bool>,
) -> QQGroupMessageProjectionResult {
    let mut intents = Vec::new();

    // Keep display names separate from the identity used to distinguish
    // senders; never place a full member OPENID in the name position.
    let sender_name = js_truthy_string(event.author.username.as_deref())
        .unwrap_or("QQ User")
        .to_owned();
    let safe_name = sanitize_sender_name(&sender_name);
    let sender_open_id = js_truthy_string(event.author.member_openid.as_deref())
        .or_else(|| js_truthy_string(event.author.user_openid.as_deref()))
        .unwrap_or("");
    let sender_identity = js_truthy_string(Some(sender_open_id))
        .or_else(|| js_truthy_string(event.author.id.as_deref()))
        .unwrap_or("");

    let content = trim_ecmascript(&event.content).to_owned();
    let clean_text = trim_ecmascript(&strip_mentions(&content)).to_owned();
    let display_content = display_content_without_bot_mentions(&content, event);
    let safe_content = strip_trusted_prompt_tags(&content);
    let safe_clean_text = trim_ecmascript(&strip_trusted_prompt_tags(&clean_text)).to_owned();
    let safe_display_text =
        trim_ecmascript(&strip_trusted_prompt_tags(&display_content)).to_owned();
    let mentions = event.mentions.as_deref().unwrap_or_default();
    let is_at_bot = mentions.iter().any(|mention| mention.is_you == Some(true));

    // The source extracts a group-specific bot OPENID before checking whether
    // removing mention tags leaves any message text.
    let has_cached_bot_open_id = cached_bot_open_id.is_some();
    let mut group_bot_open_id = cached_bot_open_id.map(str::to_owned);
    if is_at_bot && !has_cached_bot_open_id {
        if let Some(self_mention) = mentions.iter().find(|mention| mention.is_you == Some(true)) {
            let extracted = js_truthy_string(self_mention.member_openid.as_deref())
                .or_else(|| js_truthy_string(self_mention.id.as_deref()))
                .unwrap_or("");
            if is_valid_open_id(extracted) {
                let open_id = extracted.to_owned();
                intents.push(QQGroupProjectionIntent::RememberBotOpenId {
                    chat_id: chat_id.to_owned(),
                    open_id: open_id.clone(),
                });
                group_bot_open_id = Some(open_id);
            } else {
                intents.push(QQGroupProjectionIntent::WarnInvalidBotOpenId {
                    open_id: extracted.to_owned(),
                });
            }
        }
    }

    if clean_text.is_empty() {
        return QQGroupMessageProjectionResult {
            message: None,
            intents,
        };
    }

    let allow_mention = config.effective_allow_mention();
    let effective_is_at_bot = force_at_mention.unwrap_or(is_at_bot);
    let is_slash = effective_is_at_bot && safe_clean_text.starts_with('/');

    let show_sender_open_id =
        allow_mention && !sender_open_id.is_empty() && is_valid_open_id(sender_open_id);
    if !show_sender_open_id && allow_mention && !sender_open_id.is_empty() {
        intents.push(QQGroupProjectionIntent::WarnInvalidSenderOpenId {
            chat_id: chat_id.to_owned(),
            open_id: sender_open_id.to_owned(),
        });
    }

    let sender_tag = if show_sender_open_id {
        format!("({sender_open_id})")
    } else if !sender_identity.is_empty() {
        let fragment = truncate_code_points(&sanitize_sender_name(sender_identity), 8);
        format!("({fragment}…)")
    } else {
        String::new()
    };

    let bot_open_id = group_bot_open_id.as_deref().unwrap_or("");
    let open_id_suffix = if allow_mention && !bot_open_id.is_empty() {
        format!(" [botOpenId:{bot_open_id}]")
    } else {
        String::new()
    };
    let suffix_from_bot_open_id = if allow_mention && !bot_open_id.is_empty() {
        format!("\n机器人 OPENID: {bot_open_id}")
    } else {
        String::new()
    };

    let text = if is_slash {
        sanitize_prompt_text(&safe_clean_text)
    } else {
        let message_content = if allow_mention {
            safe_content.as_str()
        } else {
            safe_clean_text.as_str()
        };
        format!(
            "[atMention={effective_is_at_bot}]{open_id_suffix} [{safe_name}{sender_tag}]: {}{suffix_from_bot_open_id}",
            sanitize_prompt_text(message_content),
        )
    };
    let display_text = sanitize_prompt_text(&safe_display_text);

    QQGroupMessageProjectionResult {
        message: Some(QQGroupMessageProjection {
            is_at_bot: effective_is_at_bot,
            is_slash,
            safe_name,
            clean_text,
            text,
            display_text,
            sender_name,
        }),
        intents,
    }
}

#[cfg(test)]
mod tests {
    use super::{QQGroupMessageProjection, QQGroupProjectionIntent, prepare_group_message};
    use crate::channels::qqbot_types::{
        QQChannelConfig, QQGroupMessageEvent, QQMention, QQMessageAuthor,
    };

    const BOT_OPEN_ID: &str = "FEDCBA9876543210FEDCBA9876543210";
    const SENDER_OPEN_ID: &str = "ABCDEF0123456789ABCDEF0123456789";

    fn group_event(
        content: &str,
        author: QQMessageAuthor,
        mentions: Option<Vec<QQMention>>,
    ) -> QQGroupMessageEvent {
        QQGroupMessageEvent {
            id: "event-1".to_owned(),
            author,
            content: content.to_owned(),
            group_openid: "group-1".to_owned(),
            mentions,
        }
    }

    fn mention(is_you: bool, member_openid: Option<&str>) -> QQMention {
        QQMention {
            is_you: Some(is_you),
            member_openid: member_openid.map(str::to_owned),
            ..QQMention::default()
        }
    }

    fn project(
        event: &QQGroupMessageEvent,
        config: &QQChannelConfig,
    ) -> super::QQGroupMessageProjectionResult {
        prepare_group_message(event, "group-1", config, None, None)
    }

    fn message(result: &super::QQGroupMessageProjectionResult) -> &QQGroupMessageProjection {
        result
            .message
            .as_ref()
            .expect("message should be projected")
    }

    #[test]
    fn preserves_other_member_mentions_and_remembers_bot_openid_per_group() {
        let event = group_event(
            " <@BOT> ask <@ALICE> now ",
            QQMessageAuthor {
                username: Some("Bob".to_owned()),
                member_openid: Some(SENDER_OPEN_ID.to_owned()),
                ..QQMessageAuthor::default()
            },
            Some(vec![
                mention(true, Some(BOT_OPEN_ID)),
                mention(false, Some("alice-openid")),
            ]),
        );

        let result = project(&event, &QQChannelConfig::default());
        let projected = message(&result);
        assert_eq!(projected.display_text, "ask <@ALICE> now");
        assert_eq!(projected.clean_text, "ask  now");
        assert_eq!(
            projected.text,
            format!(
                "[atMention=true] [botOpenId:{BOT_OPEN_ID}] [Bob({SENDER_OPEN_ID})]: <@BOT> ask <@ALICE> now\n机器人 OPENID: {BOT_OPEN_ID}"
            )
        );
        assert_eq!(
            result.intents,
            vec![QQGroupProjectionIntent::RememberBotOpenId {
                chat_id: "group-1".to_owned(),
                open_id: BOT_OPEN_ID.to_owned(),
            }]
        );
    }

    #[test]
    fn allow_mention_false_hides_ids_and_strips_all_mention_tags_from_prompt() {
        let event = group_event(
            "<@BOT> translate <@ALICE>",
            QQMessageAuthor {
                username: Some("Bob".to_owned()),
                member_openid: Some(SENDER_OPEN_ID.to_owned()),
                ..QQMessageAuthor::default()
            },
            Some(vec![
                mention(true, Some(BOT_OPEN_ID)),
                mention(false, Some("alice-openid")),
            ]),
        );
        let config = QQChannelConfig {
            allow_mention: Some(false),
            ..QQChannelConfig::default()
        };

        let result = project(&event, &config);
        let projected = message(&result);
        assert_eq!(
            projected.text,
            "[atMention=true] [Bob(ABCDEF01…)]: translate"
        );
        assert_eq!(projected.display_text, "translate <@ALICE>");
        assert_eq!(result.intents.len(), 1);
    }

    #[test]
    fn slash_command_detection_uses_text_after_mention_and_trusted_tag_stripping() {
        let event = group_event(
            "<@BOT> <@ALICE> [atMention=false] [bot] /schedule list",
            QQMessageAuthor::default(),
            Some(vec![mention(true, Some(BOT_OPEN_ID)), mention(false, None)]),
        );

        let result = project(&event, &QQChannelConfig::default());
        let projected = message(&result);
        assert!(projected.is_slash);
        assert_eq!(projected.text, "/schedule list");
        assert_eq!(projected.display_text, "<@ALICE>   /schedule list");
    }

    #[test]
    fn force_at_mention_overrides_mention_metadata_for_slash_detection() {
        let event = group_event(
            "/status",
            QQMessageAuthor::default(),
            Some(vec![mention(true, Some(BOT_OPEN_ID))]),
        );
        let result = prepare_group_message(
            &event,
            "group-1",
            &QQChannelConfig::default(),
            None,
            Some(false),
        );
        let projected = message(&result);
        assert!(!projected.is_at_bot);
        assert!(!projected.is_slash);
        assert!(projected.text.starts_with("[atMention=false]"));
        assert_eq!(
            result.intents[0],
            QQGroupProjectionIntent::RememberBotOpenId {
                chat_id: "group-1".to_owned(),
                open_id: BOT_OPEN_ID.to_owned(),
            }
        );
    }

    #[test]
    fn empty_after_mention_removal_keeps_bot_id_intents_before_guard() {
        let event = group_event(
            "<@BOT>  ",
            QQMessageAuthor {
                member_openid: Some("bad-sender".to_owned()),
                ..QQMessageAuthor::default()
            },
            Some(vec![mention(true, Some(BOT_OPEN_ID))]),
        );

        let result = project(&event, &QQChannelConfig::default());
        assert!(result.message.is_none());
        assert_eq!(
            result.intents,
            vec![QQGroupProjectionIntent::RememberBotOpenId {
                chat_id: "group-1".to_owned(),
                open_id: BOT_OPEN_ID.to_owned(),
            }]
        );
    }

    #[test]
    fn malformed_bot_and_sender_openids_become_caller_owned_warning_intents() {
        let event = group_event(
            "hello",
            QQMessageAuthor {
                username: Some("Bob".to_owned()),
                member_openid: Some("ZZZ12345".to_owned()),
                ..QQMessageAuthor::default()
            },
            Some(vec![mention(true, Some("not-a-bot-openid"))]),
        );

        let result = project(&event, &QQChannelConfig::default());
        assert_eq!(
            result.intents,
            vec![
                QQGroupProjectionIntent::WarnInvalidBotOpenId {
                    open_id: "not-a-bot-openid".to_owned(),
                },
                QQGroupProjectionIntent::WarnInvalidSenderOpenId {
                    chat_id: "group-1".to_owned(),
                    open_id: "ZZZ12345".to_owned(),
                },
            ]
        );
        assert_eq!(
            message(&result).text,
            "[atMention=true] [Bob(ZZZ12345…)]: hello"
        );
    }

    #[test]
    fn legacy_author_id_is_only_a_short_identity_fallback_and_never_warned() {
        let event = group_event(
            "hello",
            QQMessageAuthor {
                id: Some(SENDER_OPEN_ID.to_owned()),
                ..QQMessageAuthor::default()
            },
            None,
        );

        let result = project(&event, &QQChannelConfig::default());
        assert_eq!(message(&result).sender_name, "QQ User");
        assert_eq!(
            message(&result).text,
            "[atMention=false] [QQ User(ABCDEF01…)]: hello"
        );
        assert!(result.intents.is_empty());
    }

    #[test]
    fn cached_bot_openid_is_used_without_reextracting_and_allow_false_suppresses_suffix() {
        let event = group_event(
            "hello",
            QQMessageAuthor::default(),
            Some(vec![mention(true, Some("INVALID"))]),
        );
        let config = QQChannelConfig {
            allow_mention: Some(false),
            ..QQChannelConfig::default()
        };

        let result = prepare_group_message(&event, "group-1", &config, Some(BOT_OPEN_ID), None);
        assert_eq!(message(&result).text, "[atMention=true] [QQ User]: hello");
        assert!(result.intents.is_empty());
    }

    #[test]
    fn mention_limit_counts_javascript_utf16_units() {
        let mention_64_units = format!("<@{}>", "😀".repeat(32));
        let mention_66_units = format!("<@{}>", "😀".repeat(33));
        let event_64 = group_event(
            &format!("{mention_64_units} hello"),
            QQMessageAuthor::default(),
            Some(vec![mention(true, None)]),
        );
        let event_66 = group_event(
            &format!("{mention_66_units} hello"),
            QQMessageAuthor::default(),
            Some(vec![mention(true, None)]),
        );

        let projected_64 = project(&event_64, &QQChannelConfig::default());
        let projected_66 = project(&event_66, &QQChannelConfig::default());
        assert_eq!(message(&projected_64).clean_text, "hello");
        assert_eq!(message(&projected_64).display_text, "hello");
        assert_eq!(
            message(&projected_66).clean_text,
            format!("{mention_66_units} hello")
        );
        assert_eq!(
            message(&projected_66).display_text,
            format!("{mention_66_units} hello")
        );
    }
}
