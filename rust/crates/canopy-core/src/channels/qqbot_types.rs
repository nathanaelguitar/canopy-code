//! QQ Bot gateway protocol and channel configuration types.
//!
//! Port of `packages/channels/qqbot/src/types.ts`. Optional TypeScript
//! properties are `Option`s and are omitted when absent during serialization.

use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;

/// Gateway operation codes sent in QQ Bot WebSocket frames.
pub struct OpCode;

impl OpCode {
    pub const DISPATCH: i64 = 0;
    pub const HEARTBEAT: i64 = 1;
    pub const IDENTIFY: i64 = 2;
    pub const RESUME: i64 = 6;
    pub const RECONNECT: i64 = 7;
    pub const INVALID_SESSION: i64 = 9;
    pub const HELLO: i64 = 10;
    pub const HEARTBEAT_ACK: i64 = 11;
}

/// Bit flags identifying QQ Bot gateway events.
pub struct Intent;

impl Intent {
    pub const C2C_MESSAGE: i64 = 1 << 12;
    pub const GROUP_AT_MESSAGE: i64 = 1 << 25;
    pub const GROUP_MESSAGE: i64 = 1 << 26;
}

/// Handling policy for unmentioned group messages.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupAllPolicy {
    Log,
    Keyword,
    All,
}

/// Route kind stored in `QQChannelConfig.chatTypes`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum QQChatType {
    #[serde(rename = "c2c")]
    C2c,
    #[serde(rename = "group")]
    Group,
}

/// Mention scope included in group-message gateway events.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QQMentionScope {
    All,
    Single,
}

/// Author fields shared by C2C and group message events.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct QQMessageAuthor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_openid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_openid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot: Option<bool>,
    /// Legacy author identifier, optional in some event variants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Legacy display name, optional in some event variants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

/// C2C or group-at message event.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QQMessageEvent {
    pub id: String,
    pub author: QQMessageAuthor,
    pub content: String,
}

/// Optional group mention metadata.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct QQMention {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_openid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_you: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<QQMentionScope>,
}

/// Extended fields available on group message events.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QQGroupMessageEvent {
    pub id: String,
    pub author: QQMessageAuthor,
    pub content: String,
    pub group_openid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mentions: Option<Vec<QQMention>>,
}

/// QQ Bot channel settings. These remain optional to preserve the input config
/// shape; `effective_*` helpers apply defaults documented by the source.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct QQChannelConfig {
    #[serde(default, rename = "appID", skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(default, rename = "appSecret", skip_serializing_if = "Option::is_none")]
    pub app_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<bool>,
    #[serde(
        default,
        rename = "groupAllPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_all_policy: Option<GroupAllPolicy>,
    #[serde(
        default,
        rename = "keywordTriggers",
        skip_serializing_if = "Option::is_none"
    )]
    pub keyword_triggers: Option<Vec<String>>,
    #[serde(
        default,
        rename = "allowMention",
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_mention: Option<bool>,
    #[serde(default, rename = "chatTypes", skip_serializing_if = "Option::is_none")]
    pub chat_types: Option<HashMap<String, QQChatType>>,
    #[serde(
        default,
        rename = "cron-msg-experimental",
        skip_serializing_if = "Option::is_none"
    )]
    pub cron_msg_experimental: Option<bool>,
    #[serde(
        default,
        rename = "bufferFlushLength",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number"
    )]
    pub buffer_flush_length: Option<f64>,
    #[serde(
        default,
        rename = "maxReconnectAttempts",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number"
    )]
    pub max_reconnect_attempts: Option<f64>,
    #[serde(
        default,
        rename = "maxFlushRetries",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number"
    )]
    pub max_flush_retries: Option<f64>,
    #[serde(
        default,
        rename = "maxGwRetries",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_number"
    )]
    pub max_gw_retries: Option<f64>,
}

fn serialize_optional_number<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(value) = value else {
        return serializer.serialize_none();
    };
    if value.is_finite() && value.fract() == 0.0 {
        if *value >= 0.0 && *value < u64::MAX as f64 {
            return serializer.serialize_u64(*value as u64);
        }
        if *value >= i64::MIN as f64 && *value <= i64::MAX as f64 {
            return serializer.serialize_i64(*value as i64);
        }
    }
    serializer.serialize_f64(*value)
}

pub const DEFAULT_GROUP_ALL_POLICY: GroupAllPolicy = GroupAllPolicy::Log;
pub const DEFAULT_ALLOW_MENTION: bool = true;
pub const DEFAULT_BUFFER_FLUSH_LENGTH: f64 = 4096.0;
pub const DEFAULT_MAX_RECONNECT_ATTEMPTS: f64 = 20.0;
pub const DEFAULT_MAX_FLUSH_RETRIES: f64 = 3.0;
pub const DEFAULT_MAX_GW_RETRIES: f64 = 5.0;

impl QQChannelConfig {
    pub fn effective_group_all_policy(&self) -> GroupAllPolicy {
        self.group_all_policy.unwrap_or(DEFAULT_GROUP_ALL_POLICY)
    }

    pub fn effective_allow_mention(&self) -> bool {
        self.allow_mention.unwrap_or(DEFAULT_ALLOW_MENTION)
    }

    /// Mirrors the channel constructor: missing, fractional, non-positive, or
    /// over-limit values fall back to the fixed 4096-character maximum.
    pub fn effective_buffer_flush_length(&self) -> f64 {
        self.buffer_flush_length
            .filter(|value| {
                value.is_finite()
                    && value.fract() == 0.0
                    && *value > 0.0
                    && *value <= DEFAULT_BUFFER_FLUSH_LENGTH
            })
            .unwrap_or(DEFAULT_BUFFER_FLUSH_LENGTH)
    }

    pub fn effective_max_reconnect_attempts(&self) -> f64 {
        self.max_reconnect_attempts
            .unwrap_or(DEFAULT_MAX_RECONNECT_ATTEMPTS)
    }

    pub fn effective_max_flush_retries(&self) -> f64 {
        self.max_flush_retries.unwrap_or(DEFAULT_MAX_FLUSH_RETRIES)
    }

    pub fn effective_max_gw_retries(&self) -> f64 {
        self.max_gw_retries.unwrap_or(DEFAULT_MAX_GW_RETRIES)
    }
}

/// Robot added to a group.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GroupAddRobotEvent {
    pub group_openid: String,
    pub op_member_openid: String,
    pub timestamp: f64,
}

/// Robot removed from a group.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GroupDelRobotEvent {
    pub group_openid: String,
    pub op_member_openid: String,
    pub timestamp: f64,
}

/// Active message permission toggle.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GroupMsgToggleEvent {
    pub group_openid: String,
    pub op_member_openid: String,
    pub timestamp: f64,
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ALLOW_MENTION, DEFAULT_BUFFER_FLUSH_LENGTH, DEFAULT_GROUP_ALL_POLICY,
        DEFAULT_MAX_FLUSH_RETRIES, DEFAULT_MAX_GW_RETRIES, DEFAULT_MAX_RECONNECT_ATTEMPTS,
        GroupAddRobotEvent, GroupAllPolicy, GroupDelRobotEvent, GroupMsgToggleEvent, Intent,
        OpCode, QQChannelConfig, QQChatType, QQGroupMessageEvent, QQMention, QQMessageAuthor,
        QQMessageEvent,
    };
    use serde_json::{json, to_value};
    use std::collections::HashMap;

    #[test]
    fn op_codes_and_intent_masks_match_typescript_values() {
        assert_eq!(OpCode::DISPATCH, 0);
        assert_eq!(OpCode::HEARTBEAT, 1);
        assert_eq!(OpCode::IDENTIFY, 2);
        assert_eq!(OpCode::RESUME, 6);
        assert_eq!(OpCode::RECONNECT, 7);
        assert_eq!(OpCode::INVALID_SESSION, 9);
        assert_eq!(OpCode::HELLO, 10);
        assert_eq!(OpCode::HEARTBEAT_ACK, 11);
        assert_eq!(Intent::C2C_MESSAGE, 1 << 12);
        assert_eq!(Intent::GROUP_AT_MESSAGE, 1 << 25);
        assert_eq!(Intent::GROUP_MESSAGE, 1 << 26);
        assert_eq!(
            Intent::C2C_MESSAGE | Intent::GROUP_AT_MESSAGE | Intent::GROUP_MESSAGE,
            100_667_392
        );
    }

    #[test]
    fn message_event_preserves_optional_author_fields() {
        let input = json!({
            "id": "event-1",
            "author": {
                "user_openid": "user-openid",
                "member_role": "admin",
                "bot": false,
                "username": "legacy-name"
            },
            "content": "hello"
        });
        let event: QQMessageEvent = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(to_value(event).unwrap(), input);
        assert_eq!(to_value(QQMessageAuthor::default()).unwrap(), json!({}));
    }

    #[test]
    fn group_message_preserves_group_and_mention_scope_union_values() {
        let input = json!({
            "id": "group-event",
            "author": {
                "member_openid": "member-1",
                "member_role": "member"
            },
            "content": "hello @all",
            "group_openid": "group-1",
            "mentions": [
                {"scope": "all", "is_you": true},
                {"id": "legacy", "member_openid": "member-2", "bot": false, "scope": "single"}
            ]
        });
        let event: QQGroupMessageEvent = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(to_value(event).unwrap(), input);
        assert!(serde_json::from_value::<QQMention>(json!({"scope": "other"})).is_err());
    }

    #[test]
    fn config_keeps_optional_names_and_discriminated_unions() {
        let input = json!({
            "appID": "app-id",
            "appSecret": "app-secret",
            "sandbox": false,
            "groupAllPolicy": "keyword",
            "keywordTriggers": ["hello", "help"],
            "allowMention": false,
            "chatTypes": {"group-1": "group", "user-1": "c2c"},
            "cron-msg-experimental": true,
            "bufferFlushLength": 2048,
            "maxReconnectAttempts": 0,
            "maxFlushRetries": 3,
            "maxGwRetries": 5
        });
        let config: QQChannelConfig = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(to_value(config.clone()).unwrap(), input);
        assert_eq!(config.group_all_policy, Some(GroupAllPolicy::Keyword));
        assert_eq!(
            config.chat_types.as_ref().unwrap()["group-1"],
            QQChatType::Group
        );
        assert_eq!(
            config.chat_types.as_ref().unwrap()["user-1"],
            QQChatType::C2c
        );
        assert!(
            serde_json::from_value::<QQChannelConfig>(json!({
                "groupAllPolicy": "sometimes"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<QQChannelConfig>(json!({
                "chatTypes": {"chat": "private"}
            }))
            .is_err()
        );
    }

    #[test]
    fn config_defaults_match_source_comments_and_constructor_defaults() {
        let config = QQChannelConfig::default();
        assert_eq!(to_value(config.clone()).unwrap(), json!({}));
        assert_eq!(config.effective_group_all_policy(), GroupAllPolicy::Log);
        assert!(config.effective_allow_mention());
        assert_eq!(config.effective_buffer_flush_length(), 4096.0);
        assert_eq!(config.effective_max_reconnect_attempts(), 20.0);
        assert_eq!(config.effective_max_flush_retries(), 3.0);
        assert_eq!(config.effective_max_gw_retries(), 5.0);
        assert_eq!(DEFAULT_GROUP_ALL_POLICY, GroupAllPolicy::Log);
        const {
            assert!(DEFAULT_ALLOW_MENTION);
        }
        assert_eq!(DEFAULT_BUFFER_FLUSH_LENGTH, 4096.0);
        assert_eq!(DEFAULT_MAX_RECONNECT_ATTEMPTS, 20.0);
        assert_eq!(DEFAULT_MAX_FLUSH_RETRIES, 3.0);
        assert_eq!(DEFAULT_MAX_GW_RETRIES, 5.0);

        for invalid in [0.0, -1.0, 4096.5, 4097.0, f64::INFINITY] {
            let config = QQChannelConfig {
                buffer_flush_length: Some(invalid),
                ..QQChannelConfig::default()
            };
            assert_eq!(config.effective_buffer_flush_length(), 4096.0);
        }
        let config = QQChannelConfig {
            buffer_flush_length: Some(100.0),
            max_reconnect_attempts: Some(0.0),
            ..QQChannelConfig::default()
        };
        assert_eq!(config.effective_buffer_flush_length(), 100.0);
        assert_eq!(config.effective_max_reconnect_attempts(), 0.0);
    }

    #[test]
    fn group_robot_events_keep_numeric_timestamps() {
        let input = json!({
            "group_openid": "group-1",
            "op_member_openid": "operator-1",
            "timestamp": 1712345678.5
        });
        let added: GroupAddRobotEvent = serde_json::from_value(input.clone()).unwrap();
        let deleted: GroupDelRobotEvent = serde_json::from_value(input.clone()).unwrap();
        let toggled: GroupMsgToggleEvent = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(to_value(added).unwrap(), input);
        assert_eq!(to_value(deleted).unwrap(), input);
        assert_eq!(to_value(toggled).unwrap(), input);
    }

    #[test]
    fn config_chat_type_map_is_stable_when_built_programmatically() {
        let config = QQChannelConfig {
            chat_types: Some(HashMap::from([("group-1".to_owned(), QQChatType::Group)])),
            ..QQChannelConfig::default()
        };
        assert_eq!(
            to_value(config).unwrap()["chatTypes"],
            json!({"group-1": "group"})
        );
    }
}
