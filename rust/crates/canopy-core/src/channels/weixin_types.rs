//! Wire models for the Weixin iLink Bot API.
//!
//! Port of `packages/channels/weixin/src/types.ts`. Optional fields are
//! omitted during serialization, matching `JSON.stringify` for absent
//! TypeScript properties. Numeric protocol fields are represented as integers
//! because the iLink message and status values are integer-valued.

use serde::{Deserialize, Serialize};

/// Numeric message-type constants from the iLink protocol.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageType;

impl MessageType {
    pub const NONE: i64 = 0;
    pub const USER: i64 = 1;
    pub const BOT: i64 = 2;
}

/// Numeric message-item-type constants from the iLink protocol.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageItemType;

impl MessageItemType {
    pub const NONE: i64 = 0;
    pub const TEXT: i64 = 1;
    pub const IMAGE: i64 = 2;
    pub const VOICE: i64 = 3;
    pub const FILE: i64 = 4;
    pub const VIDEO: i64 = 5;
}

/// Numeric message-state constants from the iLink protocol.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MessageState;

impl MessageState {
    pub const NEW: i64 = 0;
    pub const GENERATING: i64 = 1;
    pub const FINISH: i64 = 2;
}

/// Numeric typing-status constants from the iLink protocol.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TypingStatus;

impl TypingStatus {
    pub const TYPING: i64 = 1;
    pub const CANCEL: i64 = 2;
}

/// Client version metadata included with API requests.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BaseInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_version: Option<String>,
}

/// Encrypted media metadata used by Weixin's CDN.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CDNMedia {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypt_query_param: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aes_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypt_type: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TextItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImageItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumb_media: Option<CDNMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aeskey: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mid_size: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct VoiceItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub len: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct VideoItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<CDNMedia>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_size: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RefMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_item: Option<Box<MessageItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MessageItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_item: Option<TextItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_item: Option<ImageItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice_item: Option<VoiceItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_item: Option<FileItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_item: Option<VideoItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_msg: Option<RefMessage>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WeixinMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_time_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_state: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_list: Option<Vec<MessageItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_token: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GetUpdatesReq {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get_updates_buf: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_info: Option<BaseInfo>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GetUpdatesResp {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ret: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errcode: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errmsg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msgs: Option<Vec<WeixinMessage>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get_updates_buf: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub longpolling_timeout_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendMessageReq {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg: Option<WeixinMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_info: Option<BaseInfo>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GetConfigResp {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ret: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errmsg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typing_ticket: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendTypingReq {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ilink_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typing_ticket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_info: Option<BaseInfo>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendTypingResp {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ret: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errmsg: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        BaseInfo, CDNMedia, FileItem, GetConfigResp, GetUpdatesReq, GetUpdatesResp, ImageItem,
        MessageItem, MessageItemType, MessageState, MessageType, RefMessage, SendMessageReq,
        SendTypingReq, SendTypingResp, TextItem, TypingStatus, VideoItem, VoiceItem, WeixinMessage,
    };
    use serde_json::{Value, json};

    #[test]
    fn protocol_constants_match_typescript_values() {
        assert_eq!(MessageType::NONE, 0);
        assert_eq!(MessageType::USER, 1);
        assert_eq!(MessageType::BOT, 2);
        assert_eq!(MessageItemType::NONE, 0);
        assert_eq!(MessageItemType::TEXT, 1);
        assert_eq!(MessageItemType::IMAGE, 2);
        assert_eq!(MessageItemType::VOICE, 3);
        assert_eq!(MessageItemType::FILE, 4);
        assert_eq!(MessageItemType::VIDEO, 5);
        assert_eq!(MessageState::NEW, 0);
        assert_eq!(MessageState::GENERATING, 1);
        assert_eq!(MessageState::FINISH, 2);
        assert_eq!(TypingStatus::TYPING, 1);
        assert_eq!(TypingStatus::CANCEL, 2);
    }

    #[test]
    fn nested_message_and_media_round_trip_preserves_snake_case_wire_shape() {
        let value = json!({
            "seq": 19,
            "message_id": 2048,
            "from_user_id": "user-1",
            "to_user_id": "bot-1",
            "client_id": "client-1",
            "create_time_ms": 1720000000000_i64,
            "session_id": "session-1",
            "message_type": MessageType::BOT,
            "message_state": MessageState::FINISH,
            "item_list": [{
                "type": MessageItemType::IMAGE,
                "text_item": { "text": "caption" },
                "image_item": {
                    "media": {
                        "encrypt_query_param": "query",
                        "aes_key": "media-key",
                        "encrypt_type": 1
                    },
                    "thumb_media": { "encrypt_query_param": "thumb-query" },
                    "aeskey": "image-key",
                    "url": "https://cdn.example/image",
                    "mid_size": 4096
                },
                "voice_item": {
                    "media": { "aes_key": "voice-key" },
                    "text": "spoken text"
                },
                "file_item": {
                    "media": { "encrypt_type": 2 },
                    "file_name": "report.pdf",
                    "md5": "abc123",
                    "len": "512"
                },
                "video_item": {
                    "media": { "encrypt_query_param": "video-query" },
                    "video_size": 8192
                },
                "ref_msg": {
                    "message_item": {
                        "type": MessageItemType::TEXT,
                        "text_item": { "text": "quoted" }
                    },
                    "title": "Previous message"
                }
            }],
            "context_token": "context-1"
        });

        let decoded: WeixinMessage = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }

    #[test]
    fn omitted_optional_fields_are_omitted_at_every_level() {
        let empty_values = [
            serde_json::to_value(BaseInfo::default()).unwrap(),
            serde_json::to_value(CDNMedia::default()).unwrap(),
            serde_json::to_value(TextItem::default()).unwrap(),
            serde_json::to_value(ImageItem::default()).unwrap(),
            serde_json::to_value(VoiceItem::default()).unwrap(),
            serde_json::to_value(FileItem::default()).unwrap(),
            serde_json::to_value(VideoItem::default()).unwrap(),
            serde_json::to_value(RefMessage::default()).unwrap(),
            serde_json::to_value(MessageItem::default()).unwrap(),
            serde_json::to_value(WeixinMessage::default()).unwrap(),
            serde_json::to_value(GetUpdatesReq::default()).unwrap(),
            serde_json::to_value(GetUpdatesResp::default()).unwrap(),
            serde_json::to_value(SendMessageReq::default()).unwrap(),
            serde_json::to_value(GetConfigResp::default()).unwrap(),
            serde_json::to_value(SendTypingReq::default()).unwrap(),
            serde_json::to_value(SendTypingResp::default()).unwrap(),
        ];

        assert!(empty_values.iter().all(Value::is_object));
        assert!(
            empty_values
                .iter()
                .all(|value| value.as_object().unwrap().is_empty())
        );
    }

    #[test]
    fn sparse_requests_omit_unset_properties_but_keep_present_values() {
        let updates = GetUpdatesReq {
            get_updates_buf: Some("cursor".to_owned()),
            ..GetUpdatesReq::default()
        };
        assert_eq!(
            serde_json::to_value(updates).unwrap(),
            json!({ "get_updates_buf": "cursor" })
        );

        let typing = SendTypingReq {
            status: Some(TypingStatus::CANCEL),
            ..SendTypingReq::default()
        };
        assert_eq!(
            serde_json::to_value(typing).unwrap(),
            json!({ "status": 2 })
        );

        let response = GetUpdatesResp {
            ret: Some(0),
            msgs: Some(Vec::new()),
            ..GetUpdatesResp::default()
        };
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({ "ret": 0, "msgs": [] })
        );
    }
}
