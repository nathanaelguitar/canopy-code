//! QQ Bot chat-ID validation and outbound route selection.
//!
//! Port of `isValidChatId` and the path/base decision in
//! `QQChannel.resolveRoute` from `packages/channels/qqbot/src/QQChannel.ts`.
//! Channel disposal and token-refresh handling remain with the caller.

use crate::channels::qqbot_api::get_api_base;
use crate::channels::qqbot_types::{QQChannelConfig, QQChatType};
use std::collections::HashMap;

/// Validate IDs before embedding them in QQ Bot request paths.
///
/// The source accepts only ASCII letters, digits, underscore, and hyphen, with
/// a maximum of 128 characters. All accepted characters occupy one UTF-16
/// code unit, so the byte and JavaScript string lengths coincide here.
pub fn is_valid_chat_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// A resolved API base and message path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QQRoute {
    pub base: &'static str,
    pub path: String,
}

/// Select the QQ Bot route for a chat ID.
///
/// Runtime-learned `chat_type_map` entries take precedence over configured
/// `chat_types`. Invalid IDs and IDs without either route type return `None`.
/// Disposed-state and token-refresh checks intentionally remain in the channel
/// caller, matching the split in `resolveRoute`.
pub fn resolve_route(
    chat_id: &str,
    chat_type_map: &HashMap<String, QQChatType>,
    config: &QQChannelConfig,
) -> Option<QQRoute> {
    if !is_valid_chat_id(chat_id) {
        return None;
    }

    let route_type = chat_type_map
        .get(chat_id)
        .copied()
        .or_else(|| config.chat_types.as_ref()?.get(chat_id).copied())?;

    let (path_prefix, sandbox) = match route_type {
        QQChatType::Group => ("groups", config.sandbox.unwrap_or(false)),
        QQChatType::C2c => ("users", config.sandbox.unwrap_or(false)),
    };
    Some(QQRoute {
        base: get_api_base(sandbox),
        path: format!("/v2/{path_prefix}/{chat_id}/messages"),
    })
}

#[cfg(test)]
mod tests {
    use super::{is_valid_chat_id, resolve_route};
    use crate::channels::qqbot_types::{QQChannelConfig, QQChatType};
    use std::collections::HashMap;

    fn route_config(sandbox: bool, chat_types: HashMap<String, QQChatType>) -> QQChannelConfig {
        QQChannelConfig {
            sandbox: Some(sandbox),
            chat_types: Some(chat_types),
            ..QQChannelConfig::default()
        }
    }

    #[test]
    fn accepts_safe_ascii_ids_at_the_length_limit() {
        assert!(is_valid_chat_id("aZ09_-"));
        assert!(is_valid_chat_id(&"a".repeat(128)));
    }

    #[test]
    fn rejects_empty_unsafe_or_oversized_ids() {
        for id in [
            "",
            "id/segment",
            "id.segment",
            "id with space",
            "id?x",
            "用户",
        ] {
            assert!(!is_valid_chat_id(id), "accepted invalid chat ID {id:?}");
        }
        assert!(!is_valid_chat_id(&"x".repeat(129)));
    }

    #[test]
    fn runtime_chat_type_takes_precedence_over_config_override() {
        let config = route_config(
            false,
            HashMap::from([("chat-1".to_owned(), QQChatType::C2c)]),
        );
        let runtime = HashMap::from([("chat-1".to_owned(), QQChatType::Group)]);

        let route = resolve_route("chat-1", &runtime, &config).unwrap();
        assert_eq!(route.path, "/v2/groups/chat-1/messages");
    }

    #[test]
    fn uses_config_route_type_when_runtime_map_has_no_entry() {
        let config = route_config(
            false,
            HashMap::from([("user-2".to_owned(), QQChatType::C2c)]),
        );

        let route = resolve_route("user-2", &HashMap::new(), &config).unwrap();
        assert_eq!(route.path, "/v2/users/user-2/messages");
    }

    #[test]
    fn returns_none_when_route_type_is_missing_or_chat_id_is_invalid() {
        let config = QQChannelConfig::default();
        assert!(resolve_route("unknown", &HashMap::new(), &config).is_none());

        let configured = route_config(
            false,
            HashMap::from([("valid".to_owned(), QQChatType::C2c)]),
        );
        assert!(resolve_route("bad/id", &HashMap::new(), &configured).is_none());
    }

    #[test]
    fn selects_standard_or_sandbox_base() {
        let standard = route_config(false, HashMap::from([("user".to_owned(), QQChatType::C2c)]));
        let sandbox = route_config(true, HashMap::from([("user".to_owned(), QQChatType::C2c)]));

        assert_eq!(
            resolve_route("user", &HashMap::new(), &standard)
                .unwrap()
                .base,
            "https://api.sgroup.qq.com"
        );
        assert_eq!(
            resolve_route("user", &HashMap::new(), &sandbox)
                .unwrap()
                .base,
            "https://sandbox.api.sgroup.qq.com"
        );
    }

    #[test]
    fn resolves_c2c_and_group_message_paths() {
        let config = route_config(
            false,
            HashMap::from([
                ("person-openid".to_owned(), QQChatType::C2c),
                ("group-openid".to_owned(), QQChatType::Group),
            ]),
        );

        assert_eq!(
            resolve_route("person-openid", &HashMap::new(), &config)
                .unwrap()
                .path,
            "/v2/users/person-openid/messages"
        );
        assert_eq!(
            resolve_route("group-openid", &HashMap::new(), &config)
                .unwrap()
                .path,
            "/v2/groups/group-openid/messages"
        );
    }
}
