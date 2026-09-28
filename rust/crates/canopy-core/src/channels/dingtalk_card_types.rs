//! DingTalk interactive-card configuration and callback types.
//!
//! Port of `packages/channels/dingtalk/src/interactive-card-types.ts`.
//! Callback objects may arrive as JSON strings nested inside the top-level
//! callback; parsing follows the source's source-priority and identity rules.

use serde::Serialize;
use serde_json::{Map, Value};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

pub const DINGTALK_INTERACTIVE_CARD_TIMEOUT_EXCLUSIVE_MINIMUM: f64 = 0.0;
pub const DINGTALK_INTERACTIVE_CARD_TIMEOUT_MAXIMUM_MS: f64 = 2_147_483_647.0;
pub const DEFAULT_DINGTALK_QUESTION_TIMEOUT_MS: f64 = 270_000.0;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DingtalkInteractiveCardConfig {
    pub enabled: bool,
    pub status_card: DingtalkStatusCardConfig,
    pub question_card: DingtalkQuestionCardConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DingtalkStatusCardConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DingtalkQuestionCardConfig {
    pub enabled: bool,
    #[serde(serialize_with = "serialize_javascript_number")]
    pub timeout_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DingtalkCardCallback {
    pub out_track_id: String,
    pub action_id: String,
    pub actor_id: String,
    pub form_data: Map<String, Value>,
    /// The TypeScript fields are optional. The parser always supplies them,
    /// while callers constructing a callback directly may omit either value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_business_payload: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_cancel: Option<bool>,
}

pub type DingtalkCardExecutionError = Box<dyn Error + Send + Sync + 'static>;
pub type DingtalkCardExecutionFuture =
    Pin<Box<dyn Future<Output = Result<(), DingtalkCardExecutionError>> + Send + 'static>>;
pub type DingtalkCardExecute = Box<dyn FnOnce() -> DingtalkCardExecutionFuture + Send + 'static>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DingtalkCardCallbackTarget {
    pub chat_id: String,
    pub is_group: bool,
}

/// Rust representation of the TypeScript accepted/forbidden/ignored result.
pub enum DingtalkCardCallbackResult {
    Accepted {
        execute: DingtalkCardExecute,
    },
    Forbidden {
        actor_id: String,
        target: DingtalkCardCallbackTarget,
    },
    Ignored {
        actor_id: Option<String>,
    },
}

/// Parses configuration while keeping the source's `undefined` versus `null`
/// distinction: `None` means the key was omitted, while `Some(Value::Null)` is
/// an invalid configured value.
pub fn parse_dingtalk_interactive_card_config(
    value: Option<&Value>,
) -> Result<DingtalkInteractiveCardConfig, String> {
    if value.is_some_and(|value| !matches!(value, Value::Object(_))) {
        return Err("DingTalk interactiveCards must be an object.".to_owned());
    }

    let configured = value.is_some();
    let root = value.and_then(Value::as_object);
    let status = nested_record(
        root,
        "statusCard",
        "DingTalk interactiveCards.statusCard must be an object.",
    )?;
    let question = nested_record(
        root,
        "questionCard",
        "DingTalk interactiveCards.questionCard must be an object.",
    )?;

    let enabled = optional_boolean(
        root.and_then(|record| record.get("enabled")),
        "enabled",
        configured,
    )?;
    let status_enabled = optional_boolean(
        status.and_then(|record| record.get("enabled")),
        "statusCard.enabled",
        true,
    )?;
    let question_enabled = optional_boolean(
        question.and_then(|record| record.get("enabled")),
        "questionCard.enabled",
        true,
    )?;

    let timeout_ms = match question.and_then(|record| record.get("timeoutMs")) {
        None => DEFAULT_DINGTALK_QUESTION_TIMEOUT_MS,
        Some(value) => {
            let timeout = value.as_f64().filter(|timeout| {
                timeout.is_finite()
                    && *timeout > DINGTALK_INTERACTIVE_CARD_TIMEOUT_EXCLUSIVE_MINIMUM
            });
            let Some(timeout) = timeout else {
                return Err(
                    "DingTalk interactiveCards.questionCard.timeoutMs must be a finite positive number."
                        .to_owned(),
                );
            };
            timeout.min(DINGTALK_INTERACTIVE_CARD_TIMEOUT_MAXIMUM_MS)
        }
    };

    Ok(DingtalkInteractiveCardConfig {
        enabled,
        status_card: DingtalkStatusCardConfig {
            enabled: status_enabled,
        },
        question_card: DingtalkQuestionCardConfig {
            enabled: question_enabled,
            timeout_ms,
        },
    })
}

/// Parses a DingTalk callback from a JSON value or a JSON-encoded object.
/// Missing callback identifiers and malformed nested records fail closed.
pub fn parse_dingtalk_card_callback(value: &Value) -> Option<DingtalkCardCallback> {
    let root = parse_embedded_record(Some(value))?;
    let embedded_value = parse_embedded_record(root.get("value"));
    let embedded_content = parse_embedded_record(root.get("content"));

    let private_sources = [
        embedded_value.as_ref(),
        embedded_content.as_ref(),
        Some(&root),
    ]
    .into_iter()
    .flatten()
    .filter_map(|source| parse_embedded_record(source.get("cardPrivateData")))
    .collect::<Vec<_>>();

    let mut sources = Vec::with_capacity(3 + private_sources.len());
    if let Some(source) = embedded_value.as_ref() {
        sources.push(source);
    }
    if let Some(source) = embedded_content.as_ref() {
        sources.push(source);
    }
    sources.extend(private_sources.iter());
    sources.push(&root);

    let private_data = private_sources.first();
    let action_id = private_data
        .and_then(|private| private.get("actionIds"))
        .and_then(Value::as_array)
        .and_then(|ids| ids.first())
        .and_then(trimmed_string)
        .or_else(|| pick_string(&sources, &["actionValue", "eventKey", "actionId"]))?;
    let out_track_id = pick_string(&sources, &["outTrackId"])?;
    let actor_id = parse_dingtalk_card_actor_id(&Value::Object(root.clone()))?;

    let params = sources
        .iter()
        .find_map(|source| parse_embedded_record(source.get("params")));

    let mut form_data = None;
    for source in &sources {
        if let Some(record) = parse_embedded_record(source.get("formData")) {
            form_data = Some(record);
            break;
        }
        if let Some(record) = source
            .get("params")
            .and_then(|params| parse_embedded_record(Some(params)))
            .and_then(|params| parse_embedded_record(params.get("form")))
        {
            form_data = Some(record);
            break;
        }
    }

    let has_cancel_field = params
        .as_ref()
        .is_some_and(|params| params.contains_key("user_cancel"));
    let is_cancel = has_cancel_field
        && params
            .as_ref()
            .and_then(|params| params.get("user_cancel"))
            .is_some_and(parse_boolean_like);

    let has_business_payload = form_data.is_some() || has_cancel_field;
    Some(DingtalkCardCallback {
        out_track_id,
        action_id,
        actor_id,
        form_data: form_data.unwrap_or_default(),
        has_business_payload: Some(has_business_payload),
        is_cancel: Some(is_cancel),
    })
}

/// Reads actor identity only from the top-level callback, never an embedded
/// `value` or `content` payload.
pub fn parse_dingtalk_card_actor_id(value: &Value) -> Option<String> {
    let root = parse_embedded_record(Some(value))?;
    ["userId", "senderStaffId", "senderId"]
        .into_iter()
        .find_map(|key| root.get(key).and_then(trimmed_string))
}

fn nested_record<'a>(
    root: Option<&'a Map<String, Value>>,
    key: &str,
    error: &str,
) -> Result<Option<&'a Map<String, Value>>, String> {
    match root.and_then(|root| root.get(key)) {
        None => Ok(None),
        Some(Value::Object(record)) => Ok(Some(record)),
        Some(_) => Err(error.to_owned()),
    }
}

fn serialize_javascript_number<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if value.is_finite() && value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991.0 {
        serializer.serialize_i64(*value as i64)
    } else {
        serializer.serialize_f64(*value)
    }
}

fn optional_boolean(value: Option<&Value>, path: &str, fallback: bool) -> Result<bool, String> {
    match value {
        None => Ok(fallback),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(format!(
            "DingTalk interactiveCards.{path} must be a boolean."
        )),
    }
}

fn parse_embedded_record(value: Option<&Value>) -> Option<Map<String, Value>> {
    let value = match value? {
        Value::String(serialized) => serde_json::from_str::<Value>(serialized).ok()?,
        value => value.clone(),
    };
    match value {
        Value::Object(record) => Some(record),
        _ => None,
    }
}

fn pick_string(sources: &[&Map<String, Value>], keys: &[&str]) -> Option<String> {
    for source in sources {
        for key in keys {
            if let Some(value) = source.get(*key).and_then(trimmed_string) {
                return Some(value);
            }
        }
    }
    None
}

fn trimmed_string(value: &Value) -> Option<String> {
    let Value::String(value) = value else {
        return None;
    };
    let trimmed = trim_ecmascript_whitespace(value);
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn parse_boolean_like(value: &Value) -> bool {
    match value {
        Value::Bool(true) => true,
        Value::Number(number) => number.as_f64() == Some(1.0),
        Value::String(value) => value == "true" || value == "1",
        _ => false,
    }
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_DINGTALK_QUESTION_TIMEOUT_MS, DINGTALK_INTERACTIVE_CARD_TIMEOUT_EXCLUSIVE_MINIMUM,
        DINGTALK_INTERACTIVE_CARD_TIMEOUT_MAXIMUM_MS, DingtalkCardCallbackTarget,
        DingtalkInteractiveCardConfig, DingtalkQuestionCardConfig, DingtalkStatusCardConfig,
        parse_dingtalk_card_actor_id, parse_dingtalk_card_callback,
        parse_dingtalk_interactive_card_config,
    };
    use serde_json::{Value, json};

    fn default_config(enabled: bool) -> DingtalkInteractiveCardConfig {
        DingtalkInteractiveCardConfig {
            enabled,
            status_card: DingtalkStatusCardConfig { enabled: true },
            question_card: DingtalkQuestionCardConfig {
                enabled: true,
                timeout_ms: DEFAULT_DINGTALK_QUESTION_TIMEOUT_MS,
            },
        }
    }

    #[test]
    fn omitted_config_is_disabled_but_object_config_opts_in() {
        assert_eq!(
            parse_dingtalk_interactive_card_config(None).unwrap(),
            default_config(false)
        );
        assert_eq!(
            parse_dingtalk_interactive_card_config(Some(&json!({}))).unwrap(),
            default_config(true)
        );
    }

    #[test]
    fn serializes_config_and_callback_fields_with_typescript_names() {
        let config = parse_dingtalk_interactive_card_config(None).unwrap();
        assert_eq!(
            serde_json::to_string(&config).unwrap(),
            r#"{"enabled":false,"statusCard":{"enabled":true},"questionCard":{"enabled":true,"timeoutMs":270000}}"#
        );
        let callback = parse_dingtalk_card_callback(&json!({
            "userId": "owner",
            "outTrackId": "track",
            "value": { "actionId": "go", "formData": {} }
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(callback).unwrap(),
            json!({
                "outTrackId": "track",
                "actionId": "go",
                "actorId": "owner",
                "formData": {},
                "hasBusinessPayload": true,
                "isCancel": false
            })
        );
    }

    #[test]
    fn accepts_descriptor_admitted_samples() {
        let samples = [
            json!({ "enabled": false }),
            json!({ "statusCard": {} }),
            json!({ "statusCard": { "enabled": false } }),
            json!({ "questionCard": {} }),
            json!({ "questionCard": { "enabled": false, "timeoutMs": 1 } }),
        ];
        for sample in samples {
            assert!(parse_dingtalk_interactive_card_config(Some(&sample)).is_ok());
        }
        assert_eq!(DINGTALK_INTERACTIVE_CARD_TIMEOUT_EXCLUSIVE_MINIMUM, 0.0);
    }

    #[test]
    fn supports_explicit_and_independent_card_disabling() {
        let config = json!({
            "enabled": true,
            "statusCard": { "enabled": false },
            "questionCard": { "enabled": true, "timeoutMs": 1000 }
        });
        let parsed = parse_dingtalk_interactive_card_config(Some(&config)).unwrap();
        assert!(parsed.enabled);
        assert!(!parsed.status_card.enabled);
        assert!(parsed.question_card.enabled);
        assert_eq!(parsed.question_card.timeout_ms, 1000.0);
    }

    #[test]
    fn rejects_non_object_roots_and_nested_cards() {
        for value in [Value::Null, json!(false), json!("cards"), json!([])] {
            assert_eq!(
                parse_dingtalk_interactive_card_config(Some(&value)).unwrap_err(),
                "DingTalk interactiveCards must be an object."
            );
        }
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({ "statusCard": null })))
                .unwrap_err()
                .contains("statusCard must be an object")
        );
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({ "questionCard": [] })))
                .unwrap_err()
                .contains("questionCard must be an object")
        );
    }

    #[test]
    fn rejects_invalid_booleans_and_timeout_values() {
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({ "enabled": 1 })))
                .unwrap_err()
                .contains("enabled must be a boolean")
        );
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({
                "statusCard": { "enabled": "yes" }
            })))
            .unwrap_err()
            .contains("statusCard.enabled")
        );
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({
                "questionCard": { "timeoutMs": 0 }
            })))
            .unwrap_err()
            .contains("questionCard.timeoutMs")
        );
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({
                "questionCard": { "timeoutMs": -2 }
            })))
            .is_err()
        );
        assert!(
            parse_dingtalk_interactive_card_config(Some(&json!({
                "questionCard": { "timeoutMs": "10" }
            })))
            .is_err()
        );
    }

    #[test]
    fn clamps_timeout_to_the_runtime_timer_maximum() {
        let config = json!({
            "questionCard": { "timeoutMs": DINGTALK_INTERACTIVE_CARD_TIMEOUT_MAXIMUM_MS as u64 + 1 }
        });
        assert_eq!(
            parse_dingtalk_interactive_card_config(Some(&config))
                .unwrap()
                .question_card
                .timeout_ms,
            DINGTALK_INTERACTIVE_CARD_TIMEOUT_MAXIMUM_MS
        );
    }

    #[test]
    fn reads_top_level_actor_from_incomplete_or_json_encoded_callback() {
        assert_eq!(
            parse_dingtalk_card_actor_id(&json!({
                "userId": " actor-1 ",
                "value": "{\"outTrackId\":\"missing-action\"}"
            })),
            Some("actor-1".to_owned())
        );
        assert_eq!(
            parse_dingtalk_card_actor_id(&json!("{\"senderStaffId\":\"staff-2\"}")),
            Some("staff-2".to_owned())
        );
    }

    #[test]
    fn normalizes_embedded_value_owner_action_and_form_data() {
        let callback = json!({
            "userId": " owner-1 ",
            "value": serde_json::to_string(&json!({
                "outTrackId": "question-1",
                "cardPrivateData": { "actionIds": ["submit"] },
                "formData": { "0": "Beijing" }
            })).unwrap()
        });
        let parsed = parse_dingtalk_card_callback(&callback).unwrap();
        assert_eq!(parsed.out_track_id, "question-1");
        assert_eq!(parsed.action_id, "submit");
        assert_eq!(parsed.actor_id, "owner-1");
        assert_eq!(
            parsed.form_data,
            json!({ "0": "Beijing" }).as_object().unwrap().clone()
        );
        assert_eq!(parsed.has_business_payload, Some(true));
        assert_eq!(parsed.is_cancel, Some(false));
    }

    #[test]
    fn parses_builtin_template_form_from_card_private_params() {
        let callback = json!({
            "userId": "owner-1",
            "outTrackId": "question-1",
            "content": serde_json::to_string(&json!({
                "cardPrivateData": {
                    "actionIds": ["request-1"],
                    "params": { "form": { "0": "Beijing" } }
                }
            })).unwrap()
        });
        let parsed = parse_dingtalk_card_callback(&callback).unwrap();
        assert_eq!(parsed.action_id, "request-1");
        assert_eq!(
            parsed.form_data,
            json!({ "0": "Beijing" }).as_object().unwrap().clone()
        );
        assert_eq!(parsed.has_business_payload, Some(true));
    }

    #[test]
    fn recognizes_cancel_and_non_business_callback_shapes() {
        let cancel = json!({
            "userId": "owner-1",
            "outTrackId": "question-1",
            "content": serde_json::to_string(&json!({
                "cardPrivateData": {
                    "actionIds": ["request-1"],
                    "params": { "user_cancel": "true" }
                }
            })).unwrap()
        });
        let parsed_cancel = parse_dingtalk_card_callback(&cancel).unwrap();
        assert_eq!(parsed_cancel.has_business_payload, Some(true));
        assert_eq!(parsed_cancel.is_cancel, Some(true));

        let non_business = json!({
            "userId": "owner-1",
            "outTrackId": "question-1",
            "content": serde_json::to_string(&json!({
                "cardPrivateData": {
                    "actionIds": ["request-1"],
                    "params": { "fromConfig": true }
                }
            })).unwrap()
        });
        let parsed_non_business = parse_dingtalk_card_callback(&non_business).unwrap();
        assert_eq!(parsed_non_business.has_business_payload, Some(false));
        assert_eq!(parsed_non_business.is_cancel, Some(false));
    }

    #[test]
    fn recognizes_all_cancel_truthy_encodings_only_for_user_cancel() {
        for cancel_value in [json!(true), json!("true"), json!(1), json!(1.0), json!("1")] {
            let callback = json!({
                "userId": "owner",
                "outTrackId": "card",
                "value": {
                    "actionId": "go",
                    "params": { "user_cancel": cancel_value }
                }
            });
            assert_eq!(
                parse_dingtalk_card_callback(&callback).unwrap().is_cancel,
                Some(true)
            );
        }
        for cancel_value in [
            json!(false),
            json!("TRUE"),
            json!(0),
            json!("yes"),
            Value::Null,
        ] {
            let callback = json!({
                "userId": "owner",
                "outTrackId": "card",
                "value": {
                    "actionId": "go",
                    "params": { "user_cancel": cancel_value }
                }
            });
            assert_eq!(
                parse_dingtalk_card_callback(&callback).unwrap().is_cancel,
                Some(false)
            );
        }
    }

    #[test]
    fn callback_sources_follow_private_data_and_identity_precedence() {
        let callback = json!({
            "senderStaffId": " staff-owner ",
            "senderId": "second-owner",
            "value": {
                "outTrackId": "value-track",
                "actionValue": "value-action",
                "cardPrivateData": { "actionIds": ["private-action"] }
            },
            "content": {
                "outTrackId": "content-track",
                "actionValue": "content-action"
            },
            "outTrackId": "root-track",
            "actionId": "root-action"
        });
        let parsed = parse_dingtalk_card_callback(&callback).unwrap();
        assert_eq!(parsed.actor_id, "staff-owner");
        assert_eq!(parsed.out_track_id, "value-track");
        assert_eq!(parsed.action_id, "private-action");
    }

    #[test]
    fn falls_back_through_action_value_event_key_and_action_id() {
        for (payload, expected) in [
            (json!({ "actionValue": " value " }), "value"),
            (json!({ "eventKey": " event " }), "event"),
            (json!({ "actionId": " action " }), "action"),
        ] {
            let callback = json!({ "userId": "owner", "outTrackId": "track", "value": payload });
            assert_eq!(
                parse_dingtalk_card_callback(&callback).unwrap().action_id,
                expected
            );
        }
    }

    #[test]
    fn malformed_or_incomplete_callbacks_fail_closed() {
        assert!(parse_dingtalk_card_callback(&json!("{broken")).is_none());
        assert!(
            parse_dingtalk_card_callback(&json!({
                "value": { "outTrackId": "card-1", "actionValue": "stop" }
            }))
            .is_none()
        );
        assert!(
            parse_dingtalk_card_callback(&json!({
                "userId": "owner",
                "value": { "outTrackId": "card-1" }
            }))
            .is_none()
        );
    }

    #[test]
    fn trusts_only_top_level_callback_identity_fields() {
        let callback = json!({
            "userId": "real-owner",
            "value": {
                "userId": "spoofed-owner",
                "outTrackId": "card-1",
                "actionValue": "stop"
            }
        });
        assert_eq!(
            parse_dingtalk_card_callback(&callback).unwrap().actor_id,
            "real-owner"
        );
        let embedded_only = json!({
            "value": {
                "userId": "spoofed-owner",
                "outTrackId": "card-1",
                "actionValue": "stop"
            }
        });
        assert!(parse_dingtalk_card_callback(&embedded_only).is_none());
        assert_eq!(parse_dingtalk_card_actor_id(&embedded_only), None);
    }

    #[test]
    fn trims_ecmascript_whitespace_and_ignores_non_string_ids() {
        assert_eq!(
            parse_dingtalk_card_actor_id(&json!({ "userId": "\u{feff} actor \u{feff}" })),
            Some("actor".to_owned())
        );
        assert_eq!(parse_dingtalk_card_actor_id(&json!({ "userId": 7 })), None);
        let callback = json!({
            "userId": "owner",
            "value": {
                "outTrackId": " \u{feff}track\u{feff} ",
                "actionId": " go "
            }
        });
        assert_eq!(
            parse_dingtalk_card_callback(&callback)
                .unwrap()
                .out_track_id,
            "track"
        );
    }

    #[test]
    fn exposes_a_typed_callback_target_for_result_variants() {
        let target = DingtalkCardCallbackTarget {
            chat_id: "chat-1".to_owned(),
            is_group: true,
        };
        assert_eq!(target.chat_id, "chat-1");
        assert!(target.is_group);
    }
}
