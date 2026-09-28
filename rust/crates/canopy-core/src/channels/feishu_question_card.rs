//! Feishu interactive cards and action parsing for user questions.
//!
//! This ports the pure helpers from `packages/channels/feishu/src/question-card.ts`.

use serde_json::{Map, Value, json};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeishuQuestionOption {
    pub label: String,
    pub description: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeishuQuestion {
    pub answer_key: String,
    pub header: String,
    pub question: String,
    pub options: Vec<FeishuQuestionOption>,
    pub multi_select: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeishuQuestionTerminalState {
    Processing,
    Submitted,
    Cancelled,
    Expired,
}

impl FeishuQuestionTerminalState {
    fn label(self) -> &'static str {
        match self {
            Self::Processing => "正在处理...",
            Self::Submitted => "已提交",
            Self::Cancelled => "已取消",
            Self::Expired => "已过期",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FeishuQuestionAction {
    Submit {
        request_id: String,
        operator_id: Option<String>,
        chat_id: Option<String>,
        message_id: Option<String>,
        form_value: Option<Map<String, Value>>,
    },
    Cancel {
        request_id: String,
        operator_id: Option<String>,
        chat_id: Option<String>,
        message_id: Option<String>,
    },
    Unhandled,
}

/// Builds the Card V2 form used to present questions to a Feishu user.
pub fn build_question_card(request_id: &str, questions: &[FeishuQuestion]) -> Value {
    let mut elements = Vec::with_capacity(questions.len() * 2 + 2);

    for question in questions {
        let descriptions = question
            .options
            .iter()
            .map(|option| format!("> **{}**: {}", option.label, option.description))
            .collect::<Vec<_>>()
            .join("\n");
        elements.push(json!({
            "tag": "markdown",
            "text_size": "notation",
            "content": format!(
                "**{}**\n> {}\n\n{}",
                question.header, question.question, descriptions
            ),
        }));

        let options = question
            .options
            .iter()
            .map(|option| {
                json!({
                    "text": { "tag": "plain_text", "content": option.label },
                    "value": option.label,
                })
            })
            .collect::<Vec<_>>();
        elements.push(json!({
            "tag": if question.multi_select { "multi_select_static" } else { "select_static" },
            "name": question.answer_key,
            "options": options,
        }));
    }

    elements.push(json!({
        "tag": "button",
        "text": { "tag": "plain_text", "content": "提交" },
        "type": "primary",
        "name": format!("qwen_ask_submit_{request_id}"),
        "form_action_type": "submit",
        "value": {
            "action": "qwen_ask_submit",
            "operation_id": request_id,
        },
    }));
    elements.push(json!({
        "tag": "button",
        "text": { "tag": "plain_text", "content": "取消" },
        "name": format!("qwen_ask_cancel_{request_id}"),
        "value": {
            "action": "qwen_ask_cancel",
            "operation_id": request_id,
        },
    }));

    json!({
        "schema": "2.0",
        "body": { "elements": [{
            "tag": "form",
            "name": "qwen_ask_form",
            "elements": elements,
        }] },
    })
}

/// Builds the read-only card shown after a question request reaches a terminal state.
pub fn build_question_terminal_card(
    questions: &[FeishuQuestion],
    state: FeishuQuestionTerminalState,
    answers: Option<&Map<String, Value>>,
) -> Value {
    let details = questions
        .iter()
        .map(|question| {
            let answer = answers
                .and_then(|answers| answers.get(&question.answer_key))
                .and_then(Value::as_str)
                .filter(|answer| !answer.is_empty());
            match answer {
                Some(answer) => format!("**{}**\n{}", question.header, answer),
                None => format!("**{}**\n{}", question.header, question.question),
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    json!({
        "schema": "2.0",
        "body": { "elements": [{
            "tag": "markdown",
            "content": format!("**{}**\n\n{}", state.label(), details),
        }] },
    })
}

/// Parses a Feishu callback payload, including name-based recovery when `value` is absent.
pub fn parse_question_action(data: &Value) -> FeishuQuestionAction {
    let Some(payload) = data.as_object() else {
        return FeishuQuestionAction::Unhandled;
    };
    let Some(action) = payload.get("action").and_then(Value::as_object) else {
        return FeishuQuestionAction::Unhandled;
    };

    let raw_value = action.get("value");
    let value = raw_value.and_then(Value::as_object);
    let mut request_id = value
        .and_then(|value| non_empty_string(value.get("operation_id")))
        .map(str::to_owned);
    let mut action_id = value
        .and_then(|value| non_empty_string(value.get("action")))
        .map(str::to_owned);
    let name = action.get("name").and_then(Value::as_str);

    if raw_value.is_none()
        && let Some(suffix) = name.and_then(|name| name.strip_prefix("qwen_ask_submit_"))
    {
        request_id = (!suffix.is_empty()).then(|| suffix.to_owned());
        action_id = request_id.as_ref().map(|_| "qwen_ask_submit".to_owned());
    }
    if raw_value.is_none()
        && let Some(suffix) = name.and_then(|name| name.strip_prefix("qwen_ask_cancel_"))
    {
        request_id = (!suffix.is_empty()).then(|| suffix.to_owned());
        action_id = request_id.as_ref().map(|_| "qwen_ask_cancel".to_owned());
    }

    let (Some(request_id), Some(action_id), Some(name)) = (request_id, action_id, name) else {
        return FeishuQuestionAction::Unhandled;
    };

    let operator_id = payload
        .get("operator")
        .and_then(Value::as_object)
        .and_then(|operator| non_empty_string(operator.get("open_id")))
        .map(str::to_owned);
    let context = payload.get("context").and_then(Value::as_object);
    let chat_id = non_empty_string(context.and_then(|context| context.get("open_chat_id")))
        .or_else(|| non_empty_string(payload.get("open_chat_id")))
        .map(str::to_owned);
    let message_id = non_empty_string(context.and_then(|context| context.get("open_message_id")))
        .or_else(|| non_empty_string(payload.get("open_message_id")))
        .map(str::to_owned);

    if action_id == "qwen_ask_submit" && name == format!("qwen_ask_submit_{request_id}") {
        let form_value = action.get("form_value").and_then(Value::as_object).cloned();
        return FeishuQuestionAction::Submit {
            request_id,
            operator_id,
            chat_id,
            message_id,
            form_value,
        };
    }

    if action_id == "qwen_ask_cancel" && name == format!("qwen_ask_cancel_{request_id}") {
        return FeishuQuestionAction::Cancel {
            request_id,
            operator_id,
            chat_id,
            message_id,
        };
    }

    FeishuQuestionAction::Unhandled
}

/// Validates submitted values against the questions and normalizes answers to strings.
pub fn parse_question_answers(
    questions: &[FeishuQuestion],
    form_value: Option<&Map<String, Value>>,
) -> Option<Map<String, Value>> {
    let form_value = form_value?;
    let answer_keys = questions
        .iter()
        .map(|question| question.answer_key.as_str())
        .collect::<std::collections::HashSet<_>>();
    if form_value.len() != questions.len()
        || form_value
            .keys()
            .any(|submitted_key| !answer_keys.contains(submitted_key.as_str()))
    {
        return None;
    }

    let mut answers = Map::new();
    for question in questions {
        let raw_answer = form_value.get(&question.answer_key)?;
        let values: Vec<Value> = if question.multi_select {
            if let Some(values) = raw_answer.as_array() {
                values.clone()
            } else {
                let serialized = raw_answer.as_str()?;
                let parsed: Value = serde_json::from_str(serialized).ok()?;
                parsed.as_array()?.clone()
            }
        } else if raw_answer.as_str().is_some_and(|answer| !answer.is_empty()) {
            vec![raw_answer.clone()]
        } else {
            return None;
        };

        if values.is_empty()
            || (!question.multi_select && values.len() != 1)
            || (question.multi_select && has_duplicate_strings(&values))
            || values.iter().any(|value| {
                let Some(value) = value.as_str() else {
                    return true;
                };
                !question.options.iter().any(|option| option.label == value)
            })
        {
            return None;
        }

        let normalized = values
            .iter()
            .map(|value| value.as_str().expect("validated string answer"))
            .collect::<Vec<_>>()
            .join(", ");
        answers.insert(question.answer_key.clone(), Value::String(normalized));
    }

    Some(answers)
}

fn has_duplicate_strings(values: &[Value]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    values
        .iter()
        .filter_map(Value::as_str)
        .any(|value| !seen.insert(value))
}

fn non_empty_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{
        FeishuQuestion, FeishuQuestionAction, FeishuQuestionOption, FeishuQuestionTerminalState,
        build_question_card, build_question_terminal_card, parse_question_action,
        parse_question_answers,
    };
    use serde_json::{Map, Value, json};

    fn questions() -> Vec<FeishuQuestion> {
        vec![
            FeishuQuestion {
                answer_key: "region".into(),
                header: "Region".into(),
                question: "Which region should I use?".into(),
                options: vec![
                    FeishuQuestionOption {
                        label: "Beijing".into(),
                        description: "Use the Beijing region.".into(),
                    },
                    FeishuQuestionOption {
                        label: "Shanghai".into(),
                        description: "Use the Shanghai region.".into(),
                    },
                ],
                multi_select: false,
            },
            FeishuQuestion {
                answer_key: "sources".into(),
                header: "Sources".into(),
                question: "Which sources should I inspect?".into(),
                options: vec![
                    FeishuQuestionOption {
                        label: "Logs".into(),
                        description: "Inspect application logs.".into(),
                    },
                    FeishuQuestionOption {
                        label: "Metrics".into(),
                        description: "Inspect service metrics.".into(),
                    },
                ],
                multi_select: true,
            },
        ]
    }

    fn object(value: &Value) -> &Map<String, Value> {
        value.as_object().expect("expected JSON object")
    }

    #[test]
    fn builds_card_v2_form_with_question_descriptions_and_actions() {
        let card = build_question_card("request-1", &questions());
        let expected = json!({
            "schema": "2.0",
            "body": { "elements": [{
                "tag": "form",
                "name": "qwen_ask_form",
                "elements": [
                    {
                        "tag": "markdown",
                        "text_size": "notation",
                        "content": "**Region**\n> Which region should I use?\n\n> **Beijing**: Use the Beijing region.\n> **Shanghai**: Use the Shanghai region.",
                    },
                    {
                        "tag": "select_static",
                        "name": "region",
                        "options": [
                            { "text": { "tag": "plain_text", "content": "Beijing" }, "value": "Beijing" },
                            { "text": { "tag": "plain_text", "content": "Shanghai" }, "value": "Shanghai" },
                        ],
                    },
                    {
                        "tag": "markdown",
                        "text_size": "notation",
                        "content": "**Sources**\n> Which sources should I inspect?\n\n> **Logs**: Inspect application logs.\n> **Metrics**: Inspect service metrics.",
                    },
                    {
                        "tag": "multi_select_static",
                        "name": "sources",
                        "options": [
                            { "text": { "tag": "plain_text", "content": "Logs" }, "value": "Logs" },
                            { "text": { "tag": "plain_text", "content": "Metrics" }, "value": "Metrics" },
                        ],
                    },
                    {
                        "tag": "button",
                        "text": { "tag": "plain_text", "content": "提交" },
                        "type": "primary",
                        "name": "qwen_ask_submit_request-1",
                        "form_action_type": "submit",
                        "value": { "action": "qwen_ask_submit", "operation_id": "request-1" },
                    },
                    {
                        "tag": "button",
                        "text": { "tag": "plain_text", "content": "取消" },
                        "name": "qwen_ask_cancel_request-1",
                        "value": { "action": "qwen_ask_cancel", "operation_id": "request-1" },
                    },
                ],
            }] },
        });
        assert_eq!(card, expected);
    }

    #[test]
    fn builds_processing_and_terminal_cards_with_answer_fallback() {
        let questions = questions();
        let mut answers = Map::new();
        answers.insert("region".into(), json!("Beijing"));
        answers.insert("sources".into(), json!("Logs, Metrics"));
        let processing = build_question_terminal_card(
            &questions,
            FeishuQuestionTerminalState::Processing,
            Some(&answers),
        );
        assert_eq!(
            processing["body"]["elements"][0]["content"],
            "**正在处理...**\n\n**Region**\nBeijing\n\n**Sources**\nLogs, Metrics"
        );
        assert!(processing["body"]["elements"][0]["tag"] == "markdown");

        let cancelled =
            build_question_terminal_card(&questions, FeishuQuestionTerminalState::Cancelled, None);
        assert_eq!(
            cancelled["body"]["elements"][0]["content"],
            "**已取消**\n\n**Region**\nWhich region should I use?\n\n**Sources**\nWhich sources should I inspect?"
        );
        for (state, label) in [
            (FeishuQuestionTerminalState::Submitted, "已提交"),
            (FeishuQuestionTerminalState::Cancelled, "已取消"),
            (FeishuQuestionTerminalState::Expired, "已过期"),
        ] {
            let card = build_question_terminal_card(&questions, state, None);
            assert!(
                card["body"]["elements"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains(label)
            );
        }
    }

    #[test]
    fn parses_submit_with_top_level_correlation_and_form_value() {
        let parsed = parse_question_action(&json!({
            "open_chat_id": "oc_1",
            "open_message_id": "om_1",
            "operator": { "open_id": "ou_1" },
            "action": {
                "name": "qwen_ask_submit_request-1",
                "value": { "action": "qwen_ask_submit", "operation_id": "request-1" },
                "form_value": { "region": "Beijing" },
            },
        }));
        assert_eq!(
            parsed,
            FeishuQuestionAction::Submit {
                request_id: "request-1".into(),
                operator_id: Some("ou_1".into()),
                chat_id: Some("oc_1".into()),
                message_id: Some("om_1".into()),
                form_value: Some(Map::from_iter([("region".into(), json!("Beijing"))])),
            }
        );
    }

    #[test]
    fn recovers_omitted_button_values_and_uses_nested_context_precedence() {
        assert_eq!(
            parse_question_action(&json!({
                "context": { "open_chat_id": "oc_1", "open_message_id": "om_1" },
                "operator": { "open_id": "ou_1" },
                "action": {
                    "name": "qwen_ask_submit_request-1",
                    "form_value": { "region": "Beijing" },
                },
            })),
            FeishuQuestionAction::Submit {
                request_id: "request-1".into(),
                operator_id: Some("ou_1".into()),
                chat_id: Some("oc_1".into()),
                message_id: Some("om_1".into()),
                form_value: Some(Map::from_iter([("region".into(), json!("Beijing"))])),
            }
        );
        assert_eq!(
            parse_question_action(&json!({
                "open_chat_id": "oc_fallback",
                "open_message_id": "om_fallback",
                "context": { "open_chat_id": "oc_1", "open_message_id": "om_1" },
                "action": {
                    "name": "qwen_ask_cancel_request-1",
                    "value": { "action": "qwen_ask_cancel", "operation_id": "request-1" },
                },
            })),
            FeishuQuestionAction::Cancel {
                request_id: "request-1".into(),
                operator_id: None,
                chat_id: Some("oc_1".into()),
                message_id: Some("om_1".into()),
            }
        );
    }

    #[test]
    fn recovers_cancel_without_value_and_omits_empty_optional_fields() {
        assert_eq!(
            parse_question_action(&json!({
                "operator": { "open_id": "" },
                "action": { "name": "qwen_ask_cancel_request-1" },
            })),
            FeishuQuestionAction::Cancel {
                request_id: "request-1".into(),
                operator_id: None,
                chat_id: None,
                message_id: None,
            }
        );
    }

    #[test]
    fn ignores_malformed_actions_and_requires_matching_name_and_value() {
        for data in [
            json!(null),
            json!([]),
            json!({"action": []}),
            json!({"action": { "name": "qwen_ask_submit_request-1", "value": { "action": "qwen_ask_submit" } }}),
            json!({"action": { "name": "qwen_ask_submit_wrong-request", "value": { "action": "qwen_ask_submit", "operation_id": "request-1" } }}),
            json!({"action": { "name": "qwen_ask_cancel_request-1", "value": { "action": "stop", "operation_id": "request-1" } }}),
            json!({"action": { "name": "qwen_ask_cancel_wrong-request", "value": { "action": "qwen_ask_cancel", "operation_id": "request-1" } }}),
            json!({"action": { "name": "qwen_ask_submit_", "form_value": {} }}),
        ] {
            assert_eq!(
                parse_question_action(&data),
                FeishuQuestionAction::Unhandled,
                "{data}"
            );
        }
        assert_eq!(
            parse_question_action(&json!({
                "action": {
                    "name": "qwen_ask_submit_request-1",
                    "value": null,
                },
            })),
            FeishuQuestionAction::Unhandled,
            "explicit null is not the same as an omitted value"
        );
    }

    #[test]
    fn parses_and_normalizes_single_and_multi_select_answers() {
        let questions = questions();
        let parsed = parse_question_answers(
            &questions,
            Some(object(&json!({
                "region": "Beijing",
                "sources": ["Logs", "Metrics"],
            }))),
        );
        assert_eq!(
            parsed,
            Some(Map::from_iter([
                ("region".into(), json!("Beijing")),
                ("sources".into(), json!("Logs, Metrics")),
            ]))
        );

        let serialized = json!({ "region": "Shanghai", "sources": "[\"Metrics\", \"Logs\"]" });
        assert_eq!(
            parse_question_answers(&questions, Some(object(&serialized))),
            Some(Map::from_iter([
                ("region".into(), json!("Shanghai")),
                ("sources".into(), json!("Metrics, Logs")),
            ]))
        );
    }

    #[test]
    fn rejects_missing_extra_malformed_duplicate_and_unknown_answers() {
        let questions = questions();
        let invalid_values = [
            json!({"region": "Beijing"}),
            json!({"region": "Unknown", "sources": ["Logs"]}),
            json!({"region": ["Beijing"], "sources": ["Logs"]}),
            json!({"region": "Beijing", "sources": ["Logs", "Logs"]}),
            json!({"region": "Beijing", "sources": ["Logs"], "extra": "value"}),
            json!({"region": "Beijing", "sources": []}),
            json!({"region": "Beijing", "sources": "[]"}),
            json!({"region": "Beijing", "sources": "\"Logs\""}),
            json!({"region": "Beijing", "sources": "not-json"}),
            json!({"region": "Beijing", "sources": "{\"a\":1}"}),
            json!({"region": "Beijing", "sources": 42}),
        ];
        for value in invalid_values {
            assert_eq!(
                parse_question_answers(&questions, Some(object(&value))),
                None,
                "accepted invalid form value {value}"
            );
        }
        assert_eq!(parse_question_answers(&questions, None), None);
    }
}
