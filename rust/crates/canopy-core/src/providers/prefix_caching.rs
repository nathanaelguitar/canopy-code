//! Official OpenAI prompt-cache key and explicit-breakpoint handling.

use reqwest::Url;
use serde_json::{Map, Value, json};

const CACHE_KEY_PREFIX: &str = "canopy-code:";
const EXPLICIT_BREAKPOINT_COUNT: usize = 2;

/// Auth modes that Canopy routes through OpenAI credentials or OAuth.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OpenAiAuthMode {
    OpenAi,
    CanopyOAuth,
    #[default]
    Other,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OpenAiPrefixCacheConfig {
    pub auth_mode: OpenAiAuthMode,
    pub base_url: Option<String>,
}

pub fn supports_openai_prefix_caching(config: &OpenAiPrefixCacheConfig) -> bool {
    matches!(
        config.auth_mode,
        OpenAiAuthMode::OpenAi | OpenAiAuthMode::CanopyOAuth
    )
}

pub fn is_official_openai_endpoint(config: &OpenAiPrefixCacheConfig) -> bool {
    if config.auth_mode != OpenAiAuthMode::OpenAi {
        return false;
    }
    let Some(base_url) = config.base_url.as_deref() else {
        return false;
    };
    Url::parse(base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host == "api.openai.com")
}

pub fn supports_explicit_openai_prompt_caching(model: &str) -> bool {
    let Some(suffix) = model
        .get(..4)
        .filter(|prefix| prefix.eq_ignore_ascii_case("gpt-"))
        .map(|_| &model[4..])
    else {
        return false;
    };
    let major_end = suffix.bytes().take_while(u8::is_ascii_digit).count();
    if major_end == 0 {
        return false;
    }
    let Ok(major) = suffix[..major_end].parse::<u32>() else {
        return false;
    };
    let rest = &suffix[major_end..];
    let (minor, delimiter) = if let Some(after_dot) = rest.strip_prefix('.') {
        let minor_len = after_dot.bytes().take_while(u8::is_ascii_digit).count();
        if minor_len == 0 {
            (0, &rest[1..])
        } else {
            let Ok(minor) = after_dot[..minor_len].parse::<u32>() else {
                return false;
            };
            (minor, &after_dot[minor_len..])
        }
    } else {
        (0, rest)
    };
    if !delimiter.is_empty() && !delimiter.starts_with('-') && !delimiter.starts_with('.') {
        return false;
    }
    major > 5 || (major == 5 && minor >= 6)
}

/// Add the official OpenAI prompt cache key and, when requested and supported,
/// mark up to two earlier user/tool message boundaries for explicit caching.
pub fn apply_official_openai_prompt_caching(
    request: &Value,
    session_id: Option<&str>,
    cache_sharing: bool,
    cache_key_partition: Option<&str>,
) -> Value {
    let mut result = request.clone();
    let Some(result_object) = result.as_object_mut() else {
        return result;
    };
    if session_id.is_some_and(|id| !id.is_empty())
        && result_object
            .get("prompt_cache_key")
            .is_none_or(|value| !is_truthy(value))
    {
        let partition = cache_key_partition
            .filter(|partition| !partition.is_empty())
            .map_or_else(String::new, |partition| format!(":{partition}"));
        result_object.insert(
            "prompt_cache_key".to_owned(),
            Value::String(format!(
                "{CACHE_KEY_PREFIX}{}{partition}",
                session_id.unwrap_or_default()
            )),
        );
    }

    if !cache_sharing
        || !request
            .get("model")
            .and_then(Value::as_str)
            .is_some_and(supports_explicit_openai_prompt_caching)
    {
        return result;
    }

    let Some(messages) = request.get("messages").and_then(Value::as_array) else {
        return result;
    };
    if messages.len() < 2 {
        return result;
    }
    let mut messages = messages.clone();
    let mut marked = 0;
    let mut index = messages.len().saturating_sub(2);
    while index < messages.len() && marked < EXPLICIT_BREAKPOINT_COUNT {
        if let Some(updated) = with_cache_breakpoint(&messages[index]) {
            messages[index] = updated;
            marked += 1;
        }
        if index == 0 {
            break;
        }
        index -= 1;
    }
    if marked == 0 {
        return result;
    }

    if let Some(result_object) = result.as_object_mut() {
        result_object.insert("messages".to_owned(), Value::Array(messages));
        let options = result_object
            .entry("prompt_cache_options")
            .or_insert_with(|| json!({}));
        if let Some(options) = options.as_object_mut() {
            options.insert("mode".to_owned(), Value::String("explicit".to_owned()));
        }
    }
    result
}

fn with_cache_breakpoint(message: &Value) -> Option<Value> {
    let object = message.as_object()?;
    let role = object.get("role").and_then(Value::as_str)?;
    if role != "user" && role != "tool" {
        return None;
    }
    let mut message = message.clone();
    let content = object.get("content")?;
    let updated = match content {
        Value::String(text) => Value::Array(vec![json!({
            "type": "text",
            "text": text,
            "prompt_cache_breakpoint": {"mode": "explicit"},
        })]),
        Value::Array(content) if !content.is_empty() => {
            let mut content = content.clone();
            let last = content.last_mut()?;
            let mut spread = spread_value(last);
            spread.insert(
                "prompt_cache_breakpoint".to_owned(),
                json!({"mode": "explicit"}),
            );
            *last = Value::Object(spread);
            Value::Array(content)
        }
        _ => return None,
    };
    message
        .as_object_mut()?
        .insert("content".to_owned(), updated);
    Some(message)
}

fn spread_value(value: &Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object.clone(),
        Value::Array(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        Value::String(text) => text
            .chars()
            .enumerate()
            .map(|(index, character)| (index.to_string(), Value::String(character.to_string())))
            .collect(),
        _ => Map::new(),
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn official() -> OpenAiPrefixCacheConfig {
        OpenAiPrefixCacheConfig {
            auth_mode: OpenAiAuthMode::OpenAi,
            base_url: Some("https://api.openai.com/v1".to_owned()),
        }
    }

    #[test]
    fn endpoint_detection_requires_official_host_and_openai_auth() {
        assert!(is_official_openai_endpoint(&official()));
        assert!(supports_openai_prefix_caching(&OpenAiPrefixCacheConfig {
            auth_mode: OpenAiAuthMode::CanopyOAuth,
            base_url: None,
        }));
        assert!(!is_official_openai_endpoint(&OpenAiPrefixCacheConfig {
            auth_mode: OpenAiAuthMode::CanopyOAuth,
            base_url: Some("https://api.openai.com/v1".to_owned()),
        }));
        assert!(!is_official_openai_endpoint(&OpenAiPrefixCacheConfig {
            auth_mode: OpenAiAuthMode::OpenAi,
            base_url: Some("https://proxy.example.test/v1".to_owned()),
        }));
        assert!(!is_official_openai_endpoint(&OpenAiPrefixCacheConfig {
            auth_mode: OpenAiAuthMode::OpenAi,
            base_url: None,
        }));
    }

    #[test]
    fn explicit_cache_support_matches_model_version_pattern() {
        for model in ["gpt-5.6", "GPT-5.6-preview", "gpt-6", "gpt-10.0-x"] {
            assert!(supports_explicit_openai_prompt_caching(model), "{model}");
        }
        for model in ["gpt-5.5", "gpt-5", "gpt-4.1", "gpt-6x", "other-6"] {
            assert!(!supports_explicit_openai_prompt_caching(model), "{model}");
        }
    }

    #[test]
    fn adds_partitioned_key_without_changing_existing_key_or_messages() {
        let request = json!({"model":"gpt-5.6","messages":[{"role":"user","content":"hello"}]});
        let result = apply_official_openai_prompt_caching(
            &request,
            Some("session-1"),
            false,
            Some("agent-2"),
        );
        assert_eq!(result["prompt_cache_key"], "canopy-code:session-1:agent-2");
        assert_eq!(result["messages"], request["messages"]);
        let existing = json!({"model":"gpt-5.6","prompt_cache_key":"existing","messages":[]});
        assert_eq!(
            apply_official_openai_prompt_caching(&existing, Some("session-1"), false, None)["prompt_cache_key"],
            "existing"
        );
    }

    #[test]
    fn puts_two_explicit_breakpoints_before_the_trailing_directive() {
        let request = json!({
            "model":"gpt-5.6-preview",
            "messages":[
                {"role":"system","content":"system"},
                {"role":"user","content":"first"},
                {"role":"assistant","content":"answer"},
                {"role":"tool","content":[{"type":"text","text":"tool output"}]},
                {"role":"user","content":"compression directive"}
            ],
            "prompt_cache_options":{"ttl":"1h"}
        });
        let result = apply_official_openai_prompt_caching(&request, None, true, None);
        assert_eq!(
            result["messages"][1]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert_eq!(
            result["messages"][3]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert!(result["messages"][4]["content"].is_string());
        assert_eq!(result["prompt_cache_options"]["mode"], "explicit");
        assert_eq!(result["prompt_cache_options"]["ttl"], "1h");
        assert!(request["messages"][1]["content"].is_string());
    }

    #[test]
    fn unsupported_models_keep_only_implicit_cache_key() {
        let request = json!({"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}]});
        let result = apply_official_openai_prompt_caching(&request, Some("session-1"), true, None);
        assert_eq!(result["prompt_cache_key"], "canopy-code:session-1");
        assert_eq!(result["messages"], request["messages"]);
        assert!(result.get("prompt_cache_options").is_none());
    }

    #[test]
    fn does_not_mark_the_only_message_as_a_pre_directive_boundary() {
        let request = json!({
            "model":"gpt-5.6",
            "messages":[{"role":"user","content":"only message"}]
        });
        let result = apply_official_openai_prompt_caching(&request, None, true, None);
        assert_eq!(result["messages"], request["messages"]);
        assert!(result.get("prompt_cache_options").is_none());
    }
}
