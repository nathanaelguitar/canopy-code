//! Context-window overflow detection and token-count extraction.
//!
//! This mirrors `packages/core/src/utils/contextLengthError.ts`, including its
//! bounded recursive collection, embedded JSON parsing, ECMAScript whitespace
//! trimming, ASCII regex word/digit boundaries, fragment-local timeout veto,
//! and optional token-count fields.

use std::collections::HashSet;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

const MAX_COLLECT_DEPTH: usize = 4;

/// JavaScript-like error input. `Error` is separate from plain objects because
/// JavaScript reads `Error.name`, `Error.message`, and `Error.cause` specially
/// before visiting enumerable own properties.
#[derive(Clone, Debug, PartialEq)]
pub enum ContextLengthErrorValue {
    Undefined,
    Null,
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<Self>),
    Object(Vec<(String, EnumerableProperty)>),
    Error(ContextErrorObject),
}

/// A property encountered by JavaScript `Object.values`.
#[derive(Clone, Debug, PartialEq)]
pub enum EnumerableProperty {
    /// An ordinary enumerable data property.
    Data(ContextLengthErrorValue),
    /// An enumerable accessor. If evaluating it throws, `Object.values`
    /// fails and the source falls back to descriptors, which omit accessors.
    Accessor {
        value: Option<Box<ContextLengthErrorValue>>,
        throws: bool,
    },
}

/// Value of one of the special properties read from a JavaScript `Error`.
#[derive(Clone, Debug, PartialEq)]
pub enum ErrorField<T> {
    Missing,
    Value(T),
    ThrowingAccessor,
}

/// The special and enumerable properties used when collecting an Error.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextErrorObject {
    pub name: ErrorField<String>,
    pub message: ErrorField<String>,
    pub cause: ErrorField<Box<ContextLengthErrorValue>>,
    pub enumerable_properties: Vec<(String, EnumerableProperty)>,
}

impl ContextLengthErrorValue {
    /// Construct an Error with the usual JavaScript `Error` name.
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error(ContextErrorObject {
            name: ErrorField::Value("Error".to_owned()),
            message: ErrorField::Value(message.into()),
            cause: ErrorField::Missing,
            enumerable_properties: Vec::new(),
        })
    }

    /// Construct an Error with a cause, mirroring `new Error(message, { cause })`.
    pub fn error_with_cause(message: impl Into<String>, cause: ContextLengthErrorValue) -> Self {
        Self::Error(ContextErrorObject {
            name: ErrorField::Value("Error".to_owned()),
            message: ErrorField::Value(message.into()),
            cause: ErrorField::Value(Box::new(cause)),
            enumerable_properties: Vec::new(),
        })
    }

    /// Convert a JSON-shaped provider error while retaining object insertion
    /// order. JSON has no Error prototype or accessor properties, so callers
    /// that need those semantics should construct [`ContextErrorObject`].
    pub fn from_json(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Boolean(*value),
            Value::Number(value) => Self::Number(value.as_f64().unwrap_or(f64::NAN)),
            Value::String(value) => Self::String(value.clone()),
            Value::Array(values) => Self::Array(values.iter().map(Self::from_json).collect()),
            Value::Object(values) => Self::Object(
                values
                    .iter()
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            EnumerableProperty::Data(Self::from_json(value)),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

impl From<&Value> for ContextLengthErrorValue {
    fn from(value: &Value) -> Self {
        Self::from_json(value)
    }
}

impl From<Value> for ContextLengthErrorValue {
    fn from(value: Value) -> Self {
        Self::from_json(&value)
    }
}

impl From<&ContextLengthErrorValue> for ContextLengthErrorValue {
    fn from(value: &ContextLengthErrorValue) -> Self {
        value.clone()
    }
}

impl From<&str> for ContextLengthErrorValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for ContextLengthErrorValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for ContextLengthErrorValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

/// Result corresponding to the TypeScript `ContextLengthExceededInfo`.
/// `None` for either count corresponds to an omitted/undefined property.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextLengthExceededInfo {
    pub is_exceeded: bool,
    pub message: String,
    pub actual_tokens: Option<f64>,
    pub limit_tokens: Option<f64>,
}

/// Extract all unique textual fragments from an error-like input and classify
/// context-window overflow. Errors are represented by [`ContextLengthErrorValue`]
/// so callers can preserve Error causes and throwing-accessor behavior when
/// adapting dynamic provider errors.
pub fn get_context_length_exceeded_info(
    error: impl Into<ContextLengthErrorValue>,
) -> ContextLengthExceededInfo {
    let error = error.into();
    let mut fragments = Vec::new();
    let mut seen_objects = HashSet::new();
    collect_strings(&error, &mut seen_objects, 0, &mut fragments);

    let mut seen_fragments = HashSet::new();
    let fragments: Vec<String> = fragments
        .into_iter()
        .filter_map(|fragment| {
            let fragment = trim_ecmascript_whitespace(&fragment).to_owned();
            if fragment.is_empty() || !seen_fragments.insert(fragment.clone()) {
                None
            } else {
                Some(fragment)
            }
        })
        .collect();

    let message = fragments.join("\n");
    // Veto only the fragment that contains both the timeout and overflow
    // wording. Retry metadata in a sibling property must not suppress a real
    // overflow elsewhere on the error object.
    let is_exceeded = fragments.iter().any(|fragment| {
        matches_any(context_length_patterns(), fragment)
            && !matches_any(timeout_patterns(), fragment)
    });
    let (actual_tokens, limit_tokens) = if is_exceeded {
        parse_token_counts(&message)
    } else {
        (None, None)
    };

    ContextLengthExceededInfo {
        is_exceeded,
        message,
        actual_tokens,
        limit_tokens,
    }
}

/// Convenience predicate corresponding to `isContextLengthExceededError`.
pub fn is_context_length_exceeded_error(error: impl Into<ContextLengthErrorValue>) -> bool {
    get_context_length_exceeded_info(error).is_exceeded
}

fn collect_strings(
    value: &ContextLengthErrorValue,
    seen_objects: &mut HashSet<usize>,
    depth: usize,
    output: &mut Vec<String>,
) {
    if depth > MAX_COLLECT_DEPTH {
        return;
    }

    match value {
        ContextLengthErrorValue::Undefined | ContextLengthErrorValue::Null => {}
        ContextLengthErrorValue::String(value) => {
            output.push(value.clone());
            if let Some(parsed) = try_parse_embedded_json(value) {
                collect_strings(&parsed, seen_objects, depth + 1, output);
            }
        }
        ContextLengthErrorValue::Number(value) => output.push(js_number_string(*value)),
        ContextLengthErrorValue::Boolean(value) => output.push(value.to_string()),
        ContextLengthErrorValue::Array(values) => {
            if !mark_seen(value, seen_objects) {
                return;
            }
            for value in values {
                collect_strings(value, seen_objects, depth + 1, output);
            }
        }
        ContextLengthErrorValue::Object(properties) => {
            if !mark_seen(value, seen_objects) {
                return;
            }
            let values = enumerable_values(properties);
            for value in values {
                collect_strings(value, seen_objects, depth + 1, output);
            }
        }
        ContextLengthErrorValue::Error(error) => {
            if !mark_seen(value, seen_objects) {
                return;
            }
            if let ErrorField::Value(name) = &error.name {
                output.push(name.clone());
            }
            if let ErrorField::Value(message) = &error.message {
                output.push(message.clone());
            }
            if let ErrorField::Value(cause) = &error.cause {
                collect_strings(cause, seen_objects, depth + 1, output);
            }
            let values = enumerable_values(&error.enumerable_properties);
            for value in values {
                collect_strings(value, seen_objects, depth + 1, output);
            }
        }
    }
}

fn mark_seen(value: &ContextLengthErrorValue, seen_objects: &mut HashSet<usize>) -> bool {
    seen_objects.insert(std::ptr::from_ref(value) as usize)
}

fn enumerable_values(properties: &[(String, EnumerableProperty)]) -> Vec<&ContextLengthErrorValue> {
    let ordered = javascript_property_order(properties);
    if ordered
        .iter()
        .any(|(_, property)| matches!(property, EnumerableProperty::Accessor { throws: true, .. }))
    {
        // Object.values throws if an enumerable getter throws. The source then
        // reads descriptors and keeps enumerable data properties only.
        ordered
            .into_iter()
            .filter_map(|(_, property)| match property {
                EnumerableProperty::Data(value) => Some(value),
                EnumerableProperty::Accessor { .. } => None,
            })
            .collect()
    } else {
        ordered
            .into_iter()
            .filter_map(|(_, property)| match property {
                EnumerableProperty::Data(value) => Some(value),
                EnumerableProperty::Accessor {
                    value: Some(value),
                    throws: false,
                } => Some(value.as_ref()),
                EnumerableProperty::Accessor { .. } => None,
            })
            .collect()
    }
}

fn javascript_property_order(
    properties: &[(String, EnumerableProperty)],
) -> Vec<(&str, &EnumerableProperty)> {
    let mut indices = Vec::new();
    let mut names = Vec::new();
    for (name, property) in properties {
        if let Some(index) = array_index(name) {
            indices.push((index, name.as_str(), property));
        } else {
            names.push((name.as_str(), property));
        }
    }
    indices.sort_by_key(|(index, _, _)| *index);
    indices
        .into_iter()
        .map(|(_, name, property)| (name, property))
        .chain(names)
        .collect()
}

fn array_index(name: &str) -> Option<u32> {
    if name.is_empty() || (name.len() > 1 && name.starts_with('0')) {
        return None;
    }
    let index = name.parse::<u32>().ok()?;
    (index != u32::MAX && index.to_string() == name).then_some(index)
}

fn try_parse_embedded_json(text: &str) -> Option<ContextLengthErrorValue> {
    let trimmed = trim_ecmascript_whitespace(text);
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
            return Some(ContextLengthErrorValue::from_json(&value));
        }
    }

    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    let embedded = text.get(start..=end)?;
    let parsed = serde_json::from_str::<Value>(embedded).ok()?;
    Some(ContextLengthErrorValue::from_json(&parsed))
}

/// JavaScript `String.prototype.trim` whitespace (including FEFF, excluding
/// NEL), which differs slightly from Rust's Unicode `str::trim` set.
fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else if value == 0.0 {
        // JavaScript stringifies negative zero as "0".
        "0".to_owned()
    } else {
        let raw = format!("{value:?}");
        let (negative, raw) = raw
            .strip_prefix('-')
            .map_or((false, raw.as_str()), |unsigned| (true, unsigned));
        let (mantissa, exponent) = raw
            .split_once(['e', 'E'])
            .map(|(mantissa, exponent)| (mantissa, exponent.parse::<i32>().unwrap_or_default()))
            .unwrap_or((raw, 0));
        let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exponent;
        let mut digits: String = mantissa.chars().filter(|ch| *ch != '.').collect();
        let leading_zeroes = digits.bytes().take_while(|byte| *byte == b'0').count();
        if leading_zeroes > 0 {
            digits.drain(..leading_zeroes);
        }
        let decimal_position = decimal_position - leading_zeroes as i32;
        while digits.ends_with('0') {
            digits.pop();
        }

        let magnitude = if decimal_position > 0 && decimal_position <= 21 {
            if decimal_position as usize >= digits.len() {
                format!(
                    "{}{}",
                    digits,
                    "0".repeat(decimal_position as usize - digits.len())
                )
            } else {
                let position = decimal_position as usize;
                format!("{}.{}", &digits[..position], &digits[position..])
            }
        } else if decimal_position <= 0 && decimal_position > -6 {
            format!("0.{}{}", "0".repeat((-decimal_position) as usize), digits)
        } else {
            let exponent = decimal_position - 1;
            let fraction = &digits[1..];
            let sign = if exponent >= 0 { "+" } else { "" };
            if fraction.is_empty() {
                format!("{}e{sign}{exponent}", &digits[..1])
            } else {
                format!("{}.{}e{sign}{exponent}", &digits[..1], fraction)
            }
        };
        if negative {
            format!("-{magnitude}")
        } else {
            magnitude
        }
    }
}

fn matches_any(patterns: &[Regex], text: &str) -> bool {
    patterns.iter().any(|pattern| pattern.is_match(text))
}

// JS `\s`, `\d`, and `\b` are written explicitly to avoid Rust regex's
// Unicode digit/word-boundary behavior differing at non-ASCII characters.
const JS_WS: &str = r"[\t\n\x0B\x0C\r \u{00A0}\u{1680}\u{2000}-\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}\u{FEFF}]";

fn context_length_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            r"(?i)\bcontext[_\s-]?length[_\s-]?exceeded\b",
            r"(?i)\bmaximum context length\b",
            r"(?i)\bprompt\s+(?:is\s+)?too long\b",
            r"(?i)\binput\s+(?:token\s+)?(?:count\s+|length\s+)?(?:is\s+)?too long\b",
            r"(?i)\brange of input length should be\b",
            r"(?i)\btoo many tokens\b",
            r"(?i)\btokens?\s*>\s*[0-9,]+\s+(?:maximum|max|limit)\b",
            r"(?i)\b(?:input|prompt|messages?|context)\b[^\n]{0,120}\btokens?\b[^\n]{0,120}\bexceed(?:s|ed|ing)?\b",
        ]
        .iter()
        .map(|pattern| {
            let replaced = pattern
                .replace("\\s", JS_WS)
                .replace("\\b", r"(?-u:\b)");
            Regex::new(&replaced)
                .expect("static context-length regex must compile")
        })
        .collect()
    })
}

fn timeout_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            r"(?i)\bcontext deadline exceeded\b",
            r"(?i)\bdeadline exceeded\b",
            r"(?i)\b(?:request|connection|read|context)\s+timed out\b",
            r"(?i)\b(?:request|connection|read|context)\s+timeout\b",
            r"(?i)\b(?:timeout|timed out)\s+(?:after|while|during)\b",
        ]
        .iter()
        .map(|pattern| {
            let replaced = pattern.replace("\\s", JS_WS).replace("\\b", r"(?-u:\b)");
            Regex::new(&replaced).expect("static timeout regex must compile")
        })
        .collect()
    })
}

fn count_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            r"(?i)([0-9][0-9,]*)\s*tokens?\s*>\s*([0-9][0-9,]*)",
            r"(?i)maximum context length is\s*([0-9][0-9,]*)\s*tokens?[\s\S]*?(?:resulted in|requested|used)\s*([0-9][0-9,]*)\s*tokens?",
            r"(?i)maximum context length is\s*([0-9][0-9,]*)\s*tokens?",
            r"(?i)input\s+token\s+(?:count|length)[^0-9]*([0-9][0-9,]*)[\s\S]*?exceed(?:s|ed)?[\s\S]*?(?:maximum|limit)[^0-9]*([0-9][0-9,]*)",
        ]
        .iter()
        .map(|pattern| {
            let replaced = pattern
                .replace("[\\s\\S]", "(?s:.)")
                .replace("\\s", JS_WS);
            Regex::new(&replaced).expect("static token-count regex must compile")
        })
        .collect()
    })
}

fn parse_token_counts(text: &str) -> (Option<f64>, Option<f64>) {
    for (index, pattern) in count_patterns().iter().enumerate() {
        let Some(captures) = pattern.captures(text) else {
            continue;
        };
        let first = captures
            .get(1)
            .and_then(|capture| parse_integer(capture.as_str()));
        let second = captures
            .get(2)
            .and_then(|capture| parse_integer(capture.as_str()));
        return match index {
            0 | 3 => (first, second),
            1 => (second, first),
            2 => (None, first),
            _ => unreachable!("there are four token count regex patterns"),
        };
    }
    (None, None)
}

fn parse_integer(value: &str) -> Option<f64> {
    // The regex guarantees a digit/comma sequence. JS parseInt removes commas
    // first and parses the remaining decimal integer into an IEEE-754 Number.
    value.replace(',', "").parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn classifies_the_typescript_positive_examples() {
        let positives = [
            "This model's maximum context length is 128000 tokens. However, your messages resulted in 135000 tokens.",
            "context_length_exceeded",
            "prompt is too long: 137500 tokens > 135000 maximum",
            "Range of input length should be [1, 30000]",
            "Input token length is too long",
            "The input token count (127234) exceeds the maximum number of tokens allowed (100000).",
            r#"{"error":{"code":"context_length_exceeded","message":"too many tokens in prompt"}}"#,
        ];
        for message in positives {
            assert!(
                is_context_length_exceeded_error(ContextLengthErrorValue::error(message)),
                "expected overflow: {message}"
            );
        }
    }

    #[test]
    fn rejects_the_typescript_unrelated_examples() {
        let negatives = [
            "rate limit exceeded",
            "Throttling: TPM(1/1)",
            "connection timeout",
            "finishReason: MAX_TOKENS",
            "max_tokens",
            "Request failed: maximum schema depth exceeded",
            "Request contains an invalid argument",
            "context deadline exceeded",
            "deadline exceeded",
            "Request timeout after 60s. Try reducing input length or increasing timeout in config.",
            "connection timed out while waiting for response",
        ];
        for message in negatives {
            assert!(
                !is_context_length_exceeded_error(ContextLengthErrorValue::error(message)),
                "unexpected overflow: {message}"
            );
        }
    }

    #[test]
    fn parses_prompt_and_openai_token_counts() {
        let prompt = get_context_length_exceeded_info(ContextLengthErrorValue::error(
            "prompt is too long: 137,500 tokens > 135,000 maximum",
        ));
        assert_eq!(prompt.actual_tokens, Some(137500.0));
        assert_eq!(prompt.limit_tokens, Some(135000.0));

        let openai = get_context_length_exceeded_info(ContextLengthErrorValue::error(
            "This model's maximum context length is 128000 tokens. However, your messages resulted in 135000 tokens.",
        ));
        assert_eq!(openai.actual_tokens, Some(135000.0));
        assert_eq!(openai.limit_tokens, Some(128000.0));

        let only_limit = get_context_length_exceeded_info(ContextLengthErrorValue::error(
            "This model's maximum context length is 128000 tokens.",
        ));
        assert_eq!(only_limit.actual_tokens, None);
        assert_eq!(only_limit.limit_tokens, Some(128000.0));
    }

    #[test]
    fn parses_generic_input_exceeds_counts_and_preserves_pattern_precedence() {
        let info = get_context_length_exceeded_info(ContextLengthErrorValue::error(
            "Input token count (127234) exceeds the maximum number of tokens allowed (100000).",
        ));
        assert_eq!(info.actual_tokens, Some(127234.0));
        assert_eq!(info.limit_tokens, Some(100000.0));

        let less_structured = get_context_length_exceeded_info(ContextLengthErrorValue::error(
            "input token length 5000 exceeds the limit 4096; maximum context length is 7000 tokens",
        ));
        // The OpenAI maximum-context pattern is tried first, like the source.
        assert_eq!(less_structured.actual_tokens, None);
        assert_eq!(less_structured.limit_tokens, Some(7000.0));
    }

    #[test]
    fn extracts_json_and_nested_object_messages_without_matching_keys() {
        let embedded = get_context_length_exceeded_info(ContextLengthErrorValue::error(
            r#"HTTP 400 {"error":{"code":"context_length_exceeded","message":"prompt is too long: 137500 tokens > 135000 maximum"}}"#,
        ));
        assert!(embedded.is_exceeded);
        assert_eq!(embedded.actual_tokens, Some(137500.0));
        assert_eq!(embedded.limit_tokens, Some(135000.0));

        let nested: ContextLengthErrorValue = json!({
            "status": 400,
            "error": { "code": "BadRequest", "message": "Input token length is too long" }
        })
        .into();
        let info = get_context_length_exceeded_info(nested);
        assert!(info.is_exceeded);
        assert!(info.message.contains("Input token length is too long"));

        let no_keys: ContextLengthErrorValue = json!({
            "context": "request body",
            "detail": "tokens are available",
            "status": "exceeded"
        })
        .into();
        let info = get_context_length_exceeded_info(no_keys);
        assert!(!info.is_exceeded);
        assert!(!info.message.contains("context"));
        assert!(info.message.contains("tokens are available"));
    }

    #[test]
    fn broad_token_terms_must_occur_in_the_same_fragment() {
        let value = ContextLengthErrorValue::Object(vec![
            (
                "message".into(),
                EnumerableProperty::Data("context window check".into()),
            ),
            (
                "detail".into(),
                EnumerableProperty::Data("tokens exceeded by policy wording".into()),
            ),
        ]);
        assert!(!get_context_length_exceeded_info(value).is_exceeded);
    }

    #[test]
    fn timeout_veto_is_limited_to_its_own_fragment() {
        let mut error = match ContextLengthErrorValue::error(
            "This model's maximum context length is 128000 tokens, however you requested 200000 tokens",
        ) {
            ContextLengthErrorValue::Error(error) => error,
            _ => unreachable!(),
        };
        error.enumerable_properties.push((
            "detail".into(),
            EnumerableProperty::Data("previous attempt: request timed out after 60s".into()),
        ));
        let info = get_context_length_exceeded_info(ContextLengthErrorValue::Error(error));
        assert!(info.is_exceeded);
        assert_eq!(info.actual_tokens, Some(200000.0));
        assert_eq!(info.limit_tokens, Some(128000.0));

        let nested_cause = ContextLengthErrorValue::Object(vec![(
            "attempts".into(),
            EnumerableProperty::Data(ContextLengthErrorValue::Array(vec![
                json!({"note":"connection timed out while waiting for response"}).into(),
            ])),
        )]);
        let root = ContextLengthErrorValue::error_with_cause(
            "This model's maximum context length is 128000 tokens, however you requested 200000 tokens",
            nested_cause,
        );
        assert!(is_context_length_exceeded_error(root));

        assert!(!is_context_length_exceeded_error(
            ContextLengthErrorValue::error(
                "request timed out while checking maximum context length",
            )
        ));
    }

    #[test]
    fn throwing_accessors_are_skipped_like_the_source_fallback() {
        let error = ContextLengthErrorValue::Error(ContextErrorObject {
            name: ErrorField::ThrowingAccessor,
            message: ErrorField::Value("Connection error.".into()),
            cause: ErrorField::Missing,
            enumerable_properties: vec![
                (
                    "name".into(),
                    EnumerableProperty::Accessor {
                        value: None,
                        throws: true,
                    },
                ),
                (
                    "details".into(),
                    EnumerableProperty::Accessor {
                        value: None,
                        throws: true,
                    },
                ),
            ],
        });
        let info = get_context_length_exceeded_info(error);
        assert!(!info.is_exceeded);
        assert!(info.message.contains("Connection error."));

        let plain = ContextLengthErrorValue::Object(vec![
            (
                "detail".into(),
                EnumerableProperty::Accessor {
                    value: None,
                    throws: true,
                },
            ),
            (
                "message".into(),
                EnumerableProperty::Data("context_length_exceeded: too many tokens".into()),
            ),
        ]);
        let info = get_context_length_exceeded_info(plain);
        assert!(info.is_exceeded);
        assert!(info.message.contains("context_length_exceeded"));
    }

    #[test]
    fn collects_depth_limited_values_in_javascript_property_order() {
        let nested = ContextLengthErrorValue::Object(vec![
            ("10".into(), EnumerableProperty::Data("ten".into())),
            ("2".into(), EnumerableProperty::Data("two".into())),
            ("label".into(), EnumerableProperty::Data("label".into())),
            ("01".into(), EnumerableProperty::Data("leading zero".into())),
        ]);
        let info = get_context_length_exceeded_info(nested);
        assert_eq!(info.message, "two\nten\nlabel\nleading zero");

        let mut deep = ContextLengthErrorValue::String("too many tokens".into());
        for _ in 0..5 {
            deep = ContextLengthErrorValue::Array(vec![deep]);
        }
        assert!(!is_context_length_exceeded_error(deep));
    }

    #[test]
    fn deduplicates_and_trims_with_ecmascript_whitespace() {
        let value = ContextLengthErrorValue::Object(vec![
            (
                "first".into(),
                EnumerableProperty::Data("\u{feff}context_length_exceeded\u{feff}".into()),
            ),
            (
                "second".into(),
                EnumerableProperty::Data("context_length_exceeded".into()),
            ),
            ("nel".into(), EnumerableProperty::Data("\u{0085}".into())),
        ]);
        let info = get_context_length_exceeded_info(value);
        assert_eq!(info.message, "context_length_exceeded\n\u{0085}");
        assert!(info.is_exceeded);
    }

    #[test]
    fn javascript_ascii_word_boundaries_and_digits_are_preserved() {
        assert!(is_context_length_exceeded_error(
            "é context_length_exceeded"
        ));
        assert!(!is_context_length_exceeded_error(
            "context_length_exceededSuffix"
        ));
        assert!(!is_context_length_exceeded_error("tokens > ١ maximum"));
    }

    #[test]
    fn primitive_numbers_use_javascript_string_formatting() {
        assert_eq!(
            get_context_length_exceeded_info(ContextLengthErrorValue::Number(-0.0)).message,
            "0"
        );
        assert_eq!(
            get_context_length_exceeded_info(ContextLengthErrorValue::Number(1e-6)).message,
            "0.000001"
        );
        assert_eq!(
            get_context_length_exceeded_info(ContextLengthErrorValue::Number(1e-7)).message,
            "1e-7"
        );
        assert_eq!(
            get_context_length_exceeded_info(ContextLengthErrorValue::Number(1e20)).message,
            "100000000000000000000"
        );
        assert_eq!(
            get_context_length_exceeded_info(ContextLengthErrorValue::Number(1e21)).message,
            "1e+21"
        );
    }

    #[test]
    fn exact_result_distinguishes_false_and_missing_counts() {
        let info = get_context_length_exceeded_info(ContextLengthErrorValue::Undefined);
        assert_eq!(
            info,
            ContextLengthExceededInfo {
                is_exceeded: false,
                message: String::new(),
                actual_tokens: None,
                limit_tokens: None,
            }
        );
        assert_eq!(
            get_context_length_exceeded_info(ContextLengthErrorValue::Null).message,
            ""
        );
    }
}
