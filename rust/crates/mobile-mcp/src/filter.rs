//! Reversible model-payload filtering used by the mobile-mcp fork.

use regex::{Captures, Regex};
use serde_json::{Map, Value};
use std::sync::OnceLock;

const TOKEN_PREFIX: &str = "__mcp_ref_";
const FILTERED_TERMS: &[&str] = &[
    r"dash[-_ ]?scope",
    r"ali[-_ ]?baba",
    r"ali[-_ ]?yuncs",
    r"ali[-_ ]?yun",
    r"ali[-_ ]?cloud",
    r"tong[-_ ]?yi",
    r"qian[-_ ]?wen",
    r"ant[-_ ]?group",
    "modelscope",
    "maxcompute",
    "bailian",
    "dingtalk",
    "lingma",
    "alipay",
    "antfin",
    "taobao",
    "qoder",
    r"q[-_ ]?wen",
    "damo",
    "yuque",
    "tmall",
    "wanx",
    "通义",
    "千问",
    "阿里",
    "百炼",
    "魔搭",
    "达摩",
    "灵码",
    "万相",
    "支付宝",
    "蚂蚁",
    "语雀",
    "钉钉",
    "淘宝",
    "天猫",
];

fn term_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(&format!("(?iu)(?:{})", FILTERED_TERMS.join("|"))).expect("valid term regex")
    })
}

fn token_prefix_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new("(?iu)__mcp_ref_").expect("valid prefix regex"))
}

fn reference_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new("(?iu)__mcp_ref_([0-9a-f]+)__").expect("valid reference regex"))
}

fn encode_reference(value: &str) -> String {
    format!("{TOKEN_PREFIX}{}__", hex(value.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub fn encode_text(input: &str) -> String {
    let escaped = token_prefix_regex()
        .replace_all(input, |captures: &Captures<'_>| {
            encode_reference(&captures[0])
        })
        .into_owned();
    term_regex()
        .replace_all(&escaped, |captures: &Captures<'_>| {
            encode_reference(&captures[0])
        })
        .into_owned()
}

pub fn decode_text(input: &str) -> String {
    reference_regex()
        .replace_all(input, |captures: &Captures<'_>| {
            let encoded = &captures[1];
            if encoded.len() % 2 != 0 {
                return captures[0].to_owned();
            }
            let Some(bytes) = decode_hex(encoded) else {
                return captures[0].to_owned();
            };
            let Ok(decoded) = String::from_utf8(bytes) else {
                return captures[0].to_owned();
            };
            if hex(decoded.as_bytes()) != encoded.to_ascii_lowercase() {
                return captures[0].to_owned();
            }
            decoded
        })
        .into_owned()
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = (pair[0] as char).to_digit(16)? as u8;
        let low = (pair[1] as char).to_digit(16)? as u8;
        output.push((high << 4) | low);
    }
    (value.len() % 2 == 0).then_some(output)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterDirection {
    Encode,
    Decode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyCollision;

impl std::fmt::Display for KeyCollision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Decoded payload contains duplicate object keys")
    }
}
impl std::error::Error for KeyCollision {}

pub fn transform_value(value: &Value, direction: FilterDirection) -> Result<Value, KeyCollision> {
    match value {
        Value::String(text) => Ok(Value::String(transform_text(text, direction))),
        Value::Array(items) => items
            .iter()
            .map(|item| transform_value(item, direction))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(object) => {
            let binary_content = object
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| matches!(kind, "image" | "audio"));
            let mut transformed = Map::new();
            for (key, item) in object {
                let new_key = transform_text(key, direction);
                if transformed.contains_key(&new_key) {
                    return Err(KeyCollision);
                }
                let new_value = if binary_content && key == "data" {
                    item.clone()
                } else {
                    transform_value(item, direction)?
                };
                transformed.insert(new_key, new_value);
            }
            Ok(Value::Object(transformed))
        }
        _ => Ok(value.clone()),
    }
}

pub fn transform_message(
    message: &Value,
    direction: FilterDirection,
) -> Result<Value, KeyCollision> {
    let Some(object) = message.as_object() else {
        return Ok(message.clone());
    };
    let mut transformed = object.clone();
    for key in ["params", "result", "error"] {
        if let Some(value) = transformed.get(key).cloned() {
            transformed.insert(key.to_owned(), transform_value(&value, direction)?);
        }
    }
    Ok(Value::Object(transformed))
}

fn transform_text(text: &str, direction: FilterDirection) -> String {
    match direction {
        FilterDirection::Encode => encode_text(text),
        FilterDirection::Decode => decode_text(text),
    }
}
