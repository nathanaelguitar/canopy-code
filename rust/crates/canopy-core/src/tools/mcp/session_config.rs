use serde_json::{Map, Value};
use std::collections::HashSet;

pub fn normalize_mcp_include_entry(entry: &str) -> &str {
    entry.split_once('(').map_or(entry, |(name, _)| name)
}

pub fn coerce_mcp_filter_entries(entries: Option<&Value>) -> Vec<&str> {
    entries
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn normalize_filter(entries: Option<&Value>, strip_suffix: bool) -> Vec<String> {
    let mut unique = HashSet::new();
    let mut normalized = coerce_mcp_filter_entries(entries)
        .into_iter()
        .map(|entry| {
            if strip_suffix {
                normalize_mcp_include_entry(entry).to_owned()
            } else {
                entry.to_owned()
            }
        })
        .filter(|entry| unique.insert(entry.clone()))
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
    normalized
}

/// Stable key for session-scoped MCP tool filters. Transport fields are
/// intentionally excluded so filter edits update a session view without
/// respawning the shared server.
pub fn mcp_session_metadata_key(config: &Value) -> Result<String, serde_json::Error> {
    let object = config.as_object();
    let include = object.and_then(|value| value.get("includeTools"));
    let include_tools = match include {
        None | Some(Value::Null) => Value::Null,
        Some(value) => Value::Array(
            normalize_filter(Some(value), true)
                .into_iter()
                .map(Value::String)
                .collect(),
        ),
    };
    let trust = object
        .and_then(|value| value.get("trust"))
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or(Value::Null);
    let always_load_tools = object
        .and_then(|value| value.get("alwaysLoadTools"))
        .and_then(Value::as_bool)
        == Some(true);
    let exclude_tools = Value::Array(
        normalize_filter(object.and_then(|value| value.get("excludeTools")), false)
            .into_iter()
            .map(Value::String)
            .collect(),
    );

    let mut metadata = Map::new();
    metadata.insert("trust".to_owned(), trust);
    metadata.insert("alwaysLoadTools".to_owned(), Value::Bool(always_load_tools));
    metadata.insert("includeTools".to_owned(), include_tools);
    metadata.insert("excludeTools".to_owned(), exclude_tools);
    serde_json::to_string(&Value::Object(metadata))
}

#[cfg(test)]
mod tests {
    use super::{coerce_mcp_filter_entries, mcp_session_metadata_key, normalize_mcp_include_entry};
    use serde_json::json;

    #[test]
    fn include_normalization_strips_parenthesized_arguments() {
        assert_eq!(normalize_mcp_include_entry("search(query)"), "search");
        assert_eq!(normalize_mcp_include_entry("search"), "search");
        assert_eq!(normalize_mcp_include_entry("(query)"), "");
    }

    #[test]
    fn malformed_filter_shapes_coerce_to_string_arrays() {
        assert!(coerce_mcp_filter_entries(Some(&json!("not-an-array"))).is_empty());
        assert_eq!(
            coerce_mcp_filter_entries(Some(&json!(["tool", 1, null, "other"]))),
            ["tool", "other"]
        );
    }

    #[test]
    fn equivalent_filters_share_stable_session_metadata() {
        let first = json!({
            "trust": true,
            "alwaysLoadTools": true,
            "includeTools": ["z", "search(query)", "z", "a"],
            "excludeTools": ["x", "x", "b"]
        });
        let equivalent = json!({
            "excludeTools": ["b", "x"],
            "includeTools": ["a", "search(args)", "z"],
            "alwaysLoadTools": true,
            "trust": true
        });
        assert_eq!(
            mcp_session_metadata_key(&first).unwrap(),
            mcp_session_metadata_key(&equivalent).unwrap()
        );
    }

    #[test]
    fn absent_and_empty_include_filters_remain_distinct() {
        let absent = mcp_session_metadata_key(&json!({"command":"node"})).unwrap();
        let empty = mcp_session_metadata_key(&json!({"includeTools":[]})).unwrap();
        let null = mcp_session_metadata_key(&json!({"includeTools":null})).unwrap();
        assert_eq!(absent, null);
        assert_ne!(absent, empty);
    }
}
