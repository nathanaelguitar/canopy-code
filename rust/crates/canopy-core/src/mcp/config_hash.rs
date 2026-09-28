use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const NON_BEHAVIORAL_FIELDS: [&str; 3] = ["scope", "extensionName", "description"];

/// Return the stable SHA-256 approval fingerprint for an MCP server config.
///
/// Object keys are sorted recursively, array order is preserved, and the
/// top-level provenance/display fields used by Canopy are excluded. Nested
/// values with those same keys remain behavioral and are included.
pub fn hash_mcp_server_config(config: &Value) -> Result<String, serde_json::Error> {
    let canonical = canonicalize(config, true);
    let serialized = serde_json::to_vec(&canonical)?;
    let digest = Sha256::digest(serialized);
    let mut result = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(result)
}

fn canonicalize(value: &Value, top_level: bool) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .iter()
                .filter(|(key, _)| !top_level || !NON_BEHAVIORAL_FIELDS.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), canonicalize(value, false)))
                .collect();
            let mut canonical = Map::new();
            for (key, value) in sorted {
                canonical.insert(key, value);
            }
            Value::Object(canonical)
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(|item| canonicalize(item, false)).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::hash_mcp_server_config;
    use serde_json::json;

    #[test]
    fn hashes_as_full_lowercase_sha256() {
        let hash = hash_mcp_server_config(&json!({"command":"node"})).unwrap();
        assert_eq!(hash.len(), 64);
        assert!(
            hash.bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
    }

    #[test]
    fn recursively_sorts_object_keys_and_preserves_array_order() {
        let first = json!({"command":"node","env":{"A":"1","B":"2"},"args":["a","b"]});
        let reordered = json!({"args":["a","b"],"env":{"B":"2","A":"1"},"command":"node"});
        let reversed_args = json!({"command":"node","env":{"A":"1","B":"2"},"args":["b","a"]});
        assert_eq!(
            hash_mcp_server_config(&first).unwrap(),
            hash_mcp_server_config(&reordered).unwrap()
        );
        assert_ne!(
            hash_mcp_server_config(&first).unwrap(),
            hash_mcp_server_config(&reversed_args).unwrap()
        );
    }

    #[test]
    fn excludes_only_top_level_provenance_and_display_fields() {
        let base = json!({"command":"node","env":{"description":"one"}});
        let decorated = json!({"description":"friendly name","extensionName":"ext","scope":"project","command":"node","env":{"description":"one"}});
        let changed_nested = json!({"command":"node","env":{"description":"two"}});
        assert_eq!(
            hash_mcp_server_config(&base).unwrap(),
            hash_mcp_server_config(&decorated).unwrap()
        );
        assert_ne!(
            hash_mcp_server_config(&base).unwrap(),
            hash_mcp_server_config(&changed_nested).unwrap()
        );
    }

    #[test]
    fn behavioral_values_change_the_hash() {
        assert_ne!(
            hash_mcp_server_config(&json!({"command":"node"})).unwrap(),
            hash_mcp_server_config(&json!({"command":"python"})).unwrap()
        );
        assert_ne!(
            hash_mcp_server_config(&json!({"url":"https://a.example"})).unwrap(),
            hash_mcp_server_config(&json!({"url":"https://b.example"})).unwrap()
        );
    }
}
