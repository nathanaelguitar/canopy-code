use indexmap::IndexMap;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

pub type PoolKey = String;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum McpTransportKind {
    Stdio,
    Sse,
    Http,
    Websocket,
    Sdk,
    Unknown,
}

impl McpTransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::Sse => "sse",
            Self::Http => "http",
            Self::Websocket => "websocket",
            Self::Sdk => "sdk",
            Self::Unknown => "unknown",
        }
    }
}

pub const POOLED_TRANSPORTS_DEFAULT: [McpTransportKind; 2] =
    [McpTransportKind::Stdio, McpTransportKind::Websocket];

pub fn mcp_transport_of(config: &Value) -> McpTransportKind {
    let Some(object) = config.as_object() else {
        return McpTransportKind::Unknown;
    };
    if object.get("type").and_then(Value::as_str) == Some("sdk") {
        return McpTransportKind::Sdk;
    }
    if object.get("httpUrl").and_then(Value::as_str).is_some() {
        return McpTransportKind::Http;
    }
    if object.get("url").and_then(Value::as_str).is_some() {
        return McpTransportKind::Sse;
    }
    if object.get("tcp").and_then(Value::as_str).is_some() {
        return McpTransportKind::Websocket;
    }
    if object.get("command").and_then(Value::as_str).is_some() {
        return McpTransportKind::Stdio;
    }
    McpTransportKind::Unknown
}

pub fn is_poolable(config: &Value, pooled_transports: &[McpTransportKind]) -> bool {
    mcp_transport_of(config) != McpTransportKind::Sdk
        && pooled_transports.contains(&mcp_transport_of(config))
}

pub fn canonical_oauth(oauth: Option<&Value>) -> Option<Value> {
    let oauth = oauth?.as_object()?;
    if !oauth.get("enabled").is_some_and(js_truthy) {
        return None;
    }

    let mut result = Map::new();
    result.insert("enabled".to_owned(), Value::Bool(true));
    for (source, destination) in [
        ("clientId", "clientId"),
        ("clientSecret", "clientSecret"),
        ("authorizationUrl", "authorizationUrl"),
        ("tokenUrl", "tokenUrl"),
        ("redirectUri", "redirectUri"),
        ("tokenParamName", "tokenParamName"),
        ("registrationUrl", "registrationUrl"),
    ] {
        result.insert(
            destination.to_owned(),
            oauth
                .get(source)
                .filter(|value| !value.is_null())
                .cloned()
                .unwrap_or(Value::Null),
        );
    }
    result.insert(
        "scopes".to_owned(),
        sorted_nullable_strings(oauth.get("scopes")),
    );
    result.insert(
        "audiences".to_owned(),
        sorted_nullable_strings(oauth.get("audiences")),
    );
    Some(Value::Object(result))
}

fn sorted_nullable_strings(value: Option<&Value>) -> Value {
    let Some(values) = value.and_then(Value::as_array) else {
        return Value::Null;
    };
    let mut values = values.clone();
    values.sort_by(|left, right| {
        js_string(left)
            .encode_utf16()
            .cmp(js_string(right).encode_utf16())
    });
    Value::Array(values)
}

fn sorted_entries(value: Option<&Value>) -> Vec<(String, Value)> {
    let Some(object) = value.and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut entries = object
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.encode_utf16().cmp(right.0.encode_utf16()));
    entries
}

pub fn fingerprint(config: &Value) -> PoolKey {
    let object = config.as_object();
    let mut canonical = IndexMap::<String, Value>::new();
    canonical.insert(
        "transport".to_owned(),
        Value::String(mcp_transport_of(config).as_str().to_owned()),
    );
    canonical.insert(
        "command".to_owned(),
        object
            .and_then(|value| value.get("command"))
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or(Value::Null),
    );
    canonical.insert(
        "args".to_owned(),
        object
            .and_then(|value| value.get("args"))
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or_else(|| json!([])),
    );
    canonical.insert(
        "cwd".to_owned(),
        object
            .and_then(|value| value.get("cwd"))
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or(Value::Null),
    );
    canonical.insert(
        "env".to_owned(),
        entries_to_value(sorted_entries(object.and_then(|value| value.get("env")))),
    );
    for field in ["url", "httpUrl", "tcp"] {
        canonical.insert(
            field.to_owned(),
            object
                .and_then(|value| value.get(field))
                .filter(|value| !value.is_null())
                .cloned()
                .unwrap_or(Value::Null),
        );
    }
    canonical.insert(
        "headers".to_owned(),
        entries_to_value(sorted_entries(
            object.and_then(|value| value.get("headers")),
        )),
    );
    canonical.insert(
        "timeout".to_owned(),
        object
            .and_then(|value| value.get("timeout"))
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or(Value::Null),
    );
    canonical.insert(
        "oauth".to_owned(),
        canonical_oauth(object.and_then(|value| value.get("oauth"))).unwrap_or(Value::Null),
    );
    for field in ["authProviderType", "targetAudience", "targetServiceAccount"] {
        canonical.insert(
            field.to_owned(),
            object
                .and_then(|value| value.get(field))
                .filter(|value| !value.is_null())
                .cloned()
                .unwrap_or(Value::Null),
        );
    }
    let bytes = serde_json::to_vec(&canonical).expect("MCP pool config JSON is serializable");
    let digest = Sha256::digest(bytes);
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn entries_to_value(entries: Vec<(String, Value)>) -> Value {
    // The TypeScript source hashes `sortedEntries` as an array of pairs,
    // rather than as an object. Keep the exact shape so fingerprints are
    // portable across the two implementations as well as deterministic.
    Value::Array(
        entries
            .into_iter()
            .map(|(key, value)| Value::Array(vec![Value::String(key), value]))
            .collect(),
    )
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_string(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}

pub fn connection_id_of(server_name: &str, config: &Value) -> String {
    format!("{server_name}::{}", fingerprint(config))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedConnectionId {
    pub server_name: String,
    pub fingerprint: PoolKey,
}

pub fn parse_connection_id(id: &str) -> Result<ParsedConnectionId, String> {
    let Some(separator) = id.rfind("::") else {
        return Err(format!("Invalid ConnectionId: {id}"));
    };
    Ok(ParsedConnectionId {
        server_name: id[..separator].to_owned(),
        fingerprint: id[separator + 2..].to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        McpTransportKind, POOLED_TRANSPORTS_DEFAULT, canonical_oauth, connection_id_of,
        fingerprint, is_poolable, mcp_transport_of, parse_connection_id,
    };
    use serde_json::json;

    #[test]
    fn fingerprints_transport_credentials_but_excludes_session_filters() {
        let first = json!({"command":"node","args":["server.js"],"env":{"A":"1","B":"2"},"includeTools":["x"],"trust":false});
        let second = json!({"trust":true,"excludeTools":["tool"],"env":{"B":"2","A":"1"},"args":["server.js"],"command":"node"});
        assert_eq!(fingerprint(&first), fingerprint(&second));
        let credential_change =
            json!({"command":"node","args":["server.js"],"env":{"A":"1","B":"changed"}});
        assert_ne!(fingerprint(&first), fingerprint(&credential_change));
    }

    #[test]
    fn fingerprint_matches_typescript_sorted_entry_pair_encoding() {
        let config = json!({
            "command":"node",
            "args":["srv"],
            "env":{"B":"2","A":"1"},
            "headers":{"X-B":"b","X-A":"a"}
        });
        assert_eq!(fingerprint(&config), "aeffc70fa7e6722f");
    }

    #[test]
    fn canonical_oauth_sorts_scopes_and_hashes_every_auth_field() {
        assert_eq!(canonical_oauth(None), None);
        assert_eq!(canonical_oauth(Some(&json!({"enabled":false}))), None);
        let one = json!({"enabled":true,"scopes":["b","a"],"audiences":["z","x"]});
        let two = json!({"enabled":true,"scopes":["a","b"],"audiences":["x","z"]});
        assert_eq!(canonical_oauth(Some(&one)), canonical_oauth(Some(&two)));
        let base = json!({"command":"node","oauth":{"enabled":true,"clientId":"id"}});
        let changed = json!({"command":"node","oauth":{"enabled":true,"clientId":"id","clientSecret":"secret"}});
        assert_ne!(fingerprint(&base), fingerprint(&changed));
    }

    #[test]
    fn resolves_transport_precedence_and_pooling_policy() {
        let sdk = json!({"type":"sdk","command":"node"});
        let http = json!({"httpUrl":"https://mcp"});
        let tcp = json!({"tcp":"ws://mcp","command":"node"});
        assert_eq!(mcp_transport_of(&sdk), McpTransportKind::Sdk);
        assert_eq!(mcp_transport_of(&http), McpTransportKind::Http);
        assert_eq!(mcp_transport_of(&tcp), McpTransportKind::Websocket);
        assert_eq!(
            POOLED_TRANSPORTS_DEFAULT,
            [McpTransportKind::Stdio, McpTransportKind::Websocket]
        );
        assert!(!is_poolable(&sdk, &POOLED_TRANSPORTS_DEFAULT));
        assert!(is_poolable(&tcp, &POOLED_TRANSPORTS_DEFAULT));
        assert!(!is_poolable(&http, &POOLED_TRANSPORTS_DEFAULT));
    }

    #[test]
    fn connection_ids_round_trip_server_names_containing_separators() {
        let config = json!({"command":"node"});
        let id = connection_id_of("extension::server", &config);
        let parsed = parse_connection_id(&id).unwrap();
        assert_eq!(parsed.server_name, "extension::server");
        assert_eq!(parsed.fingerprint, fingerprint(&config));
        assert!(parse_connection_id("malformed").is_err());
    }
}
