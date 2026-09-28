//! Validated provenance metadata attached to daemon-created ACP sessions.

use serde::{Deserialize, Serialize};

pub const SESSION_SOURCE_META_KEY: &str = "qwen.session.source";
pub const MAX_SESSION_SOURCE_ID_LENGTH: usize = 256;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSourceMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
}

pub fn parse_session_source(
    source_type: Option<&str>,
    source_id: Option<&str>,
) -> Result<SessionSourceMetadata, String> {
    let Some(source_type) = source_type else {
        return if source_id.is_none() {
            Ok(SessionSourceMetadata::default())
        } else {
            Err("`sourceType` must match [a-z][a-z0-9_-]{0,63} when provided".into())
        };
    };
    let bytes = source_type.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !bytes[0].is_ascii_lowercase()
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return Err("`sourceType` must match [a-z][a-z0-9_-]{0,63} when provided".into());
    }
    if let Some(id) = source_id {
        if id.is_empty()
            || id.chars().count() > MAX_SESSION_SOURCE_ID_LENGTH
            || id.chars().any(char::is_control)
        {
            return Err(format!(
                "`sourceId` must be a non-empty string of at most {MAX_SESSION_SOURCE_ID_LENGTH} characters without control characters"
            ));
        }
    }
    Ok(SessionSourceMetadata {
        source_type: Some(source_type.to_owned()),
        source_id: source_id.map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_fields_are_validated_as_a_pair_and_serialize_camel_case() {
        assert_eq!(
            parse_session_source(None, None).unwrap(),
            SessionSourceMetadata::default()
        );
        assert!(parse_session_source(None, Some("id")).is_err());
        let parsed = parse_session_source(Some("browser_tab"), Some("tab-1")).unwrap();
        assert_eq!(
            serde_json::to_value(parsed).unwrap(),
            serde_json::json!({"sourceType":"browser_tab","sourceId":"tab-1"})
        );
    }
}
