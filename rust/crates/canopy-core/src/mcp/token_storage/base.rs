use super::types::OAuthCredentials;
use thiserror::Error;

const EXPIRY_BUFFER_MS: i64 = 5 * 60 * 1000;

#[derive(Debug, Error)]
pub enum TokenStorageError {
    #[error("invalid OAuth credentials: {0}")]
    InvalidCredentials(String),
    #[error("token storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("token storage JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("token storage backend unavailable")]
    Unavailable,
    #[error("no credentials found for {0}")]
    CredentialsNotFound(String),
    #[error("no secret found for key: {0}")]
    SecretNotFound(String),
    #[error("token storage backend failed: {0}")]
    Backend(String),
}

#[derive(Clone, Debug)]
pub struct BaseTokenStorage {
    service_name: String,
}

impl BaseTokenStorage {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn validate_credentials(credentials: &OAuthCredentials) -> Result<(), TokenStorageError> {
        if credentials.server_name.is_empty() {
            return Err(TokenStorageError::InvalidCredentials(
                "Server name is required".to_owned(),
            ));
        }
        if credentials.token.access_token.is_empty() {
            return Err(TokenStorageError::InvalidCredentials(
                "Access token is required".to_owned(),
            ));
        }
        if credentials.token.token_type.is_empty() {
            return Err(TokenStorageError::InvalidCredentials(
                "Token type is required".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn is_token_expired(credentials: &OAuthCredentials) -> bool {
        let Some(expires_at) = credentials.token.expires_at.filter(|value| *value != 0) else {
            return false;
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        now_ms.saturating_add(EXPIRY_BUFFER_MS) >= expires_at
    }

    pub fn is_token_expired_at(credentials: &OAuthCredentials, now_ms: i64) -> bool {
        let Some(expires_at) = credentials.token.expires_at.filter(|value| *value != 0) else {
            return false;
        };
        now_ms.saturating_add(EXPIRY_BUFFER_MS) >= expires_at
    }

    pub fn sanitize_server_name(server_name: &str) -> String {
        server_name
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                    character
                } else {
                    '_'
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{BaseTokenStorage, TokenStorageError};
    use crate::mcp::token_storage::{OAuthCredentials, OAuthToken};

    fn credentials(expires_at: Option<i64>) -> OAuthCredentials {
        OAuthCredentials {
            server_name: "server".to_owned(),
            token: OAuthToken {
                access_token: "access".to_owned(),
                refresh_token: None,
                expires_at,
                token_type: "Bearer".to_owned(),
                scope: None,
            },
            client_id: None,
            token_url: None,
            mcp_server_url: None,
            updated_at: 0,
        }
    }

    #[test]
    fn validates_required_credential_fields() {
        let mut value = credentials(None);
        assert!(BaseTokenStorage::validate_credentials(&value).is_ok());
        value.token.access_token.clear();
        assert!(matches!(
            BaseTokenStorage::validate_credentials(&value),
            Err(TokenStorageError::InvalidCredentials(message)) if message == "Access token is required"
        ));
    }

    #[test]
    fn applies_expiration_buffer_and_treats_missing_or_zero_as_no_expiry() {
        assert!(!BaseTokenStorage::is_token_expired_at(
            &credentials(None),
            1_000
        ));
        assert!(!BaseTokenStorage::is_token_expired_at(
            &credentials(Some(0)),
            1_000
        ));
        assert!(BaseTokenStorage::is_token_expired_at(
            &credentials(Some(300_999)),
            1_000
        ));
        assert!(!BaseTokenStorage::is_token_expired_at(
            &credentials(Some(301_001)),
            1_000
        ));
    }

    #[test]
    fn sanitizes_keychain_account_names() {
        assert_eq!(
            BaseTokenStorage::sanitize_server_name("service/a b:1"),
            "service_a_b_1"
        );
        assert_eq!(
            BaseTokenStorage::sanitize_server_name("safe-name_2.test"),
            "safe-name_2.test"
        );
    }
}
