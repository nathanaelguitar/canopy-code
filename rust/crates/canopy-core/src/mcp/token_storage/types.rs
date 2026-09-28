use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthToken {
    pub access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    pub token_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthCredentials {
    pub server_name: String,
    pub token: OAuthToken,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_server_url: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenStorageType {
    Keychain,
    EncryptedFile,
}

#[allow(async_fn_in_trait)]
pub trait TokenStorage: Send + Sync {
    async fn get_credentials(
        &self,
        server_name: &str,
    ) -> Result<Option<OAuthCredentials>, crate::mcp::token_storage::TokenStorageError>;
    async fn set_credentials(
        &self,
        credentials: OAuthCredentials,
    ) -> Result<(), crate::mcp::token_storage::TokenStorageError>;
    async fn delete_credentials(
        &self,
        server_name: &str,
    ) -> Result<(), crate::mcp::token_storage::TokenStorageError>;
    async fn list_servers(
        &self,
    ) -> Result<Vec<String>, crate::mcp::token_storage::TokenStorageError>;
    async fn get_all_credentials(
        &self,
    ) -> Result<IndexMap<String, OAuthCredentials>, crate::mcp::token_storage::TokenStorageError>;
    async fn clear_all(&self) -> Result<(), crate::mcp::token_storage::TokenStorageError>;
}

#[allow(async_fn_in_trait)]
pub trait SecretStorage: Send + Sync {
    async fn is_available(&self) -> Result<bool, crate::mcp::token_storage::TokenStorageError>;
    async fn set_secret(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(), crate::mcp::token_storage::TokenStorageError>;
    async fn get_secret(
        &self,
        key: &str,
    ) -> Result<Option<String>, crate::mcp::token_storage::TokenStorageError>;
    async fn delete_secret(
        &self,
        key: &str,
    ) -> Result<(), crate::mcp::token_storage::TokenStorageError>;
    async fn list_secrets(
        &self,
    ) -> Result<Vec<String>, crate::mcp::token_storage::TokenStorageError>;
}
