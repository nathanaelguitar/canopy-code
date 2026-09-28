use std::time::Duration;

use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{Value, json};
use url::Url;

use crate::config::ProviderConfig;
use crate::context::ExternalContextItem;

const MAX_PROVIDER_ITEMS: usize = 5;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MEM0_BASE_URL: &str = "https://api.mem0.ai/";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RememberResult {
    Stored {
        provider_operation_id: Option<String>,
    },
    Accepted {
        provider_operation_id: String,
    },
    Failed,
    Unknown,
}

#[derive(Clone, Debug)]
pub enum ProviderError {
    HttpStatus(u16),
    Failed,
}

impl ProviderError {
    pub fn is_definitive_write_rejection(&self) -> bool {
        matches!(self, Self::HttpStatus(400 | 401 | 403 | 404))
    }
}

trait ExternalContextProvider {
    async fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ExternalContextItem>, ProviderError>;
}

trait ExternalMemoryWriter {
    async fn remember(&self, content: &str) -> Result<RememberResult, ProviderError>;
}

#[derive(Clone)]
pub struct ProviderClient {
    config: ProviderConfig,
    client: Client,
}

impl ProviderClient {
    pub fn new(config: ProviderConfig) -> Result<Self, String> {
        let client = crate::proxy::client()?;
        Ok(Self { config, client })
    }

    async fn post_json(
        &self,
        url: Url,
        authorization: String,
        body: Value,
    ) -> Result<Value, ProviderError> {
        let response = self
            .client
            .post(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::AUTHORIZATION, authorization)
            .json(&body)
            .send()
            .await
            .map_err(|_| ProviderError::Failed)?;

        let status = response.status();
        if status.is_redirection() {
            return Err(ProviderError::Failed);
        }
        if !status.is_success() {
            return Err(ProviderError::HttpStatus(status.as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(ProviderError::Failed);
        }

        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(next) = stream.next().await {
            let chunk = next.map_err(|_| ProviderError::Failed)?;
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(ProviderError::Failed);
            }
            bytes.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(bytes).map_err(|_| ProviderError::Failed)?;
        serde_json::from_str(&text).map_err(|_| ProviderError::Failed)
    }

    async fn request_with_timeout(
        &self,
        url: Url,
        authorization: String,
        body: Value,
        timeout: Duration,
    ) -> Result<Value, ProviderError> {
        tokio::time::timeout(timeout, self.post_json(url, authorization, body))
            .await
            .unwrap_or(Err(ProviderError::Failed))
    }
}

impl ExternalContextProvider for ProviderClient {
    async fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ExternalContextItem>, ProviderError> {
        let response = match &self.config {
            ProviderConfig::Mem0PlatformV3 { api_key, app_id } => {
                let url = Url::parse(MEM0_BASE_URL)
                    .expect("the built-in Mem0 URL is valid")
                    .join("/v3/memories/search/")
                    .expect("the built-in Mem0 path is valid");
                let body = json!({
                    "query": query,
                    "filters": {"app_id": app_id},
                    "top_k": limit.min(MAX_PROVIDER_ITEMS),
                    "threshold": 0.1,
                    "rerank": false
                });
                self.request_with_timeout(
                    url,
                    format!("Token {api_key}"),
                    body,
                    Duration::from_millis(30_000),
                )
                .await?
            }
            ProviderConfig::GenericHttpSearchV1 { base_url, token } => {
                let url = base_url
                    .join("/v1/context/search")
                    .expect("the configured base URL can accept a fixed path");
                let body = json!({"query": query, "limit": limit});
                self.request_with_timeout(
                    url,
                    format!("Bearer {token}"),
                    body,
                    Duration::from_millis(30_000),
                )
                .await?
            }
        };
        parse_items(&self.config, response)
    }
}

impl ExternalMemoryWriter for ProviderClient {
    async fn remember(&self, content: &str) -> Result<RememberResult, ProviderError> {
        let ProviderConfig::Mem0PlatformV3 { api_key, app_id } = &self.config else {
            return Ok(RememberResult::Failed);
        };
        let url = Url::parse(MEM0_BASE_URL)
            .expect("the built-in Mem0 URL is valid")
            .join("/v3/memories/add/")
            .expect("the built-in Mem0 path is valid");
        let body = json!({
            "messages": [{"role": "user", "content": content}],
            "app_id": app_id,
            "infer": false
        });
        let response = self
            .request_with_timeout(
                url,
                format!("Token {api_key}"),
                body,
                Duration::from_millis(30_000),
            )
            .await?;
        Ok(parse_mem0_remember_result(&response))
    }
}

pub async fn search_with_timeout(
    provider: &ProviderClient,
    query: &str,
    limit: usize,
    timeout: Duration,
) -> Result<Vec<ExternalContextItem>, ProviderError> {
    tokio::time::timeout(timeout, provider.search(query, limit))
        .await
        .unwrap_or(Err(ProviderError::Failed))
}

pub async fn remember_with_timeout(
    provider: &ProviderClient,
    content: &str,
    timeout: Duration,
) -> Result<RememberResult, ProviderError> {
    tokio::time::timeout(timeout, provider.remember(content))
        .await
        .unwrap_or(Err(ProviderError::Failed))
}

fn parse_items(
    config: &ProviderConfig,
    response: Value,
) -> Result<Vec<ExternalContextItem>, ProviderError> {
    let (values, content_key) = match config {
        ProviderConfig::Mem0PlatformV3 { .. } => (
            response
                .as_object()
                .and_then(|object| object.get("results"))
                .and_then(Value::as_array),
            "memory",
        ),
        ProviderConfig::GenericHttpSearchV1 { .. } => (
            response
                .as_object()
                .and_then(|object| object.get("items"))
                .and_then(Value::as_array),
            "content",
        ),
    };
    let Some(values) = values else {
        return Err(ProviderError::Failed);
    };
    Ok(values
        .iter()
        .filter_map(|value| parse_item(value, content_key))
        .take(MAX_PROVIDER_ITEMS)
        .collect())
}

fn parse_item(value: &Value, content_key: &str) -> Option<ExternalContextItem> {
    let object = value.as_object()?;
    let id = object.get("id")?.as_str()?;
    let content = object.get(content_key)?.as_str()?;
    if id.is_empty() || content.is_empty() {
        return None;
    }
    let updated_at = object
        .get("updated_at")
        .filter(|value| !value.is_null())
        .or_else(|| object.get("updatedAt"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let score = object
        .get("score")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite());
    Some(ExternalContextItem {
        id: id.to_owned(),
        content: content.to_owned(),
        title: object
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_owned),
        uri: object.get("uri").and_then(Value::as_str).map(str::to_owned),
        updated_at,
        score,
    })
}

fn parse_mem0_remember_result(response: &Value) -> RememberResult {
    let Some(object) = response.as_object() else {
        return RememberResult::Unknown;
    };
    let status = object.get("status").and_then(Value::as_str);
    let raw_operation_id = object.get("event_id");
    let operation_id = raw_operation_id
        .and_then(Value::as_str)
        .filter(|value| is_uuid(value))
        .map(str::to_owned);
    match status {
        Some("FAILED") => RememberResult::Failed,
        Some("PENDING") => operation_id
            .map(|provider_operation_id| RememberResult::Accepted {
                provider_operation_id,
            })
            .unwrap_or(RememberResult::Unknown),
        Some("SUCCEEDED") => {
            if raw_operation_id.is_some() && operation_id.is_none() {
                RememberResult::Unknown
            } else {
                RememberResult::Stored {
                    provider_operation_id: operation_id,
                }
            }
        }
        _ => RememberResult::Unknown,
    }
}

fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || ![8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
    {
        return false;
    }
    bytes
        .iter()
        .enumerate()
        .all(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) || byte.is_ascii_hexdigit())
}

pub fn write_result_json(result: &RememberResult) -> Value {
    match result {
        RememberResult::Stored {
            provider_operation_id: Some(provider_operation_id),
        } => json!({
            "status": "stored",
            "providerOperationId": provider_operation_id,
        }),
        RememberResult::Stored {
            provider_operation_id: None,
        } => json!({"status":"stored"}),
        RememberResult::Accepted {
            provider_operation_id,
        } => json!({
            "status": "accepted",
            "providerOperationId": provider_operation_id,
        }),
        RememberResult::Failed => json!({"status":"failed"}),
        RememberResult::Unknown => json!({"status":"unknown"}),
    }
}
