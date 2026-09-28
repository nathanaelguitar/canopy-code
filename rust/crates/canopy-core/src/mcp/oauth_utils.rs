use futures_util::StreamExt;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

const MAX_METADATA_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum OAuthUrlError {
    #[error("invalid OAuth URL: {0}")]
    InvalidUrl(String),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OAuthAuthorizationServerMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_endpoint_auth_methods_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_endpoint_auth_methods_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_types_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_types_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_challenge_methods_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes_supported: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OAuthProtectedResourceMetadata {
    pub resource: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization_servers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_methods_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_documentation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_signing_alg_values_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_encryption_alg_values_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_encryption_enc_values_supported: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scopes_supported: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpOAuthConfig {
    pub authorization_url: String,
    pub token_url: String,
    pub scopes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WellKnownUrls {
    pub protected_resource: String,
    pub authorization_server: String,
}

pub struct OAuthUtils;

impl OAuthUtils {
    pub fn build_well_known_urls(
        base_url: &str,
        include_path_suffix: bool,
    ) -> Result<WellKnownUrls, OAuthUrlError> {
        let parsed = parse_url(base_url)?;
        let suffix = if include_path_suffix {
            parsed.path().trim_end_matches('/').to_owned()
        } else {
            String::new()
        };
        let protected_path = format!("/.well-known/oauth-protected-resource{suffix}");
        let auth_path = format!("/.well-known/oauth-authorization-server{suffix}");
        Ok(WellKnownUrls {
            protected_resource: with_path(&parsed, &protected_path),
            authorization_server: with_path(&parsed, &auth_path),
        })
    }

    pub fn metadata_to_oauth_config(metadata: &OAuthAuthorizationServerMetadata) -> McpOAuthConfig {
        McpOAuthConfig {
            authorization_url: metadata.authorization_endpoint.clone(),
            token_url: metadata.token_endpoint.clone(),
            scopes: metadata.scopes_supported.clone().unwrap_or_default(),
            registration_url: metadata.registration_endpoint.clone(),
        }
    }

    pub async fn fetch_protected_resource_metadata(
        client: &Client,
        metadata_url: &str,
    ) -> Option<OAuthProtectedResourceMetadata> {
        fetch_json(client, metadata_url).await
    }

    pub async fn fetch_authorization_server_metadata(
        client: &Client,
        metadata_url: &str,
    ) -> Option<OAuthAuthorizationServerMetadata> {
        fetch_json(client, metadata_url).await
    }

    pub async fn discover_authorization_server_metadata(
        client: &Client,
        auth_server_url: &str,
    ) -> Result<Option<OAuthAuthorizationServerMetadata>, OAuthUrlError> {
        let server_url = parse_url(auth_server_url)?;
        let path = server_url.path();
        let mut endpoints = Vec::with_capacity(5);

        if path != "/" {
            endpoints.push(with_path(
                &server_url,
                &format!("/.well-known/oauth-authorization-server{path}"),
            ));
            endpoints.push(with_path(
                &server_url,
                &format!("/.well-known/openid-configuration{path}"),
            ));
            endpoints.push(with_path(
                &server_url,
                &format!("{path}/.well-known/openid-configuration"),
            ));
        }

        endpoints.push(with_path(
            &server_url,
            "/.well-known/oauth-authorization-server",
        ));
        endpoints.push(with_path(&server_url, "/.well-known/openid-configuration"));

        for endpoint in endpoints {
            if let Some(metadata) =
                Self::fetch_authorization_server_metadata(client, &endpoint).await
            {
                return Ok(Some(metadata));
            }
        }
        Ok(None)
    }

    pub async fn discover_oauth_config(
        client: &Client,
        server_url: &str,
    ) -> Option<McpOAuthConfig> {
        async {
            let root_urls = Self::build_well_known_urls(server_url, false).ok()?;
            let mut resource =
                Self::fetch_protected_resource_metadata(client, &root_urls.protected_resource)
                    .await;

            if resource.is_none() {
                let parsed = parse_url(server_url).ok()?;
                if parsed.path() != "/" {
                    let path_urls = Self::build_well_known_urls(server_url, true).ok()?;
                    resource = Self::fetch_protected_resource_metadata(
                        client,
                        &path_urls.protected_resource,
                    )
                    .await;
                }
            }

            if let Some(resource) = resource {
                if let Some(auth_server) = resource
                    .authorization_servers
                    .as_ref()
                    .and_then(|servers| servers.first())
                {
                    if let Some(metadata) =
                        Self::discover_authorization_server_metadata(client, auth_server)
                            .await
                            .ok()?
                    {
                        let mut config = Self::metadata_to_oauth_config(&metadata);
                        if let Some(scopes) = resource
                            .scopes_supported
                            .filter(|scopes| !scopes.is_empty())
                        {
                            config.scopes = scopes;
                        }
                        return Some(config);
                    }
                }
            }

            let metadata = Self::discover_authorization_server_metadata(client, server_url)
                .await
                .ok()??;
            Some(Self::metadata_to_oauth_config(&metadata))
        }
        .await
    }

    pub fn parse_www_authenticate_header(header: &str) -> Option<String> {
        split_auth_params(header)
            .iter()
            .find_map(|param| parse_resource_metadata_param(param))
    }

    pub async fn discover_oauth_from_www_authenticate(
        client: &Client,
        www_authenticate: &str,
    ) -> Result<Option<McpOAuthConfig>, OAuthUrlError> {
        let Some(resource_metadata_url) = Self::parse_www_authenticate_header(www_authenticate)
        else {
            return Ok(None);
        };
        let Some(resource) =
            Self::fetch_protected_resource_metadata(client, &resource_metadata_url).await
        else {
            return Ok(None);
        };
        let Some(auth_server) = resource
            .authorization_servers
            .as_ref()
            .and_then(|servers| servers.first())
        else {
            return Ok(None);
        };
        let Some(metadata) =
            Self::discover_authorization_server_metadata(client, auth_server).await?
        else {
            return Ok(None);
        };
        let mut config = Self::metadata_to_oauth_config(&metadata);
        if let Some(scopes) = resource
            .scopes_supported
            .filter(|scopes| !scopes.is_empty())
        {
            config.scopes = scopes;
        }
        Ok(Some(config))
    }

    pub fn extract_base_url(mcp_server_url: &str) -> Result<String, OAuthUrlError> {
        origin(&parse_url(mcp_server_url)?)
    }

    pub fn is_sse_endpoint(url: &str) -> bool {
        url.contains("/sse") || !url.contains("/mcp")
    }

    pub fn build_resource_parameter(endpoint_url: &str) -> Result<String, OAuthUrlError> {
        let url = parse_url(endpoint_url)?;
        let path = if url.path() == "/" { "" } else { url.path() };
        let mut canonical = format!("{}{}", origin(&url)?, path);
        if canonical.ends_with('/') && !path.is_empty() {
            canonical.pop();
        }
        Ok(canonical)
    }
}

async fn fetch_json<T>(client: &Client, url: &str) -> Option<T>
where
    T: for<'de> Deserialize<'de>,
{
    let response = client.get(url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_METADATA_BODY_BYTES as u64)
    {
        return None;
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::with_capacity(4096);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        if body.len().saturating_add(chunk.len()) > MAX_METADATA_BODY_BYTES {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).ok()
}

fn parse_url(value: &str) -> Result<Url, OAuthUrlError> {
    let url = Url::parse(value).map_err(|error| OAuthUrlError::InvalidUrl(error.to_string()))?;
    if url.host_str().is_none() {
        return Err(OAuthUrlError::InvalidUrl("URL has no host".to_owned()));
    }
    Ok(url)
}

fn origin(url: &Url) -> Result<String, OAuthUrlError> {
    if url.host_str().is_none() {
        return Err(OAuthUrlError::InvalidUrl("URL has no host".to_owned()));
    }
    let mut origin = url.clone();
    origin
        .set_username("")
        .map_err(|_| OAuthUrlError::InvalidUrl("could not remove URL user info".to_owned()))?;
    origin
        .set_password(None)
        .map_err(|_| OAuthUrlError::InvalidUrl("could not remove URL password".to_owned()))?;
    origin.set_path("/");
    origin.set_query(None);
    origin.set_fragment(None);
    Ok(origin.as_str().trim_end_matches('/').to_owned())
}

fn with_path(template: &Url, path: &str) -> String {
    let mut url = template.clone();
    url.set_username("").ok();
    url.set_password(None).ok();
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn split_auth_params(value: &str) -> Vec<&str> {
    let mut params = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;

    for (index, ch) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if let Some(quoted) = quote {
            if ch == '\\' {
                escaped = true;
            } else if ch == quoted {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' if value[start..index].trim_end().ends_with('=') => quote = Some(ch),
            ',' => {
                params.push(value[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    params.push(value[start..].trim());
    params
}

fn parse_resource_metadata_param(raw_param: &str) -> Option<String> {
    let param = strip_bearer_prefix(raw_param.trim());
    let split_at = param.find('=')?;
    if param[..split_at].trim() != "resource_metadata" {
        return None;
    }
    let rest = param[split_at + 1..].trim_start();
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let mut value = String::new();
    let mut escaped = false;
    let mut closing = None;
    for (offset, ch) in rest.char_indices().skip(1) {
        if escaped {
            value.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == quote {
            closing = Some(offset + ch.len_utf8());
            break;
        } else {
            value.push(ch);
        }
    }
    let closing = closing?;
    if !rest[closing..].trim().is_empty() || value.is_empty() {
        return None;
    }
    Some(value)
}

fn strip_bearer_prefix(value: &str) -> &str {
    let Some(scheme) = value.get(..6) else {
        return value;
    };
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return value;
    }
    let Some(rest) = value.get(6..) else {
        return value;
    };
    if !rest.chars().next().is_some_and(char::is_whitespace) {
        return value;
    }
    rest.trim_start()
}

/// Construct the bounded HTTP client used for OAuth metadata discovery.
pub fn oauth_metadata_client() -> Result<Client, reqwest::Error> {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(20))
        .build()
}

#[cfg(test)]
mod tests {
    use super::OAuthUtils;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    async fn serve_json(mut stream: TcpStream, body: &str) {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        while line != "\r\n" && !line.is_empty() {
            line.clear();
            reader.read_line(&mut line).await.unwrap();
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        reader
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .unwrap();
    }

    #[test]
    fn creates_standard_and_path_based_well_known_urls() {
        let root = OAuthUtils::build_well_known_urls("https://example.com/mcp", false).unwrap();
        assert_eq!(
            root.protected_resource,
            "https://example.com/.well-known/oauth-protected-resource"
        );
        let path = OAuthUtils::build_well_known_urls("https://example.com/mcp/", true).unwrap();
        assert_eq!(
            path.protected_resource,
            "https://example.com/.well-known/oauth-protected-resource/mcp"
        );
        assert_eq!(
            path.authorization_server,
            "https://example.com/.well-known/oauth-authorization-server/mcp"
        );
    }

    #[test]
    fn parses_www_authenticate_values_without_splitting_quoted_commas() {
        assert_eq!(
            OAuthUtils::parse_www_authenticate_header(
                "Bearer realm=\"a,b\", resource_metadata = \"https://example.com/meta?name=o'hara\""
            ),
            Some("https://example.com/meta?name=o'hara".to_owned())
        );
        assert_eq!(
            OAuthUtils::parse_www_authenticate_header(
                "Bearer ext=can't, resource_metadata='https://example.com/meta'"
            ),
            Some("https://example.com/meta".to_owned())
        );
        assert_eq!(
            OAuthUtils::parse_www_authenticate_header(
                "Bearer resource_metadata=https://example.com/meta"
            ),
            None
        );
        assert_eq!(
            OAuthUtils::parse_www_authenticate_header(
                "Bearer error_description=\"missing, resource_metadata='https://example.com/meta'\""
            ),
            None
        );
    }

    #[test]
    fn builds_canonical_resource_and_classifies_sse_urls() {
        assert_eq!(
            OAuthUtils::build_resource_parameter("https://example.com:8080/mcp/?x=1#part").unwrap(),
            "https://example.com:8080/mcp"
        );
        assert_eq!(
            OAuthUtils::build_resource_parameter("https://example.com/").unwrap(),
            "https://example.com"
        );
        assert_eq!(
            OAuthUtils::build_resource_parameter("https://[::1]:8443/").unwrap(),
            "https://[::1]:8443"
        );
        assert!(OAuthUtils::is_sse_endpoint("https://example.com/api/sse"));
        assert!(OAuthUtils::is_sse_endpoint("https://example.com/api"));
        assert!(!OAuthUtils::is_sse_endpoint(
            "https://example.com/api/mcp/v1"
        ));
    }

    #[test]
    fn extracts_origin_including_non_default_port() {
        assert_eq!(
            OAuthUtils::extract_base_url("https://example.com:8080/mcp/v1").unwrap(),
            "https://example.com:8080"
        );
    }

    #[tokio::test]
    async fn discovers_oauth_and_prefers_protected_resource_scopes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_url = format!("http://{address}");
        let resource = json!({
            "resource": server_url.clone(),
            "authorization_servers": [server_url.clone()],
            "scopes_supported": ["mcp-read", "mcp-write"]
        })
        .to_string();
        let authorization = json!({
            "issuer": server_url,
            "authorization_endpoint": format!("{server_url}/authorize"),
            "token_endpoint": format!("{server_url}/token"),
            "scopes_supported": ["read", "write", "admin"]
        })
        .to_string();

        let server = tokio::spawn(async move {
            for body in [&resource, &authorization] {
                let (stream, _) = listener.accept().await.unwrap();
                serve_json(stream, body).await;
            }
        });
        let client = super::oauth_metadata_client().unwrap();
        let config = OAuthUtils::discover_oauth_config(&client, &server_url).await;
        server.await.unwrap();

        let config = config.expect("metadata should be discovered");
        assert_eq!(config.scopes, ["mcp-read", "mcp-write"]);
        assert_eq!(config.authorization_url, format!("{server_url}/authorize"));
        assert_eq!(config.token_url, format!("{server_url}/token"));
    }
}
