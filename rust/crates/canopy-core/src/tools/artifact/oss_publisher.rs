//! Native Aliyun OSS V1 publisher.

use std::collections::BTreeMap;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use md5::Md5;
use reqwest::StatusCode;
use sha1::{Digest, Sha1};

use crate::utils::cancellation::CancellationToken;

use super::publisher::{
    ArtifactOssConfig, ArtifactPublisher, ArtifactPublisherKind, PublishArtifactInput,
    PublishedArtifact, PublisherFuture,
};

const CONTENT_TYPE: &str = "text/html";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OssCredentials {
    pub access_key_id: String,
    pub access_key_secret: String,
    pub security_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OssPublisher {
    config: ArtifactOssConfig,
}

impl OssPublisher {
    pub fn new(config: ArtifactOssConfig) -> Self {
        Self { config }
    }
}

impl ArtifactPublisher for OssPublisher {
    fn kind(&self) -> ArtifactPublisherKind {
        ArtifactPublisherKind::Oss
    }

    fn publish<'a>(
        &'a self,
        input: &'a PublishArtifactInput,
        cancellation: &'a CancellationToken,
    ) -> PublisherFuture<'a> {
        Box::pin(async move {
            let bucket = self.config.bucket.trim();
            if bucket.is_empty() {
                return Err("artifact.oss.bucket is not configured.".to_owned());
            }
            let raw_endpoint = self.config.endpoint.trim();
            if raw_endpoint.is_empty() {
                return Err("artifact.oss.endpoint is not configured (e.g. \"oss-cn-hangzhou.aliyuncs.com\").".to_owned());
            }
            let endpoint = normalize_endpoint(raw_endpoint)?;
            let credentials = oss_credentials_from_env().ok_or_else(|| {
                "OSS credentials not found. Set OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET (or ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET).".to_owned()
            })?;
            let prefix = normalize_key_prefix(self.config.key_prefix.as_deref())?;
            let key = format!("{prefix}/{}/index.html", input.id);
            let public_base = normalize_public_base_url(self.config.public_base_url.as_deref())?;
            let acl = self.config.acl.as_deref().unwrap_or("public-read");
            let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            let content_md5 = BASE64.encode(Md5::digest(input.html.as_bytes()));
            let (authorization, oss_headers) = sign_oss_put(
                &credentials,
                bucket,
                &key,
                &content_md5,
                CONTENT_TYPE,
                &date,
                Some(acl),
            );
            let url = format!("https://{bucket}.{endpoint}/{key}");
            if cancellation.is_cancelled() {
                return Err("Artifact publishing was cancelled.".to_owned());
            }
            let request = reqwest::Client::new()
                .put(&url)
                .timeout(Duration::from_secs(60))
                .header("Date", date)
                .header("Content-MD5", content_md5)
                .header("Content-Type", CONTENT_TYPE)
                .header("Authorization", authorization)
                .headers(oss_headers)
                .body(input.html.clone())
                .send();
            let response = tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err("Artifact publishing was cancelled.".to_owned());
                }
                result = request => result.map_err(|error| {
                    format!("OSS upload to {url} failed: {error}")
                })?,
            };
            if !response.status().is_success() {
                let status: StatusCode = response.status();
                let reason = status.canonical_reason().unwrap_or("");
                return Err(format!("OSS upload failed: {status} {reason}")
                    .trim()
                    .to_owned());
            }
            drop(response);
            let public_url = public_base.map_or(url, |base| format!("{base}/{key}"));
            Ok(PublishedArtifact {
                id: input.id.clone(),
                url: public_url,
                file_path: None,
            })
        })
    }
}

pub fn oss_credentials_from_env() -> Option<OssCredentials> {
    let access_key_id = std::env::var("OSS_ACCESS_KEY_ID")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("ALIBABA_CLOUD_ACCESS_KEY_ID")
                .ok()
                .filter(|value| !value.is_empty())
        })?;
    let access_key_secret = std::env::var("OSS_ACCESS_KEY_SECRET")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
                .ok()
                .filter(|value| !value.is_empty())
        })?;
    let security_token = std::env::var("OSS_SESSION_TOKEN")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("ALIBABA_CLOUD_SECURITY_TOKEN")
                .ok()
                .filter(|value| !value.is_empty())
        });
    Some(OssCredentials {
        access_key_id,
        access_key_secret,
        security_token,
    })
}

pub fn sign_oss_put(
    credentials: &OssCredentials,
    bucket: &str,
    key: &str,
    content_md5: &str,
    content_type: &str,
    date: &str,
    acl: Option<&str>,
) -> (String, reqwest::header::HeaderMap) {
    let mut oss_headers = BTreeMap::<String, String>::new();
    if let Some(acl) = acl {
        oss_headers.insert("x-oss-object-acl".to_owned(), acl.to_owned());
    }
    if let Some(token) = credentials.security_token.as_deref() {
        oss_headers.insert("x-oss-security-token".to_owned(), token.to_owned());
    }
    let canonicalized_headers = oss_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let canonicalized_resource = format!("/{bucket}/{key}");
    let string_to_sign = format!(
        "PUT\n{content_md5}\n{content_type}\n{date}\n{canonicalized_headers}{canonicalized_resource}"
    );
    let signature = hmac_sha1_base64(
        credentials.access_key_secret.as_bytes(),
        string_to_sign.as_bytes(),
    );
    let authorization = format!("OSS {}:{signature}", credentials.access_key_id);
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in oss_headers {
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
    (authorization, headers)
}

fn hmac_sha1_base64(key: &[u8], message: &[u8]) -> String {
    const BLOCK_SIZE: usize = 64;
    let mut normalized_key = [0u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        let digest = Sha1::digest(key);
        normalized_key[..digest.len()].copy_from_slice(&digest);
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36u8; BLOCK_SIZE];
    let mut outer_pad = [0x5cu8; BLOCK_SIZE];
    for index in 0..BLOCK_SIZE {
        inner_pad[index] ^= normalized_key[index];
        outer_pad[index] ^= normalized_key[index];
    }
    let mut inner = Sha1::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha1::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    BASE64.encode(outer.finalize())
}

fn normalize_endpoint(raw: &str) -> Result<String, String> {
    let endpoint = raw
        .trim()
        .strip_prefix("https://")
        .or_else(|| raw.trim().strip_prefix("http://"))
        .unwrap_or(raw.trim())
        .trim_end_matches('/');
    let valid = endpoint.to_ascii_lowercase();
    if !valid.ends_with(".aliyuncs.com")
        || valid.is_empty()
        || !valid.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '.' || character == '-'
        })
    {
        return Err(format!(
            "artifact.oss.endpoint does not look like a valid Aliyun OSS endpoint: {endpoint}"
        ));
    }
    Ok(endpoint.to_owned())
}

fn normalize_key_prefix(raw: Option<&str>) -> Result<String, String> {
    let prefix = raw.unwrap_or("").trim_matches('/');
    let prefix = if prefix.is_empty() && raw.is_none_or(str::is_empty) {
        "artifacts"
    } else {
        prefix
    };
    if prefix.is_empty() {
        return Err(
            "artifact.oss.keyPrefix must not be empty or \"/\" after stripping slashes.".to_owned(),
        );
    }
    if prefix.chars().any(|character| {
        character == '#' || character == '?' || character == '%' || character.is_whitespace()
    }) {
        return Err("artifact.oss.keyPrefix must not contain #, ?, %, or whitespace.".to_owned());
    }
    Ok(prefix.to_owned())
}

fn normalize_public_base_url(raw: Option<&str>) -> Result<Option<String>, String> {
    let Some(base) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if !base.to_ascii_lowercase().starts_with("http://")
        && !base.to_ascii_lowercase().starts_with("https://")
    {
        return Err("artifact.oss.publicBaseUrl must start with http:// or https://.".to_owned());
    }
    Ok(Some(base.trim_end_matches('/').to_owned()))
}
