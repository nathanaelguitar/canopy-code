//! Bounded image generation and safe PNG retrieval from OpenAI-compatible
//! image providers.

use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::time::Duration;

use reqwest::header::{ACCEPT, LOCATION};
use reqwest::redirect::Policy;
use reqwest::{Client, Response, StatusCode, Url};
use serde_json::{Value, json};

use crate::extension_network_policy::{
    AddressResolveFuture, ExtensionAddressResolver, ExtensionNetworkPolicy, NetworkPolicyError,
    ResolvedNetworkTarget, is_blocked_address, resolve_network_target,
};
use crate::utils::cancellation::{CancellationReason, CancellationToken};

const GENERATION_TIMEOUT: Duration = Duration::from_secs(240);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_API_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_IMAGE_BYTES: usize = 10 * 1024 * 1024;
const MAX_DOWNLOAD_REDIRECTS: usize = 3;
const PNG_SIGNATURE: &[u8; 8] = b"\x89PNG\r\n\x1a\n";

const INVALID_BASE_URL: &str =
    "Image generation baseUrl must be a valid HTTPS URL without credentials, query, or fragment.";
const INVALID_RESULT_URL: &str = "Image generation returned an invalid image URL.";
const UNSAFE_RESULT_URL: &str =
    "Image generation returned an image URL that is not a safe public HTTPS URL.";
const DOWNLOAD_FAILED: &str = "Generated image download failed before completion.";

/// Input accepted by the image generation service.
pub struct ImageGenerationRequest<'a> {
    pub base_url: &'a str,
    pub api_key: &'a str,
    pub model: &'a str,
    pub prompt: &'a str,
    pub size: Option<&'a str>,
}

/// A downloaded PNG and the provider's optional request identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedImage {
    pub bytes: Vec<u8>,
    pub mime_type: &'static str,
    pub request_id: Option<String>,
}

/// Errors use the same user-facing messages as the TypeScript image service.
#[derive(Debug, thiserror::Error)]
pub enum ImageGenerationError {
    #[error("{INVALID_BASE_URL}")]
    InvalidBaseUrl,
    #[error("Image generation request failed: {0}")]
    RequestFailed(String),
    #[error("Image generation endpoint returned malformed JSON (HTTP {0}).")]
    MalformedJson(u16),
    #[error("Response exceeds the {0}-byte limit.")]
    ResponseTooLarge(usize),
    #[error("Could not read the image generation response.")]
    ResponseReadFailed,
    #[error("Image generation response did not contain an image URL.")]
    MissingImageUrl,
    #[error("{0}")]
    Provider(String),
    #[error("{INVALID_RESULT_URL}")]
    InvalidResultUrl,
    #[error("{UNSAFE_RESULT_URL}")]
    UnsafeResultUrl,
    #[error("The operation was aborted.")]
    Aborted,
    #[error("{0}")]
    AbortedWithReason(String),
    #[error("{DOWNLOAD_FAILED}")]
    DownloadFailed,
    #[error("Generated image redirect is missing a Location header.")]
    MissingRedirectLocation,
    #[error("Generated image redirect URL is invalid.")]
    InvalidRedirectUrl,
    #[error("Generated image download failed with HTTP {0}.")]
    DownloadHttpStatus(u16),
    #[error("Generated image download exceeded {MAX_DOWNLOAD_REDIRECTS} redirects.")]
    TooManyRedirects,
    #[error("Downloaded result is not a valid PNG image.")]
    InvalidPng,
}

/// Resolver backed by the platform DNS configuration.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemAddressResolver;

impl ExtensionAddressResolver for SystemAddressResolver {
    type Error = io::Error;

    fn resolve_all<'a>(&'a self, hostname: &'a str) -> AddressResolveFuture<'a, Self::Error> {
        Box::pin(async move {
            tokio::net::lookup_host((hostname, 443))
                .await
                .map(|addresses| addresses.map(|address| address.ip()).collect())
        })
    }
}

/// Normalize the configured provider URL. The generation endpoint is an
/// explicitly configured service; public-network DNS validation is applied to
/// provider-returned image URLs before they are downloaded.
pub fn normalize_image_generation_base_url(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let url = Url::parse(value).ok()?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some_and(|password| !password.is_empty())
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    Some(url.as_str().trim_end_matches('/').to_owned())
}

/// Generate one image and safely download the first returned PNG URL.
pub async fn generate_image(
    request: &ImageGenerationRequest<'_>,
    cancellation: Option<&CancellationToken>,
) -> Result<GeneratedImage, ImageGenerationError> {
    generate_image_with_resolver(request, &SystemAddressResolver, cancellation).await
}

/// Variant that accepts the address resolver used by the existing public
/// network policy. Each image URL and redirect is resolved and pinned anew.
pub async fn generate_image_with_resolver<R: ExtensionAddressResolver + ?Sized>(
    request: &ImageGenerationRequest<'_>,
    resolver: &R,
    cancellation: Option<&CancellationToken>,
) -> Result<GeneratedImage, ImageGenerationError> {
    let base_url = normalize_image_generation_base_url(Some(request.base_url))
        .ok_or(ImageGenerationError::InvalidBaseUrl)?;
    let generation_url = generation_url(&base_url)?;
    let deadline = tokio::time::Instant::now() + GENERATION_TIMEOUT;
    let client = Client::builder()
        .redirect(Policy::none())
        .timeout(GENERATION_TIMEOUT)
        .build()
        .map_err(|error| ImageGenerationError::RequestFailed(error.to_string()))?;

    let mut parameters = json!({
        "n": 1,
        "prompt_extend": true,
        "watermark": false,
    });
    if let Some(size) = request.size.filter(|size| !size.is_empty()) {
        parameters["size"] = json!(size);
    }
    let body = json!({
        "model": request.model,
        "input": {
            "messages": [{
                "role": "user",
                "content": [{"text": request.prompt}],
            }],
        },
        "parameters": parameters,
    });
    let response = cancellable(
        cancellation,
        deadline,
        ImageGenerationError::RequestFailed("request timed out".to_owned()),
        request_abort_error,
        async {
            client
                .post(generation_url)
                .bearer_auth(request.api_key)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|error| ImageGenerationError::RequestFailed(error.to_string()))
        },
    )
    .await?;

    let status = response.status();
    let payload = if !status.is_success() {
        cancellable(
            cancellation,
            deadline,
            ImageGenerationError::AbortedWithReason(timeout_abort_message().to_owned()),
            abort_error,
            read_json_response(response, MAX_API_RESPONSE_BYTES),
        )
        .await
        .unwrap_or(Value::Null)
    } else {
        cancellable(
            cancellation,
            deadline,
            ImageGenerationError::AbortedWithReason(timeout_abort_message().to_owned()),
            abort_error,
            read_json_response(response, MAX_API_RESPONSE_BYTES),
        )
        .await?
    };
    if !status.is_success() {
        return Err(ImageGenerationError::Provider(
            format_image_generation_error(status.as_u16(), &payload),
        ));
    }

    let image_url =
        find_generated_image_url(&payload).ok_or(ImageGenerationError::MissingImageUrl)?;
    let bytes = download_png(image_url, resolver, cancellation).await?;
    Ok(GeneratedImage {
        bytes,
        mime_type: "image/png",
        request_id: read_string(&payload, &["request_id", "requestId"]),
    })
}

fn generation_url(base_url: &str) -> Result<Url, ImageGenerationError> {
    const ENDPOINT: &str = "/services/aigc/multimodal-generation/generation";
    let value = if base_url.ends_with(ENDPOINT) {
        base_url.to_owned()
    } else {
        format!("{}{}", base_url.trim_end_matches('/'), ENDPOINT)
    };
    Url::parse(&value).map_err(|_| ImageGenerationError::InvalidBaseUrl)
}

async fn read_json_response(
    response: Response,
    limit: usize,
) -> Result<Value, ImageGenerationError> {
    let status = response.status().as_u16();
    let bytes = read_bounded_body(response, limit).await?;
    if bytes.is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_slice(&bytes).map_err(|_| ImageGenerationError::MalformedJson(status))
}

async fn read_bounded_body(
    mut response: Response,
    limit: usize,
) -> Result<Vec<u8>, ImageGenerationError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ImageGenerationError::ResponseTooLarge(limit));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ImageGenerationError::ResponseReadFailed)?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ImageGenerationError::ResponseTooLarge(limit));
        }
        body.try_reserve(chunk.len())
            .map_err(|_| ImageGenerationError::ResponseReadFailed)?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn format_image_generation_error(status: u16, payload: &Value) -> String {
    let code = read_string(payload, &["code"]);
    let message = read_string(payload, &["message"]);
    let suffix = [code.as_deref(), message.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(": ");
    let has_suffix = !suffix.is_empty();
    let lowered = format!(
        "{} {}",
        code.as_deref().unwrap_or_default(),
        message.as_deref().unwrap_or_default()
    )
    .to_ascii_lowercase();

    if status == 429 || lowered.contains("throttl") || contains_rate_limit(&lowered) {
        return format!(
            "Image generation rate limit reached{}.",
            if has_suffix {
                format!(" ({suffix})")
            } else {
                String::new()
            }
        );
    }
    if status == 401
        || status == 403
        || code.as_deref().is_some_and(|code| {
            let lower = code.to_ascii_lowercase();
            lower.contains("access") || lower.contains("permission")
        })
    {
        return format!(
            "Image generation access denied{}{}. Check the API key, endpoint, and model access.",
            if has_suffix { " (" } else { "" },
            if has_suffix {
                format!("{suffix})")
            } else {
                String::new()
            }
        );
    }
    if code
        .as_deref()
        .is_some_and(|code| code.to_ascii_lowercase().contains("datainspectionfailed"))
    {
        return match message {
            Some(message) => format!(
                "The image generation endpoint blocked the prompt during content moderation: {message}"
            ),
            None => "The image generation endpoint blocked the prompt during content moderation."
                .to_owned(),
        };
    }
    format!(
        "Image generation failed with HTTP {status}{}.",
        if has_suffix {
            format!(" ({suffix})")
        } else {
            String::new()
        }
    )
}

fn contains_rate_limit(value: &str) -> bool {
    value.match_indices("rate").any(|(index, _)| {
        let suffix = &value[index + "rate".len()..];
        if suffix.starts_with("limit") {
            return true;
        }
        let Some((separator_index, separator)) = suffix.char_indices().next() else {
            return false;
        };
        !matches!(separator, '\n' | '\r' | '\u{2028}' | '\u{2029}')
            && suffix[separator_index + separator.len_utf8()..].starts_with("limit")
    })
}

fn find_generated_image_url(payload: &Value) -> Option<String> {
    payload
        .get("output")?
        .get("choices")?
        .as_array()?
        .iter()
        .filter_map(|choice| choice.get("message"))
        .filter_map(|message| message.get("content"))
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(|part| part.get("image").and_then(Value::as_str))
        .map(str::trim)
        .find(|image| !image.is_empty())
        .map(str::to_owned)
}

async fn download_png<R: ExtensionAddressResolver + ?Sized>(
    image_url: String,
    resolver: &R,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, ImageGenerationError> {
    let deadline = tokio::time::Instant::now() + DOWNLOAD_TIMEOUT;
    cancellable(
        cancellation,
        deadline,
        ImageGenerationError::DownloadFailed,
        |_| ImageGenerationError::Aborted,
        download_png_inner(image_url, resolver, cancellation, deadline),
    )
    .await
}

async fn download_png_inner<R: ExtensionAddressResolver + ?Sized>(
    image_url: String,
    resolver: &R,
    cancellation: Option<&CancellationToken>,
    deadline: tokio::time::Instant,
) -> Result<Vec<u8>, ImageGenerationError> {
    let mut current_target =
        validate_result_url(&image_url, resolver, cancellation, deadline).await?;

    for redirect_count in 0..=MAX_DOWNLOAD_REDIRECTS {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ImageGenerationError::DownloadFailed);
        }
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .timeout(remaining);
        if let Some(pin) = &current_target.pin {
            builder = pin.apply_to_client_builder(builder).no_proxy();
        }
        let client = builder
            .build()
            .map_err(|_| ImageGenerationError::DownloadFailed)?;
        let response = client
            .get(current_target.url.clone())
            .header(ACCEPT, "image/png")
            .send()
            .await
            .map_err(|_| ImageGenerationError::DownloadFailed)?;

        if is_redirect(response.status()) {
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
                .ok_or(ImageGenerationError::MissingRedirectLocation);
            drop(response);
            let location = location?;
            let redirect_url = current_target
                .url
                .join(&location)
                .map_err(|_| ImageGenerationError::InvalidRedirectUrl)?;
            current_target =
                validate_result_url(redirect_url.as_str(), resolver, cancellation, deadline)
                    .await?;
            if redirect_count == MAX_DOWNLOAD_REDIRECTS {
                return Err(ImageGenerationError::TooManyRedirects);
            }
            continue;
        }
        if !response.status().is_success() {
            return Err(ImageGenerationError::DownloadHttpStatus(
                response.status().as_u16(),
            ));
        }
        let bytes = match read_bounded_body(response, MAX_IMAGE_BYTES).await {
            Ok(bytes) => bytes,
            Err(ImageGenerationError::ResponseTooLarge(limit)) => {
                return Err(ImageGenerationError::ResponseTooLarge(limit));
            }
            Err(_) => return Err(ImageGenerationError::DownloadFailed),
        };
        if bytes.len() < PNG_SIGNATURE.len() || &bytes[..PNG_SIGNATURE.len()] != PNG_SIGNATURE {
            return Err(ImageGenerationError::InvalidPng);
        }
        return Ok(bytes);
    }
    Err(ImageGenerationError::TooManyRedirects)
}

async fn validate_result_url<R: ExtensionAddressResolver + ?Sized>(
    value: &str,
    resolver: &R,
    cancellation: Option<&CancellationToken>,
    deadline: tokio::time::Instant,
) -> Result<ResolvedNetworkTarget, ImageGenerationError> {
    let url = Url::parse(value).map_err(|_| ImageGenerationError::InvalidResultUrl)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some_and(|password| !password.is_empty())
        || is_private_host(&url)
    {
        return Err(ImageGenerationError::UnsafeResultUrl);
    }
    let result = cancellable(
        cancellation,
        deadline,
        ImageGenerationError::AbortedWithReason(timeout_abort_message().to_owned()),
        abort_error,
        async {
            resolve_network_target(
                url.as_str(),
                Some(ExtensionNetworkPolicy::Public),
                resolver,
                cancellation,
            )
            .await
            .map_err(|error| match error {
                NetworkPolicyError::Cancelled(reason) => abort_from_reason(reason),
                _ => ImageGenerationError::UnsafeResultUrl,
            })
        },
    )
    .await?;
    Ok(result)
}

fn is_private_host(url: &Url) -> bool {
    let Some(hostname) = url.host_str() else {
        return true;
    };
    let hostname = hostname.to_ascii_lowercase();
    let unbracketed = hostname.trim_start_matches('[').trim_end_matches(']');
    if hostname == "localhost"
        || hostname.ends_with(".localhost")
        || hostname == "host.docker.internal"
        || (!hostname.contains('.') && !hostname.starts_with('['))
        || [".local", ".internal", ".lan", ".home.arpa"]
            .iter()
            .any(|suffix| hostname.ends_with(suffix))
    {
        return true;
    }
    unbracketed.parse::<IpAddr>().is_ok_and(is_blocked_address)
}

fn read_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_owned)
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

async fn cancellable<T, F>(
    cancellation: Option<&CancellationToken>,
    deadline: tokio::time::Instant,
    timeout_error: ImageGenerationError,
    cancel_map: fn(Option<CancellationReason>) -> ImageGenerationError,
    future: F,
) -> Result<T, ImageGenerationError>
where
    F: Future<Output = Result<T, ImageGenerationError>>,
{
    tokio::select! {
        biased;
        reason = wait_for_cancellation(cancellation) => Err(cancel_map(reason)),
        result = tokio::time::timeout_at(deadline, future) => {
            result.unwrap_or(Err(timeout_error))
        }
    }
}

async fn wait_for_cancellation(
    cancellation: Option<&CancellationToken>,
) -> Option<CancellationReason> {
    match cancellation {
        Some(cancellation) => cancellation.cancelled().await,
        None => std::future::pending().await,
    }
}

fn abort_error(reason: Option<CancellationReason>) -> ImageGenerationError {
    match reason {
        Some(CancellationReason::Explicit(reason)) => {
            ImageGenerationError::AbortedWithReason(reason.to_string())
        }
        Some(CancellationReason::Timeout) => {
            ImageGenerationError::AbortedWithReason(timeout_abort_message().to_owned())
        }
        None => ImageGenerationError::Aborted,
    }
}

fn request_abort_error(reason: Option<CancellationReason>) -> ImageGenerationError {
    ImageGenerationError::RequestFailed(abort_message(reason))
}

fn abort_from_reason(reason: Option<CancellationReason>) -> ImageGenerationError {
    abort_error(reason)
}

fn abort_message(reason: Option<CancellationReason>) -> String {
    match reason {
        Some(CancellationReason::Explicit(reason)) => reason.to_string(),
        Some(CancellationReason::Timeout) => timeout_abort_message().to_owned(),
        None => "The operation was aborted.".to_owned(),
    }
}

fn timeout_abort_message() -> &'static str {
    "The operation was aborted due to timeout."
}
