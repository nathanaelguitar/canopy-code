//! Weixin image send flow.
//!
//! Port of `sendImage` from `packages/channels/weixin/src/send.ts`. It uses
//! the existing path allowlist, crypto helpers, and API upload/send functions.

use super::weixin_api::{
    ApiHttpClient, ApiRuntime, WeixinApiError, get_upload_url, get_upload_url_with_http,
    send_message, send_message_with_http, upload_to_cdn, upload_to_cdn_with_http,
};
use super::weixin_media::{WeixinMediaError, compute_md5, encrypt_aes_ecb};
use super::weixin_send_utils::validate_image_path;
use super::weixin_types::{
    CDNMedia, ImageItem, MessageItem, MessageItemType, MessageState, MessageType, WeixinMessage,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::Client;
use serde_json::to_value;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const IMAGE_UPLOAD_TIMEOUT: Duration = Duration::from_secs(40);

/// Send-image randomness seam, returning a fresh 16-byte key per call.
pub trait ImageSendRandom: Send + Sync {
    fn random_16_bytes(&self) -> [u8; 16];
}

struct SystemImageSendRandom;

impl ImageSendRandom for SystemImageSendRandom {
    fn random_16_bytes(&self) -> [u8; 16] {
        // UUID v4 is the existing system CSPRNG path used by the API runtime.
        // The first four bytes contain no fixed version/variant bits. Taking
        // them from four independent UUIDs yields 16 random bytes.
        let mut bytes = [0_u8; 16];
        for chunk in bytes.chunks_exact_mut(4) {
            chunk.copy_from_slice(&Uuid::new_v4().as_bytes()[..4]);
        }
        bytes
    }
}

/// Inputs for the four-step image upload and message flow.
#[derive(Clone, Copy, Debug)]
pub struct SendImageParams<'a> {
    pub to: &'a str,
    pub image_path: &'a Path,
    pub base_url: &'a str,
    pub token: &'a str,
    pub context_token: &'a str,
    pub workspace_dirs: &'a [PathBuf],
}

#[derive(Debug)]
pub enum SendImageError {
    InvalidImage(String),
    ReadImage(io::Error),
    Media(WeixinMediaError),
    Api(WeixinApiError),
}

impl fmt::Display for SendImageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidImage(error) => formatter.write_str(error),
            Self::ReadImage(error) => write!(formatter, "{error}"),
            Self::Media(error) => write!(formatter, "{error}"),
            Self::Api(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for SendImageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidImage(_) => None,
            Self::ReadImage(error) => Some(error),
            Self::Media(error) => Some(error),
            Self::Api(error) => Some(error),
        }
    }
}

impl From<WeixinMediaError> for SendImageError {
    fn from(error: WeixinMediaError) -> Self {
        Self::Media(error)
    }
}

impl From<WeixinApiError> for SendImageError {
    fn from(error: WeixinApiError) -> Self {
        Self::Api(error)
    }
}

impl From<io::Error> for SendImageError {
    fn from(error: io::Error) -> Self {
        Self::ReadImage(error)
    }
}

/// Send an image through the production Weixin API client.
pub async fn send_image(
    client: &Client,
    params: SendImageParams<'_>,
) -> Result<(), SendImageError> {
    let random = SystemImageSendRandom;
    let prepared = prepare_image(&random, params)?;

    let upload_param = get_upload_url(
        client,
        params.base_url,
        params.token,
        params.to,
        &prepared.filekey,
        prepared.rawsize,
        &prepared.rawfilemd5,
        prepared.encrypted_size,
        &prepared.aeskey_hex,
    )
    .await?;
    let encrypted = encrypt_aes_ecb(&prepared.file, &prepared.aeskey_bytes)?;
    let cdn_encrypt_param =
        upload_to_cdn(client, &upload_param, &prepared.filekey, &encrypted).await?;
    let message = make_image_message(params, &prepared, &cdn_encrypt_param);
    send_message(
        client,
        params.base_url,
        params.token,
        Some(to_value(message).expect("Weixin image message is serializable")),
    )
    .await?;
    Ok(())
}

/// Mockable variant that uses the API module's HTTP/runtime seams and an
/// injected image-key generator. It executes the same four operations as
/// [`send_image`].
pub async fn send_image_with_http(
    http: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    random: &dyn ImageSendRandom,
    params: SendImageParams<'_>,
) -> Result<(), SendImageError> {
    let prepared = prepare_image(random, params)?;

    let upload_param = get_upload_url_with_http(
        http,
        runtime,
        params.base_url,
        params.token,
        params.to,
        &prepared.filekey,
        prepared.rawsize,
        &prepared.rawfilemd5,
        prepared.encrypted_size,
        &prepared.aeskey_hex,
    )
    .await?;
    let encrypted = encrypt_aes_ecb(&prepared.file, &prepared.aeskey_bytes)?;
    let cdn_encrypt_param = upload_to_cdn_with_http(
        http,
        runtime,
        &upload_param,
        &prepared.filekey,
        &encrypted,
        IMAGE_UPLOAD_TIMEOUT,
    )
    .await?;
    let message = make_image_message(params, &prepared, &cdn_encrypt_param);
    send_message_with_http(
        http,
        runtime,
        params.base_url,
        params.token,
        Some(to_value(message).expect("Weixin image message is serializable")),
    )
    .await?;
    Ok(())
}

struct PreparedImage {
    file: Vec<u8>,
    rawsize: u64,
    rawfilemd5: String,
    aeskey_bytes: [u8; 16],
    aeskey_hex: String,
    filekey: String,
    encrypted_size: u64,
}

fn prepare_image(
    random: &dyn ImageSendRandom,
    params: SendImageParams<'_>,
) -> Result<PreparedImage, SendImageError> {
    let resolved_path = validate_image_path(params.image_path, params.workspace_dirs)
        .map_err(SendImageError::InvalidImage)?;
    let file = std::fs::read(resolved_path)?;
    let rawsize = file.len() as u64;
    let rawfilemd5 = compute_md5(&file);
    let aeskey_bytes = random.random_16_bytes();
    let aeskey_hex = encode_hex(&aeskey_bytes);
    let filekey = encode_hex(&random.random_16_bytes());
    // PKCS#7 always appends at least one byte, including for block-aligned
    // and empty inputs, so this matches ceil((rawsize + 1) / 16) * 16.
    let encrypted_size = (rawsize / 16 + 1) * 16;

    Ok(PreparedImage {
        file,
        rawsize,
        rawfilemd5,
        aeskey_bytes,
        aeskey_hex,
        filekey,
        encrypted_size,
    })
}

fn make_image_message(
    params: SendImageParams<'_>,
    image: &PreparedImage,
    cdn_encrypt_param: &str,
) -> WeixinMessage {
    let aeskey_base64 = BASE64_STANDARD.encode(image.aeskey_hex.as_bytes());
    WeixinMessage {
        to_user_id: Some(params.to.to_owned()),
        from_user_id: Some(String::new()),
        client_id: Some(Uuid::new_v4().to_string()),
        message_type: Some(MessageType::BOT),
        message_state: Some(MessageState::FINISH),
        context_token: Some(params.context_token.to_owned()),
        item_list: Some(vec![MessageItem {
            r#type: Some(MessageItemType::IMAGE),
            image_item: Some(ImageItem {
                aeskey: Some(image.aeskey_hex.clone()),
                mid_size: Some(image.encrypted_size as i64),
                media: Some(CDNMedia {
                    encrypt_query_param: Some(cdn_encrypt_param.to_owned()),
                    aes_key: Some(aeskey_base64),
                    encrypt_type: Some(1),
                }),
                ..ImageItem::default()
            }),
            ..MessageItem::default()
        }]),
        ..WeixinMessage::default()
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{ImageSendRandom, SendImageParams, encode_hex, send_image_with_http};
    use crate::channels::weixin_api::{
        ApiFuture, ApiHttpClient, ApiHttpRequest, ApiHttpResponse, ApiRuntime, ApiTransportError,
    };
    use crate::channels::weixin_media::{compute_md5, decrypt_aes_ecb};
    use crate::channels::weixin_types::{MessageItemType, MessageState, MessageType};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use serde_json::{Value, json};
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::Duration;
    use uuid::Uuid;

    const IMAGE_BYTES: &[u8] = b"\x89PNG\r\n\x1a\nsmall image payload";

    struct TempImage(PathBuf);

    impl TempImage {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-send-{}.png", Uuid::new_v4()));
            std::fs::write(&path, IMAGE_BYTES).unwrap();
            Self(path)
        }
    }

    impl Drop for TempImage {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn params(image: &TempImage) -> SendImageParams<'_> {
        SendImageParams {
            to: "user-123",
            image_path: &image.0,
            base_url: "https://api.example.com",
            token: "token-abc",
            context_token: "ctx-456",
            workspace_dirs: &[],
        }
    }

    struct FixedRandom {
        keys: Mutex<VecDeque<[u8; 16]>>,
        calls: Mutex<usize>,
    }

    impl FixedRandom {
        fn new(keys: impl IntoIterator<Item = [u8; 16]>) -> Self {
            Self {
                keys: Mutex::new(keys.into_iter().collect()),
                calls: Mutex::new(0),
            }
        }

        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }

    impl ImageSendRandom for FixedRandom {
        fn random_16_bytes(&self) -> [u8; 16] {
            *self.calls.lock().unwrap() += 1;
            self.keys.lock().unwrap().pop_front().unwrap_or([0; 16])
        }
    }

    struct MockRuntime {
        delays: Mutex<Vec<Duration>>,
    }

    impl MockRuntime {
        fn new() -> Self {
            Self {
                delays: Mutex::new(Vec::new()),
            }
        }

        fn delays(&self) -> Vec<Duration> {
            self.delays.lock().unwrap().clone()
        }
    }

    impl ApiRuntime for MockRuntime {
        fn random_uin_bytes(&self) -> [u8; 4] {
            [1, 2, 3, 4]
        }

        fn sleep<'a>(&'a self, duration: Duration) -> ApiFuture<'a, ()> {
            Box::pin(async move {
                self.delays.lock().unwrap().push(duration);
            })
        }
    }

    struct MockHttp {
        requests: Mutex<Vec<ApiHttpRequest>>,
        responses: Mutex<VecDeque<Result<ApiHttpResponse, ApiTransportError>>>,
    }

    impl MockHttp {
        fn new(
            responses: impl IntoIterator<Item = Result<ApiHttpResponse, ApiTransportError>>,
        ) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }

        fn requests(&self) -> Vec<ApiHttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl ApiHttpClient for MockHttp {
        fn execute<'a>(
            &'a self,
            request: ApiHttpRequest,
        ) -> ApiFuture<'a, Result<ApiHttpResponse, ApiTransportError>> {
            let response = self.responses.lock().unwrap().pop_front();
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                response.unwrap_or_else(|| Err(ApiTransportError::Other("no mock response".into())))
            })
        }
    }

    fn success_json(body: Value) -> Result<ApiHttpResponse, ApiTransportError> {
        Ok(ApiHttpResponse::json(200, body))
    }

    fn runtime() -> MockRuntime {
        MockRuntime::new()
    }

    fn random() -> FixedRandom {
        FixedRandom::new([[0x42; 16], [0x24; 16]])
    }

    fn image_request(requests: &[ApiHttpRequest], path_suffix: &str) -> Value {
        let request = requests
            .iter()
            .find(|request| request.url.ends_with(path_suffix))
            .unwrap_or_else(|| panic!("missing request to {path_suffix}: {requests:#?}"));
        serde_json::from_slice(&request.body).unwrap()
    }

    #[tokio::test]
    async fn sends_image_through_upload_url_cdn_and_message_steps() {
        let temp_image = TempImage::new();
        let key = [0x42; 16];
        let filekey = [0x24; 16];
        let cdn_response = ApiHttpResponse::raw(200, Vec::new())
            .with_header("x-encrypted-param", "cdn-encrypt-param");
        let http = MockHttp::new([
            success_json(json!({"upload_param": "upload-param-value"})),
            Ok(cdn_response),
            success_json(json!({"ret": 0})),
        ]);
        let runtime = runtime();
        let random = random();

        send_image_with_http(&http, &runtime, &random, params(&temp_image))
            .await
            .unwrap();

        let requests = http.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0].url,
            "https://api.example.com/ilink/bot/getuploadurl"
        );
        assert_eq!(
            requests[1].url,
            format!(
                "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=upload-param-value&filekey={}",
                encode_hex(&filekey)
            )
        );
        assert_eq!(
            requests[2].url,
            "https://api.example.com/ilink/bot/sendmessage"
        );
        assert!(requests.iter().all(|request| request.method == "POST"));
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(name, value)| { name == "Authorization" && value == "Bearer token-abc" })
        );

        let encrypted_size = ((IMAGE_BYTES.len() as u64 / 16) + 1) * 16;
        let upload_json = image_request(&requests, "/ilink/bot/getuploadurl");
        assert_eq!(
            upload_json,
            json!({
                "filekey": encode_hex(&filekey),
                "media_type": 1,
                "to_user_id": "user-123",
                "rawsize": IMAGE_BYTES.len(),
                "rawfilemd5": compute_md5(IMAGE_BYTES),
                "filesize": encrypted_size,
                "no_need_thumb": true,
                "aeskey": encode_hex(&key),
                "base_info": {"channel_version": "2.1.3"},
            })
        );

        assert_eq!(requests[1].body.len() as u64, encrypted_size);
        assert_eq!(
            decrypt_aes_ecb(&requests[1].body, &key).unwrap(),
            IMAGE_BYTES
        );
        assert!(
            requests[1]
                .headers
                .iter()
                .any(|(name, value)| name == "Content-Type" && value == "application/octet-stream")
        );

        let message_json = image_request(&requests, "/ilink/bot/sendmessage");
        assert_eq!(message_json["base_info"]["channel_version"], "2.1.3");
        let message = &message_json["msg"];
        assert_eq!(message["to_user_id"], "user-123");
        assert_eq!(message["from_user_id"], "");
        assert!(!message["client_id"].as_str().unwrap().is_empty());
        assert_eq!(message["message_type"], MessageType::BOT);
        assert_eq!(message["message_state"], MessageState::FINISH);
        assert_eq!(message["context_token"], "ctx-456");
        assert_eq!(message["item_list"][0]["type"], MessageItemType::IMAGE);
        assert_eq!(
            message["item_list"][0]["image_item"]["aeskey"],
            encode_hex(&key)
        );
        assert_eq!(
            message["item_list"][0]["image_item"]["mid_size"],
            encrypted_size
        );
        assert_eq!(
            message["item_list"][0]["image_item"]["media"],
            json!({
                "encrypt_query_param": "cdn-encrypt-param",
                "aes_key": BASE64_STANDARD.encode(encode_hex(&key).as_bytes()),
                "encrypt_type": 1,
            })
        );
        assert_eq!(random.calls(), 2);
        assert!(runtime.delays().is_empty());
    }

    #[tokio::test]
    async fn upload_url_auth_failure_stops_before_cdn_and_send() {
        let temp_image = TempImage::new();
        let http = MockHttp::new([success_json(json!({
            "ret": 1,
            "errmsg": "Auth expired",
        }))]);
        let runtime = runtime();
        let random = random();

        let error = send_image_with_http(&http, &runtime, &random, params(&temp_image))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Auth expired"));
        let requests = http.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].url.ends_with("/ilink/bot/getuploadurl"));
        assert_eq!(random.calls(), 2);
        assert!(runtime.delays().is_empty());
    }

    #[tokio::test]
    async fn cdn_failure_stops_before_send_and_preserves_error() {
        let temp_image = TempImage::new();
        let http = MockHttp::new([
            success_json(json!({"upload_param": "upload-param-value"})),
            Ok(ApiHttpResponse::json(
                400,
                json!({"errmsg": "CDN unavailable"}),
            )),
        ]);
        let runtime = runtime();
        let random = random();

        let error = send_image_with_http(&http, &runtime, &random, params(&temp_image))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("CDN unavailable"));
        let requests = http.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].url.ends_with("/ilink/bot/getuploadurl"));
        assert!(requests[1].url.contains("/c2c/upload?"));
        assert_eq!(runtime.delays(), []);
    }

    #[tokio::test]
    async fn send_message_failure_is_propagated_after_both_upload_steps() {
        let temp_image = TempImage::new();
        let cdn_response = ApiHttpResponse::raw(200, Vec::new())
            .with_header("x-encrypted-param", "cdn-encrypt-param");
        let http = MockHttp::new([
            success_json(json!({"upload_param": "upload-param-value"})),
            Ok(cdn_response),
            success_json(json!({"ret": 1, "errmsg": "send rejected"})),
        ]);
        let runtime = runtime();
        let random = random();

        let error = send_image_with_http(&http, &runtime, &random, params(&temp_image))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("send rejected"));
        let requests = http.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].url.ends_with("/ilink/bot/sendmessage"));
        assert!(runtime.delays().is_empty());
    }

    #[tokio::test]
    async fn invalid_image_path_fails_before_randomness_or_network() {
        let http = MockHttp::new([]);
        let runtime = runtime();
        let random = random();
        let image_path = Path::new("/tmp/missing-weixin-send.png");
        let params = SendImageParams {
            to: "user-123",
            image_path,
            base_url: "https://api.example.com",
            token: "token-abc",
            context_token: "ctx-456",
            workspace_dirs: &[],
        };

        let error = send_image_with_http(&http, &runtime, &random, params)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Image file not found"));
        assert!(http.requests().is_empty());
        assert_eq!(random.calls(), 0);
    }
}
