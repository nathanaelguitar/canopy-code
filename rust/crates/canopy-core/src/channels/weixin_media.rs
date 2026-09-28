//! Weixin CDN media crypto and download helpers.
//!
//! Port of `packages/channels/weixin/src/media.ts`. The HTTP trait allows the
//! CDN request, timeout, and failure paths to be covered without networking.
//! AES-128-ECB with PKCS#7 padding is retained because the Weixin protocol uses
//! it; it provides no ciphertext authenticity.

use aes::Aes128;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use md5::{Digest, Md5};
use reqwest::Client;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";
pub const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(40);

pub type MediaFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeixinMediaError(String);

impl WeixinMediaError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for WeixinMediaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for WeixinMediaError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl MediaHttpResponse {
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

/// Minimal GET transport. `timeout` covers receiving the response body, as
/// the source's abort timer remains armed until `arrayBuffer()` completes.
pub trait MediaHttpClient: Send + Sync {
    fn get<'a>(
        &'a self,
        url: &'a str,
        timeout: Duration,
    ) -> MediaFuture<'a, Result<MediaHttpResponse, String>>;
}

/// Decode a CDN AES key, accepting the two encodings used by Weixin:
/// base64 of 16 raw bytes or base64 of a 32-character hexadecimal key.
pub fn parse_aes_key(aes_key_base64: &str) -> Result<[u8; 16], WeixinMediaError> {
    let decoded = decode_node_base64(aes_key_base64);
    if decoded.len() == 16 {
        let mut key = [0_u8; 16];
        key.copy_from_slice(&decoded);
        return Ok(key);
    }

    if decoded.len() == 32 {
        let ascii = decoded.iter().map(|byte| byte & 0x7f).collect::<Vec<_>>();
        if ascii.iter().all(u8::is_ascii_hexdigit) {
            let mut key = [0_u8; 16];
            for (index, pair) in ascii.chunks_exact(2).enumerate() {
                key[index] = (hex_value(pair[0]) << 4) | hex_value(pair[1]);
            }
            return Ok(key);
        }
    }

    Err(WeixinMediaError::new(format!(
        "Invalid aes_key: expected 16 raw bytes or 32 hex chars, got {} bytes",
        decoded.len()
    )))
}

/// Encrypt bytes using protocol-compatible AES-128-ECB and PKCS#7 padding.
pub fn encrypt_aes_ecb(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>, WeixinMediaError> {
    let cipher = aes_cipher(key)?;
    let padding = 16 - (plaintext.len() % 16);
    let mut encrypted = Vec::with_capacity(plaintext.len() + padding);
    encrypted.extend_from_slice(plaintext);
    encrypted.resize(plaintext.len() + padding, padding as u8);

    for chunk in encrypted.chunks_exact_mut(16) {
        let mut block = GenericArray::clone_from_slice(chunk);
        cipher.encrypt_block(&mut block);
        chunk.copy_from_slice(&block);
    }
    Ok(encrypted)
}

/// Decrypt protocol-compatible AES-128-ECB ciphertext and remove PKCS#7
/// padding. Empty or non-block-aligned ciphertext is rejected, matching the
/// Node crypto decipher finalization behavior.
pub fn decrypt_aes_ecb(ciphertext: &[u8], key: &[u8]) -> Result<Vec<u8>, WeixinMediaError> {
    decrypt_aes_ecb_owned(ciphertext.to_vec(), key)
}

fn decrypt_aes_ecb_owned(mut plaintext: Vec<u8>, key: &[u8]) -> Result<Vec<u8>, WeixinMediaError> {
    if plaintext.is_empty() || plaintext.len() % 16 != 0 {
        return Err(WeixinMediaError::new(
            "Invalid AES-128-ECB ciphertext length",
        ));
    }
    let cipher = aes_cipher(key)?;
    for chunk in plaintext.chunks_exact_mut(16) {
        let mut block = GenericArray::clone_from_slice(chunk);
        cipher.decrypt_block(&mut block);
        chunk.copy_from_slice(&block);
    }

    let padding = usize::from(*plaintext.last().expect("non-empty block-aligned input"));
    if !(1..=16).contains(&padding)
        || plaintext[plaintext.len() - padding..]
            .iter()
            .any(|byte| usize::from(*byte) != padding)
    {
        return Err(WeixinMediaError::new("Invalid AES-128-ECB PKCS#7 padding"));
    }
    plaintext.truncate(plaintext.len() - padding);
    Ok(plaintext)
}

/// Compute lowercase MD5 hex, as required by the Weixin upload protocol.
pub fn compute_md5(data: &[u8]) -> String {
    let digest = Md5::digest(data);
    let mut encoded = String::with_capacity(32);
    for byte in digest {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

/// Build the CDN download endpoint using JavaScript `encodeURIComponent`
/// escaping for the encrypted query parameter.
pub fn build_cdn_download_url(encrypted_query_param: &str) -> String {
    format!(
        "{CDN_BASE_URL}/download?encrypted_query_param={}",
        encode_uri_component(encrypted_query_param)
    )
}

/// Download and decrypt media with a pooled reqwest client.
pub async fn download_and_decrypt(
    client: &Client,
    encrypted_query_param: &str,
    aes_key_base64: &str,
) -> Result<Vec<u8>, WeixinMediaError> {
    let http = ReqwestMediaHttpClient { client };
    download_and_decrypt_with_http(&http, encrypted_query_param, aes_key_base64).await
}

/// Download and decrypt media while bounding its encrypted and plaintext
/// sizes. The response is collected a chunk at a time, so oversized media is
/// rejected before the whole body is retained.
pub async fn download_and_decrypt_limited(
    client: &Client,
    encrypted_query_param: &str,
    aes_key_base64: &str,
    max_plaintext_bytes: usize,
) -> Result<Vec<u8>, WeixinMediaError> {
    let url = build_cdn_download_url(encrypted_query_param);
    let max_ciphertext_bytes = max_plaintext_bytes.saturating_add(16);
    let mut response = client
        .get(url)
        .timeout(DOWNLOAD_TIMEOUT)
        .send()
        .await
        .map_err(|error| WeixinMediaError::new(error.to_string()))?;
    if !(200..300).contains(&response.status().as_u16()) {
        return Err(WeixinMediaError::new(format!(
            "CDN download failed: HTTP {}",
            response.status()
        )));
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_ciphertext_bytes as u64)
    {
        return Err(media_size_error(max_plaintext_bytes));
    }

    let capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(max_ciphertext_bytes);
    let mut ciphertext = Vec::with_capacity(capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| WeixinMediaError::new(error.to_string()))?
    {
        if ciphertext.len().saturating_add(chunk.len()) > max_ciphertext_bytes {
            return Err(media_size_error(max_plaintext_bytes));
        }
        ciphertext.extend_from_slice(&chunk);
    }

    let key = parse_aes_key(aes_key_base64)?;
    let plaintext = decrypt_aes_ecb_owned(ciphertext, &key)?;
    if plaintext.len() > max_plaintext_bytes {
        return Err(media_size_error(max_plaintext_bytes));
    }
    Ok(plaintext)
}

fn media_size_error(max_plaintext_bytes: usize) -> WeixinMediaError {
    WeixinMediaError::new(format!(
        "CDN media exceeds the {max_plaintext_bytes}-byte limit"
    ))
}

/// Download and decrypt media through an injected HTTP transport.
pub async fn download_and_decrypt_with_http(
    client: &dyn MediaHttpClient,
    encrypted_query_param: &str,
    aes_key_base64: &str,
) -> Result<Vec<u8>, WeixinMediaError> {
    let url = build_cdn_download_url(encrypted_query_param);
    let response = client
        .get(&url, DOWNLOAD_TIMEOUT)
        .await
        .map_err(WeixinMediaError::new)?;
    if !(200..300).contains(&response.status) {
        return Err(WeixinMediaError::new(format!(
            "CDN download failed: HTTP {}",
            response.status
        )));
    }

    let key = parse_aes_key(aes_key_base64)?;
    decrypt_aes_ecb_owned(response.body, &key)
}

fn aes_cipher(key: &[u8]) -> Result<Aes128, WeixinMediaError> {
    Aes128::new_from_slice(key).map_err(|_| {
        WeixinMediaError::new(format!(
            "Invalid AES-128 key length: expected 16 bytes, got {}",
            key.len()
        ))
    })
}

/// Node's `Buffer.from(text, "base64")` accepts URL-safe characters, skips
/// invalid characters, ignores whitespace, and tolerates omitted padding.
/// Preserve those permissive decode semantics before validating the result.
fn decode_node_base64(encoded: &str) -> Vec<u8> {
    let mut normalized = Vec::with_capacity(encoded.len() + 3);
    for byte in encoded.bytes() {
        match byte {
            b'=' => break,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' => normalized.push(byte),
            b'-' => normalized.push(b'+'),
            b'_' => normalized.push(b'/'),
            _ => {}
        }
    }

    if normalized.len() % 4 == 1 {
        normalized.pop();
    }
    let remainder = normalized.len() % 4;
    normalized.extend(std::iter::repeat_n(b'=', (4 - remainder) % 4));
    BASE64_STANDARD.decode(normalized).unwrap_or_default()
}

fn hex_value(character: u8) -> u8 {
    match character {
        b'0'..=b'9' => character - b'0',
        b'a'..=b'f' => character - b'a' + 10,
        b'A'..=b'F' => character - b'A' + 10,
        _ => unreachable!("validated hexadecimal key bytes"),
    }
}

fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            )
        {
            encoded.push(char::from(byte));
        } else {
            use fmt::Write as _;
            write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    encoded
}

struct ReqwestMediaHttpClient<'a> {
    client: &'a Client,
}

impl MediaHttpClient for ReqwestMediaHttpClient<'_> {
    fn get<'a>(
        &'a self,
        url: &'a str,
        timeout: Duration,
    ) -> MediaFuture<'a, Result<MediaHttpResponse, String>> {
        Box::pin(async move {
            let response = self
                .client
                .get(url)
                .timeout(timeout)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .map_err(|error| error.to_string())?
                .to_vec();
            Ok(MediaHttpResponse { status, body })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DOWNLOAD_TIMEOUT, MediaFuture, MediaHttpClient, MediaHttpResponse, build_cdn_download_url,
        compute_md5, decrypt_aes_ecb, download_and_decrypt_with_http, encrypt_aes_ecb,
        parse_aes_key,
    };
    use aes::Aes128;
    use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RequestRecord {
        url: String,
        timeout: Duration,
    }

    struct MockHttp {
        result: Mutex<Result<MediaHttpResponse, String>>,
        request: Mutex<Option<RequestRecord>>,
    }

    impl MockHttp {
        fn response(result: Result<MediaHttpResponse, String>) -> Self {
            Self {
                result: Mutex::new(result),
                request: Mutex::new(None),
            }
        }

        fn request(&self) -> Option<RequestRecord> {
            self.request
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl MediaHttpClient for MockHttp {
        fn get<'a>(
            &'a self,
            url: &'a str,
            timeout: Duration,
        ) -> MediaFuture<'a, Result<MediaHttpResponse, String>> {
            *self
                .request
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(RequestRecord {
                url: url.to_owned(),
                timeout,
            });
            let result = self
                .result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            Box::pin(async move { result })
        }
    }

    fn encoded_key(key: &[u8]) -> String {
        BASE64_STANDARD.encode(key)
    }

    #[test]
    fn parse_aes_key_accepts_raw_and_hex_forms() {
        let raw = [0xab; 16];
        assert_eq!(parse_aes_key(&encoded_key(&raw)).unwrap(), raw);

        let hex = b"aabbccdd11223344aabbccdd11223344";
        let parsed = parse_aes_key(&encoded_key(hex)).unwrap();
        assert_eq!(
            parsed,
            [
                0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44, 0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22,
                0x33, 0x44,
            ]
        );
    }

    #[test]
    fn parse_aes_key_preserves_permissive_base64_decoding() {
        let raw = [0x19; 16];
        let unpadded = BASE64_STANDARD.encode(raw).trim_end_matches('=').to_owned();
        let decorated = format!("  {}!\n", unpadded.replace('+', "-").replace('/', "_"));
        assert_eq!(parse_aes_key(&decorated).unwrap(), raw);
    }

    #[test]
    fn parse_aes_key_reports_decoded_length_and_non_hex_content() {
        let wrong_length = encoded_key(&[0; 20]);
        let error = parse_aes_key(&wrong_length).unwrap_err();
        assert!(error.to_string().starts_with("Invalid aes_key"));
        assert!(error.to_string().ends_with("got 20 bytes"));

        let non_hex = encoded_key(b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
        let error = parse_aes_key(&non_hex).unwrap_err();
        assert!(error.to_string().starts_with("Invalid aes_key"));
        assert!(error.to_string().ends_with("got 32 bytes"));
    }

    #[test]
    fn aes_block_matches_nist_aes128_ecb_vector() {
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let input = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        let expected = [
            0x69, 0xc4, 0xe0, 0xd8, 0x6a, 0x7b, 0x04, 0x30, 0xd8, 0xcd, 0xb7, 0x80, 0x70, 0xb4,
            0xc5, 0x5a,
        ];
        let cipher = Aes128::new_from_slice(&key).unwrap();
        let mut block = GenericArray::clone_from_slice(&input);
        cipher.encrypt_block(&mut block);
        assert_eq!(&block[..], &expected);
    }

    #[test]
    fn encryption_and_decryption_round_trip_with_pkcs7() {
        for plaintext in [
            b"Hello, WeChat media decryption!".as_slice(),
            b"sixteen bytes!!!".as_slice(),
            b"".as_slice(),
        ] {
            let key = [0x42; 16];
            let ciphertext = encrypt_aes_ecb(plaintext, &key).unwrap();
            assert_eq!(ciphertext.len() % 16, 0);
            assert_eq!(decrypt_aes_ecb(&ciphertext, &key).unwrap(), plaintext);
        }
    }

    #[test]
    fn aes_rejects_bad_keys_ciphertext_lengths_and_padding() {
        assert!(encrypt_aes_ecb(b"data", &[0; 15]).is_err());
        assert!(decrypt_aes_ecb(&[], &[0; 16]).is_err());
        assert!(decrypt_aes_ecb(&[0; 15], &[0; 16]).is_err());

        // A one-block all-zero value deterministically has invalid PKCS#7.
        assert!(decrypt_aes_ecb(&[0; 16], &[0; 16]).is_err());
    }

    #[test]
    fn md5_matches_known_vectors() {
        assert_eq!(
            compute_md5(b"hello world"),
            "5eb63bbbe01eeed093cb22bb8f5acdc3"
        );
        assert_eq!(compute_md5(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn url_encoding_matches_encode_uri_component() {
        assert_eq!(
            build_cdn_download_url("a b!~*'()é"),
            "https://novac2c.cdn.weixin.qq.com/c2c/download?encrypted_query_param=a%20b!~*'()%C3%A9"
        );
    }

    #[tokio::test]
    async fn download_decrypts_success_and_uses_forty_second_timeout() {
        let key = [0x33; 16];
        let ciphertext = encrypt_aes_ecb(b"weixin media", &key).unwrap();
        let http = MockHttp::response(Ok(MediaHttpResponse::new(200, ciphertext)));
        let result = download_and_decrypt_with_http(&http, "param +/", &encoded_key(&key))
            .await
            .unwrap();
        assert_eq!(result, b"weixin media");
        assert_eq!(
            http.request(),
            Some(RequestRecord {
                url: "https://novac2c.cdn.weixin.qq.com/c2c/download?encrypted_query_param=param%20%2B%2F".to_owned(),
                timeout: DOWNLOAD_TIMEOUT,
            })
        );
        assert_eq!(DOWNLOAD_TIMEOUT, Duration::from_secs(40));
    }

    #[tokio::test]
    async fn download_returns_http_and_transport_failures() {
        let http = MockHttp::response(Ok(MediaHttpResponse::new(503, b"busy".to_vec())));
        let error = download_and_decrypt_with_http(&http, "param", &encoded_key(&[0; 16]))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "CDN download failed: HTTP 503");

        let http = MockHttp::response(Err("request timed out".to_owned()));
        let error = download_and_decrypt_with_http(&http, "param", &encoded_key(&[0; 16]))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "request timed out");
        assert_eq!(http.request().unwrap().timeout, DOWNLOAD_TIMEOUT);
    }

    #[tokio::test]
    async fn download_parses_key_after_successful_response() {
        let http = MockHttp::response(Ok(MediaHttpResponse::new(200, vec![0; 16])));
        let error = download_and_decrypt_with_http(&http, "param", "bad key")
            .await
            .unwrap_err();
        assert!(error.to_string().starts_with("Invalid aes_key"));
        assert!(http.request().is_some());
    }
}
