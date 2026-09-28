//! WebFetch's opportunistic HTTPS upgrade and narrowly scoped HTTP fallback.

use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr};

use reqwest::Url;

use super::fetch_policy::{
    FetchPolicyClient, FetchPolicyError, FetchPolicyOptions, FetchPolicyResult,
};
use super::validate_web_fetch_url;

/// Fetch output plus whether the response came from a plaintext fallback.
/// Callers must not cache fallback responses under an HTTPS cache key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedFetch {
    pub result: FetchPolicyResult,
    pub used_http_fallback: bool,
    pub fetch_url: String,
}

/// Upgrade public HTTP URLs on the default port to HTTPS.
///
/// Internal hosts and explicit service ports retain the requested scheme.
pub fn upgraded_https_url(url: &str) -> Result<Option<String>, FetchPolicyError> {
    validate_web_fetch_url(url).map_err(|error| FetchPolicyError::InvalidUrl(error.to_owned()))?;
    let mut parsed =
        Url::parse(url).map_err(|error| FetchPolicyError::InvalidUrl(error.to_string()))?;
    if parsed.scheme() != "http" || parsed.port().is_some() || is_private_host(url) {
        return Ok(None);
    }
    parsed
        .set_scheme("https")
        .map_err(|_| FetchPolicyError::InvalidUrl(url.to_owned()))?;
    Ok(Some(parsed.to_string()))
}

/// Fetch with the WebFetch HTTPS preference and original-HTTP fallback.
///
/// The fallback only applies when an upgraded request fails at the connection
/// or TLS layer. Timeouts imposed by the full-transfer budget and HTTP errors
/// are returned without retrying over plaintext.
pub async fn fetch_with_https_upgrade(
    client: &FetchPolicyClient,
    url: &str,
    options: &FetchPolicyOptions,
) -> Result<PlannedFetch, FetchPolicyError> {
    let upgraded = upgraded_https_url(url)?;
    let fetch_url = upgraded.as_deref().unwrap_or(url);
    match client.fetch(fetch_url, options).await {
        Ok(result) => Ok(PlannedFetch {
            result,
            used_http_fallback: false,
            fetch_url: fetch_url.to_owned(),
        }),
        Err(error) if upgraded.is_some() && is_connection_level_error(&error) => {
            let result = client.fetch(url, options).await?;
            Ok(PlannedFetch {
                result,
                used_http_fallback: true,
                fetch_url: url.to_owned(),
            })
        }
        Err(error) => Err(error),
    }
}

/// Determine whether an HTTP URL targets a host conventionally kept on plain
/// HTTP for local development or private networks.
pub fn is_private_host(url: &str) -> bool {
    let Ok(parsed) = Url::parse(url) else {
        return false;
    };
    let Some(hostname) = parsed.host_str() else {
        return false;
    };
    let hostname = hostname
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(hostname)
        .to_ascii_lowercase();
    if let Ok(address) = hostname.parse::<IpAddr>() {
        return is_private_ip(address);
    }

    hostname == "localhost"
        || hostname.ends_with(".localhost")
        || hostname == "host.docker.internal"
        || !hostname.contains('.')
        || [".local", ".internal", ".lan", ".home.arpa"]
            .iter()
            .any(|suffix| hostname.ends_with(suffix))
}

fn is_private_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_private_ipv4(address),
        IpAddr::V6(address) => {
            address.to_ipv4_mapped().is_some_and(is_private_ipv4)
                || address.is_loopback()
                || address.is_unspecified()
                || (address.segments()[0] & 0xfe00) == 0xfc00
                || (address.segments()[0] == 0xfe80 || (address.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

fn is_private_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, _, _] = address.octets();
    first == 10
        || first == 127
        || (first == 172 && (16..=31).contains(&second))
        || (first == 192 && second == 168)
        || (first == 169 && second == 254)
        || (first == 100 && (64..=127).contains(&second))
        || address.is_unspecified()
}

fn is_connection_level_error(error: &FetchPolicyError) -> bool {
    let FetchPolicyError::Request(error) = error else {
        return false;
    };
    if error.is_connect() {
        return true;
    }
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            if matches!(
                io_error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            ) {
                return true;
            }
        }
        source = cause.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrades_public_http_urls_only_on_the_default_port() {
        assert_eq!(
            upgraded_https_url("http://docs.example/path?q=1").unwrap(),
            Some("https://docs.example/path?q=1".to_owned())
        );
        assert_eq!(
            upgraded_https_url("http://docs.example:8080/path").unwrap(),
            None
        );
        assert_eq!(upgraded_https_url("http://localhost/path").unwrap(), None);
        assert_eq!(upgraded_https_url("http://192.168.1.8/path").unwrap(), None);
        assert_eq!(
            upgraded_https_url("https://docs.example/path").unwrap(),
            None
        );
    }

    #[test]
    fn recognizes_private_ip_ranges_and_internal_hostnames() {
        for url in [
            "http://10.1.2.3/",
            "http://127.0.0.1/",
            "http://172.31.0.1/",
            "http://192.168.0.1/",
            "http://169.254.169.254/",
            "http://100.64.0.1/",
            "http://[::1]/",
            "http://[fd00::1]/",
            "http://[fe80::1]/",
            "http://buildbox/",
            "http://printer.local/",
            "http://service.internal/",
        ] {
            assert!(is_private_host(url), "{url}");
        }
        assert!(!is_private_host("http://8.8.8.8/"));
        assert!(!is_private_host("http://docs.example/"));
    }
}
