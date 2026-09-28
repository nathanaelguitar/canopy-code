use std::env;

use reqwest::{Client, Proxy};
use url::Url;

const PROXY_CONFIGURATION_ERROR: &str =
    "Proxy environment configuration is invalid. Check HTTP_PROXY, HTTPS_PROXY, and NO_PROXY.";
const CLIENT_INITIALIZATION_ERROR: &str = "Could not initialize the external context HTTP client.";

/// Build the external-context HTTP client with the same proxy variable order
/// and scheme fallback as Undici's `EnvHttpProxyAgent`.
pub fn client() -> Result<Client, String> {
    let http_proxy = proxy_from_environment("http_proxy", "HTTP_PROXY")?;
    let https_proxy =
        proxy_from_environment("https_proxy", "HTTPS_PROXY")?.or_else(|| http_proxy.clone());
    // Reqwest adds ALL_PROXY and reads proxy variables with its own precedence
    // by default. Disable that implicit matcher so the explicit rules below
    // preserve the TypeScript Extension's environment contract.
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy();

    if http_proxy.is_some() || https_proxy.is_some() {
        builder = builder.proxy(Proxy::custom(move |target| {
            if no_proxy_matches(
                target,
                &environment_value("no_proxy", "NO_PROXY").unwrap_or_default(),
            ) {
                return None;
            }
            match target.scheme() {
                "http" => http_proxy.clone(),
                "https" => https_proxy.clone(),
                _ => None,
            }
        }));
    }

    builder
        .build()
        .map_err(|_| CLIENT_INITIALIZATION_ERROR.to_owned())
}

fn proxy_from_environment(lowercase: &str, uppercase: &str) -> Result<Option<Url>, String> {
    let Some(value) = environment_value(lowercase, uppercase).filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let mut url = Url::parse(&value).map_err(|_| PROXY_CONFIGURATION_ERROR.to_owned())?;
    if url.host_str().is_none() {
        return Err(PROXY_CONFIGURATION_ERROR.to_owned());
    }

    if url.scheme() == "socks" {
        url.set_scheme("socks5")
            .map_err(|_| PROXY_CONFIGURATION_ERROR.to_owned())?;
    }
    if !matches!(url.scheme(), "http" | "https" | "socks5") {
        return Err(PROXY_CONFIGURATION_ERROR.to_owned());
    }

    // Undici emits Basic proxy authentication only when both URL components
    // are nonempty. Reqwest otherwise authenticates an empty password when
    // only one component is present, so discard partial credentials here.
    if url.username().is_empty() || url.password().is_none_or(str::is_empty) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }

    Ok(Some(url))
}

fn environment_value(lowercase: &str, uppercase: &str) -> Option<String> {
    env::var(lowercase)
        .ok()
        .or_else(|| env::var(uppercase).ok())
}

fn no_proxy_matches(target: &Url, value: &str) -> bool {
    if value == "*" {
        return true;
    }
    let Some(raw_host) = target.host_str() else {
        return false;
    };
    let host = if raw_host.contains(':') && !raw_host.starts_with('[') {
        format!("[{}]", raw_host.to_ascii_lowercase())
    } else {
        raw_host.to_ascii_lowercase()
    };
    let port = target.port().or_else(|| match target.scheme() {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    });

    value
        .split(|character: char| character == ',' || character.is_whitespace())
        .filter(|entry| !entry.is_empty())
        .any(|entry| no_proxy_entry_matches(entry, &host, port))
}

fn no_proxy_entry_matches(entry: &str, host: &str, target_port: Option<u16>) -> bool {
    let (raw_host, entry_port) = match entry.rsplit_once(':') {
        Some((raw_host, raw_port))
            if !raw_host.is_empty()
                && !raw_port.is_empty()
                && raw_port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            (raw_host, raw_port.parse::<u16>().ok())
        }
        _ => (entry, None),
    };
    if entry_port.is_some() && entry_port != target_port {
        return false;
    }

    let normalized = raw_host
        .strip_prefix("*.")
        .or_else(|| raw_host.strip_prefix('.'))
        .unwrap_or(raw_host)
        .to_ascii_lowercase();
    host == normalized
        || host
            .strip_suffix(&normalized)
            .is_some_and(|prefix| prefix.ends_with('.'))
}
