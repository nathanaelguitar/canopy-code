// Copyright 2025 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Public-network policy for extension HTTP targets.
//!
//! DNS resolution is injected and every returned address is checked before a
//! first-address pin is exposed. The pin can be applied to a reqwest client
//! builder; the caller still owns redirects, proxies, and request execution.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;

use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::utils::cancellation::{CancellationReason, CancellationToken};

const PUBLIC_HTTPS_ERROR: &str = "Public extension network requests must use HTTPS.";
const PUBLIC_CREDENTIALS_ERROR: &str =
    "Public extension network requests must not use credentials.";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionNetworkPolicy {
    Public,
}

/// Async resolver seam. Implementations return every address that a later
/// client might use, in the resolver's preferred order.
pub trait ExtensionAddressResolver: Send + Sync {
    type Error: fmt::Display + Send + 'static;

    fn resolve_all<'a>(&'a self, hostname: &'a str) -> AddressResolveFuture<'a, Self::Error>;
}

pub type AddressResolveFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, E>> + Send + 'a>>;

/// A checked pin corresponding to the source's custom DNS `lookup` and
/// `curlResolve` values. The caller must apply `selected_address` when it
/// creates the HTTP client; merely constructing this result does not pin a
/// connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkPin {
    pub hostname: String,
    /// All DNS results checked against the block policy.
    pub checked_addresses: Vec<IpAddr>,
    /// The first resolver result, matching the source's selected lookup.
    pub selected_address: IpAddr,
    pub selected_family: u8,
    pub port: u16,
    /// curl `--resolve`-style mapping for consumers that use curl.
    pub curl_resolve: String,
}

impl NetworkPin {
    /// Apply this host's selected DNS address to a reqwest client builder.
    ///
    /// The override covers this hostname only. It does not validate or repin
    /// redirect destinations; callers must disable redirects or validate and
    /// apply a new policy pin for every redirect hop. Any configured proxy is
    /// also the caller's responsibility.
    pub fn apply_to_client_builder(
        &self,
        builder: reqwest::ClientBuilder,
    ) -> reqwest::ClientBuilder {
        if self.hostname.parse::<IpAddr>().is_ok() {
            // IP literals bypass DNS already; adding a DNS override is
            // unnecessary and may not match reqwest's resolver key format.
            builder
        } else {
            builder.resolve(&self.hostname, SocketAddr::new(self.selected_address, 0))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedNetworkTarget {
    pub url: Url,
    /// `None` when the public-network policy is not enabled.
    pub pin: Option<NetworkPin>,
}

#[derive(Debug)]
pub enum NetworkPolicyError<E> {
    InvalidUrl(String),
    HttpsRequired,
    CredentialsForbidden,
    Resolve { hostname: String, source: E },
    HostDidNotResolve { hostname: String },
    BlockedAddress { hostname: String },
    Cancelled(Option<CancellationReason>),
}

impl<E: fmt::Display> fmt::Display for NetworkPolicyError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(message) => write!(formatter, "Invalid URL: {message}"),
            Self::HttpsRequired => formatter.write_str(PUBLIC_HTTPS_ERROR),
            Self::CredentialsForbidden => formatter.write_str(PUBLIC_CREDENTIALS_ERROR),
            Self::Resolve { source, .. } => fmt::Display::fmt(source, formatter),
            Self::HostDidNotResolve { hostname } => {
                write!(
                    formatter,
                    "Extension network host did not resolve: {hostname}"
                )
            }
            Self::BlockedAddress { hostname } => write!(
                formatter,
                "Extension network host resolved to a blocked address: {hostname}"
            ),
            Self::Cancelled(Some(CancellationReason::Explicit(reason))) => {
                formatter.write_str(reason)
            }
            Self::Cancelled(Some(CancellationReason::Timeout)) => {
                formatter.write_str("The operation timed out")
            }
            Self::Cancelled(None) => formatter.write_str("The operation was aborted"),
        }
    }
}

/// Resolve and validate a URL according to the optional extension policy.
/// When `policy` is absent, the URL is returned without DNS or HTTPS checks.
/// For `Public`, the URL must be HTTPS and credential-free, all resolved
/// addresses must be public, and the first address is returned as a pin.
pub async fn resolve_network_target<R: ExtensionAddressResolver + ?Sized>(
    value: &str,
    policy: Option<ExtensionNetworkPolicy>,
    resolver: &R,
    cancellation: Option<&CancellationToken>,
) -> Result<ResolvedNetworkTarget, NetworkPolicyError<R::Error>> {
    if let Some(cancellation) = cancellation
        && cancellation.is_cancelled()
    {
        return Err(NetworkPolicyError::Cancelled(cancellation.reason()));
    }

    let url =
        Url::parse(value).map_err(|error| NetworkPolicyError::InvalidUrl(error.to_string()))?;
    if policy != Some(ExtensionNetworkPolicy::Public) {
        return Ok(ResolvedNetworkTarget { url, pin: None });
    }
    if url.scheme() != "https" {
        return Err(NetworkPolicyError::HttpsRequired);
    }
    if !url.username().is_empty() || url.password().is_some_and(|password| !password.is_empty()) {
        return Err(NetworkPolicyError::CredentialsForbidden);
    }

    let hostname = url
        .host_str()
        .map(strip_ipv6_brackets)
        .ok_or_else(|| NetworkPolicyError::InvalidUrl("URL has no hostname".to_owned()))?;
    let literal_address = hostname.parse::<IpAddr>().ok();
    let addresses = match literal_address {
        Some(address) => vec![address],
        None => resolve_all_abortably(&hostname, resolver, cancellation).await?,
    };
    if addresses.is_empty() {
        return Err(NetworkPolicyError::HostDidNotResolve { hostname });
    }
    if addresses.iter().copied().any(is_blocked_address) {
        return Err(NetworkPolicyError::BlockedAddress { hostname });
    }

    let selected_address = addresses[0];
    let selected_family = address_family(selected_address);
    let port = url.port().unwrap_or(443);
    let curl_hostname = if matches!(literal_address, Some(IpAddr::V6(_))) {
        format!("[{hostname}]")
    } else {
        hostname.clone()
    };
    let curl_address = match selected_address {
        IpAddr::V4(address) => address.to_string(),
        IpAddr::V6(address) => format!("[{address}]"),
    };
    let pin = NetworkPin {
        hostname,
        checked_addresses: addresses,
        selected_address,
        selected_family,
        port,
        curl_resolve: format!("{curl_hostname}:{port}:{curl_address}"),
    };

    Ok(ResolvedNetworkTarget {
        url,
        pin: Some(pin),
    })
}

/// Return whether an address is rejected by the source public-network policy.
/// IPv4-mapped IPv6 addresses use the embedded IPv4 rules. Other IPv6
/// addresses are allowed only within `2000::/3`, minus the explicit blocked
/// subnets.
pub fn is_blocked_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_blocked_ipv4(address),
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                return is_blocked_ipv4(mapped);
            }
            !in_ipv6_subnet(address, Ipv6Addr::new(0x2000, 0, 0, 0, 0, 0, 0, 0), 3)
                || in_ipv6_subnet(address, Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23)
                || in_ipv6_subnet(address, Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32)
                || in_ipv6_subnet(address, Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16)
                || in_ipv6_subnet(address, Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20)
        }
    }
}

async fn resolve_all_abortably<R: ExtensionAddressResolver + ?Sized>(
    hostname: &str,
    resolver: &R,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<IpAddr>, NetworkPolicyError<R::Error>> {
    if let Some(cancellation) = cancellation {
        if cancellation.is_cancelled() {
            return Err(NetworkPolicyError::Cancelled(cancellation.reason()));
        }
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                Err(NetworkPolicyError::Cancelled(reason))
            }
            result = resolver.resolve_all(hostname) => {
                result.map_err(|source| NetworkPolicyError::Resolve {
                    hostname: hostname.to_owned(),
                    source,
                })
            }
        }
    } else {
        resolver
            .resolve_all(hostname)
            .await
            .map_err(|source| NetworkPolicyError::Resolve {
                hostname: hostname.to_owned(),
                source,
            })
    }
}

fn strip_ipv6_brackets(hostname: &str) -> String {
    hostname
        .strip_prefix('[')
        .and_then(|hostname| hostname.strip_suffix(']'))
        .unwrap_or(hostname)
        .to_owned()
}

fn address_family(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(_) => 4,
        IpAddr::V6(_) => 6,
    }
}

fn is_blocked_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    first == 0
        || first == 10
        || (first == 100 && (64..=127).contains(&second))
        || first == 127
        || (first == 169 && second == 254)
        || (first == 172 && (16..=31).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || (first == 192 && second == 0 && third == 2)
        || (first == 192 && second == 88 && third == 99)
        || (first == 192 && second == 168)
        || (first == 198 && (18..=19).contains(&second))
        || (first == 198 && second == 51 && third == 100)
        || (first == 203 && second == 0 && third == 113)
        || first >= 224
}

fn in_ipv6_subnet(address: Ipv6Addr, network: Ipv6Addr, prefix_len: u32) -> bool {
    let shift = 128 - prefix_len;
    (u128::from(address) >> shift) == (u128::from(network) >> shift)
}
