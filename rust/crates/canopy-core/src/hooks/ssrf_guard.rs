//! Address classification and resolver-independent HTTP hook SSRF checks.
//!
//! The hook runner can resolve a hostname using its preferred async runtime and
//! pass every result through [`ssrf_guarded_lookup`]. Keeping DNS injectable
//! makes the validation deterministic and ensures the entire returned set is
//! checked before a caller chooses a single address.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Error code used when an address is rejected by the hook SSRF policy.
pub const BLOCKED_ADDRESS_CODE: &str = "ERR_HTTP_HOOK_BLOCKED_ADDRESS";

/// Address and address family in the form returned by a DNS lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupAddress {
    pub address: String,
    pub family: u8,
}

/// Result shape corresponding to Node's `dns.lookup` callback with `all` on
/// or off.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GuardedLookup {
    One { address: String, family: u8 },
    All(Vec<LookupAddress>),
}

/// DNS results are supplied by the caller so async DNS implementations and
/// tests can share the same address validation and first/all selection logic.
pub trait AddressResolver {
    type Error;

    /// Resolve a hostname to all addresses, in resolver-preferred order.
    fn resolve_all(&self, hostname: &str) -> Result<Vec<IpAddr>, Self::Error>;
}

/// Errors returned by [`ssrf_guarded_lookup`]. Resolver failures are retained
/// as their original value; empty DNS answers are reported as `ENOTFOUND`.
#[derive(Debug)]
pub enum GuardedLookupError<E> {
    BlockedAddress { hostname: String, address: String },
    NotFound { hostname: String },
    Resolve { hostname: String, source: E },
}

impl<E> GuardedLookupError<E> {
    /// Error code matching the Node guard's `code` field where one is defined.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::BlockedAddress { .. } => Some(BLOCKED_ADDRESS_CODE),
            Self::NotFound { .. } => Some("ENOTFOUND"),
            Self::Resolve { .. } => None,
        }
    }

    /// Hostname that was being validated or resolved.
    pub fn hostname(&self) -> &str {
        match self {
            Self::BlockedAddress { hostname, .. }
            | Self::NotFound { hostname }
            | Self::Resolve { hostname, .. } => hostname,
        }
    }

    /// Rejected address for a blocked-address error.
    pub fn blocked_address(&self) -> Option<&str> {
        match self {
            Self::BlockedAddress { address, .. } => Some(address),
            Self::NotFound { .. } | Self::Resolve { .. } => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for GuardedLookupError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockedAddress { hostname, address } => write!(
                f,
                "HTTP hook blocked: {hostname} resolves to {address} (private/link-local address). Loopback (127.0.0.1, ::1) is allowed for local dev."
            ),
            Self::NotFound { hostname } => write!(f, "ENOTFOUND {hostname}"),
            Self::Resolve { hostname, source } => {
                write!(f, "DNS lookup for {hostname} failed: {source}")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for GuardedLookupError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resolve { source, .. } => Some(source),
            Self::BlockedAddress { .. } | Self::NotFound { .. } => None,
        }
    }
}

/// Returns whether an IP literal is blocked for HTTP hooks.
///
/// Blocked ranges are `0.0.0.0/8`, `10.0.0.0/8`, `100.64.0.0/10`,
/// `169.254.0.0/16`, `172.16.0.0/12`, `192.168.0.0/16`, `::`, `fc00::/7`,
/// and `fe80::/10`. IPv4-mapped IPv6 addresses inherit the embedded IPv4
/// policy. IPv4 and IPv6 loopback remain allowed. Non-IP input returns false,
/// matching the TypeScript guard's behavior for hostnames.
pub fn is_blocked_address(address: &str) -> bool {
    match address.parse::<IpAddr>() {
        Ok(ip) => is_blocked_ip(ip),
        Err(_) => false,
    }
}

/// Returns whether an IP literal is one of the cloud metadata endpoints that
/// stay blocked even when a caller relaxes the general private-network rule.
/// Recognized endpoints are `169.254.169.254` and `100.100.100.200`, including
/// IPv4-mapped IPv6 representations.
pub fn is_metadata_address(address: &str) -> bool {
    let Ok(ip) = address.parse::<IpAddr>() else {
        return false;
    };
    let ip = match ip {
        IpAddr::V4(v4) => v4,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4,
            None => return false,
        },
    };
    ip == Ipv4Addr::new(169, 254, 169, 254) || ip == Ipv4Addr::new(100, 100, 100, 200)
}

/// Validates an IP literal directly, or resolves a hostname and validates all
/// returned addresses before returning either the first address or the full
/// list. The injected resolver must return every address the connection may
/// use; validating only the first address would permit a mixed public/private
/// DNS answer to bypass the guard.
pub fn ssrf_guarded_lookup<R: AddressResolver>(
    hostname: &str,
    all: bool,
    resolver: &R,
) -> Result<GuardedLookup, GuardedLookupError<R::Error>> {
    if let Ok(ip) = hostname.parse::<IpAddr>() {
        if is_blocked_ip(ip) {
            return Err(GuardedLookupError::BlockedAddress {
                hostname: hostname.to_owned(),
                address: hostname.to_owned(),
            });
        }
        let family = address_family(ip);
        return if all {
            Ok(GuardedLookup::All(vec![LookupAddress {
                // Node returns the literal supplied by the caller, preserving
                // its textual IPv6 representation.
                address: hostname.to_owned(),
                family,
            }]))
        } else {
            Ok(GuardedLookup::One {
                address: hostname.to_owned(),
                family,
            })
        };
    }

    let addresses =
        resolver
            .resolve_all(hostname)
            .map_err(|source| GuardedLookupError::Resolve {
                hostname: hostname.to_owned(),
                source,
            })?;

    for address in &addresses {
        if is_blocked_ip(*address) {
            return Err(GuardedLookupError::BlockedAddress {
                hostname: hostname.to_owned(),
                address: address.to_string(),
            });
        }
    }

    let Some(first) = addresses.first().copied() else {
        return Err(GuardedLookupError::NotFound {
            hostname: hostname.to_owned(),
        });
    };

    if all {
        Ok(GuardedLookup::All(
            addresses
                .into_iter()
                .map(|address| LookupAddress {
                    address: address.to_string(),
                    family: address_family(address),
                })
                .collect(),
        ))
    } else {
        Ok(GuardedLookup::One {
            address: first.to_string(),
            family: address_family(first),
        })
    }
}

fn address_family(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(_) => 4,
        IpAddr::V6(_) => 6,
    }
}

fn is_blocked_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        IpAddr::V6(v6) => is_blocked_v6(v6),
    }
}

fn is_blocked_v4(address: Ipv4Addr) -> bool {
    let [a, b, _, _] = address.octets();

    // Loopback is intentionally allowed for local development policy hooks.
    if a == 127 {
        return false;
    }

    a == 0
        || a == 10
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 168)
}

fn is_blocked_v6(address: Ipv6Addr) -> bool {
    if address == Ipv6Addr::LOCALHOST {
        return false;
    }
    if address == Ipv6Addr::UNSPECIFIED {
        return true;
    }

    if let Some(mapped_v4) = address.to_ipv4_mapped() {
        return is_blocked_v4(mapped_v4);
    }

    let first_segment = address.segments()[0];
    // fc00::/7 (fc00:: through fdff::).
    if first_segment & 0xfe00 == 0xfc00 {
        return true;
    }
    // fe80::/10 (fe80:: through febf::).
    first_segment & 0xffc0 == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[derive(Clone, Debug)]
    struct TestResolverError(String);

    impl fmt::Display for TestResolverError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for TestResolverError {}

    struct FixedResolver<E> {
        result: Result<Vec<IpAddr>, E>,
        calls: Cell<usize>,
    }

    impl<E> FixedResolver<E> {
        fn addresses(addresses: &[&str]) -> Self {
            Self {
                result: Ok(addresses
                    .iter()
                    .map(|address| address.parse().expect("valid test IP"))
                    .collect()),
                calls: Cell::new(0),
            }
        }
    }

    impl<E: Clone> AddressResolver for FixedResolver<E> {
        type Error = E;

        fn resolve_all(&self, _hostname: &str) -> Result<Vec<IpAddr>, Self::Error> {
            self.calls.set(self.calls.get() + 1);
            self.result.clone()
        }
    }

    #[test]
    fn blocks_v4_ranges_and_preserves_boundaries() {
        let cases = [
            ("0.0.0.0", true),
            ("0.255.255.255", true),
            ("10.255.255.255", true),
            ("100.63.255.255", false),
            ("100.64.0.0", true),
            ("100.127.255.255", true),
            ("100.128.0.0", false),
            ("169.254.0.0", true),
            ("169.254.255.255", true),
            ("172.15.255.255", false),
            ("172.16.0.0", true),
            ("172.31.255.255", true),
            ("172.32.0.0", false),
            ("192.168.0.0", true),
            ("192.168.255.255", true),
            ("127.0.0.1", false),
            ("127.255.255.255", false),
            ("8.8.8.8", false),
        ];
        for (address, expected) in cases {
            assert_eq!(is_blocked_address(address), expected, "{address}");
        }
    }

    #[test]
    fn blocks_v6_ranges_and_allows_loopback_and_adjacent_ranges() {
        let cases = [
            ("::", true),
            ("::1", false),
            ("fc00::1", true),
            ("fdff::1", true),
            ("fe00::1", false),
            ("fe80::1", true),
            ("febf::1", true),
            ("fec0::1", false),
            ("8:8::8:8", false),
            ("fe80:0000:0000:0000:0000:0000:0000:0001", true),
        ];
        for (address, expected) in cases {
            assert_eq!(is_blocked_address(address), expected, "{address}");
        }
    }

    #[test]
    fn mapped_ipv4_uses_v4_policy_in_hex_and_dotted_forms() {
        assert!(is_blocked_address("::ffff:a9fe:a9fe"));
        assert!(is_blocked_address("::ffff:c0a8:101"));
        assert!(is_blocked_address("::ffff:169.254.169.254"));
        assert!(!is_blocked_address("::ffff:7f00:1"));
        assert!(!is_blocked_address("::ffff:8.8.8.8"));
    }

    #[test]
    fn non_ip_input_is_not_classified_as_an_address() {
        assert!(!is_blocked_address("api.example.com"));
        assert!(!is_blocked_address("localhost"));
        assert!(!is_metadata_address("metadata.google.internal"));
    }

    #[test]
    fn metadata_matches_only_exact_endpoints_and_mapped_forms() {
        for address in [
            "169.254.169.254",
            "100.100.100.200",
            "::ffff:a9fe:a9fe",
            "::ffff:169.254.169.254",
            "0:0:0:0:0:ffff:a9fe:a9fe",
            "::ffff:6464:64c8",
            "::ffff:100.100.100.200",
        ] {
            assert!(is_metadata_address(address), "{address}");
        }
        for address in [
            "169.254.169.253",
            "100.100.100.201",
            "10.0.0.1",
            "127.0.0.1",
            "::1",
            "8.8.8.8",
            "::ffff:c0a8:101",
        ] {
            assert!(!is_metadata_address(address), "{address}");
        }
    }

    #[test]
    fn literal_lookup_skips_resolver_and_keeps_loopback_allowed() {
        let resolver = FixedResolver::<TestResolverError>::addresses(&[]);
        assert_eq!(
            ssrf_guarded_lookup("127.0.0.1", false, &resolver).unwrap(),
            GuardedLookup::One {
                address: "127.0.0.1".to_owned(),
                family: 4,
            }
        );
        assert_eq!(
            ssrf_guarded_lookup("::1", true, &resolver).unwrap(),
            GuardedLookup::All(vec![LookupAddress {
                address: "::1".to_owned(),
                family: 6,
            }])
        );
        assert_eq!(resolver.calls.get(), 0);
    }

    #[test]
    fn literal_private_lookup_returns_blocked_address_error() {
        let resolver = FixedResolver::<TestResolverError>::addresses(&[]);
        let error = ssrf_guarded_lookup("169.254.169.254", false, &resolver).unwrap_err();
        assert_eq!(error.code(), Some(BLOCKED_ADDRESS_CODE));
        assert_eq!(error.hostname(), "169.254.169.254");
        assert_eq!(error.blocked_address(), Some("169.254.169.254"));
        assert!(
            error
                .to_string()
                .contains("Loopback (127.0.0.1, ::1) is allowed")
        );
        assert_eq!(resolver.calls.get(), 0);
    }

    #[test]
    fn hostname_lookup_validates_every_answer_before_selecting_first() {
        let resolver = FixedResolver::<TestResolverError>::addresses(&["8.8.8.8", "10.0.0.1"]);
        let error = ssrf_guarded_lookup("mixed.example", false, &resolver).unwrap_err();
        assert_eq!(error.blocked_address(), Some("10.0.0.1"));
        assert_eq!(resolver.calls.get(), 1);
    }

    #[test]
    fn hostname_lookup_preserves_first_and_all_address_semantics() {
        let resolver =
            FixedResolver::<TestResolverError>::addresses(&["8.8.8.8", "2001:4860:4860::8888"]);
        assert_eq!(
            ssrf_guarded_lookup("public.example", false, &resolver).unwrap(),
            GuardedLookup::One {
                address: "8.8.8.8".to_owned(),
                family: 4,
            }
        );
        assert_eq!(
            ssrf_guarded_lookup("public.example", true, &resolver).unwrap(),
            GuardedLookup::All(vec![
                LookupAddress {
                    address: "8.8.8.8".to_owned(),
                    family: 4,
                },
                LookupAddress {
                    address: "2001:4860:4860::8888".to_owned(),
                    family: 6,
                },
            ])
        );
    }

    #[test]
    fn empty_answer_and_resolver_error_are_distinct() {
        let empty = FixedResolver::<TestResolverError>::addresses(&[]);
        let not_found = ssrf_guarded_lookup("missing.example", false, &empty).unwrap_err();
        assert_eq!(not_found.code(), Some("ENOTFOUND"));
        assert_eq!(not_found.to_string(), "ENOTFOUND missing.example");

        let failed = FixedResolver {
            result: Err(TestResolverError("resolver failed".to_owned())),
            calls: Cell::new(0),
        };
        let error = ssrf_guarded_lookup("broken.example", false, &failed).unwrap_err();
        assert_eq!(error.code(), None);
        assert!(error.to_string().contains("resolver failed"));
        assert!(std::error::Error::source(&error).is_some());
    }
}
