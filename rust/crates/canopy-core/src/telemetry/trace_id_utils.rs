use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Derive the 32-character lowercase hexadecimal trace ID from a session ID.
///
/// The SHA-256 digest is truncated to its first 128 bits, matching the
/// OpenTelemetry trace ID width and the TypeScript implementation.
pub fn derive_trace_id(session_id: &str) -> String {
    let digest = Sha256::digest(session_id.as_bytes());
    let mut trace_id = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write as _;
        write!(&mut trace_id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    trace_id
}

/// Generate a 16-character lowercase hexadecimal span ID using OS-backed
/// cryptographic randomness supplied by UUID v4.
pub fn random_span_id() -> String {
    random_hex_string(16)
}

/// Generate a lowercase hexadecimal string of the requested length.
///
/// UUID v4 supplies cryptographically secure random bytes. Its canonical
/// version and variant bits make a few nibbles non-random, but the resulting
/// identifiers retain ample random entropy and match the source's hex format.
/// Multiple UUIDs are concatenated for lengths greater than 32 characters.
pub fn random_hex_string(length: usize) -> String {
    let mut output = String::with_capacity(length);
    while output.len() < length {
        let random_hex = Uuid::new_v4().simple().to_string();
        let remaining = length - output.len();
        output.push_str(&random_hex[..remaining.min(random_hex.len())]);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{derive_trace_id, random_hex_string, random_span_id};

    fn is_lowercase_hex(text: &str) -> bool {
        text.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    #[test]
    fn derives_32_character_lowercase_hex_trace_id() {
        let trace_id = derive_trace_id("test-session-id");
        assert_eq!(trace_id.len(), 32);
        assert!(is_lowercase_hex(&trace_id));
    }

    #[test]
    fn trace_id_is_deterministic_for_the_same_session_id() {
        let session_id = "stable-session-id";
        assert_eq!(derive_trace_id(session_id), derive_trace_id(session_id));
    }

    #[test]
    fn different_session_ids_produce_different_trace_ids() {
        assert_ne!(derive_trace_id("session-a"), derive_trace_id("session-b"));
    }

    #[test]
    fn span_id_is_16_character_lowercase_hex_and_changes_between_calls() {
        let span_id = random_span_id();
        assert_eq!(span_id.len(), 16);
        assert!(is_lowercase_hex(&span_id));
        assert_ne!(random_span_id(), random_span_id());
    }

    #[test]
    fn random_hex_string_has_requested_length() {
        assert_eq!(random_hex_string(32).len(), 32);
        assert_eq!(random_hex_string(16).len(), 16);
        assert_eq!(random_hex_string(0), "");
    }

    #[test]
    fn random_hex_string_handles_odd_lengths() {
        assert_eq!(random_hex_string(15).len(), 15);
        assert_eq!(random_hex_string(7).len(), 7);
    }

    #[test]
    fn random_hex_string_contains_only_lowercase_hex_characters() {
        let value = random_hex_string(32);
        assert!(is_lowercase_hex(&value));
    }
}
