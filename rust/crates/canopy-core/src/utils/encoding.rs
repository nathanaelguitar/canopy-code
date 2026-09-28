//! Encoding-name predicates shared with the TypeScript utility layer.

/// Whether a label names UTF-8 or one of the ASCII-compatible aliases accepted
/// by Canopy. Punctuation, spacing, and underscores are ignored, and the
/// comparison is case-insensitive.
pub fn is_utf8_compatible_encoding(encoding: &str) -> bool {
    let normalized: String = encoding
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_ascii_alphanumeric())
        .collect();
    matches!(normalized.as_str(), "utf8" | "ascii" | "usascii")
}

#[cfg(test)]
mod tests {
    use super::is_utf8_compatible_encoding;

    #[test]
    fn accepts_utf8_and_ascii_aliases_after_normalization() {
        for encoding in ["utf-8", "UTF_8", "Utf 8", "ascii", "US-ASCII", "us_ascii"] {
            assert!(is_utf8_compatible_encoding(encoding), "{encoding}");
        }
    }

    #[test]
    fn rejects_other_or_empty_encoding_labels() {
        for encoding in ["", "utf16", "latin1", "binary", "not-ascii"] {
            assert!(!is_utf8_compatible_encoding(encoding), "{encoding}");
        }
    }
}
