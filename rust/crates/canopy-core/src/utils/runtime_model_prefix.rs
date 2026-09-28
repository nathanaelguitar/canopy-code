//! Runtime snapshot model ID prefix helpers.
//!
//! This module stays dependency-free so model configuration and ID parsing can
//! share the same prefix without introducing an import cycle.

/// Runtime model snapshot ID prefix, formatted as
/// `$runtime|${auth_type}|${model_id}`.
pub const RUNTIME_SNAPSHOT_PREFIX: &str = "$runtime|";

/// Return the bare model ID from a possibly runtime-prefixed model string.
///
/// Every valid prefix layer is removed so nested prefixes self-heal. If a
/// prefix is malformed and has no non-empty model portion, the current value
/// is preserved, matching the TypeScript implementation.
pub fn strip_runtime_snapshot_prefix(model_id: &str) -> &str {
    let mut id = model_id;

    while let Some(after_prefix) = id.strip_prefix(RUNTIME_SNAPSHOT_PREFIX) {
        let Some(separator) = after_prefix.find('|') else {
            break;
        };
        let stripped = &after_prefix[separator + 1..];
        if stripped.is_empty() {
            break;
        }
        id = stripped;
    }

    id
}

#[cfg(test)]
mod tests {
    use super::{RUNTIME_SNAPSHOT_PREFIX, strip_runtime_snapshot_prefix};

    #[test]
    fn keeps_bare_model_ids_unchanged() {
        assert_eq!(
            strip_runtime_snapshot_prefix("canopy3.6-27b-autoround"),
            "canopy3.6-27b-autoround"
        );
    }

    #[test]
    fn strips_a_single_runtime_snapshot_prefix() {
        assert_eq!(
            strip_runtime_snapshot_prefix("$runtime|openai|canopy3.6-27b-autoround"),
            "canopy3.6-27b-autoround"
        );
    }

    #[test]
    fn strips_nested_runtime_snapshot_prefixes() {
        assert_eq!(
            strip_runtime_snapshot_prefix(
                "$runtime|openai|$runtime|openai|canopy3.6-27b-autoround"
            ),
            "canopy3.6-27b-autoround"
        );
    }

    #[test]
    fn preserves_malformed_prefixes_without_a_model_id() {
        assert_eq!(
            strip_runtime_snapshot_prefix("$runtime|openai|"),
            "$runtime|openai|"
        );
        assert_eq!(strip_runtime_snapshot_prefix("$runtime|"), "$runtime|");
        assert_eq!(
            strip_runtime_snapshot_prefix("$runtime|openai"),
            "$runtime|openai"
        );
    }

    #[test]
    fn preserves_model_id_pipes_and_accepts_an_empty_auth_segment() {
        assert_eq!(
            strip_runtime_snapshot_prefix("$runtime|openai|model|variant"),
            "model|variant"
        );
        assert_eq!(strip_runtime_snapshot_prefix("$runtime||model"), "model");
    }

    #[test]
    fn keeps_a_malformed_nested_prefix_after_stripping_the_outer_layer() {
        assert_eq!(
            strip_runtime_snapshot_prefix("$runtime|openai|$runtime|malformed"),
            "$runtime|malformed"
        );
    }

    #[test]
    fn exports_the_runtime_snapshot_prefix_constant() {
        assert_eq!(RUNTIME_SNAPSHOT_PREFIX, "$runtime|");
    }
}
