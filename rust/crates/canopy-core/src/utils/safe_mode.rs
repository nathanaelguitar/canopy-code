//! Environment-controlled safe-mode selection.
//!
//! Truthy-value parsing is shared with [`crate::utils::bare_mode`]; only the
//! environment variable differs between safe mode and bare mode.

use crate::utils::bare_mode::is_truthy;

pub const CANOPY_CODE_SAFE_MODE_ENV_VAR: &str = "CANOPY_CODE_SAFE_MODE";

/// Evaluate safe mode using an injected environment lookup.
pub fn is_safe_mode_env_with<F>(mut read_environment: F) -> bool
where
    F: FnMut(&str) -> Option<String>,
{
    is_truthy(read_environment(CANOPY_CODE_SAFE_MODE_ENV_VAR).as_deref())
}

/// Evaluate safe mode from the current process environment.
pub fn is_safe_mode_env() -> bool {
    is_safe_mode_env_with(|key| std::env::var(key).ok())
}

#[cfg(test)]
mod tests {
    use super::{CANOPY_CODE_SAFE_MODE_ENV_VAR, is_safe_mode_env_with};

    #[test]
    fn reads_only_the_safe_mode_environment_key_and_reuses_truthy_tokens() {
        for value in ["1", "true", "YES", " On "] {
            let mut looked_up = Vec::new();
            assert!(is_safe_mode_env_with(|key| {
                looked_up.push(key.to_owned());
                Some(value.to_owned())
            }));
            assert_eq!(looked_up, [CANOPY_CODE_SAFE_MODE_ENV_VAR]);
        }
    }

    #[test]
    fn missing_and_non_truthy_environment_values_disable_safe_mode() {
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("no"),
            Some("off"),
        ] {
            assert!(!is_safe_mode_env_with(|_| value.map(str::to_owned)));
        }
    }

    #[test]
    fn inherits_bare_modes_case_folding_and_ecmascript_whitespace_behavior() {
        assert!(is_safe_mode_env_with(|_| Some(
            "\u{feff}TrUe\u{feff}".to_owned()
        )));
        assert!(!is_safe_mode_env_with(|_| Some("\u{0085}true".to_owned())));
    }
}
