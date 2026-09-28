//! Shell pager environment values ported from
//! `packages/core/src/utils/shell-pager-env.ts`.

use std::collections::BTreeMap;

/// Platform classes relevant to the source's pager choice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellPagerPlatform {
    Windows,
    Unix,
}

impl ShellPagerPlatform {
    /// Return the current build target's pager platform.
    pub const fn current() -> Self {
        if cfg!(windows) {
            Self::Windows
        } else {
            Self::Unix
        }
    }
}

/// Variables to apply to a child process when pager output must be disabled
/// or configured. Empty strings intentionally clear inherited values.
pub type ShellPagerEnv = BTreeMap<String, String>;

/// The source defaults to `cat` on every platform except Windows.
pub const fn get_default_shell_pager(platform: ShellPagerPlatform) -> Option<&'static str> {
    match platform {
        ShellPagerPlatform::Windows => None,
        ShellPagerPlatform::Unix => Some("cat"),
    }
}

/// Build pager variables for a child command.
///
/// `None` selects the platform default. An explicit empty string remains an
/// explicit request to disable pager programs, matching the TypeScript
/// nullish default followed by its falsy-value check.
pub fn get_shell_pager_env(
    pager: Option<&str>,
    include_git_pager: bool,
    platform: ShellPagerPlatform,
) -> ShellPagerEnv {
    let effective_pager = pager.or_else(|| get_default_shell_pager(platform));
    let value = effective_pager
        .filter(|value| !value.is_empty())
        .unwrap_or("");

    let mut environment = BTreeMap::from([("PAGER".to_owned(), value.to_owned())]);
    if include_git_pager {
        environment.insert("GIT_PAGER".to_owned(), value.to_owned());
    }
    environment
}

#[cfg(test)]
mod tests {
    use super::{ShellPagerPlatform, get_default_shell_pager, get_shell_pager_env};

    #[test]
    fn defaults_to_cat_only_on_non_windows_platforms() {
        assert_eq!(
            get_default_shell_pager(ShellPagerPlatform::Unix),
            Some("cat")
        );
        assert_eq!(get_default_shell_pager(ShellPagerPlatform::Windows), None);
    }

    #[test]
    fn applies_unix_default_and_optional_git_pager() {
        assert_eq!(
            get_shell_pager_env(None, true, ShellPagerPlatform::Unix),
            [
                ("GIT_PAGER".to_owned(), "cat".to_owned()),
                ("PAGER".to_owned(), "cat".to_owned()),
            ]
            .into()
        );
        assert_eq!(
            get_shell_pager_env(None, false, ShellPagerPlatform::Unix),
            [("PAGER".to_owned(), "cat".to_owned())].into()
        );
    }

    #[test]
    fn clears_unset_windows_pagers_when_git_output_is_requested() {
        assert_eq!(
            get_shell_pager_env(None, true, ShellPagerPlatform::Windows),
            [
                ("GIT_PAGER".to_owned(), String::new()),
                ("PAGER".to_owned(), String::new()),
            ]
            .into()
        );
    }

    #[test]
    fn preserves_explicit_values_and_empty_disable_requests() {
        assert_eq!(
            get_shell_pager_env(Some("more"), true, ShellPagerPlatform::Windows),
            [
                ("GIT_PAGER".to_owned(), "more".to_owned()),
                ("PAGER".to_owned(), "more".to_owned()),
            ]
            .into()
        );
        assert_eq!(
            get_shell_pager_env(Some(""), true, ShellPagerPlatform::Unix),
            [
                ("GIT_PAGER".to_owned(), String::new()),
                ("PAGER".to_owned(), String::new()),
            ]
            .into()
        );
    }
}
