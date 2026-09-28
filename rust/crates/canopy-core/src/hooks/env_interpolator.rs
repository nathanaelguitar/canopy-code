//! Whitelist based environment interpolation for HTTP hook templates.
//!
//! Port of `packages/core/src/hooks/envInterpolator.ts`.

use std::collections::HashMap;

const DANGEROUS_VAR_NAMES: &[&str] = &[
    "__proto__",
    "constructor",
    "prototype",
    "__defineGetter__",
    "__defineSetter__",
    "__lookupGetter__",
    "__lookupSetter__",
];

/// Strip CR, LF, and NUL bytes from a header value.
pub fn sanitize_header_value(value: &str) -> String {
    value
        .chars()
        .filter(|character| !matches!(character, '\r' | '\n' | '\0'))
        .collect()
}

/// Interpolate `$NAME` and `${NAME}` references using only whitelisted values.
///
/// Missing, empty, disallowed, and prototype-sensitive variables become empty
/// strings, matching the hook template behavior in the TypeScript runtime.
/// The final string is sanitized for use as an HTTP header value.
pub fn interpolate_env_vars(
    value: &str,
    allowed_vars: &[&str],
    environment: &HashMap<String, String>,
) -> String {
    let bytes = value.as_bytes();
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0;

    while let Some(relative_dollar) = value[cursor..].find('$') {
        let dollar = cursor + relative_dollar;
        output.push_str(&value[cursor..dollar]);

        let mut name_start = dollar + 1;
        if bytes.get(name_start) == Some(&b'{') {
            name_start += 1;
        }
        let Some(first) = bytes.get(name_start).copied() else {
            output.push('$');
            cursor = dollar + 1;
            continue;
        };
        if !is_name_start(first) {
            output.push('$');
            cursor = dollar + 1;
            continue;
        }

        let mut name_end = name_start + 1;
        while bytes
            .get(name_end)
            .is_some_and(|byte| is_name_continue(*byte))
        {
            name_end += 1;
        }
        // The source regex makes the opening and closing braces independently
        // optional, so `$NAME}` consumes the trailing brace as well.
        let placeholder_end = if bytes.get(name_end) == Some(&b'}') {
            name_end + 1
        } else {
            name_end
        };
        let name = &value[name_start..name_end];

        if !DANGEROUS_VAR_NAMES.contains(&name) && allowed_vars.contains(&name) {
            if let Some(replacement) = environment.get(name).filter(|value| !value.is_empty()) {
                output.push_str(replacement);
            }
        }
        cursor = placeholder_end;
    }

    output.push_str(&value[cursor..]);
    sanitize_header_value(&output)
}

/// Interpolate using the current process environment, reading only names that
/// appear in `allowed_vars`.
pub fn interpolate_env_vars_from_process(value: &str, allowed_vars: &[&str]) -> String {
    let environment = allowed_vars
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| ((*name).to_owned(), value))
        })
        .collect();
    interpolate_env_vars(value, allowed_vars, &environment)
}

/// Interpolate each header value while preserving header insertion order.
pub fn interpolate_headers(
    headers: &[(String, String)],
    allowed_vars: &[&str],
    environment: &HashMap<String, String>,
) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                interpolate_env_vars(value, allowed_vars, environment),
            )
        })
        .collect()
}

/// Interpolate a hook URL using the same whitelist and sanitization rules.
pub fn interpolate_url(
    url: &str,
    allowed_vars: &[&str],
    environment: &HashMap<String, String>,
) -> String {
    interpolate_env_vars(url, allowed_vars, environment)
}

/// Whether a string contains a variable reference recognized by the source
/// template regex.
pub fn has_env_var_references(value: &str) -> bool {
    variable_ranges(value).next().is_some()
}

/// Return referenced names once each, in first-seen order.
pub fn extract_env_var_names(value: &str) -> Vec<String> {
    let mut names = Vec::new();
    for (start, end) in variable_ranges(value) {
        let name = &value[start..end];
        if !names.iter().any(|existing| existing == name) {
            names.push(name.to_owned());
        }
    }
    names
}

fn variable_ranges(value: &str) -> impl Iterator<Item = (usize, usize)> + '_ {
    let bytes = value.as_bytes();
    let mut cursor = 0;
    std::iter::from_fn(move || {
        while let Some(relative_dollar) = value[cursor..].find('$') {
            let dollar = cursor + relative_dollar;
            let mut start = dollar + 1;
            if bytes.get(start) == Some(&b'{') {
                start += 1;
            }
            let Some(first) = bytes.get(start).copied() else {
                cursor = dollar + 1;
                continue;
            };
            if !is_name_start(first) {
                cursor = dollar + 1;
                continue;
            }
            let mut end = start + 1;
            while bytes.get(end).is_some_and(|byte| is_name_continue(*byte)) {
                end += 1;
            }
            cursor = if bytes.get(end) == Some(&b'}') {
                end + 1
            } else {
                end
            };
            return Some((start, end));
        }
        cursor = value.len();
        None
    })
}

fn is_name_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_name_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        extract_env_var_names, has_env_var_references, interpolate_env_vars, interpolate_headers,
        interpolate_url, sanitize_header_value,
    };

    fn env(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn interpolates_whitelisted_names_in_both_forms_and_drops_others() {
        let environment = env(&[("TOKEN", "secret"), ("KEY", "key")]);
        assert_eq!(
            interpolate_env_vars(
                "Bearer $TOKEN:${KEY}:$OTHER",
                &["TOKEN", "KEY"],
                &environment
            ),
            "Bearer secret:key:"
        );
        assert_eq!(interpolate_env_vars("$TOKEN", &[], &environment), "");
        assert_eq!(
            interpolate_env_vars("TOKEN", &["TOKEN"], &environment),
            "TOKEN"
        );
    }

    #[test]
    fn consumes_optional_closing_brace_and_rejects_dangerous_names() {
        let environment = env(&[
            ("TOKEN", "value"),
            ("constructor", "unsafe"),
            ("__proto__", "unsafe"),
        ]);
        assert_eq!(
            interpolate_env_vars(
                "$TOKEN}:$constructor:$__proto__",
                &["TOKEN", "constructor", "__proto__"],
                &environment
            ),
            "value::"
        );
    }

    #[test]
    fn missing_or_empty_values_are_empty_and_header_controls_are_removed() {
        let environment = env(&[("EMPTY", ""), ("EVIL", "token\r\nX-Injected: 1\0more")]);
        assert_eq!(
            interpolate_env_vars(
                "$MISSING:$EMPTY:$EVIL",
                &["MISSING", "EMPTY", "EVIL"],
                &environment
            ),
            "::tokenX-Injected: 1more"
        );
    }

    #[test]
    fn header_and_url_helpers_preserve_order_and_interpolate_values() {
        let environment = env(&[("TOKEN", "secret"), ("HOST", "api.example.test")]);
        let headers = vec![
            ("Authorization".to_owned(), "Bearer $TOKEN".to_owned()),
            ("Content-Type".to_owned(), "application/json".to_owned()),
        ];
        assert_eq!(
            interpolate_headers(&headers, &["TOKEN"], &environment),
            vec![
                ("Authorization".to_owned(), "Bearer secret".to_owned()),
                ("Content-Type".to_owned(), "application/json".to_owned()),
            ]
        );
        assert_eq!(
            interpolate_url("https://$HOST/hook", &["HOST"], &environment),
            "https://api.example.test/hook"
        );
    }

    #[test]
    fn reference_detection_and_extraction_match_template_syntax() {
        assert!(has_env_var_references("${TOKEN}:$KEY"));
        assert!(!has_env_var_references("plain ${} $9 bad"));
        assert_eq!(
            extract_env_var_names("${TOKEN}:$KEY:$TOKEN:$9"),
            vec!["TOKEN", "KEY"]
        );
    }

    #[test]
    fn sanitizes_only_cr_lf_and_nul() {
        assert_eq!(sanitize_header_value("a\r\nb\0c\tdé"), "abc\tdé");
        assert_eq!(sanitize_header_value(""), "");
    }
}
